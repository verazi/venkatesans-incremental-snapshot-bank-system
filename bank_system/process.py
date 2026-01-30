import json
from socket import socket, AF_INET, SOCK_STREAM
from threading import Thread, Lock
from time import sleep
from collections import defaultdict
from dataclasses import dataclass
from select import select
from random import random, choice

from .snap_completed_message import ProcessSnapshot, SnapCompletedMessage, State
from .action_message import ActionMessage
from .message_factory import MessageFactory
from .initial_connection_message import InitialConnectionMessage
from .message import Message
from .config import Config
from .control_message import ControlMessage, ControlMessageType
from .process_address import ProcessAddress

BUFFER_SIZE = 2048
INTRODUCED_DELAY = 1
ENCODING = 'utf-8'

@dataclass(eq=True, frozen=True)
class GlobalSnapshot:
    snapshots: dict[ProcessAddress, ProcessSnapshot]

    def serialise(self) -> str:
        return json.dumps({
            f"{address.address}:{address.port}": json.loads(self.snapshots[address].serialise()) for address in self.snapshots
        })

    @classmethod
    def deserialise(cls, string: str):
        raw = json.loads(string)

        snapshots = {}

        for process in raw:
            key_parts = process.split(":")
            key = ProcessAddress(key_parts[0], int(key_parts[1]))

            snapshots[key] = ProcessSnapshot.deserialise(json.dumps(raw[process]))

        return cls(snapshots=snapshots)

class Process:
    """A process that can connect to and send messages to other processes.

    Attributes
    ----------
    is_primary : bool
        Is this process the primary process.
    identifier: ProcessAddress
        A unique identifier for this process. Also the address the process will listen on.

    incoming_sockets: dict[ProcessAddress, socket]
        A list of incoming sockets to read messages from.
    outgoing_sockets: dict[ProcessAddress, socket]
        A list of outgoing sockets to send messages to.

    connections: list[ProcessAddress]
        A list of processes this process will connect to. Includes itself.
    spanning_connections: list[ProcessAddress]
        A list of child processes this process is connected to in the spanning tree.

    process_state: State
        The current state of the proces. The amount of money this process has.

    mutex: Lock
        A lock on the state of the process.
    parent: ProcessAddress | None
        The parent of this process in the spanning tree.
    is_active_snapshot: bool
        If this is the primary process, records if a snapshot is currently happening.

    # From Venkatesan algorithm

    version: int
        The version of the snapshot that is currently being taken.
    Uq: set[ProcessAddress]
        List of neighbours messages have been sent to since the last snapshot completed.
    p_state: State
        Saved (cached) states of the local process.
    state: defaultdict[ProcessAddress, list[ActionMessage]]
        The state of the other processes.
    link_states: dict[ProcessAddress, list[ActionMessage]]
        Completed channel states for the current snapshot.

    # Our additions

    record: defaultdict[ProcessAddress, bool]
        If the process is recording messages received on each channel for the snapshot algorithm.
    completed_snaps: dict[ProcessAddress, ProcessSnapshot|None]
        Completed snapshots from children to send to the parent process when this process has
        completed its snapshot.
    waiting_on_completed: dict[ProcessAddress, bool]
        The children this process is waiting to finish their snapshots before this process is
        finished with its own snapshot.
    waiting_on_ack: dict[ProcessAddress, bool]
        The channels this process is waiting to send an ack to complete its snapshot.
    """

    is_primary: bool
    identifier: ProcessAddress

    incoming_sockets: dict[ProcessAddress, socket]
    outgoing_sockets: dict[ProcessAddress, socket]

    connections: list[ProcessAddress]
    spanning_connections: list[ProcessAddress]

    process_state: State

    mutex: Lock
    parent: ProcessAddress | None
    is_active_snapshot: bool

    # From Venkatesan algorithm
    version: int
    Uq: set[ProcessAddress]
    p_state: State
    state: defaultdict[ProcessAddress, list[ActionMessage]]
    link_states: dict[ProcessAddress, list[ActionMessage]]
    # loc_snap: dict[int, ProcessSnapshot] # COMMENTED INTENTIONALLY

    # Our additions
    record: defaultdict[ProcessAddress, bool]
    completed_snaps: dict[ProcessAddress, ProcessSnapshot|None]
    waiting_on_completed: dict[ProcessAddress, bool]
    waiting_on_ack: dict[ProcessAddress, bool]

    def __init__(self, config: Config, identifier: ProcessAddress, global_snapshot: GlobalSnapshot | None = None):
        self.is_primary = config.processes[identifier].primary
        self.identifier = identifier

        self.incoming_sockets = {}
        self.outgoing_sockets = {}

        # Include self connection
        self.connections = config.processes[identifier].connections + [identifier]
        self.spanning_connections = config.processes[identifier].spanning_connections

        if global_snapshot is None:
            self.process_state = State(config.processes[identifier].initial_money)
        else:
            self.process_state = global_snapshot.snapshots[self.identifier].state

        self.mutex = Lock()
        self.parent = config.processes[identifier].parent
        self.is_active_snapshot = False

        self.version = 0
        self.Uq = set(config.processes[identifier].connections)
        self.p_state = State(config.processes[identifier].initial_money)
        self.state = defaultdict(list)
        self.link_states = dict()
        # self.loc_snap = dict() # COMMENTED INTENTIONALLY

        self.record = defaultdict(lambda: False)
        self.completed_snaps = dict()
        self.waiting_on_completed = dict()
        self.waiting_on_ack = dict()

        if global_snapshot is not None:
            messages: list[ActionMessage] = []

            for connection_state in global_snapshot.snapshots[self.identifier].connection_states:
                pending_messages = global_snapshot.snapshots[self.identifier].connection_states[connection_state]
                for message in pending_messages:
                    messages.append(message)

            for message in messages:
                self._receive_message(message, message.message_from)

    def start(self):
        """Start the process."""

        # Listen for peers
        incoming = socket(AF_INET, SOCK_STREAM)

        incoming.bind((self.identifier.address, self.identifier.port))
        incoming.listen()

        accept_loop = Thread(target=self._accept_loop, args=[incoming])
        accept_loop.start()

        # Wait a moment to make connections consistant
        sleep(0.5)

        # Connect to existing peers
        for peer in self.connections:
            if peer not in self.incoming_sockets:
                try:
                    s = socket(AF_INET, SOCK_STREAM)
                    s.connect((peer.address, peer.port))

                    self.outgoing_sockets[peer] = s

                    s.send(InitialConnectionMessage(self.identifier).serialise().encode(ENCODING))
                    self._print(f"Outgoing connected to {peer.address}:{peer.port}")
                except ConnectionRefusedError:
                    pass

        # Wait to accept all incoming connections
        accept_loop.join()

        # Split sockets into incoming and outgoing. We do this because we need both sides of
        # the self connection.
        for conn in self.incoming_sockets:
            if conn not in self.outgoing_sockets:
                self.outgoing_sockets[conn] = self.incoming_sockets[conn]

        for conn in self.outgoing_sockets:
            if conn not in self.incoming_sockets:
                self.incoming_sockets[conn] = self.outgoing_sockets[conn]

        # Send actions in another thread
        action_thread = Thread(target=self._action_loop)
        action_thread.start()

        # Send initial snapshot message to self in another thread if we are the primary process
        if self.is_primary:
            snapshot_thread = Thread(target=self._snapshot_loop)
            snapshot_thread.start()

        # Listen for incoming messages
        while True:
            # Get messages on all threads synchronously
            # We don't need to do this, but it makes demonstrating dropped messages in flight with
            # snapshots easier.
            ready_sockets, _, _ = select([ self.incoming_sockets[k] for k in self.incoming_sockets ], [], [])

            for sock in ready_sockets:
                sock: socket
                data = sock.recv(BUFFER_SIZE)

                for raw_message in data.split(b"\n"):
                    if len(raw_message) == 0:
                        continue

                    message = MessageFactory.deserialise(raw_message)

                    with self.mutex:
                        message_from = message.message_from
                        self._receive_message(message, message_from)

    def _waiting_for_connections(self) -> set[ProcessAddress]:
        """The set of processes that haven't conencted to this process yet."""

        with self.mutex:
            connected = set(self.incoming_sockets.keys()) | set(self.outgoing_sockets.keys())

            return set(self.connections) - connected


    def _accept_loop(self, incoming: socket):
        """Listen for connections until all connected processors have connected."""

        while len(self._waiting_for_connections()) > 0:
            conn, _ = incoming.accept()
            message = conn.recv(BUFFER_SIZE)

            # with self.mutex:
            initial_connection_message = InitialConnectionMessage.deserialise(message)
            peer = initial_connection_message.message_from

            self._print(f"Incoming connected to {peer.address}:{peer.port}")

            self.incoming_sockets[peer] = conn


    def _action_loop(self):
        """Sends a randomised action message approximately every second."""

        while True:
            delay = random() * 1.5

            sleep(delay)

            if self.process_state.money <= 0:
                sleep(2)
                continue

            with self.mutex:
                amount = min(int(random() * 5), self.process_state.money)

                self.process_state = State(self.process_state.money - amount)

                to = choice([ conn for conn in self.connections if conn != self.identifier ])

                self._send_und(
                    self.identifier,
                    to,
                    to,
                    ActionMessage(self.identifier, amount)
                )

    def _snapshot_loop(self):
        """Runs a snapshot every few seconds if this is the primary process."""

        while self.is_primary:
            # Between 4 and 5 seconds
            sleep(5 + random())

            if not self.is_active_snapshot:
                with self.mutex:

                    self.is_active_snapshot = True
                    self._print("Start snapshot")

                    self._send_message(
                        ControlMessage(
                            self.identifier,
                            ControlMessageType.INIT_SNAP,
                            self.version+1
                        ),
                        self.identifier
                    )

    def _send_message(self, message: Message, message_to: ProcessAddress):
        """Send a message to anther process.

        Attributes
        ----------
        message : Message
            The message to send.
        message_to : ProcessAddress
            The process to send the message to.
        """
        self._print(f"Sending message type:{message.MESSAGE_TYPE} to:{message_to}")

        sock: socket
        sock = self.outgoing_sockets[message_to]

        data = message.serialise() + "\n"  # newline framing
        sock.sendall(data.encode(ENCODING))

        self._print("\tSent!")

    def _receive_message(self, message: Message, message_from: ProcessAddress) -> bool:
        """Handles receiving a message from another process.

        Attributes
        ----------
        message : ActionMessage
            The message being received.
        message_from : ProcessAddress
            The process that sent the message.
        """
        self._print(f"Received message type:{message.MESSAGE_TYPE} from:{message.message_from}")

        if isinstance(message, ControlMessage):
            self._print(f"\t{message.control_message_type}")

            # NOTE: Delay a random amount to allow time for messages to be sent for demonstration purposes
            sleep(random() * 2)

            if message.control_message_type == ControlMessageType.INIT_SNAP:
                self._receive_initiate(self.identifier, message_from, message_from, message)
            elif message.control_message_type == ControlMessageType.MARKER:
                self._receive_marker(message_from, self.identifier, message_from, message)
            elif message.control_message_type == ControlMessageType.ACK:
                self._receive_ack(message_from, self.identifier, message_from, message)
        elif isinstance(message, SnapCompletedMessage):
            # NOTE: Delay a random amount to allow time for messages to be sent for demonstration purposes
            sleep(random() * 2)

            self._receive_snap_completed(message_from, self.identifier, message_from, message)
        elif isinstance(message, ActionMessage):
            self._receive_und(message_from, self.identifier, message_from, message)
        else:
            raise Exception("Message type not recognised.")

    def _handle_message(self, message: ActionMessage, message_from: ProcessAddress):
        """Handle the logic for receiving an underlying message.

        Attributes
        ----------
        message : ActionMessage
            The action message being received.
        message_from : ProcessAddress
            The process that sent the message.
        """
        self._print("Handling underlying message")

        self.process_state = State(self.process_state.money + message.amount)

    # From Venkatesan algorithm

    def _send_und(self, q: ProcessAddress, r: ProcessAddress, c: ProcessAddress, m: Message):
        """Executed when q sends a is_primary message to r.

        Attributes
        ----------
        q : ProcessAddress
            origin.
        r : ProcessAddress
            destination
        c : ProcessAddress
            channel
        m : Message
            message
        """

        self.Uq.add(c)
        self._send_message(m, r)

    def _receive_und(self, q: ProcessAddress, r: ProcessAddress, c: ProcessAddress, m: Message):
        """Executed when a is_primary message is received.

        Attributes
        ----------
        q : ProcessAddress
            origin.
        r : ProcessAddress
            destination
        c : ProcessAddress
            channel
        m : Message
            message
        """
        if self.record[c]:
            self.state[c].append(m)

        self._handle_message(m, q)

    def _receive_marker(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        """Executed when q receives a marker from a neighbour.

        Attributes
        ----------
        r : ProcessAddress
            origin.
        q : ProcessAddress
            destination
        c : ProcessAddress
            channel
        m : ControlMessage
            message
        """

        if self.version < m.version:
            # Send init_snap(self.version+1) to self (q)
            # NOTE: Doing this as a function call instead of sending the message to self so it
            # doesn't have to wait for other message to be received.
            self._receive_initiate(q, r, c, ControlMessage(self.identifier, ControlMessageType.INIT_SNAP, m.version))
            self.state[c] = []

        self.link_states[c] = self.state[c]
        self.record[c] = False

        # Send an ack on c
        self._send_message(ControlMessage(self.identifier, ControlMessageType.ACK, m.version), c)

    def _receive_initiate(self, q: ProcessAddress, r: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        """Executed when q receives an init_snap message from it's parent.

        Attributes
        ----------
        q : ProcessAddress
            destination
        r : ProcessAddress
            parent
        c : ProcessAddress
            channel
        m : ControlMessage
            message
        """

        if self.version < m.version:

            # NOTE: I think this doesn't work. P won't start a new snapshot until the previous one
            #       is finished, so we can't send this as part of finishing.
            # COMMENTED INTENTIONALLY
            # self.loc_snap[self.version] = ProcessSnapshot(self.p_state, self.link_states)

            self.link_states = dict()
            self.p_state = self.process_state

            # NOTE: paper uses version + 1 but I think it's safer to copy the message version
            self.version = m.version

            self._save_local_snapshot()

            for connection in [ c for c in self.connections if c != self.identifier]:
                self.state[connection] = []
                self.record[connection] = True

            self.waiting_on_completed = {k: False for k in self.spanning_connections}
            self.completed_snaps = { k: None for k in self.spanning_connections if k not in self.Uq }
            self.waiting_on_ack = { k: False for k in self.Uq }

            # Send init snap on all spanning connections, unless we are going to send a marker
            for connection in [ c for c in self.spanning_connections if c not in self.Uq]:
                self._send_message(ControlMessage(self.identifier, ControlMessageType.INIT_SNAP, m.version), connection)

            # Send a marker to all channels we have sent a message on
            for connection in self.Uq:
                # Send marker on connection
                self._send_message(ControlMessage(self.identifier, ControlMessageType.MARKER, m.version), connection)

            self._print("init", self.waiting_on_ack.values(), [ a is not None for a in self.completed_snaps.values() ])

            # If there are no messages to receive instantly finish the snapshot
            if all(self.waiting_on_completed.values()) and all(self.waiting_on_ack.values()):
                self._finish_snapshot(r, q, c, m)


            # Wait for an ack on each Uq
            # Wait for a snap_completed message on each child

        else:
            # We disgard the init_snap message, already received an init_snap fo this version
            pass

    def _receive_snap_completed(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: SnapCompletedMessage):
        """Executed when q receives a snap_completed message from it's child.

        Attributes
        ----------
        r : ProcessAddress
            child
        q : ProcessAddress
            destination
        c : ProcessAddress
            channel
        m : ControlMessage
            message
        """

        self.completed_snaps |= m.snapshots
        self.waiting_on_completed[r] = True

        self._print("snap_complted", self.waiting_on_ack.values(), [ a is not None for a in self.completed_snaps.values() ])
        self._print(self.completed_snaps.keys())
        self._print([ type(a) for a in self.completed_snaps.keys() ])

        if all(self.waiting_on_completed.values()) and all(self.waiting_on_ack.values()):
            self._finish_snapshot(r, q, c, m)

    def _receive_ack(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        self.waiting_on_ack[r] = True

        self._print("ack", self.waiting_on_ack.values(), [ a is not None for a in self.completed_snaps.values() ])

        if all(self.waiting_on_ack.values()):
            self.Uq = set()

            if all(self.waiting_on_completed.values()):
                self._finish_snapshot(r, q, c, m)

    def _finish_snapshot(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        """Executed after all ack and snap_completed messages have been received.

        Attributes
        ----------
        r : ProcessAddress
            child
        q : ProcessAddress
            destination
        c : ProcessAddress
            channel
        m : ControlMessage
            message
        """

        self.completed_snaps[self.identifier]  = ProcessSnapshot(self.p_state, self.link_states)
        self.Uq = set()

        if self.is_primary:
            global_snapshot = GlobalSnapshot(self.completed_snaps)

            self._verify_snapshot(global_snapshot)

            with open(f"snapshot_{m.version}.json", "w") as f:
                f.write(global_snapshot.serialise())

            self.is_active_snapshot = False

            self._print("FINISHED")
        else:

            if self.parent is not None:
                # Send a snapshot_completed message to parent
                self._send_message(SnapCompletedMessage(self.identifier, m.version, self.completed_snaps), self.parent)

    def _verify_snapshot(self, snapshot: GlobalSnapshot):
        """Sum up the total amount of money in the system in a global snapshot.

        Attributes
        ----------
        snapshot : GlobalSnapshot
            A snapshot of the whole system.
        """

        amount = 0
        for process in snapshot.snapshots:
            process_snapshot = snapshot.snapshots[process]

            amount += process_snapshot.state.money

            for conn in process_snapshot.connection_states:
                messages = process_snapshot.connection_states[conn]

                for message in messages:
                    amount += message.amount

        self._print(f"FINAL AMOUNT: {amount}")

    def _save_local_snapshot(self):
        """Save a local snapshot of the current state of the process."""
        with open(f"local_snapshot_{self.identifier.address}_{self.identifier.port}_{self.version}.json", "w") as f:
            f.write(json.dumps({"money": self.process_state.money}))

    def _print(self, *args):
        """Print a message with a prefix showing the process's identifier.

        Attributes
        ----------
        *args : Any
            Arguments passed to print.
        """
        print(f"[{self.identifier}] ", *args)
