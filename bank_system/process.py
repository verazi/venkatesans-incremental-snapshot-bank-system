from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread, Lock
from time import sleep
from collections import defaultdict
from dataclasses import dataclass
from select import select
from random import random
from typing import Any

from .action_message import ActionMessage
from .message_factory import MessageFactory
from .initial_connection_message import InitialConnectionMessage
from .message import Message
from .config import Config, Action
from .control_message import ControlMessage, ControlMessageType
from .process_address import ProcessAddress

BUFFER_SIZE = 2048
INTRODUCED_DELAY = 1
ENCODING = 'utf-8'

@dataclass(eq=True, frozen=True)
class State:
    """The current state of the process.

    Attributes
    ----------
    money : int
        The amount of money the process has available.
    last_action : int | None
        The next action to be performed.
    """

    money: int
    last_action: Action | None

@dataclass(eq=True, frozen=True)
class Snapshot:
    """A single snapshot."""

    state: State

    connection_states: Any

class Process:
    """A process that can connect to and send messages to other processes.

    Attributes
    ----------
    TODO: THIS

    # From Venkatesan algorithm

    version : int
        The version number of the current snapshot.
    Uq : list[ProcessAddress]
        List of neighbours messages have been sent to since the last snapshot completed.
    p_state : set[State]
        Saved (cached) states of the local process.
    state : defaultdict[ProcessAddress, set[State]]
        The state of the other processes.
    link_states : set[State] ?
        Completed channel states for the current snapshot.
    loc_snap : list[set[State]]
        the ith global state according to Uq (this process).
    """

    primary: bool
    identifier: ProcessAddress
    incoming_sockets: dict[ProcessAddress, socket]
    outgoing_sockets: dict[ProcessAddress, socket]

    connections: list[ProcessAddress]

    actions: list[Action]
    process_state: State

    mutex: Lock

    # From Venkatesan algorithm
    version: int
    Uq: list[ProcessAddress]
    p_state: State
    state: defaultdict[ProcessAddress, set[ActionMessage]]
    link_states: set[State]
    loc_snap: dict[int, Snapshot]

    # # My additions
    record: defaultdict[ProcessAddress, bool]
    parent: ProcessAddress | None
    completed_snaps: dict[ProcessAddress, bool]
    acks: dict[ProcessAddress, bool]
    active_snapshot: bool

    def __init__(self, config: Config, identifier: ProcessAddress):
        self.identifier = identifier

        self.incoming_sockets = {}
        self.outgoing_sockets = {}

        # Include self connection
        self.connections = config.processes[identifier].connections + [identifier]
        self.sockets = {}

        self.mutex = Lock()

        self.primary = config.processes[identifier].primary
        self.actions = config.processes[identifier].action_list

        self.process_state = State(config.processes[identifier].initial_money, None)

        # avoid editing simultaneously
        # self.mutex = Lock()

        self.version = 0
        self.Uq = config.processes[identifier].connections
        self.p_state = State(config.processes[identifier].initial_money, None)
        self.state = defaultdict(set)
        self.link_states = set()
        self.loc_snap = dict()
        self.record = defaultdict(lambda: False)
        self.parent = None
        self.completed_snaps = dict()
        self.acks = dict()
        self.active_snapshot = False

    def start(self):
        # Listen for peers

        incoming = socket(AF_INET, SOCK_STREAM)
        # incoming.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        incoming.bind((self.identifier.address, self.identifier.port))
        incoming.listen()

        accept_loop = Thread(target=self._accept_loop, args=[incoming])
        accept_loop.start()

        sleep(0.5)

        # Connect to existing peers
        for peer in self.connections:
            # with self.mutex:
            if peer not in self.incoming_sockets:
                # self._print(f"Attempting {peer}")
                s = socket(AF_INET, SOCK_STREAM)
                s.connect((peer.address, peer.port))

                self.outgoing_sockets[peer] = s

                s.send(InitialConnectionMessage(self.identifier).serialise().encode(ENCODING))
                self._print(f"Outgoing connected to {peer.address}:{peer.port}")

        accept_loop.join()

        for conn in self.incoming_sockets:
            if conn not in self.outgoing_sockets:
                self.outgoing_sockets[conn] = self.incoming_sockets[conn]

        for conn in self.outgoing_sockets:
            if conn not in self.incoming_sockets:
                self.incoming_sockets[conn] = self.outgoing_sockets[conn]

        # Send actions in another thread
        action_thread = Thread(target=self._action_loop)
        action_thread.start()

        if self.primary:
            snapshot_thread = Thread(target=self._snapshot_loop)
            snapshot_thread.start()

        # Listen for incoming messages
        while True:
            ready_sockets, _, _ = select(
                [ self.incoming_sockets[k] for k in self.incoming_sockets ],
                [],
                [],
            )

            for sock in ready_sockets:
                self._print("\treceived")

                sock: socket
                data = sock.recv(BUFFER_SIZE)

                self._print("\tread {data}")

                for raw_message in data.split(b"\n"):
                    if len(raw_message) == 0:
                        continue

                    message = MessageFactory.deserialise(raw_message)

                    # with self.mutex:
                    message_from = message.message_from
                    self._receive_message(message, message_from)

                    self._print("\tdone")

                    # TODO: introduce a delay here to better demonstrate crashing with messages in flight


    def _waiting_for_connections(self):
        with self.mutex:
            connected = set(self.incoming_sockets.keys()) | set(self.outgoing_sockets.keys())

            return set(self.connections) - connected


    def _accept_loop(self, incoming: socket):
        while len(self._waiting_for_connections()) > 0:
            conn, _ = incoming.accept()

            self._print("ACCEPTED")
            message = conn.recv(BUFFER_SIZE)

            # with self.mutex:
            initial_connection_message = InitialConnectionMessage.deserialise(message)
            peer = initial_connection_message.message_from

            self._print(f"Incoming connected to {peer.address}:{peer.port}")

            self.incoming_sockets[peer] = conn


    def _action_loop(self):
        for action in self.actions:
            self._print(f"Waiting {action.delay} to send {action.amount} to {action.to}")

            sleep(action.delay)

            # self._print(f"Action message {action.amount} to {action.to.address}:{action.to.port}")
            self._send_message(message=action.to_message(self.identifier), message_to=action.to)

            with self.mutex:
                self.process_state = State(
                    self.process_state.money - action.amount,
                    action,
                )

        self._print("Finished actions")

    def _snapshot_loop(self):
        while self.primary:
            # Between 4 and 5 seconds
            sleep(4 + random())
            if not self.active_snapshot:
                with self.mutex:
                    self.active_snapshot = True

                    self._print("Start snapshot?")

                    self._send_message(
                        ControlMessage(
                            self.identifier,
                            ControlMessageType.INIT_SNAP,
                            self.version+1
                        ),
                        self.identifier
                    )

    def _print(self, *args):
        print(f"[{self.identifier}] ", *args)

    def _send_message(self, message: Message, message_to: ProcessAddress):
        self._print(f"Sending message type:{message.MESSAGE_TYPE} to:{message_to}")

        sock: socket
        sock = self.outgoing_sockets[message_to]

        data = message.serialise() + "\n"  # newline framing
        sock.sendall(data.encode(ENCODING))

        self._print("\tSent!")

    def _receive_message(self, message: Message, message_from: ProcessAddress) -> bool:
        """Handles received a message from any other process."""
        self._print(f"Received message type:{message.MESSAGE_TYPE} from:{message.message_from}")

        if isinstance(message, ControlMessage):
            self._print(f"\t{message.control_message_type}")
            if message.control_message_type == ControlMessageType.INIT_SNAP:
                self._receive_initiate(self.identifier, message_from, message_from, message)
            elif message.control_message_type == ControlMessageType.MARKER:
                self._receive_marker(message_from, self.identifier, message_from, message)
            elif message.control_message_type == ControlMessageType.ACK:
                self._receive_ack(message_from, self.identifier, message_from, message)
            elif message.control_message_type == ControlMessageType.SNAP_COMPLETED:
                self._receive_snap_completed(message_from, self.identifier, message_from, message)
        elif isinstance(message, ActionMessage): # TODO underlying message type
            self._receive_und(message_from, self.identifier, message_from, message)
        else:
            self._print("NOT MESSAGE TYPE")

    def _handle_message(self, message: Message, message_from: ProcessAddress):
        """Handle the logic for receiving an underlying message."""
        self._print("Handling underlying message")

        self.process_state = State(
            self.process_state.money + message.amount,
            self.process_state.last_action,
        )

    # From Venkatesan algorithm

    def _send_und(self, q: ProcessAddress, r: ProcessAddress, c: ProcessAddress, m: Message):
        """Executed when q sends a primary message to r.

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

        self.Uq += c
        self._send_message(m, q)

    def _receive_und(self, q: ProcessAddress, r: ProcessAddress, c: ProcessAddress, m: Message):
        """Executed when a primary message is received.

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
            self.state[c].add(m)

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
            self._send_message(ControlMessage(self.identifier, ControlMessageType.INIT_SNAP, self.version + 1), q)
            self.state[c] = set()

        self.link_states |= self.state[c]
        self.record[c] = False

        # Send an ack on c
        # TODO: What does this actually accomplish?
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
            self.loc_snap[self.version] = { self.p_state } | self.link_states # TODO: I don't think this makes sense

            self.link_states = set()
            self.p_state = self.process_state # TODO: set to current process state (amount of money)

            # NOTE: algorithm uses version + 1 but I think it's safer to copy the message version
            self.version = m.version

            for connection in [ c for c in self.connections if c != self.identifier]:
                self.state[connection] = set()
                self.record[connection] = True

            self.acks = { k: False for k in self.Uq }

            self.parent = r
            self.completed_snaps = { k: False for k in self.connections if k != self.identifier }

            for connection in [ c for c in self.connections if c != self.identifier]:
                # Send init snap to all incoming channels
                self._send_message(ControlMessage(self.identifier, ControlMessageType.INIT_SNAP, m.version), connection)

            for connection in self.Uq:
                # Send marker on connection
                self._send_message(ControlMessage(self.identifier, ControlMessageType.MARKER, m.version), connection)


            # Wait for an ack on each Uq
            # Wait for a snap_completed message on each child

        else:
            pass # We disgard the init_snap message, already received an init_snap fo this version

    def _receive_snap_completed(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        """Executed when q receives a snap_completed message from it's child."""

        self.completed_snaps[r] = True

        self._print(self.acks.values())
        self._print(self.completed_snaps.values())

        if all(self.completed_snaps.values()) and all(self.acks.values()):
            self.finish_initiate(r, q, c, m)

    def _receive_ack(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        self.acks[r] = True

        if all(self.acks.values()):
            self.Uq = set()

            if all(self.completed_snaps.values()):
                self.finish_initiate(r, q, c, m)

    def finish_initiate(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        if self.primary:
            self._print("FINISHED TODO THIS")
            self.active_snapshot = False
            pass # TODO: Snapshot is complete, save it somehow
        else:
            if self.parent is not None:
                # Send a snapshot_completed message to parent
                self._send_message(ControlMessage(self.identifier, ControlMessageType.SNAP_COMPLETED, m.version), self.parent)
                self.parent = None
