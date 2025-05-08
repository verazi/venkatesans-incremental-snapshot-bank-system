from typing import Any
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread, Lock
from time import sleep
from collections import defaultdict
from dataclasses import dataclass
from select import select
from random import random

from .message_factory import MessageFactory
from .initial_connection_message import InitialConnectionMessage
from .message import Message
from .config import Config, Action
from .control_message import ControlMessage, ControlMessageType
from .process_address import ProcessAddress

BUFFER_SIZE = 2048
INTRODUCED_DELAY = 1

@dataclass
class State:
    """The current state of the process.

    Attributes
    ----------
    money : int
        The amount of money the process has available.
    next_action : int | None
        The next action to be performed.
    """

    money: int
    next_action: int | None

class Process:
    """A process that can connect to and send messages to other processes.

    Attributes
    ----------
    primary : bool
        If the process is the primary process in charge of starting snapshots.

    identifier: ProcessAddress
        The identifier of the process.

    connections : dict[ProcessAddress, Any]
        All processes this process is directly connected to. TODO: the type

    incoming_socket : socket
        Socket connections to receive information on.

    actions : list[Action]
        A list of actions the process will take in the system.

    process_state : int
        The current state of this process.

    mutex : Lock
        A mutex to stop sending and receiving a message at the same time, possibly causing incorrect
        behaviour with the systems state.


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
    connections: dict[ProcessAddress, socket]
    incoming_socket: socket
    actions: list[Action]
    process_state: int

    # Connection variables
    mutex: Lock
    addresses: dict[Any, ProcessAddress] # Any should be _RetAddress
    buffers: dict[Any, str]

    # From Venkatesan algorithm
    version: int
    Uq: list[ProcessAddress]
    p_state: set[State]
    state: defaultdict[ProcessAddress, set[State]]
    link_states: set[State]
    loc_snap: list[set[State]]

    # My additions
    record: defaultdict[ProcessAddress, bool]
    parent: ProcessAddress | None

    def __init__(self, config: Config, identifier: ProcessAddress):
        self.identifier = identifier
        self.primary = config.processes[identifier].primary
        self.actions = config.processes[identifier].action_list
        self.process_state = config.processes[identifier].initial_money
        self.sent_actions: list[Action] = []

        self.incoming_socket = socket(AF_INET, SOCK_STREAM) # TCP -- stream-oriented
        self.incoming_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

        self.mutex = Lock() # avoid edit doc simultaneously
        self.buffers = dict()

        self.connections = {}
        for connection in config.processes[identifier].connections:
            self.connections[connection] = None

        # Self channel
        # self.connections[self.identifier] = None

        self.version = 0
        self.link_states = set()
        self.Uq = config.processes[identifier].connections
        self.state = defaultdict(set)
        self.record = defaultdict(lambda: False)
        self.loc_snap = []

        self.parent = None

    def start(self):
        # Listen for peers
        self.incoming_socket.bind((self.identifier.address, self.identifier.port))
        self.incoming_socket.listen()

        self._process_print(f"Listening on {self.identifier}")

        accept_loop = Thread(target=self._accept_loop, daemon=True)
        accept_loop.start()

        sleep(2)

        # Try sending a message to peers
        for peer_addr in self.connections:

            if peer_addr == self.identifier:
                continue

            # Having a try here raises: "RuntimeError: dictionary changed size during iteration" ????
            s = socket(AF_INET, SOCK_STREAM)
            # at most 10 times retry
            for _ in range(10):
                try:
                    s.connect((peer_addr.address, peer_addr.port))
                    break
                except ConnectionRefusedError:
                    sleep(0.5)
            else:
                raise RuntimeError(f"Connection to {peer_addr} fails")

            # s.connect((peer_addr.address, peer_addr.port))

            self.connections[peer_addr] = s
            self.buffers[s] = ""

            s.send(InitialConnectionMessage(self.identifier).serialise().encode('utf-8'))

            self._process_print(f"Connected to {peer_addr.address}:{peer_addr.port}")

        # Wait for all connections before starting to send messages
        while any(conn is None for conn in self.connections.values()):
            sleep(0.1)
        accept_loop.join()

        connections = self.connections.__repr__()
        self._process_print(self.identifier, "finished listening for peers", connections)

        # Send actions in another thread
        action_thread = Thread(target=self._action_loop)
        action_thread.start()

        if self.primary:
            snapshot_thread = Thread(target=self._snapshot_loop)
            snapshot_thread.start()

        print("DO THE ACTUAL THING")

        # Listen for incoming messages
        while True:
            # self._process_print("start to listen")
            ready_sockets, write_sockets, err_sockets = select(
                [ self.connections[k] for k in self.connections ],
                [ self.connections[k] for k in self.connections ],
                [ self.connections[k] for k in self.connections ],
                1
            ) # save bandwidth
            for sock in ready_sockets:
                self._process_print("\treceived")
                sock: socket
                data = sock.recv(BUFFER_SIZE)
                self._process_print("\tread")


                # self.buffers[sock] += data.decode('utf-8')
                # while '\n' in self.buffers[sock]:
                #     line, self.buffers[sock] = self.buffers[sock].split('\n', 1)

                message = MessageFactory.deserialise(data)

                with self.mutex:
                    message_from = message.message_from
                    self._receive_message(message, message_from)

                self._process_print("\tdone")

                # TODO: introduce a delay here to better demonstrate crashing with messages in flight
            for conn in write_sockets:
                pass
            for conn in err_sockets:
                print("ERR")

    def _send_message(self, message: Message, message_to: ProcessAddress) -> bool:
        self._process_print(f"Sending message type:{message.MESSAGE_TYPE} to:{message_to}")

        sock: socket
        sock = self.connections[message_to]

        data = message.serialise() + "\n"  # newline framing
        sock.sendall(data.encode('utf-8'))

        self._process_print("\tSent!")

        return True


    def _accept_loop(self):
        # Only run until all connections are made
        while len([conn for conn in self.connections.values() if conn is None]) > 0:
            conn, (peer_ip, peer_port) = self.incoming_socket.accept()

            self._process_print(f"Accepted connection from {peer_ip}:{peer_port}")

            initial_connection_message = InitialConnectionMessage.deserialise(conn.recv(BUFFER_SIZE))
            peer_addr = initial_connection_message.message_from

            with self.mutex:
                self.connections[peer_addr] = conn
                self.buffers[conn] = ""

    def _action_loop(self):
        for action in self.actions:
            self._process_print(f"Waiting {action.delay} to send {action.amount} to {action.to}")

            sleep(action.delay)

            with self.mutex:
                self.sent_actions.append(action)

                self._process_print(f"Sending {action.amount} to {action.to.address}:{action.to.port}")
                self._send_message(message=action.to_message(self.identifier), message_to=action.to)

        self._process_print("Finished actions")

    def _snapshot_loop(self):
        while self.primary:
            # Between 4 and 5 seconds
            sleep(4 + random())
            with self.mutex:
                self._process_print("Start snapshot?")

                # Create the INIT_SNAP control message
                ctrl = ControlMessage(
                    self.identifier,
                    ControlMessageType.INIT_SNAP,
                    self.version + 1
                )

                # 1. Locally handle the snapshot initiation without using a socket
                #    (treat it as if we just received the INIT_SNAP ourselves)
                self._receive_initiate(
                    q=self.identifier,
                    r=self.identifier,
                    c=self.identifier,
                    m=ctrl
                )

                # 2. Send a MARKER control message to each neighbour in Uq
                for peer in self.Uq:
                    self._send_message(
                        ControlMessage(
                            self.identifier,
                            ControlMessageType.MARKER,
                            ctrl.version
                        ),
                        peer
                    )

                # 3. Clear Uq for the next snapshot round
                self.Uq = set()

                # self._send_message(
                #     ControlMessage(
                #         self.identifier,
                #         ControlMessageType.INIT_SNAP,
                #         self.version+1
                #     ),
                #     self.identifier
                # )

    def _process_print(self, *args):
        print(f"[{self.identifier}] ", *args)

    def _receive_message(self, message: Message, message_from: ProcessAddress) -> bool:
        """Handles received a message from any other process."""
        self._process_print(f"Received message type:{message.MESSAGE_TYPE} from:{message.message_from}")

        if isinstance(message, ControlMessage):
            if message.message_type == ControlMessageType.INIT_SNAP:
                self._receive_initiate(self.identifier, message_from, message_from, message)
            elif message.message_type == ControlMessageType.MARKER:
                self._receive_marker(message_from, self.identifier, message_from, message)
            elif message.message_type == ControlMessageType.ACK:
                pass # TODO: what do we do
            elif message.message_type == ControlMessageType.SNAP_COMPLETED:
                pass # TODO: what do we do
        elif isinstance(message, Message): # TODO underlying message type
            self._receive_und(message_from, self.identifier, message_from, message)

    def _handle_message(self, message: Message, message_from: ProcessAddress):
        """Handle the logic for receiving an underlying message."""
        self._process_print("Handling underlying message")

        self.process_state += message.amount # TODO: Update variable name and Message type

    # From Venkatesan algorithm

    # TODO: c is always one of q or r, should maybe deduplicate? Sticking to the algorithm might
    #       be easier to read.
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
            self.state[c] += m

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
            self._send_message(ControlMessage(ControlMessageType.INIT_SNAP, self.version + 1), q)
            self.state[c] = set()

        self.link_states += self.state[c]
        self.record[c] = False

        # Send an ack on c
        # TODO: What does this actually accomplish?
        self._send_message(ControlMessage(ControlMessageType.ACK, m.version), c)

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
            self.loc_snap[self.version] = self.p_state[q] + self.link_states # TODO: I don't think this makes sense

            self.link_states = set()
            self.p_state[q] = State(self.process_state, Any) # TODO: set to current process state (amount of money)

            # NOTE: algorithm uses version + 1 but I think it's safer to copy the message version
            self.version = m.version

            for connection in self.connections:
                self.state[connection] = set()
                self.record[connection] = True

            for connection in self.Uq:
                # Send marker on connection
                self._send_message(ControlMessage(ControlMessageType.MARKER, m.version), connection)

            self.Uq = set()

            # Wait for a snap_completed message on each child
            self.parent = r

        else:
            pass # We disgard the init_snap message, already received an init_snap fo this version

    def _receive_snap_completed(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        """Executed when q receives a snap_completed message from it's child."""

        if self.primary:
            pass # TODO: Snapshot is complete, save it somehow
        else:
            if self.parent is not None:
                # Send a snapshot_completed message to parent
                self._send_message(ControlMessage(ControlMessageType.SNAP_COMPLETED, m.version), self.parent)
                self.parent = None
