from typing import Any
from socket import socket, AF_INET, SOCK_STREAM
from time import sleep
from collections import defaultdict
from dataclasses import dataclass
from select import select
from threading import Thread, Lock

from .initial_connection_message import InitialConnectionMessage
from .message import Message
from .config import Config, ProcessAddress, Action
from .control_message import ControlMessage, ControlMessageType

BUFFER_SIZE = 2048

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
    connections: dict[ProcessAddress, Any]
    incoming_socket: socket
    actions: list[Action]
    process_state: int

    mutex: Lock

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

        self.incoming_socket = socket(AF_INET, SOCK_STREAM) # TCP over IPv4

        self.mutex = Lock()

        self.connections = {}
        for connection in config.processes[identifier].connections:
            # Initialise the connection in the dict as none, connect later when we start()
            self.connections[connection] = None

        self.version = 0
        self.link_states = set()
        self.Uq = config.processes[identifier].connections
        self.state = defaultdict(set)
        self.record = defaultdict(False)
        self.loc_snap = []

        self.parent = None

    def start(self):
        """Start the process.

        Open connections to all connected processes. Start sending money to connected processes.
        """
        for peer_addr in self.connections:
            try:
                s = socket(AF_INET, SOCK_STREAM)
                s.connect((peer_addr.address, peer_addr.port))
                self.connections[peer_addr] = s

                s.send(InitialConnectionMessage(self.identifier).serialise())

                print(f"Connected to {peer_addr.address}:{peer_addr.port}")
            except Exception as e:
                print(f"Failed to connect to {peer_addr.address}:{peer_addr.port} - {e}")

        # Wait for all connections to be made
        while not all(filter(lambda con: con is None, self.connections)):
            s = socket(AF_INET, SOCK_STREAM)
            s.bind((self.identifier.address, self.identifier.port))
            s.listen(5)

            (client_sock, _address) = s.accept()

            initial_connection_message = InitialConnectionMessage.deserialise(client_sock.recv(BUFFER_SIZE))

            self.connections[initial_connection_message.connecting_address] = client_sock


        # Send actions in another thread
        action_thread = Thread(target=self.handle_sending_actions)
        action_thread.start()

        # Handle incoming connections
        while True:
            sockets, _, _ = select(self.connections, [], [], 1)
            for sock in sockets:
                # This is for type hinting, remove before submission
                sock: socket

                data, addr = sock.recvfrom(BUFFER_SIZE)
                # TODO: THIS

                print(data, addr)


        # Handle stopping thread?
        action_thread.join()

    def handle_sending_actions(self):
        for action in self.actions:
            sleep(action.delay)

            with self.mutex:
                print(f"Sending {action.amount} to {action.to.address}:{action.to.port}")
                self.send_message(message=action, message_to=action.to) # action should inherit message (in config.py)

    def send_message(self, message: Message, message_to: ProcessAddress) -> bool:
        """Send a message to `message_to`."""
        if message_to not in self.connections or self.connections[message_to] is None:
            print(f"No connection to {message_to.address}:{message_to.port}") # Assuming Full Mesh
            return False

        try:
            sock = self.connections[message_to]
            sock.sendall(message.serialise().encode('utf-8'))
            # Pause to make demonstrating dropped messages during crashes easier
            sleep(1)
            return True
        except Exception as e:
            print(f"Failed to send message to {message_to.address}:{message_to.port} - {e}")
            return False



    def receive_message(self, message: Message, message_from: ProcessAddress) -> bool:
        """Handles received a message from any other process.

        TODO: This will need to handle messages one at a time, not multiple at once. Possibly that
              will be handled wherever this method is called, maybe in start?
        # while True:
        #     ready_socks, _, _ = select(self.connections, [], [], 5)
        #     for sock in ready_socks:
        """

        if isinstance(message, ControlMessage):
            if message.message_type == ControlMessageType.INIT_SNAP:
                self.receive_initiate(self.identifier, message_from, message_from, message)
            elif message.message_type == ControlMessageType.MARKER:
                self.receive_marker(message_from, self.identifier, message_from, message)
            elif message.message_type == ControlMessageType.ACK:
                pass # TODO: what do we do
            elif message.message_type == ControlMessageType.SNAP_COMPLETED:
                pass # TODO: what do we do
        elif isinstance(message, Message): # TODO underlying message type
            self.receive_und(message_from, self.identifier, message_from, message)

    def handle_message(self, message: Message, message_from: ProcessAddress):
        """Handle the logic for receiving an underlying message."""
        self.process_state += message.amount # TODO: Update variable name and Message type

    # From Venkatesan algorithm

    # TODO: c is always one of q or r, should maybe deduplicate? Sticking to the algorithm might
    #       be easier to read.
    def send_und(self, q: ProcessAddress, r: ProcessAddress, c: ProcessAddress, m: Message):
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
        self.send_message(m, q)

    def receive_und(self, q: ProcessAddress, r: ProcessAddress, c: ProcessAddress, m: Message):
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

        self.handle_message(m, q)

    def receive_marker(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: ControlMessage):
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
            self.send_message(ControlMessage(ControlMessageType.INIT_SNAP, self.version + 1), q)
            self.state[c] = set()

        self.link_states += self.state[c]
        self.record[c] = False

        # Send an ack on c
        # TODO: What does this actually accomplish?
        self.send_message(ControlMessage(ControlMessageType.ACK, m.version), c)

    def receive_initiate(self, q: ProcessAddress, r: ProcessAddress, c: ProcessAddress, m: ControlMessage):
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
                self.send_message(ControlMessage(ControlMessageType.MARKER, m.version), connection)

            self.Uq = set()

            # Wait for a snap_completed message on each child
            self.parent = r

        else:
            pass # We disgard the init_snap message, already received an init_snap fo this version

    def receive_snap_completed(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        """Executed when q receives a snap_completed message from it's child."""

        if self.primary:
            pass # TODO: Snapshot is complete, save it somehow
        else:
            if self.parent is not None:
                # Send a snapshot_completed message to parent
                self.send_message(ControlMessage(ControlMessageType.SNAP_COMPLETED, m.version), self.parent)
                self.parent = None
