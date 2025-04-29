
from typing import Any
from socket import socket
from time import sleep
from collections import defaultdict
from dataclasses import dataclass
from select import select

from .message import Message
from .config import Config, ProcessAddress, Action
from .control_message import ControlMessage, ControlMessageType

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

    port : int
        The port the process is listening on.

    connections : dict[ProcessAddress, Any]
        All processes this process is directly connected to. TODO: the type

    incoming_socket : socket
        Socket connections to receive information on.

    actions : list[Action]
        A list of actions the process will take in the system.

    process_state : int
        The current state of this process.


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
    port: int
    connections: dict[ProcessAddress, Any]
    incoming_socket: socket
    actions: list[Action]
    process_state: int

    # From Venkatesan algorithm
    version: int
    Uq: list[ProcessAddress]
    p_state: set[State]
    state: defaultdict[ProcessAddress, set[State]]
    link_states: set[State]
    loc_snap: list[set[State]]

    record: defaultdict[ProcessAddress, bool]

    def __init__(self, config: Config, identifier: ProcessAddress):
        self.port = identifier.port
        self.primary = config.processes[identifier].primary
        self.actions = config.processes[identifier].action_list

        self.incoming_socket = socket()

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

    def start(self):
        """Start the process.

        Open connections to all connected processes. Start sending money to connected processes.
        """

        # TODO: STUB
        pass

    def send_message(self, message: Message, message_to: ProcessAddress) -> bool:
        """Send a message to `message_to`."""

        # TODO: STUB

        # Pause to make demonsting dropped messages during crashes easier
        sleep(1)
        pass

    def handle_receive_message(self, message: Message, message_from: ProcessAddress) -> bool:
        """Handles received a message from any other process.

        TODO: This will need to handle messages one at a time, not multiple at once. Possibly that
              will be handled wherever this method is called, maybe in start?
        """
        while True:
            ready_socks, _, _ = select(self.connections, [], [], 5)
            for sock in ready_socks:
                if isinstance(message, ControlMessage):
                    pass # TODO: Snapshot logic
                    if message.message_type == ControlMessageType.INIT_SNAP:
                        self.receive_initiate() # TODO: THIS
                    elif message.message_type == ControlMessageType.MARKER:
                        self.receive_marker() # TODO: THIS
                    elif message.message_type == ControlMessageType.ACK:
                        pass # TODO: what do we do
                    elif message.message_type == ControlMessageType.SNAP_COMPLETED:
                        pass # TODO: what do we do
                elif isinstance(message, Message): # TODO underlying message type
                    self.receive_und() # TODO: THIS

    # From Venkatesan algorithm

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
        # TODO: Pass m to underlying code (handle message?)

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
            # TODO: send init_snap(self.version+1) to self (q)
            self.state[c] = set()

        self.link_states += self.state[c]
        self.record[c] = False

        # TODO: send an ack on c
        #       What is the ack used for?

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
            self.p_state[q] = Any # TODO: set to current process state (amount of money)

            # NOTE: algorithm uses version + 1 but I think it's safer to copy the message version
            self.version = m.version

            for connection in self.connections:
                self.state[connection] = set()
                self.record[connection] = True

            for connection in self.Uq:
                pass # TODO: send marker on connection

            self.Uq = set()

            # TODO: Wait for a snap_completed message on each child
            #       This bit doesn't make sense for this function, it should be moved to a
            #       "receive_snap_completed" function.

            if self.primary:
                pass # TODO: Snapshot is complete, save it somehow
            else:
                pass # TODO: send a snapshot_completed message to parent

        else:
            pass # We disgard the init_snap message, already received an init_snap fo this version


