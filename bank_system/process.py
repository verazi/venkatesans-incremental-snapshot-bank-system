
from typing import Any
from socket import socket
from time import sleep
from collections import defaultdict

from .message import Message
from .config import Config, ProcessAddress, Action
from .control_message import ControlMessage

class Process:
    """A process that can connect to and send messages to other processes.

    Attributes
    ----------
    primary : bool
        If the process is the primary process in charge of starting snapshots.

    port : int
        The port the process is listening on.

    actions : list[Action]
        A list of actions the process will take in the system.

    connections : dict[ProcessAddress, Any]
        All processes this process is directly connected to. TODO: the type
    """

    primary: bool
    port: int
    connections: dict[ProcessAddress, Any]
    incoming_socket: socket
    actions: list[Action]

    # From Venkatesan algorithm
    version: int
    Uq: list[ProcessAddress]
    p_state: Any
    state: defaultdict[ProcessAddress, set]
    link_states: set[Any]
    loc_snap: list[Any]

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
        self.states = defaultdict(set)

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

        if isinstance(message, ControlMessage):
            # TODO: Snapshot logic
            pass


        # TODO: STUB
        pass