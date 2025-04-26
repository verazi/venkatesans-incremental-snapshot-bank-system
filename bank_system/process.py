
from typing import Any
from socket import socket, AF_INET, SOCK_STREAM
from time import sleep

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

    def __init__(self, config: Config, identifier: ProcessAddress):
        self.port = identifier.port
        self.primary = config.processes[identifier].primary
        self.actions = config.processes[identifier].action_list

        self.incoming_socket = socket(AF_INET, SOCK_STREAM) # TCP over IPv4

        self.connections = {}
        for connection in config.processes[identifier].connections:
            # Initialise the connection in the dict as none, connect later when we start()
            self.connections[connection] = None

    def start(self):
        """Start the process.

        Open connections to all connected processes. Start sending money to connected processes.
        """
        for peer_addr in self.connections:
            try:
                s = socket(AF_INET, SOCK_STREAM)
                s.connect((peer_addr.address, peer_addr.port))
                self.connections[peer_addr] = s
                print(f"Connected to {peer_addr.address}:{peer_addr.port}")
            except Exception as e:
                print(f"Failed to connect to {peer_addr.address}:{peer_addr.port} - {e}")

        for action in self.actions:
            sleep(action.delay)
            print(f"Sending {action.amount} to {action.to.address}: {action.to.port}")
            self.send_message(message=action, message_to=action.to)

    def send_message(self, message: Action, message_to: ProcessAddress) -> bool:
        """Send a message to `message_to`."""
        if message_to not in self.connections or self.connections[message_to] is None:
            print(f"No connection to {message_to.address}:{message_to.port}") # Assuming Full Mesh
            return False

        try:
            sock = self.connections[message_to]
            sock.sendall(message.serialise().encode('utf-8'))
            return True
        except Exception as e:
            print(f"Failed to send message to {message_to.address}:{message_to.port} - {e}")
            return False

        # Pause to make demonsting dropped messages during crashes easier
        sleep(1)

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