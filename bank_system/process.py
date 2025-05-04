from typing import Any
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread
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
        self.addr = identifier.address
        self.port = identifier.port
        self.primary = config.processes[identifier].primary
        self.actions = config.processes[identifier].action_list
        self.sent_actions: list[Action] = [] # to save all actions we've ever sent

        # Server socket for incoming connections
        self.incoming_socket = socket(AF_INET, SOCK_STREAM) # Create a TCP IPv4 socket for incoming connections
        self.incoming_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1) # for restarting

        self.connections = {}
        for connection in config.processes[identifier].connections:
            # Initialise the connection in the dict as none, connect later when we start()
            self.connections[connection] = None

    def start(self):
        """
        Start listening and connect to peers, then sending money to connected processes.
        """
        # Start listening
        try:
            # Bind and listen for incoming connections
            self.incoming_socket.bind((self.addr, self.port))
            self.incoming_socket.listen()
            print(f"Listening on {self.addr}:{self.port}")
            # Run the accept loop in a background thread
            Thread(target=self._accept_loop, daemon=True).start()
        except Exception as e:
            print(f"Failed to bind/listen on {self.addr}:{self.port} - {e}")
            return

        # Connect to all peers
        for peer_addr in self.connections:
            try:
                s = socket(AF_INET, SOCK_STREAM)
                s.connect((peer_addr.address, peer_addr.port))
                self.connections[peer_addr] = s
                print(f"Connected to {peer_addr.address}:{peer_addr.port}")
            except Exception as e:
                print(f"Failed to connect to {peer_addr.address}:{peer_addr.port} - {e}")

        # Send message to peers
        for action in self.actions:
            print(f"Sending {action.amount} to {action.to.address}:{action.to.port}")
            self.send_message(message=action, message_to=action.to) # action should inherit message (in config.py)

    def send_message(self, message: Message, message_to: ProcessAddress) -> bool:
        """
        Send a message to `message_to`.
        """

        # store sent action
        if isinstance(message, Action):
            self.sent_actions.append(message)

        if message_to not in self.connections or self.connections[message_to] is None:
            print(f"No connection to {message_to.address}:{message_to.port}") # Assuming Full Mesh
            return False

        try:
            sock = self.connections[message_to]
            sock.sendall(message.serialise().encode('utf-8'))
            sleep(1) # Pause to make demonstrating dropped messages during crashes easier
            return True
        except Exception as e:
            print(f"Failed to send message to {message_to.address}:{message_to.port} - {e}")
            return False

    def _accept_loop(self):
        while True:
            try:
                conn, (peer_ip, peer_port) = self.incoming_socket.accept()
                print(f"Accepted connection from {peer_ip}:{peer_port}")

                peer_addr = ProcessAddress(peer_ip, peer_port)
                self.connections[peer_addr] = conn

                Thread(target=self._recv_loop, args=(conn, peer_addr), daemon=True).start()
            except OSError:
                break

    def _recv_loop(self, conn: socket, peer_addr: ProcessAddress):
        try:
            with conn:
                while True:
                    data = conn.recv(4096)
                    if not data:
                        print(f"Connection closed by {peer_addr.address}:{peer_addr.port}")
                        break
                    msg = Message.deserialise(data.decode('utf-8'))
                    self.handle_receive_message(msg, peer_addr)
        except Exception as e:
            print(f"Connection to {peer_addr.address}:{peer_addr.port} broken - {e}")

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