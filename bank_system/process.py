from typing import Any
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread, Lock
from time import sleep
from select import select

from .message import Message
from .config import Config, ProcessAddress, Action
from .control_message import ControlMessage


class Process:
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
        self.sent_actions: list[Action] = []

        self.incoming_socket = socket(AF_INET, SOCK_STREAM) # TCP -- stream-oriented
        self.incoming_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

        self.mutex = Lock() # avoid edit doc simultaneously
        self.addresses = dict()
        self.buffers = dict()

        self.connections = {}
        for connection in config.processes[identifier].connections:
            self.connections[connection] = None

    def start(self):
        try:
            self.incoming_socket.bind((self.addr, self.port))
            self.incoming_socket.listen()
            print(f"Listening on {self.addr}:{self.port}")
            Thread(target=self._accept_loop, daemon=True).start()
        except Exception as e:
            print(f"Failed to bind/listen on {self.addr}:{self.port} - {e}")
            return

        for peer_addr in self.connections:
            try:
                s = socket(AF_INET, SOCK_STREAM)
                s.connect((peer_addr.address, peer_addr.port))
                self.connections[peer_addr] = s
                self.buffers[s] = ""
                print(f"Connected to {peer_addr.address}:{peer_addr.port}")
            except Exception as e:
                print(f"Failed to connect to {peer_addr.address}:{peer_addr.port} - {e}")

        Thread(target=self._recv_loop, daemon=True).start()

        for action in self.actions:
            print(f"Sending {action.amount} to {action.to.address}:{action.to.port}")
            self.send_message(message=action, message_to=action.to)

    def send_message(self, message: Message, message_to: ProcessAddress) -> bool:
        if isinstance(message, Action):
            self.sent_actions.append(message)

        if message_to not in self.connections or self.connections[message_to] is None:
            print(f"No connection to {message_to.address}:{message_to.port}")
            return False

        try:
            sock = self.connections[message_to]
            data = message.serialise() + "\n"  # newline framing
            sock.sendall(data.encode('utf-8'))
            sleep(1)
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
                self.addresses[conn.getpeername()] = peer_addr
                self.buffers[conn] = ""
            except OSError:
                break

    def _recv_loop(self):
        BUFFER_SIZE = 4096
        while True:
            sockets = [sock for sock in self.connections.values() if sock is not None]
            if not sockets:
                sleep(0.1)
                continue

            try:
                ready_sockets, _, _ = select(sockets, [], [], 1.0) # save bandwidth
                for sock in ready_sockets:
                    data = sock.recv(BUFFER_SIZE)
                    if not data:
                        continue

                    self.buffers[sock] += data.decode('utf-8')
                    while '\n' in self.buffers[sock]:
                        line, self.buffers[sock] = self.buffers[sock].split('\n', 1)
                        message = Message.deserialise(line)
                        peer_addr = self.addresses.get(sock.getpeername())
                        if peer_addr:
                            with self.mutex:
                                self.handle_receive_message(message, peer_addr)
            except Exception as e:
                print(f"Error in select receive loop: {e}")

    def handle_receive_message(self, message: Message, message_from: ProcessAddress) -> bool:
        if isinstance(message, ControlMessage):
            pass
        print(f'[Received] message from {message_from.address}:{message_from.port}')
        return True
