from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread, Lock
from time import sleep, time
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
        """Initialize a process."""
        self.config = config
        self.identifier = identifier
        self.process_config = config.processes[identifier]
        
        self.primary = self.process_config.primary
        self.connections = self.process_config.connections
        self.parent = self.process_config.parent
        self.children = self.process_config.children
        
        self.incoming_sockets = {}
        self.outgoing_sockets = {}
        self.mutex = Lock()
        
        # Process state
        self.process_state = State(self.process_config.initial_money, None)
        self.actions = self.process_config.action_list
        self.active = True
        
        # Snapshot algorithm state
        self.version = 0
        self.active_snapshot = False
        self.record = {conn: False for conn in self.connections}
        self.state = defaultdict(set)
        self.link_states = set()
        self.loc_snap = {}
        self.acks = {}

    def start(self):
        """Start the process."""
        # Create incoming socket
        incoming = socket(AF_INET, SOCK_STREAM)
        incoming.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        incoming.bind((self.identifier.address, self.identifier.port))
        incoming.listen()

        # Start accept thread
        accept_thread = Thread(target=self._accept_loop, args=(incoming,))
        accept_thread.daemon = True
        accept_thread.start()

        # Wait for a short time to ensure socket is listening
        sleep(0.5)

        # Connect to peers with retries
        max_retries = 5
        retry_delay = 0.5
        for peer in self.connections:
            if peer != self.identifier:
                connected = False
                for attempt in range(max_retries):
                    try:
                        s = socket(AF_INET, SOCK_STREAM)
                        s.connect((peer.address, peer.port))
                        self._print(f"Outgoing connected to {peer}")
                        
                        # Send initial connection message
                        message = InitialConnectionMessage(self.identifier)
                        data = message.serialise() + "\n"
                        s.sendall(data.encode(ENCODING))
                        
                        self.outgoing_sockets[peer] = s
                        connected = True
                        break
                    except ConnectionRefusedError:
                        if attempt < max_retries - 1:
                            self._print(f"Failed to connect to {peer}, retrying in {retry_delay}s")
                            sleep(retry_delay)
                        else:
                            self._print(f"Failed to connect to {peer} after {max_retries} attempts")
                        s.close()

                if not connected:
                    self._print(f"Could not establish connection to {peer}")

        # Wait for incoming connections with timeout
        timeout = 10  # seconds
        start_time = time()
        while len(self._waiting_for_connections()) > 0:
            if time() - start_time > timeout:
                self._print("Timeout waiting for connections")
                break
            sleep(0.1)

        # Ensure bidirectional connections
        for peer in self.connections:
            if peer != self.identifier:
                if peer not in self.incoming_sockets or peer not in self.outgoing_sockets:
                    self._print(f"Warning: Incomplete connection with {peer}")

        # Start action thread
        action_thread = Thread(target=self._action_loop)
        action_thread.daemon = True
        action_thread.start()

        # Start snapshot thread if primary
        if self.primary:
            snapshot_thread = Thread(target=self._snapshot_loop)
            snapshot_thread.daemon = True
            snapshot_thread.start()

        # Start message receiving threads
        receive_threads = []
        for peer, sock in self.incoming_sockets.items():
            thread = Thread(target=self._receive_loop, args=(sock,))
            thread.daemon = True
            thread.start()
            receive_threads.append(thread)

        # Wait for all threads to complete
        while self.active:
            sleep(0.1)

        # Close all sockets
        for sock in self.incoming_sockets.values():
            try:
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()
            except:
                pass
        for sock in self.outgoing_sockets.values():
            try:
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()
            except:
                pass

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
        """Execute planned actions."""
        for action in self.actions:
            if not self.active:
                break
                
            self._print(f"Waiting {action.delay} to send {action.amount} to {action.to}")
            sleep(action.delay)
            
            if not self.active:
                break
            
            # Try to send message with retries
            max_retries = 3
            retry_delay = 0.5
            success = False
            
            for attempt in range(max_retries):
                try:
                    if action.to in self.outgoing_sockets:
                        self._send_message(
                            message=action.to_message(self.identifier),
                            message_to=action.to
                        )
                        
                        with self.mutex:
                            self.process_state = State(
                                self.process_state.money - action.amount,
                                action,
                            )
                        
                        success = True
                        break
                    else:
                        self._print(f"No connection to {action.to}")
                        sleep(retry_delay)
                except Exception as e:
                    if attempt < max_retries - 1:
                        self._print(f"Failed to send message to {action.to}, retrying: {e}")
                        sleep(retry_delay)
                    else:
                        self._print(f"Failed to send message to {action.to} after {max_retries} attempts")
            
            if not success:
                self._print(f"Skipping action: send {action.amount} to {action.to}")

        self._print("Finished actions")

    def _snapshot_loop(self):
        """Primary process's snapshot initiation loop."""
        while self.primary and self.active:
            # Between 4 and 5 seconds
            sleep(4 + random())
            if not self.active_snapshot:
                with self.mutex:
                    # Increment version and start new snapshot
                    self.version += 1
                    self.active_snapshot = True
                    self.acks = {child: False for child in self.children}
                    self.link_states = set()
                    self.record = {conn: True for conn in self.connections}
                    
                    # Save current state
                    self.loc_snap[self.version] = Snapshot(
                        state=State(self.process_state.money, self.process_state.last_action),
                        connection_states=set()
                    )
                    
                    self._print(f"Starting snapshot {self.version}")
                    
                    # Send INITIATE to children
                    for child in self.children:
                        message = ControlMessage(
                            message_from=self.identifier,
                            control_message_type=ControlMessageType.INIT_SNAP,
                            version=self.version
                        )
                        self._send_message(message, child)
                    
                    # Send MARKER to all neighbors
                    for neighbor in self.connections:
                        if neighbor not in self.children:
                            self._send_marker(neighbor)

    def _print(self, *args):
        print(f"[{self.identifier}] ", *args)

    def _send_message(self, message: Message, message_to: ProcessAddress):
        """Send a message to another process."""
        self._print(f"Sending message type:{message.MESSAGE_TYPE} to:{message_to}")

        if message_to not in self.outgoing_sockets:
            raise ConnectionError(f"No connection to {message_to}")

        sock = self.outgoing_sockets[message_to]
        try:
            data = message.serialise() + "\n"  # newline framing
            sock.sendall(data.encode(ENCODING))
            self._print("\tSent!")
        except Exception as e:
            self._print(f"\tFailed to send: {e}")
            # Remove the socket if it's broken
            self.outgoing_sockets.pop(message_to, None)
            raise

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
        """Executed when q receives a marker from a neighbor."""
        with self.mutex:
            if self.version < m.version:
                # Update version and start recording
                self.version = m.version
                self.active_snapshot = True
                self.link_states = set()
                self.record = {conn: True for conn in self.connections}
                
                # Save current state
                self.loc_snap[self.version] = Snapshot(
                    state=State(self.process_state.money, self.process_state.last_action),
                    connection_states=set()
                )
                
                # Send MARKER to all neighbors except sender
                for neighbor in self.connections:
                    if neighbor != r:
                        self._send_marker(neighbor)
                
            elif self.version == m.version:
                # Add recorded messages to link_states
                if c in self.state:
                    for msg in self.state[c]:
                        if isinstance(msg, ActionMessage):
                            self.link_states.add(State(msg.amount, None))
                
                # Stop recording on this channel
                self.record[c] = False
                
                # Send acknowledgment
                self._send_ack(r)
                
                # Check if snapshot is complete
                if all(not recording for recording in self.record.values()):
                    self._snapshot_complete()

    def _receive_initiate(self, q: ProcessAddress, r: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        """Executed when q receives an initiate message from r."""
        with self.mutex:
            if self.version < m.version:
                # Update version and start recording
                self.version = m.version
                self.active_snapshot = True
                self.link_states = set()
                self.record = {conn: True for conn in self.connections}
                
                # Save current state
                self.loc_snap[self.version] = Snapshot(
                    state=State(self.process_state.money, self.process_state.last_action),
                    connection_states=set()
                )
                
                # Initialize acknowledgments for children
                self.acks = {child: False for child in self.children}
                
                # Send INITIATE to children
                for child in self.children:
                    message = ControlMessage(
                        message_from=self.identifier,
                        control_message_type=ControlMessageType.INIT_SNAP,
                        version=m.version
                    )
                    self._send_message(message, child)
                
                # Send MARKER to all neighbors except parent
                for neighbor in self.connections:
                    if neighbor != r:
                        self._send_marker(neighbor)

    def _receive_snap_completed(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        """Executed when q receives a snap_completed message from its child."""
        with self.mutex:
            if self.version == m.version and r in self.acks:
                self.acks[r] = True
                
                # If all children have completed
                if all(self.acks.values()):
                    if self.primary:
                        # Primary node completes the snapshot
                        self.active_snapshot = False
                        self._print(f"Snapshot {self.version} completed")
                    else:
                        # Non-primary nodes notify their parent
                        self._send_snap_completed(self.parent)

    def _receive_ack(self, r: ProcessAddress, q: ProcessAddress, c: ProcessAddress, m: ControlMessage):
        """Executed when q receives an acknowledgment message."""
        with self.mutex:
            if self.version == m.version:
                # Stop recording on this channel
                self.record[r] = False
                
                # If all channels have stopped recording
                if all(not recording for recording in self.record.values()):
                    self._snapshot_complete()

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

    def _send_marker(self, dest: ProcessAddress):
        """Send a marker message to a destination."""
        message = ControlMessage(
            message_from=self.identifier,
            control_message_type=ControlMessageType.MARKER,
            version=self.version
        )
        self._send_message(message, dest)
        
    def _send_ack(self, dest: ProcessAddress):
        """Send an acknowledgment message."""
        message = ControlMessage(
            message_from=self.identifier,
            control_message_type=ControlMessageType.ACK,
            version=self.version
        )
        self._send_message(message, dest)

    def _snapshot_complete(self):
        """Called when a snapshot is complete."""
        # Save snapshot
        self.loc_snap[self.version] = Snapshot(
            state=State(self.process_state.money, self.process_state.last_action),
            connection_states=self.link_states
        )
        
        # Notify parent
        if self.parent:
            self._send_snap_completed(self.parent)
            
        # Reset for next snapshot
        self.active_snapshot = False
        self.link_states = set()
        
    def _send_snap_completed(self, dest: ProcessAddress):
        """Send a snapshot completion message."""
        message = ControlMessage(
            message_from=self.identifier,
            control_message_type=ControlMessageType.SNAP_COMPLETED,
            version=self.version
        )
        self._send_message(message, dest)

    def _receive_loop(self, sock: socket):
        """Message receiving loop for a socket."""
        buffer = ""
        while self.active:
            try:
                data = sock.recv(BUFFER_SIZE)
                if not data:
                    break
                
                buffer += data.decode(ENCODING)
                messages = buffer.split("\n")
                
                # Last element might be incomplete
                buffer = messages[-1]
                messages = messages[:-1]
                
                for message_str in messages:
                    if message_str:
                        message = MessageFactory.deserialise(message_str.encode(ENCODING))
                        self._receive_message(message, message.message_from)
                        
            except (ConnectionError, OSError):
                break
            
        sock.close()
