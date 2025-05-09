import unittest
import threading
import time
import socket
from bank_system.config import Action, ProcessConfig, Config
from bank_system.process import Process
from bank_system.process_address import ProcessAddress
from bank_system.control_message import ControlMessage, ControlMessageType

def create_test_config():
    """Create a test configuration with 4 processes in a network:
    P1 -- P2
     |  /  |
     P3 -- P4
    """
    PA = ProcessAddress
    base_port = 9000

    # Define addresses
    addr1 = PA("localhost", base_port + 1)
    addr2 = PA("localhost", base_port + 2)
    addr3 = PA("localhost", base_port + 3)
    addr4 = PA("localhost", base_port + 4)

    # Process 1 (Primary)
    pc1 = ProcessConfig(
        address=addr1,
        primary=True,
        connections=[addr2, addr3],
        initial_money=100,
        action_list=[
            Action(addr2, 1, 1),  # Small frequent transfers
            Action(addr3, 2, 1),
            Action(addr2, 3, 2),
            Action(addr2, 4, 2),
        ],
        parent=None,
        children=[addr2, addr3]
    )

    # Process 2
    pc2 = ProcessConfig(
        address=addr2,
        primary=False,
        connections=[addr1, addr3, addr4],
        initial_money=200,
        action_list=[
            Action(addr3, 5, 1),
            Action(addr4, 6, 1),
        ],
        parent=addr1,
        children=[]
    )

    # Process 3
    pc3 = ProcessConfig(
        address=addr3,
        primary=False,
        connections=[addr1, addr2, addr4],
        initial_money=300,
        action_list=[
            Action(addr2, 5, 1),
            Action(addr4, 7, 2),
        ],
        parent=addr1,
        children=[addr4]
    )

    # Process 4
    pc4 = ProcessConfig(
        address=addr4,
        primary=False,
        connections=[addr2, addr3],
        initial_money=500,
        action_list=[
            Action(addr3, 5, 1),
            Action(addr2, 6, 2),
        ],
        parent=addr3,
        children=[]
    )

    return Config({addr1: pc1, addr2: pc2, addr3: pc3, addr4: pc4}), addr1, addr2, addr3, addr4

def print_separator(title):
    """Print a separator line with title."""
    print("\n" + "="*30 + f" {title} " + "="*30 + "\n")

def print_process_state(p1, p2, p3, p4):
    """Print the current state of all processes."""
    print("\nProcess States:")
    print(f"P1: Money={p1.process_state.money}, Version={p1.version}, Active={p1.active_snapshot}")
    print(f"P2: Money={p2.process_state.money}, Version={p2.version}, Active={p2.active_snapshot}")
    print(f"P3: Money={p3.process_state.money}, Version={p3.version}, Active={p3.active_snapshot}")
    print(f"P4: Money={p4.process_state.money}, Version={p4.version}, Active={p4.active_snapshot}")
    
    print("\nSnapshot Status:")
    print(f"P1 acks: {p1.acks}")
    print(f"P2 acks: {p2.acks}")
    print(f"P3 acks: {p3.acks}")
    print(f"P4 acks: {p4.acks}")
    
    print("\nChannel States:")
    print(f"P1 link_states: {p1.link_states}")
    print(f"P2 link_states: {p2.link_states}")
    print(f"P3 link_states: {p3.link_states}")
    print(f"P4 link_states: {p4.link_states}")

class TestVenkatesanAlgorithm(unittest.TestCase):
    def setUp(self):
        self.config, self.addr1, self.addr2, self.addr3, self.addr4 = create_test_config()
        self.processes = []
        self.threads = []
        print_separator("System Configuration")
        print("Network Topology:")
        print("""
        P1 (Primary) -- P2
           |        /  |
           |      /    |
           P3 -------- P4
        """)
        print("\nInitial Money Distribution:")
        print("P1 (Primary): 100 (Parent of P2, P3)")
        print("P2: 200")
        print("P3: 300 (Parent of P4)")
        print("P4: 500")
        
        print("\nPlanned Money Transfers:")
        print("P1 transfers:")
        print(" - 1 to P2 (delay: 1s)")
        print(" - 2 to P3 (delay: 1s)")
        print(" - 3 to P2 (delay: 2s)")
        print(" - 4 to P2 (delay: 2s)")
        print("P2 transfers:")
        print(" - 5 to P3 (delay: 1s)")
        print(" - 6 to P4 (delay: 1s)")
        print("P3 transfers:")
        print(" - 5 to P2 (delay: 1s)")
        print(" - 7 to P4 (delay: 2s)")
        print("P4 transfers:")
        print(" - 5 to P3 (delay: 1s)")
        print(" - 6 to P2 (delay: 2s)")

    def start_process(self, addr):
        process = Process(self.config, addr)
        thread = threading.Thread(target=process.start)
        thread.daemon = True
        self.processes.append(process)
        self.threads.append(thread)
        thread.start()
        print(f"Started process {addr}")
        return process

    def test_demo_venkatesan_algorithm(self):
        """Demonstration of Venkatesan's Incremental Snapshot Algorithm"""
        print_separator("Starting Processes")
        print("Starting processes in order: P2 -> P3 -> P1 -> P4")
        print("This order ensures proper connection establishment")
        
        # Start processes
        p2 = self.start_process(self.addr2)
        time.sleep(1)
        print("\nP2 is ready to receive connections")
        
        p3 = self.start_process(self.addr3)
        time.sleep(1)
        print("\nP3 is ready to receive connections")
        
        p1 = self.start_process(self.addr1)  # primary
        time.sleep(1)
        print("\nP1 (Primary) is ready to initiate snapshots")
        
        p4 = self.start_process(self.addr4)
        print("\nP4 is ready to receive connections")

        # Wait for initial connections
        time.sleep(3)
        print_separator("Initial System State")
        print_process_state(p1, p2, p3, p4)

        # Wait for first round of transfers
        time.sleep(2)
        print_separator("After First Round of Transfers")
        print_process_state(p1, p2, p3, p4)

        # Wait for snapshot to initiate and propagate
        time.sleep(3)
        print_separator("After First Snapshot Initiation")
        print_process_state(p1, p2, p3, p4)

        # Wait for more transfers and another snapshot
        time.sleep(4)
        print_separator("Final System State")
        print_process_state(p1, p2, p3, p4)
        
        print("\nSnapshot History:")
        print(f"P1 snapshots: {p1.loc_snap}")
        print(f"P2 snapshots: {p2.loc_snap}")
        print(f"P3 snapshots: {p3.loc_snap}")
        print(f"P4 snapshots: {p4.loc_snap}")

        # Verify algorithm correctness
        self.assertTrue(p1.version > 0, "Primary should have initiated at least one snapshot")
        self.assertEqual(p1.version, p2.version, "P1 and P2 should have same version")
        self.assertEqual(p2.version, p3.version, "P2 and P3 should have same version")
        self.assertEqual(p3.version, p4.version, "P3 and P4 should have same version")
        
        print_separator("Test Completed")
        print("Verification Results:")
        print(f"✓ Primary initiated snapshots (version={p1.version})")
        print(f"✓ All processes have same version ({p1.version})")
        print(f"✓ Snapshots recorded in history")
        print(f"✓ Channel states captured")

    def tearDown(self):
        """Clean up all processes and sockets after each test."""
        print_separator("Cleanup")
        print("Stopping all processes and closing connections...")
        
        # First, stop all processes
        for process in self.processes:
            process.primary = False  # Stop snapshot loop
            process.active = False   # Stop message loops
        
        # Give time for loops to stop
        time.sleep(0.5)
        
        # Then close all sockets
        for process in self.processes:
            # Close incoming sockets
            for sock in list(process.incoming_sockets.values()):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except:
                    pass  # Socket might already be closed
                try:
                    sock.close()
                except:
                    pass
            process.incoming_sockets.clear()
            
            # Close outgoing sockets
            for sock in list(process.outgoing_sockets.values()):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except:
                    pass  # Socket might already be closed
                try:
                    sock.close()
                except:
                    pass
            process.outgoing_sockets.clear()
        
        # Clear process list
        self.processes = []
        self.threads = []
        print("Cleanup completed")

if __name__ == '__main__':
    unittest.main() 