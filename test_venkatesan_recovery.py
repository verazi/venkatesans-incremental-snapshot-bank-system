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
     |     |
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
            Action(addr2, 10, 1),  # Larger transfers for easier tracking
        ],
        parent=None,
        children=[addr2, addr3]
    )

    # Process 2 (Will be crashed and recovered)
    pc2 = ProcessConfig(
        address=addr2,
        primary=False,
        connections=[addr1, addr4],
        initial_money=200,
        action_list=[
            Action(addr4, 20, 1),
        ],
        parent=addr1,
        children=[]
    )

    # Process 3
    pc3 = ProcessConfig(
        address=addr3,
        primary=False,
        connections=[addr1, addr4],
        initial_money=300,
        action_list=[
            Action(addr4, 30, 2),
        ],
        parent=addr1,
        children=[]
    )

    # Process 4
    pc4 = ProcessConfig(
        address=addr4,
        primary=False,
        connections=[addr2, addr3],
        initial_money=400,
        action_list=[],  # No actions, just receives money
        parent=addr3,
        children=[]
    )

    return Config({addr1: pc1, addr2: pc2, addr3: pc3, addr4: pc4}), addr1, addr2, addr3, addr4

class TestVenkatesanRecovery(unittest.TestCase):
    def setUp(self):
        self.config, self.addr1, self.addr2, self.addr3, self.addr4 = create_test_config()
        self.processes = []
        self.threads = []
        print("\n=== System Configuration ===")
        print("Initial Money Distribution:")
        print("P1 (Primary): 100")
        print("P2: 200 (Will be crashed)")
        print("P3: 300")
        print("P4: 400")

    def start_process(self, addr):
        process = Process(self.config, addr)
        thread = threading.Thread(target=process.start)
        thread.daemon = True
        self.processes.append(process)
        self.threads.append(thread)
        thread.start()
        print(f"Started process {addr}")
        return process

    def test_process_recovery(self):
        """Test process recovery using snapshots"""
        print("\n=== Starting Processes ===")
        
        # Start processes in order
        p3 = self.start_process(self.addr3)
        time.sleep(1)
        p4 = self.start_process(self.addr4)
        time.sleep(1)
        p1 = self.start_process(self.addr1)
        time.sleep(1)
        p2 = self.start_process(self.addr2)
        
        # Wait for initial connections
        time.sleep(3)
        print("\n=== Initial System State ===")
        self._print_state(p1, p2, p3, p4)

        # Wait for first transfers and snapshot
        time.sleep(2)
        print("\n=== After First Transfers ===")
        self._print_state(p1, p2, p3, p4)
        
        # Store P2's snapshot for recovery
        p2_snapshot = p2.loc_snap.get(p2.version)
        self.assertIsNotNone(p2_snapshot, "Snapshot should exist for P2")
        p2_money = p2.process_state.money
        print(f"\nP2 snapshot state: Money={p2_money}")

        # Simulate P2 crash
        print("\n=== Crashing P2 ===")
        p2.active = False
        time.sleep(2)
        
        # Start new P2 with recovered state
        print("\n=== Recovering P2 ===")
        new_p2 = Process(self.config, self.addr2)
        if p2_snapshot:
            # Restore complete state
            new_p2.process_state = p2_snapshot.state
            new_p2.version = p2.version
            new_p2.link_states = p2_snapshot.connection_states
            new_p2.loc_snap = {p2.version: p2_snapshot}
            # Clear action list to prevent re-execution
            new_p2.actions = []
        
        thread = threading.Thread(target=new_p2.start)
        thread.daemon = True
        thread.start()
        self.processes.append(new_p2)
        
        # Wait for recovery and connections
        time.sleep(3)
        print("\n=== After Recovery ===")
        self._print_state(p1, new_p2, p3, p4)

        # Verify recovery
        self.assertEqual(new_p2.version, p1.version, "Recovered P2 should have same version as P1")
        self.assertEqual(new_p2.process_state.money, p2_money, "Recovered P2 should have correct money")

    def _print_state(self, p1, p2, p3, p4):
        """Print the current state of all processes."""
        print("\nProcess States:")
        print(f"P1: Money={p1.process_state.money}, Version={p1.version}")
        print(f"P2: Money={p2.process_state.money}, Version={p2.version}")
        print(f"P3: Money={p3.process_state.money}, Version={p3.version}")
        print(f"P4: Money={p4.process_state.money}, Version={p4.version}")

    def tearDown(self):
        """Clean up processes and sockets."""
        print("\n=== Cleanup ===")
        for process in self.processes:
            process.active = False
            for sock in list(process.incoming_sockets.values()):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                    sock.close()
                except:
                    pass
            for sock in list(process.outgoing_sockets.values()):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                    sock.close()
                except:
                    pass
        print("Cleanup completed")

if __name__ == '__main__':
    unittest.main() 