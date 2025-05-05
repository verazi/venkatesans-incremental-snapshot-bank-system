import socket
import threading
import time
import unittest
from bank_system.config import ProcessAddress, Action, ProcessConfig, Config
from bank_system.process import Process

def load_dummy_config():
    addr1 = ProcessAddress("127.0.0.1", 5001)
    addr2 = ProcessAddress("127.0.0.1", 5002)
    addr3 = ProcessAddress("127.0.0.1", 5003)

    action1 = Action(to=addr2, amount=100, delay=0)
    action2 = Action(to=addr3, amount=200, delay=0.1)

    pc1 = ProcessConfig(
        address=addr1,
        primary=True,
        connections=[addr2, addr3],
        initial_money=1000,
        action_list=[action1, action2]
    )

    pc2 = ProcessConfig(
        address=addr2,
        primary=False,
        connections=[addr1],
        initial_money=1000,
        action_list=[]
    )

    pc3 = ProcessConfig(
        address=addr3,
        primary=False,
        connections=[addr1],
        initial_money=1000,
        action_list=[]
    )

    return Config({addr1: pc1, addr2: pc2, addr3: pc3}), addr1, addr2, addr3


class TestProcessSenderOnly(unittest.TestCase):

    def test_only_sender_process(self):
        config, addr1, addr2, addr3 = load_dummy_config()
        received = []
        ready = threading.Event()
        ready_count = {"val": 0}
        recv_lock = threading.Lock()

        def mock_receiver(bind_addr, label):
            def recv_server():
                nonlocal ready_count
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((bind_addr.address, bind_addr.port))
                s.listen(1)
                with recv_lock:
                    ready_count["val"] += 1
                    if ready_count["val"] == 2:
                        ready.set()
                conn, _ = s.accept()
                data = conn.recv(1024).decode()
                received.append((label, data))
                conn.close()
                s.close()
            return recv_server

        t2 = threading.Thread(target=mock_receiver(addr2, "node2"), daemon=True)
        t3 = threading.Thread(target=mock_receiver(addr3, "node3"), daemon=True)
        t2.start()
        t3.start()

        self.assertTrue(ready.wait(timeout=2), "Mock receivers not ready")

        p1 = Process(config, addr1)
        p1.start()

        time.sleep(2)

        self.assertEqual(len(received), 2)
        self.assertEqual(received[0][0], "node2")
        self.assertEqual(received[1][0], "node3")
        print("\n[Test Passed] Sender process sent messages successfully.")


if __name__ == '__main__':
    unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(TestProcessSenderOnly))
