# test for TCP connection
import json
import threading
import socket
import time
import unittest
from types import SimpleNamespace

from bank_system.config import ProcessAddress
from bank_system.process import Process

TEST_CONFIG = 'test_config.json'

# config.py
ProcessAddress.__hash__ = lambda self: hash((self.address, self.port)) # hashable

def load_dummy_config():
    """
    read test_config.json and create a dummy config

    return
      - conf: .processes attribute, key: ProcessAddress; value: stub config
      - addr1, addr2: ProcessAddress instances
    """

    with open(TEST_CONFIG, 'r') as f:
        raw = json.load(f)

    # only for test: address → ProcessAddress map manually / deserialise in config
    addr_map = {
        'node1': ProcessAddress('127.0.0.1', 8001),
        'node2': ProcessAddress('127.0.0.1', 8002),
    }

    conf = SimpleNamespace(processes={})
    for name, entry in raw['nodes'].items():
        addr = addr_map[name]
        peers = [ProcessAddress(c['address'], c['port']) for c in entry['connections']]
        conf.processes[addr] = SimpleNamespace(
            primary=entry['primary'],
            action_list=entry['action_list'],
            connections=peers
        )

    return conf, addr_map['node1'], addr_map['node2']


class TestProcessStart(unittest.TestCase):
    def test_start_can_connect_to_peer(self):
        conf, addr1, addr2 = load_dummy_config()

        # start TCP server to accept node1 connection
        server_ready = threading.Event()

        def peer_server():
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((addr2.address, addr2.port))
            srv.listen(1)
            server_ready.set()
            conn, _ = srv.accept()
            conn.close()
            srv.close()

        t = threading.Thread(target=peer_server, daemon=True)
        t.start()

        # waiting server bind/listen
        self.assertTrue(server_ready.wait(timeout=2), "Peer server not ready")

        # create Process 1 and call start()
        p1 = Process(conf, addr1)
        p1.start()

        # check connections dict has socket is None
        sock = p1.connections.get(addr2)
        self.assertIsNotNone(sock, "Connection to peer was not established")

        # check socket is still open
        try:
            sock.send(b'') # zero-byte probe
        except BrokenPipeError:
            self.fail("Socket was closed unexpectedly")

        # close socket
        sock.close()
        p1.incoming_socket.close()

if __name__ == '__main__':
    time.sleep(0.1)
    unittest.main(verbosity=2)
