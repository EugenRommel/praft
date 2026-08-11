import os
import shutil
import tempfile
import time
import unittest
from concurrent import futures

import grpc

import Client
import Node
import raft_pb2
import raft_pb2_grpc

CLUSTER_SIZE = 3
ELECTION_TIMEOUT = 20.0
REPLICATION_TIMEOUT = 10.0


class RaftCluster:
    def __init__(self, size=CLUSTER_SIZE):
        self._servers = []
        self.nodes = []
        for _ in range(size):
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
            port = server.add_insecure_port("[::]:0")
            self._servers.append((server, port))
        for i, (server, port) in enumerate(self._servers):
            node_id = str(i + 1)
            peers = [Node.NodeStatus(str(j + 1), "localhost", p)
                     for j, (_, p) in enumerate(self._servers) if j != i]
            node = Node.RaftServicer(node_id, peers)
            raft_pb2_grpc.add_RaftNodeServicer_to_server(node, server)
            server.start()
            self.nodes.append(node)

    def wait_for_leader(self, timeout=ELECTION_TIMEOUT):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for node in self.nodes:
                if node._role == Node.RaftServicer.LEADER:
                    return node
            time.sleep(0.1)
        return None

    def address(self, i):
        return "localhost", self._servers[i][1]

    def config(self):
        config = {"members": [str(i + 1) for i in range(len(self.nodes))]}
        for i in range(len(self.nodes)):
            config[str(i + 1)] = {"ip": "localhost", "port": self._servers[i][1]}
        return config

    def stop(self):
        for node in self.nodes:
            node._stop_heartbeat()
            if node._election_timer is not None:
                node._election_timer.cancel()
        for server, _ in self._servers:
            server.stop(0)


class SmokeTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._old_data_dir = Node.DATA_DIR
        Node.DATA_DIR = os.path.join(self._tmp, "data")
        os.makedirs(Node.DATA_DIR, exist_ok=True)

    def tearDown(self):
        Node.DATA_DIR = self._old_data_dir
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_election_and_replication(self):
        cluster = RaftCluster()
        try:
            leader = cluster.wait_for_leader()
            self.assertIsNotNone(leader, "no leader elected in time")

            entries = [
                Node.Entry(term=leader._cur_term, op="set", data="a:1"),
                Node.Entry(term=leader._cur_term, op="set", data="b:2"),
            ]
            leader._log_entries.extend(entries)
            leader.send_append_entries_to_all()

            deadline = time.monotonic() + REPLICATION_TIMEOUT
            while time.monotonic() < deadline:
                if all(len(n._log_entries) == len(entries)
                       for n in cluster.nodes):
                    break
                time.sleep(0.1)
            self.assertTrue(
                all(len(n._log_entries) == len(entries)
                    for n in cluster.nodes),
                "log did not replicate to all nodes")

            for node in cluster.nodes:
                self.assertEqual(len(node._log_entries), len(entries))
                for local, remote in zip(leader._log_entries, node._log_entries):
                    self.assertEqual(local.term, remote.term)
                    self.assertEqual(local.op, remote.op)
                    self.assertEqual(local.data, remote.data)

            deadline = time.monotonic() + REPLICATION_TIMEOUT
            while time.monotonic() < deadline:
                if leader._commit_index == len(entries) - 1:
                    break
                time.sleep(0.1)
            self.assertEqual(leader._commit_index, len(entries) - 1,
                             "leader did not commit replicated entries")
            self.assertEqual(leader._state.get("a"), "1")
            self.assertEqual(leader._state.get("b"), "2")

            deadline = time.monotonic() + REPLICATION_TIMEOUT
            while time.monotonic() < deadline:
                if all(n._commit_index == len(entries) - 1
                       for n in cluster.nodes):
                    break
                time.sleep(0.1)
            for node in cluster.nodes:
                self.assertEqual(node._commit_index, len(entries) - 1,
                                 "follower did not advance commit index")
                self.assertEqual(node._state.get("a"), "1")
                self.assertEqual(node._state.get("b"), "2")
        finally:
            cluster.stop()

    def test_client_submit_command(self):
        cluster = RaftCluster()
        try:
            leader = cluster.wait_for_leader()
            self.assertIsNotNone(leader, "no leader elected in time")
            config = cluster.config()

            host, port = cluster.address(0)
            resp = Client.submit_command(host, port, "set", "a:1", config,
                                         "<test>")
            self.assertTrue(resp.success,
                            f"command failed with response: {resp}")
            self.assertEqual(resp.value, "1")

            deadline = time.monotonic() + REPLICATION_TIMEOUT
            while time.monotonic() < deadline:
                if all(n._state.get("a") == "1" and
                       len(n._log_entries) == 1
                       for n in cluster.nodes):
                    break
                time.sleep(0.1)
            for node in cluster.nodes:
                self.assertEqual(len(node._log_entries), 1)
                self.assertEqual(node._log_entries[0].op, "set")
                self.assertEqual(node._log_entries[0].data, "a:1")
                self.assertEqual(node._state.get("a"), "1")
        finally:
            cluster.stop()


if __name__ == "__main__":
    unittest.main()
