import os
import sys
import tempfile
import unittest
import multiprocessing
import time
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster, ClusterNode
from replica.grpc_server import run_server
from replica.client import GRPCReplicaClient


class ReplicationManagerTest(unittest.TestCase):
    def test_basic_replication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=3)
            try:
                cluster.put(0, "user:1", "A")
                v0 = cluster.get(0, "user:1")
                v1 = cluster.get(1, "user:1")
                v2 = cluster.get(2, "user:1")
                self.assertEqual(v0, "A")
                self.assertEqual(v1, "A")
                self.assertEqual(v2, "A")
            finally:
                cluster.shutdown()

    def test_duplicate_op_id_applied_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2)
            try:
                ts = int(time.time() * 1000)
                op_id = "node_0:1"
                # Send same op_id twice directly to node 1
                cluster.nodes[1].client.put(
                    "dup", "v1", timestamp=ts, node_id="node_0", op_id=op_id
                )
                time.sleep(0.2)
                cluster.nodes[1].client.put(
                    "dup", "v2", timestamp=ts + 1, node_id="node_0", op_id=op_id
                )
                time.sleep(0.5)

                v1 = cluster.get(1, "dup")
                self.assertEqual(v1, "v1")
            finally:
                cluster.shutdown()

    def test_concurrent_puts_last_timestamp_wins(self):
        """Concurrent writes should resolve by highest timestamp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2)

            try:
                coord_id = cluster.ring.get_preference_list("key", 1)[0]
                coord = cluster.nodes_by_id[coord_id]
                t1 = multiprocessing.Process(
                    target=coord.client.put,
                    args=("key", "v1"),
                    kwargs={"timestamp": 1, "node_id": coord.node_id},
                )
                t2 = multiprocessing.Process(
                    target=coord.client.put,
                    args=("key", "v2"),
                    kwargs={"timestamp": 2, "node_id": coord.node_id},
                )
                t1.start()
                t2.start()
                t1.join()
                t2.join()
                time.sleep(1)

                v0 = cluster.get(0, "key")
                v1 = cluster.get(1, "key")
                self.assertEqual(v0, "v2")
                self.assertEqual(v1, "v2")
            finally:
                cluster.shutdown()

    def test_offline_node_eventual_convergence(self):
        """Node that was offline should converge after reconnection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2)
            try:
                # Take node 1 offline
                db_path = os.path.join(tmpdir, "node_1")
                cluster.nodes[1].stop()

                with self.assertRaises(Exception):
                    cluster.nodes[0].client.put(
                        "key", "offline", timestamp=1, node_id=cluster.nodes[0].node_id
                    )
                time.sleep(0.5)

                # Restart node 1
                peers = [("localhost", 9000), ("localhost", 9001)]
                p = multiprocessing.Process(
                    target=run_server,
                    args=(db_path, "localhost", 9001, "node_1", peers),
                    daemon=True,
                )
                p.start()
                time.sleep(0.5)
                client = GRPCReplicaClient("localhost", 9001)
                cluster.nodes[1] = ClusterNode("node_1", "localhost", 9001, p, client)

                # New write with higher timestamp should propagate
                cluster.nodes[0].client.put(
                    "key", "online", timestamp=5, node_id=cluster.nodes[0].node_id
                )
                time.sleep(1)

                v0 = cluster.get(0, "key")
                v1 = cluster.get(1, "key")
                self.assertEqual(v0, "online")
                self.assertEqual(v1, "online")
            finally:
                cluster.shutdown()

    def test_offline_peer_replays_replication_log(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2)
            try:
                db_path = os.path.join(tmpdir, "node_1")
                cluster.nodes[1].stop()

                with self.assertRaises(Exception):
                    cluster.nodes[0].client.put(
                        "k", "v1", timestamp=1, node_id=cluster.nodes[0].node_id
                    )
                time.sleep(0.5)

                peers = [("localhost", 9000), ("localhost", 9001)]
                p = multiprocessing.Process(
                    target=run_server,
                    args=(db_path, "localhost", 9001, "node_1", peers),
                    daemon=True,
                )
                p.start()
                time.sleep(0.5)
                client = GRPCReplicaClient("localhost", 9001)
                cluster.nodes[1] = ClusterNode("node_1", "localhost", 9001, p, client)

                time.sleep(1.5)

                v1 = cluster.get(1, "k")
                self.assertEqual(v1, "v1")
            finally:
                cluster.shutdown()

    def test_hinted_handoff(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2)
            try:
                db_path = os.path.join(tmpdir, "node_1")
                cluster.nodes[1].stop()

                with self.assertRaises(Exception):
                    cluster.nodes[0].client.put(
                        "hint", "v1", timestamp=1, node_id=cluster.nodes[0].node_id
                    )
                time.sleep(0.5)

                hints_file = os.path.join(tmpdir, "node_0", "hints.json")
                with open(hints_file, "r", encoding="utf-8") as f:
                    hints = json.load(f)
                self.assertTrue(any(hints.values()))

                peers = [("localhost", 9000), ("localhost", 9001)]
                p = multiprocessing.Process(
                    target=run_server,
                    args=(db_path, "localhost", 9001, "node_1", peers),
                    daemon=True,
                )
                p.start()
                time.sleep(1)
                client = GRPCReplicaClient("localhost", 9001)
                cluster.nodes[1] = ClusterNode("node_1", "localhost", 9001, p, client)

                time.sleep(1.5)

                with open(hints_file, "r", encoding="utf-8") as f:
                    hints_after = json.load(f)
                self.assertFalse(any(hints_after.values()))

                v1 = cluster.get(1, "hint")
                self.assertEqual(v1, "v1")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
