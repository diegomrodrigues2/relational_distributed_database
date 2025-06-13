import os
import sys
import tempfile
import unittest
import multiprocessing
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster, ClusterNode
from replica.grpc_server import run_server
from replica.client import GRPCReplicaClient

class ReplicationManagerTest(unittest.TestCase):
    def test_basic_replication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=3)
            cluster.put(0, 'user:1', 'A')
            v0 = cluster.get(0, 'user:1')
            v1 = cluster.get(1, 'user:1')
            v2 = cluster.get(2, 'user:1')
            self.assertEqual(v0, 'A')
            self.assertEqual(v1, 'A')
            self.assertEqual(v2, 'A')
            cluster.shutdown()

    def test_concurrent_puts_last_timestamp_wins(self):
        """Concurrent writes should resolve by highest timestamp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2)

            t1 = multiprocessing.Process(
                target=cluster.nodes[0].client.put,
                args=("key", "v1"),
                kwargs={"timestamp": 1, "node_id": cluster.nodes[0].node_id},
            )
            t2 = multiprocessing.Process(
                target=cluster.nodes[1].client.put,
                args=("key", "v2"),
                kwargs={"timestamp": 2, "node_id": cluster.nodes[1].node_id},
            )
            t1.start(); t2.start(); t1.join(); t2.join()
            time.sleep(1)

            v0 = cluster.get(0, "key")
            v1 = cluster.get(1, "key")
            self.assertEqual(v0, "v2")
            self.assertEqual(v1, "v2")
            cluster.shutdown()

    def test_offline_node_eventual_convergence(self):
        """Node that was offline should converge after reconnection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2)

            # Take node 1 offline
            db_path = os.path.join(tmpdir, "node_1")
            cluster.nodes[1].stop()

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
            p.start(); time.sleep(0.5)
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
            cluster.shutdown()


if __name__ == '__main__':
    unittest.main()
