import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class HashRingUpdateRPCTest(unittest.TestCase):
    def _assert_distribution(self, cluster, key, expected_nodes):
        for node in cluster.nodes:
            present = bool(node.client.get(key))
            if node.node_id in expected_nodes:
                self.assertTrue(present)

    def test_add_node_updates_ring(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, replication_factor=2)
            try:
                cluster.add_node()
                time.sleep(1)
                key = "key-add"
                expected = cluster.partitioner.ring.get_preference_list(key, 2)
                cluster.put(0, key, "v1")
                time.sleep(0.5)
                self._assert_distribution(cluster, key, expected)
            finally:
                cluster.shutdown()

    def test_remove_node_updates_ring(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=3, replication_factor=2)
            try:
                cluster.remove_node("node_1")
                time.sleep(1)
                key = "key-rem"
                expected = cluster.partitioner.ring.get_preference_list(key, 2)
                cluster.put(0, key, "v1")
                time.sleep(0.5)
                self._assert_distribution(cluster, key, expected)
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
