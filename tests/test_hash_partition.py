import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class HashPartitionTest(unittest.TestCase):
    def test_hash_put_get(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                key1 = "alpha"
                key2 = "bravo"

                cluster.put(0, key1, "v1")
                cluster.put(0, key2, "v2")
                time.sleep(0.2)

                pid1 = cluster.get_partition_id(key1)
                pid2 = cluster.get_partition_id(key2)

                owner_idx1 = int(cluster.partition_map[pid1].split("_")[1])
                owner_idx2 = int(cluster.partition_map[pid2].split("_")[1])

                for i, node in enumerate(cluster.nodes):
                    if i == owner_idx1:
                        self.assertTrue(node.client.get(key1))
                    else:
                        self.assertFalse(node.client.get(key1))
                for i, node in enumerate(cluster.nodes):
                    if i == owner_idx2:
                        self.assertTrue(node.client.get(key2))
                    else:
                        self.assertFalse(node.client.get(key2))

                self.assertEqual(cluster.get(1, key1), "v1")
                self.assertEqual(cluster.get(2, key2), "v2")
            finally:
                cluster.shutdown()

    def test_hash_distribution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                keys = [f"k{i}" for i in range(100)]
                for k in keys:
                    cluster.put(0, k, k)
                time.sleep(0.5)

                counts = [0] * 3
                for k in keys:
                    pid = cluster.get_partition_id(k)
                    node_idx = int(cluster.partition_map[pid].split("_")[1])
                    self.assertIsNotNone(cluster.nodes[node_idx].client.get(k))
                    counts[node_idx] += 1

                expected = len(keys) / len(counts)
                for c in counts:
                    self.assertTrue(abs(c - expected) <= 10)
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
