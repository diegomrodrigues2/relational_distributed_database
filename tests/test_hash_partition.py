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

                self.assertTrue(cluster.nodes[pid1].client.get(key1))
                self.assertFalse(cluster.nodes[pid1].client.get(key2))
                self.assertTrue(cluster.nodes[pid2].client.get(key2))
                self.assertFalse(cluster.nodes[pid2].client.get(key1))

                self.assertEqual(cluster.get(1, key1), "v1")
                self.assertEqual(cluster.get(2, key2), "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
