import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster
from partitioning import compose_key


class HashPartitionAPITest(unittest.TestCase):
    def test_partition_keys_map_to_different_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                cluster.put(0, "alpha", "a", "v1")
                cluster.put(0, "bravo", "a", "v2")
                time.sleep(0.2)

                pid1 = cluster.get_partition_id("alpha")
                pid2 = cluster.get_partition_id("bravo")
                self.assertNotEqual(pid1, pid2)

                k1 = compose_key("alpha", "a")
                k2 = compose_key("bravo", "a")
                self.assertTrue(cluster.nodes[pid1].client.get(k1))
                self.assertFalse(cluster.nodes[pid1].client.get(k2))
                self.assertTrue(cluster.nodes[pid2].client.get(k2))
                self.assertFalse(cluster.nodes[pid2].client.get(k1))
            finally:
                cluster.shutdown()


class GetRangeTest(unittest.TestCase):
    def test_get_range_orders_and_filters_partition(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                cluster.put(0, "alpha", "c", "vc")
                cluster.put(0, "alpha", "a", "va")
                cluster.put(0, "alpha", "b", "vb")
                cluster.put(0, "bravo", "a", "other")
                time.sleep(0.2)

                pid = cluster.get_partition_id("alpha")
                k = compose_key("alpha", "a")
                self.assertTrue(cluster.nodes[pid].client.get(k))
                for i, n in enumerate(cluster.nodes):
                    if i != pid:
                        self.assertFalse(n.client.get(k))

                items = cluster.get_range("alpha", "a", "c")
                self.assertEqual(items, [("a", "va"), ("b", "vb"), ("c", "vc")])
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
