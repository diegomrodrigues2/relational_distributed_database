import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class AutoSplitRangeTest(unittest.TestCase):
    def test_auto_split_range_partition(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "m"), ("m", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            try:
                cluster.reset_metrics()
                for i in range(5):
                    cluster.put(0, "a", f"v{i}")
                    cluster.put(0, "b", f"v{i}")
                cluster.check_hot_partitions(threshold=1.0)
                cluster.put(0, "ga", "val")
                time.sleep(0.2)
                self.assertEqual(cluster.num_partitions, 3)
                node_new = cluster.partitions[1][1]
                node_old = cluster.partitions[0][1]
                self.assertTrue(node_new.client.get("ga"))
                self.assertFalse(node_old.client.get("ga"))
            finally:
                cluster.shutdown()


class AutoSplitHashTest(unittest.TestCase):
    def test_auto_split_hash_partition(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
                num_partitions=2,
            )
            try:
                cluster.reset_metrics()
                for i in range(3):
                    cluster.put(0, "a", f"v{i}")
                    cluster.put(0, "b", f"v{i}")
                cluster.check_hot_partitions(threshold=1.0)
                self.assertEqual(cluster.num_partitions, 3)
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
