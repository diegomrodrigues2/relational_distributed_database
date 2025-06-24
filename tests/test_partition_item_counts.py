import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class PartitionItemCountTest(unittest.TestCase):
    def test_counts_increment_and_decrement(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=1,
                replication_factor=1,
                partition_strategy="hash",
                num_partitions=2,
            )
            try:
                cluster.reset_metrics()
                cluster.put(0, "a", "v1")
                cluster.put(0, "a", "v2")
                cluster.put(0, "b", "v3")
                counts = cluster.get_partition_item_counts()
                self.assertEqual(sum(counts.values()), 2)
                cluster.delete(0, "a")
                counts = cluster.get_partition_item_counts()
                self.assertEqual(sum(counts.values()), 1)
            finally:
                cluster.shutdown()

    def test_reset_metrics_clears_counts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=1,
                replication_factor=1,
                partition_strategy="hash",
                num_partitions=2,
            )
            try:
                cluster.put(0, "a", "v1")
                cluster.reset_metrics()
                counts = cluster.get_partition_item_counts()
                self.assertTrue(all(v == 0 for v in counts.values()))
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
