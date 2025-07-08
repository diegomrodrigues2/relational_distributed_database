import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class MergeSplitRebalanceTest(unittest.TestCase):
    def test_merge_split_rebalance(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "g"), ("g", "n"), ("n", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            try:
                # Insert keys in each initial range partition
                cluster.put(0, "b", "v0")
                cluster.put(0, "h", "v1")
                cluster.put(0, "p", "v2")
                time.sleep(0.5)

                # Split first partition and merge the other two
                cluster.split_partition(0, "d")
                time.sleep(0.5)
                cluster.merge_partitions(2, 3)
                time.sleep(0.5)

                # Add a new node and allow rebalance
                new_node = cluster.add_node()
                # Ensure range partitioner assigns the new node
                if hasattr(cluster, "partitioner") and hasattr(cluster.partitioner, "add_node"):
                    cluster.partitioner.add_node(new_node)
                    cluster.partitions = cluster.partitioner.partitions
                    cluster.partition_map = cluster.partitioner.get_partition_map()
                    cluster.update_partition_map()
                time.sleep(1)

                self.assertEqual(cluster.num_partitions, 3)
                self.assertEqual(cluster.get(0, "b"), "v0")
                self.assertEqual(cluster.get(0, "h"), "v1")
                self.assertEqual(cluster.get(0, "p"), "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
