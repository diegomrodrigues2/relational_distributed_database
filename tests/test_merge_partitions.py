import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class MergePartitionsTest(unittest.TestCase):
    def test_merge_partitions_moves_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "g"), ("g", "n"), ("n", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            try:
                cluster.put(0, "b", "v1")
                cluster.put(1, "h", "v2")
                time.sleep(0.5)

                left_node = cluster.partitions[0][1]
                right_node = cluster.partitions[1][1]
                self.assertTrue(left_node.client.get("b"))
                self.assertTrue(right_node.client.get("h"))

                cluster.merge_partitions(0, 1)
                time.sleep(0.5)

                self.assertEqual(cluster.num_partitions, 2)
                dest_node = cluster.partitions[0][1]
                other_node = cluster.partitions[1][1]

                self.assertTrue(dest_node.client.get("b"))
                self.assertTrue(dest_node.client.get("h"))
                if other_node is not dest_node:
                    self.assertFalse(other_node.client.get("b"))
                    self.assertFalse(other_node.client.get("h"))
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
