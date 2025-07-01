import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class SplitOnWriteTest(unittest.TestCase):
    def test_split_partition_routes_new_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "m"), ("m", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            try:
                cluster.split_partition(0, "g")
                cluster.put(0, "ga", "val")
                time.sleep(0.2)
                self.assertEqual(cluster.num_partitions, 3)
                node_new = cluster.partitions[1][1]
                node_old = cluster.partitions[0][1]
                self.assertTrue(node_new.client.get("ga"))
                self.assertFalse(node_old.client.get("ga"))
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
