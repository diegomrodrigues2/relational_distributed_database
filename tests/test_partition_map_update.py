import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class PartitionMapUpdateTest(unittest.TestCase):
    def test_reroute_after_add_node(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "m"), ("m", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            try:
                cluster.split_partition(0, "g")
                cluster.update_partition_map()
                time.sleep(0.5)
                key = "ga"
                old_owner = cluster.partitions[0][1]
                new_owner = cluster.partitions[1][1]
                with self.assertRaises(Exception):
                    old_owner.client.put(key, "v1")
                new_owner.client.put(key, "v1")
                time.sleep(0.5)
                self.assertTrue(new_owner.client.get(key))
                self.assertFalse(old_owner.client.get(key))
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
