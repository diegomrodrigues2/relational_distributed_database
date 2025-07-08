import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class SplitPauseRebalanceTest(unittest.TestCase):
    def test_split_pause_and_rebalance(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "m"), ("m", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            try:
                keys = ["b", "h", "r", "x"]
                for k in keys:
                    cluster.put(0, k, f"v{k}")
                time.sleep(0.5)

                cluster.split_partition(0, "g")
                cluster.split_partition(2, "t")
                time.sleep(0.5)
                self.assertEqual(cluster.num_partitions, 4)

                cluster.stop_node("node_1")
                cluster.remove_node("node_1")
                time.sleep(0.5)

                for k in keys:
                    self.assertEqual(cluster.get(0, k), f"v{k}")

                cluster.add_node()
                time.sleep(1)

                for k in keys:
                    self.assertEqual(cluster.get(0, k), f"v{k}")
                    pid = cluster.get_partition_id(k)
                    owner = cluster.partition_map[pid]
                    items = cluster._load_node_items(cluster.nodes_by_id[owner])
                    self.assertIn(k, items)
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
