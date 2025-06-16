import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class PreShardRangeTest(unittest.TestCase):
    def test_more_ranges_than_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "d"), ("d", "g"), ("g", "j"), ("j", "m")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            try:
                keys = ["b", "e", "h", "k"]
                for i, k in enumerate(keys):
                    cluster.put(0, k, f"v{i}")
                time.sleep(0.2)
                for i, k in enumerate(keys):
                    node_index = i % len(cluster.nodes)
                    recs = cluster.nodes[node_index].client.get(k)
                    self.assertTrue(recs)
                    self.assertEqual(recs[0][0], f"v{i}")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
