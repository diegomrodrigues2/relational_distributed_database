import os
import sys
import tempfile
import unittest
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class KeyRangePartitionTest(unittest.TestCase):
    def test_range_put_get(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "m"), ("m", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            try:
                key1 = "alpha"
                key2 = "monkey"

                cluster.put(0, key1, "v1")
                cluster.put(0, key2, "v2")
                time.sleep(0.2)

                recs0 = cluster.nodes[0].client.get(key1)
                recs1 = cluster.nodes[1].client.get(key1)
                self.assertTrue(recs0)
                self.assertFalse(recs1)
                self.assertEqual(recs0[0][0], "v1")

                recs1b = cluster.nodes[1].client.get(key2)
                recs0b = cluster.nodes[0].client.get(key2)
                self.assertTrue(recs1b)
                self.assertFalse(recs0b)
                self.assertEqual(recs1b[0][0], "v2")

                self.assertEqual(cluster.get(0, key1), "v1")
                self.assertEqual(cluster.get(1, key2), "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
