import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class ColdPartitionMergeTest(unittest.TestCase):
    def test_merge_after_deletes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "g"), ("g", "n"), ("n", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            try:
                cluster.put(0, "h", "v1")
                cluster.delete(0, "h")
                cluster.put(0, "o", "v2")
                cluster.delete(0, "o")
                cluster.reset_metrics()
                for i in range(4):
                    cluster.put(0, "b", f"v{i}")
                cluster.check_cold_partitions(threshold=0.5, max_keys=1)
                time.sleep(0.2)
                self.assertEqual(cluster.num_partitions, 2)
            finally:
                cluster.shutdown()

    def test_merge_after_low_activity(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "g"), ("g", "n"), ("n", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            try:
                cluster.reset_metrics()
                for i in range(5):
                    cluster.put(0, "b", f"v{i}")
                cluster.put(0, "h", "v1")
                cluster.put(0, "o", "v1")
                cluster.check_cold_partitions(threshold=0.5, max_keys=1)
                time.sleep(0.2)
                self.assertEqual(cluster.num_partitions, 2)
                self.assertEqual(cluster.get(0, "h"), "v1")
                self.assertEqual(cluster.get(0, "o"), "v1")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
