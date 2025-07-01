import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class CheckHotKeysTest(unittest.TestCase):
    def test_auto_mark_after_threshold(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                for i in range(4):
                    cluster.put(0, "thk", f"v{i}")
                cluster.check_hot_keys(threshold=5, buckets=2)
                self.assertNotIn("thk", cluster.salted_keys)

                cluster.put(0, "thk", "v4")
                cluster.check_hot_keys(threshold=5, buckets=2)
                self.assertIn("thk", cluster.salted_keys)

                cluster.put(0, "thk", "v5")
                self.assertEqual(cluster.get(1, "thk"), "v5")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
