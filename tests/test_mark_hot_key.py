import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class MarkHotKeyTest(unittest.TestCase):
    def test_mark_without_migration(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                cluster.put(0, "hk", "v0")
                cluster.mark_hot_key("hk", buckets=2, migrate=False)
                # old value should be accessible only ignoring salt
                self.assertIsNone(cluster.get(1, "hk"))
                self.assertEqual(cluster.get(1, "hk", ignore_salt=True), "v0")

                cluster.put(0, "hk", "v1")
                self.assertEqual(cluster.get(1, "hk"), "v1")
            finally:
                cluster.shutdown()

    def test_mark_with_migration(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                cluster.put(0, "hot", "orig")
                time.sleep(0.1)
                cluster.mark_hot_key("hot", buckets=3, migrate=True)
                self.assertEqual(cluster.get(1, "hot"), "orig")
                for i in range(3):
                    salted = f"{i}#hot"
                    pid = cluster.get_partition_id(salted)
                    node = cluster.nodes[pid % len(cluster.nodes)]
                    recs = node.client.get(salted)
                    self.assertTrue(recs and recs[0][0] == "orig")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
