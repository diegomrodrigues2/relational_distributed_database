import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class SaltingTest(unittest.TestCase):
    def test_random_prefix_distribution_and_read(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            cluster.enable_salt("hot", buckets=3)
            try:
                for i in range(20):
                    cluster.put(0, "hot", f"v{i}")
                    time.sleep(0.001)
                time.sleep(0.5)

                used_partitions = set()
                for prefix in range(3):
                    salted = f"{prefix}#hot"
                    pid = cluster.get_partition_id(salted)
                    node = cluster.nodes[pid % len(cluster.nodes)]
                    if node.client.get(salted):
                        used_partitions.add(pid)
                self.assertGreater(len(used_partitions), 1)

                self.assertEqual(cluster.get(1, "hot"), "v19")
            finally:
                cluster.shutdown()

    def test_get_range_with_salted_key(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            cluster.enable_salt("hk", buckets=2)
            try:
                for ck, val in [("a", "va"), ("b", "vb"), ("c", "vc")]:
                    cluster.put(0, "hk", ck, val)
                    time.sleep(0.001)
                time.sleep(0.5)

                items = cluster.get_range("hk", "a", "c")
                self.assertEqual(items, [("a", "va"), ("b", "vb"), ("c", "vc")])
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
