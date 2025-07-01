import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster
from database.clustering.partitioning import compose_key

class HotspotMetricsTest(unittest.TestCase):
    def test_metrics_increment(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
                num_partitions=2,
            )
            try:
                cluster.reset_metrics()
                cluster.put(0, "k", "c", "v")
                cluster.get(1, "k", "c")
                cluster.delete(0, "k", "c")

                comp = compose_key("k", "c")
                pid = cluster.get_partition_id("k", "c")
                self.assertEqual(cluster.partition_ops[pid], 3)
                self.assertEqual(cluster.key_freq.get(comp), 3)
            finally:
                cluster.shutdown()

    def test_load_balanced_reads_hit_multiple_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=3,
                read_quorum=1,
                load_balance_reads=True,
            )
            try:
                cluster.put(0, "lb", "x", "v")
                counts = {}
                for n in cluster.nodes:
                    orig = n.client.get

                    def wrap(key, _orig=orig, nid=n.node_id):
                        counts[nid] = counts.get(nid, 0) + 1
                        return _orig(key)

                    n.client.get = wrap

                for _ in range(10):
                    cluster.get(0, "lb", "x")

                hit_nodes = [nid for nid, c in counts.items() if c > 0]
                self.assertGreaterEqual(len(hit_nodes), 2)
            finally:
                cluster.shutdown()

    def test_hot_partition_and_key_detection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
                num_partitions=4,
            )
            try:
                cluster.reset_metrics()
                # heavy load on one key
                for _ in range(10):
                    cluster.put(0, "hot", "x", "v")
                    cluster.get(1, "hot", "x")
                # small load on another key
                cluster.put(0, "cold", "y", "z")
                cluster.get(1, "cold", "y")

                hot_pid = cluster.get_partition_id("hot", "x")
                hot_parts = cluster.get_hot_partitions()
                self.assertIn(hot_pid, hot_parts)

                hot_key = compose_key("hot", "x")
                self.assertEqual(cluster.get_hot_keys(1)[0], hot_key)
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
