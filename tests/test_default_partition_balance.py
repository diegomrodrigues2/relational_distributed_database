import os
import sys
import tempfile
import time
import unittest
from collections import Counter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class DefaultPartitionBalanceTest(unittest.TestCase):
    def _fill_partitions(self, cluster):
        keys = {}
        i = 0
        while len(keys) < cluster.num_partitions:
            key = f"k{i}"
            pid = cluster.get_partition_id(key)
            if pid not in keys:
                keys[pid] = key
            i += 1
        for pid, k in keys.items():
            cluster.put(0, k, f"v{pid}")
        return keys

    def test_add_and_remove_node_balance(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                self.assertEqual(cluster.num_partitions, 128)

                keys = self._fill_partitions(cluster)
                time.sleep(1)

                cluster.add_node()
                time.sleep(1)
                counts = Counter(cluster.partition_map.values())
                avg = cluster.num_partitions / len(cluster.nodes)
                for cnt in counts.values():
                    self.assertLessEqual(abs(cnt - avg), 1)

                cluster.remove_node("node_0")
                time.sleep(1)
                counts = Counter(cluster.partition_map.values())
                avg = cluster.num_partitions / len(cluster.nodes)
                for cnt in counts.values():
                    self.assertLessEqual(abs(cnt - avg), 1)

                for pid, k in keys.items():
                    val = cluster.get(0, k)
                    self.assertEqual(val, f"v{pid}")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
