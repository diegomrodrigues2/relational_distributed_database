import os
import sys
import tempfile
import time
import unittest
import random

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class VirtualNodeRebalanceTest(unittest.TestCase):
    def _insert_keys(self, cluster, count=200):
        keys = []
        for i in range(count):
            k = f"key-{i}"
            cluster.put(0, k, f"v{i}")
            keys.append(k)
        time.sleep(0.5)
        return keys

    def _key_distribution(self, cluster, keys):
        mapping = {}
        pmap = cluster.get_partition_map()
        for k in keys:
            pid = cluster.get_partition_id(k)
            mapping[k] = pmap[pid]
        return mapping

    def test_add_node_virtual_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            random.seed(42)
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
                enable_forwarding=True,
                partitions_per_node=5,
            )
            try:
                keys = self._insert_keys(cluster, 300)
                before = self._key_distribution(cluster, keys)
                new = cluster.add_node()
                time.sleep(1)
                after = self._key_distribution(cluster, keys)
                moved = sum(1 for k in keys if before[k] != new.node_id and after[k] == new.node_id)
                ratio = moved / len(keys)
                expected = 1 / (len(cluster.nodes))  # after add node
                self.assertAlmostEqual(ratio, expected, delta=0.3)
            finally:
                cluster.shutdown()

    def test_remove_node_virtual_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            random.seed(42)
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=4,
                replication_factor=1,
                partition_strategy="hash",
                enable_forwarding=True,
                partitions_per_node=5,
            )
            try:
                keys = self._insert_keys(cluster, 300)
                before = self._key_distribution(cluster, keys)
                remove_id = cluster.nodes[1].node_id
                cluster.remove_node(remove_id)
                time.sleep(1)
                after = self._key_distribution(cluster, keys)
                moved = sum(1 for k in keys if before[k] == remove_id and after[k] != remove_id)
                ratio = moved / len(keys)
                expected = 1 / (len(cluster.nodes) + 1)  # before removal
                self.assertAlmostEqual(ratio, expected, delta=0.3)
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
