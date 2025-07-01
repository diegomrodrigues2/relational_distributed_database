import os
import sys
import unittest
import random
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.clustering.partitioning import ConsistentHashPartitioner


class ConsistentHashPartitionerTest(unittest.TestCase):
    def test_uniform_distribution(self):
        random.seed(42)
        nodes = [SimpleNamespace(node_id=f"n{i}") for i in range(3)]
        part = ConsistentHashPartitioner()
        for n in nodes:
            part.add_node(n, weight=10)

        keys = [f"k{i}" for i in range(300)]
        counts = {n.node_id: 0 for n in nodes}
        for k in keys:
            pid = part.get_partition_id(k)
            nid = part.get_partition_map()[pid]
            counts[nid] += 1

        expected = len(keys) / len(nodes)
        for c in counts.values():
            self.assertLessEqual(abs(c - expected), expected * 0.3)


if __name__ == "__main__":
    unittest.main()
