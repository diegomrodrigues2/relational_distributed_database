import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class IndexOwnerTest(unittest.TestCase):
    def test_get_index_owner_returns_expected_node(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                partition_strategy="hash",
                num_partitions=4,
                replication_factor=1,
            )
            try:
                # Partition map should be deterministic with the settings above
                self.assertEqual(
                    cluster.get_partition_map(),
                    {0: "node_0", 1: "node_1", 2: "node_0", 3: "node_1"},
                )

                cases = [
                    ("name", "alice", "node_0"),
                    ("name", "bob", "node_1"),
                    ("tag", "n", "node_0"),
                    ("tag", "x", "node_1"),
                ]

                for field, value, expected in cases:
                    owner = cluster.get_index_owner(field, value)
                    self.assertEqual(owner, expected)
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
