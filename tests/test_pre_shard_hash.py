import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class PreShardHashTest(unittest.TestCase):
    def test_more_partitions_than_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
                num_partitions=4,
            )
            try:
                keys = {}
                i = 0
                while len(keys) < cluster.num_partitions:
                    k = f"k{i}"
                    pid = cluster.get_partition_id(k)
                    if pid not in keys:
                        keys[pid] = k
                    i += 1
                for pid, k in keys.items():
                    cluster.put(0, k, f"v{pid}")
                time.sleep(0.2)
                for pid, k in keys.items():
                    node_index = pid % len(cluster.nodes)
                    recs = cluster.nodes[node_index].client.get(k)
                    self.assertTrue(recs)
                    self.assertEqual(recs[0][0], f"v{pid}")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
