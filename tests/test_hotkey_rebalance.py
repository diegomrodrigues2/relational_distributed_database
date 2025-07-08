import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class HotKeyRebalanceTest(unittest.TestCase):
    def test_hot_key_rebalance(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                key = "hk-rebalance"
                cluster.put(0, key, "v1")
                time.sleep(0.1)
                cluster.mark_hot_key(key, buckets=3, migrate=True)
                time.sleep(0.5)

                remove_id = cluster.nodes[0].node_id
                cluster.remove_node(remove_id)
                time.sleep(1)

                cluster.add_node()
                time.sleep(1)

                self.assertEqual(cluster.get(0, key), "v1")
                pmap = cluster.get_partition_map()
                for i in range(3):
                    salted = f"{i}#{key}"
                    pid = cluster.get_partition_id(salted)
                    owner_id = pmap[pid]
                    node = cluster.nodes_by_id[owner_id]
                    recs = node.client.get(salted)
                    self.assertTrue(recs and recs[0][0] == "v1")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
