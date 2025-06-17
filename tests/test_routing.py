import os
import sys
import tempfile
import time
import random
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class CoordinatorForwardingTest(unittest.TestCase):
    def test_forwarding_put_get_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
                enable_forwarding=True,
            )
            try:
                key = "route:key"
                owner_id = cluster.ring.get_preference_list(key, 1)[0]
                wrong_node = random.choice(
                    [n for n in cluster.nodes if n.node_id != owner_id]
                )

                # PUT via wrong node
                wrong_node.client.put(key, "v1")
                time.sleep(0.5)

                recs_owner = cluster.nodes_by_id[owner_id].client.get(key)
                self.assertTrue(recs_owner and recs_owner[0][0] == "v1")

                # GET via wrong node should be forwarded
                recs_wrong = wrong_node.client.get(key)
                self.assertTrue(recs_wrong and recs_wrong[0][0] == "v1")

                # DELETE via wrong node
                wrong_node.client.delete(key)
                time.sleep(0.5)
                self.assertFalse(cluster.nodes_by_id[owner_id].client.get(key))
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
