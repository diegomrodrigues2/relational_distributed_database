import os
import sys
import tempfile
import time
import random
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster
from partitioning import compose_key


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
                pid = cluster.get_partition_id(key)
                owner_id = cluster.get_partition_map()[pid]
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

    def test_forwarding_scan_range(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
                enable_forwarding=True,
            )
            try:
                pkey = "route:range"
                pid = cluster.get_partition_id(pkey)
                owner_id = cluster.get_partition_map()[pid]
                owner_node = cluster.nodes_by_id[owner_id]
                wrong_node = random.choice(
                    [n for n in cluster.nodes if n.node_id != owner_id]
                )

                for ck, val in [("a", "va"), ("b", "vb"), ("c", "vc")]:
                    owner_node.client.put(compose_key(pkey, ck), val)

                time.sleep(0.5)

                owner_items = [
                    (ck, val)
                    for ck, val, _, _ in owner_node.client.scan_range(pkey, "a", "c")
                ]
                wrong_items = [
                    (ck, val)
                    for ck, val, _, _ in wrong_node.client.scan_range(pkey, "a", "c")
                ]

                self.assertEqual(wrong_items, owner_items)
            finally:
                cluster.shutdown()

    def test_forwarded_write_replication_factor_three(self):
        """Write sent to wrong node should replicate to all replicas."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=3,
                partition_strategy="hash",
                enable_forwarding=True,
            )
            try:
                key = "route:rf3"
                pid = cluster.get_partition_id(key)
                owner_id = cluster.get_partition_map()[pid]
                wrong_node = random.choice(
                    [n for n in cluster.nodes if n.node_id != owner_id]
                )

                wrong_node.client.put(key, "v2")
                time.sleep(0.5)

                for node in cluster.nodes:
                    recs = node.client.get(key)
                    self.assertTrue(recs and recs[0][0] == "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
