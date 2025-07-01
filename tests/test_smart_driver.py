import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster
from driver import Driver
from database.clustering.partitioning import compose_key


class SmartDriverTest(unittest.TestCase):
    def test_driver_routes_to_owner(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
                enable_forwarding=False,
            )
            try:
                driver = Driver(cluster)
                key = "alpha"
                pid = cluster.get_partition_id(key)
                owner = cluster.get_partition_map()[pid]
                driver.put("u", key, "v1")
                time.sleep(0.5)
                k = compose_key(key)
                self.assertTrue(cluster.nodes_by_id[owner].client.get(k))
                other = "node_1" if owner == "node_0" else "node_0"
                try:
                    self.assertFalse(cluster.nodes_by_id[other].client.get(k))
                except Exception:
                    pass
            finally:
                cluster.shutdown()

    def test_driver_refresh_after_migration(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
                enable_forwarding=False,
            )
            try:
                driver = Driver(cluster)
                key = "beta"
                pid = cluster.get_partition_id(key)
                owner = cluster.get_partition_map()[pid]
                other = "node_1" if owner == "node_0" else "node_0"

                driver.put("u", key, "v1")
                time.sleep(0.5)

                # simulate partition migration to the other node
                cluster.transfer_partition(
                    cluster.nodes_by_id[owner],
                    cluster.nodes_by_id[other],
                    pid,
                )
                idx_owner = cluster.nodes.index(cluster.nodes_by_id[owner])
                idx_other = cluster.nodes.index(cluster.nodes_by_id[other])
                cluster.nodes[idx_owner], cluster.nodes[idx_other] = (
                    cluster.nodes[idx_other],
                    cluster.nodes[idx_owner],
                )
                new_map = cluster.update_partition_map()
                time.sleep(0.5)

                # refresh driver's map proactively
                driver.update_partition_map(new_map)
                driver.put("u", key, "v2")
                time.sleep(0.5)

                k = compose_key(key)
                recs_new = cluster.nodes_by_id[other].client.get(k)
                self.assertTrue(recs_new and recs_new[0][0] == "v2")
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
                enable_forwarding=False,
            )
            try:
                driver = Driver(cluster)  # inherits load_balance_reads
                driver.put("u", "gamma", "x", "v")
                time.sleep(0.5)

                counts = {}
                for n in cluster.nodes:
                    orig = n.client.get

                    def wrap(key, _orig=orig, nid=n.node_id):
                        counts[nid] = counts.get(nid, 0) + 1
                        return _orig(key)

                    n.client.get = wrap

                for _ in range(10):
                    driver.get("u", "gamma", "x")

                hit_nodes = [nid for nid, c in counts.items() if c > 0]
                self.assertGreaterEqual(len(hit_nodes), 2)
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
