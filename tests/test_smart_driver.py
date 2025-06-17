import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster
from driver import Driver
from partitioning import compose_key


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

                # simulate stale cache
                driver.partition_map[pid] = other

                # driver will refresh after receiving NotOwner
                driver.put("u", key, "v2")
                time.sleep(0.5)

                k = compose_key(key)
                recs_new = cluster.nodes_by_id[owner].client.get(k)
                self.assertTrue(recs_new and recs_new[0][0] == "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
