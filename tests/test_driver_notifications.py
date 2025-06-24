import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster
from driver import Driver
from partitioning import compose_key


class DriverNotificationTest(unittest.TestCase):
    def test_update_after_add_node(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=1,
                replication_factor=1,
                partition_strategy="hash",
                num_partitions=2,
            )
            driver = Driver(cluster)
            try:
                key = "alpha"
                pid = cluster.get_partition_id(key)
                driver.put("u", key, "v1")
                time.sleep(0.2)
                cluster.add_node()
                self.assertEqual(driver.partition_map, cluster.get_partition_map())
                driver.put("u", key, "v2")
                time.sleep(0.2)
                owner = cluster.get_partition_map()[pid]
                recs = cluster.nodes_by_id[owner].client.get(compose_key(key))
                self.assertTrue(recs and recs[0][0] == "v2")
            finally:
                cluster.shutdown()

    def test_update_after_split_partition(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "m"), ("m", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            driver = Driver(cluster)
            try:
                cluster.split_partition(0, "g")
                self.assertEqual(driver.partition_map, cluster.get_partition_map())
                key = "ga"
                driver.put("u", key, "v1")
                time.sleep(0.2)
                pid = cluster.get_partition_id(key)
                owner = cluster.get_partition_map()[pid]
                recs = cluster.nodes_by_id[owner].client.get(compose_key(key))
                self.assertTrue(recs and recs[0][0] == "v1")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
