import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster
from partitioning import compose_key


class RangePartitioningBasicTest(unittest.TestCase):
    def test_range_partitioning_basic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "m"), ("m", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
            try:
                cluster.put(0, "alpha", "v1")
                cluster.put(1, "monkey", "v2")
                cluster.put(0, "m", "boundary")
                time.sleep(0.2)

                recs0_a = cluster.nodes[0].client.get("alpha")
                recs1_a = cluster.nodes[1].client.get("alpha")
                self.assertTrue(recs0_a)
                self.assertFalse(recs1_a)

                recs1_m = cluster.nodes[1].client.get("monkey")
                recs0_m = cluster.nodes[0].client.get("monkey")
                self.assertTrue(recs1_m)
                self.assertFalse(recs0_m)

                # boundary key should be in second range
                recs_boundary = cluster.nodes[1].client.get("m")
                self.assertTrue(recs_boundary)
                self.assertFalse(cluster.nodes[0].client.get("m"))
            finally:
                cluster.shutdown()


class HashPartitioningBalanceTest(unittest.TestCase):
    def test_hash_partitioning_balance(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                keys = [f"k{i}" for i in range(90)]
                for k in keys:
                    cluster.put(0, k, k)
                time.sleep(0.5)

                counts = [0] * 3
                for k in keys:
                    pid = cluster.get_partition_id(k)
                    idx = int(cluster.partition_map[pid].split("_")[1])
                    counts[idx] += 1
                expected = len(keys) / len(counts)
                for c in counts:
                    self.assertTrue(abs(c - expected) <= expected * 0.3)
            finally:
                cluster.shutdown()


class HybridPartitioningQueryTest(unittest.TestCase):
    def test_hybrid_partitioning_query(self):
        self.skipTest("hybrid partitioning not implemented")


class HotspotSaltingTest(unittest.TestCase):
    def test_hotspot_salting(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=1,
                partition_strategy="hash",
            )
            cluster.enable_salt("hot", buckets=3)
            try:
                for i in range(20):
                    cluster.put(0, "hot", f"v{i}")
                    time.sleep(0.001)
                time.sleep(0.5)

                used = set()
                for prefix in range(3):
                    salted = f"{prefix}#hot"
                    pid = cluster.get_partition_id(salted)
                    idx = int(cluster.partition_map[pid].split("_")[1])
                    if cluster.nodes[idx].client.get(salted):
                        used.add(idx)
                self.assertGreater(len(used), 1)

                self.assertEqual(cluster.get(1, "hot"), "v19")
            finally:
                cluster.shutdown()


class RebalanceAddNodeTest(unittest.TestCase):
    def test_rebalance_add_node(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
                num_partitions=6,
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
                time.sleep(1)
                cluster.add_node()
                time.sleep(1)
                for pid, k in keys.items():
                    val = cluster.get(0, k)
                    self.assertEqual(val, f"v{pid}")
            finally:
                cluster.shutdown()


class RoutingForwardingTest(unittest.TestCase):
    def test_routing_forwarding(self):
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
                wrong_node = next(n for n in cluster.nodes if n.node_id != owner_id)

                wrong_node.client.put(key, "v1")
                time.sleep(0.5)

                recs = wrong_node.client.get(key)
                self.assertTrue(recs and recs[0][0] == "v1")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
