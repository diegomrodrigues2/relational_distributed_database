import os
import sys
import tempfile
import time
import json
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class RebalanceHashClusterTest(unittest.TestCase):
    def _partition_keys(self, cluster):
        keys = {}
        i = 0
        while len(keys) < cluster.num_partitions:
            k = f"k{i}"
            pid = cluster.get_partition_id(k)
            if pid not in keys:
                keys[pid] = k
            i += 1
        return keys

    def test_add_node_rebalances(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                keys = self._partition_keys(cluster)
                for pid, k in keys.items():
                    cluster.put(0, k, f"v{pid}")
                time.sleep(1)
                cluster.add_node()
                for pid, k in keys.items():
                    val = cluster.get(0, k)
                    self.assertEqual(val, f"v{pid}")
            finally:
                cluster.shutdown()

    def test_remove_node_rebalances(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=2,
                partition_strategy="hash",
            )
            try:
                keys = self._partition_keys(cluster)
                pid, key = next(iter(keys.items()))
                cluster.put(0, key, f"v{pid}")
                time.sleep(1)
                cluster.nodes_by_id["node_1"].stop()
                time.sleep(1)
                val = cluster.get(0, key)
                self.assertEqual(val, f"v{pid}")
            finally:
                cluster.shutdown()


class ThrottleOnTransferTest(unittest.TestCase):
    def test_transfer_partition_throttled_via_setter(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
            )
            try:
                cluster.set_max_transfer_rate(50)  # bytes per second
                key = "k0"
                value = "x" * 10
                cluster.put(0, key, value)
                time.sleep(0.5)
                pid = cluster.get_partition_id(key)
                recs = cluster.nodes[0].client.get(key)
                val, ts, vc_dict = recs[0]
                record_size = (
                    len(key.encode("utf-8"))
                    + len(val.encode("utf-8"))
                    + len(json.dumps(vc_dict).encode("utf-8"))
                )
                start = time.time()
                cluster.transfer_partition(cluster.nodes[0], cluster.nodes[1], pid)
                elapsed = time.time() - start
                self.assertGreaterEqual(elapsed, record_size / 50)
                recs2 = cluster.nodes[1].client.get(key)
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
