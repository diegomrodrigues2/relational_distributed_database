import os
import sys
import tempfile
import time
import json
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class TransferRateThrottleTest(unittest.TestCase):
    def test_transfer_partition_throttled(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            rate = 50  # bytes per second
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
                topology={},
                max_transfer_rate=rate,
            )
            try:
                key = "k0"  # maps to partition 0
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
                self.assertGreaterEqual(elapsed, record_size / rate)
                recs2 = cluster.nodes[1].client.get(key)
                self.assertTrue(recs2 and recs2[0][0] == value)
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
