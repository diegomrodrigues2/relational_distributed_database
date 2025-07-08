import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class HandoffPauseSplitTest(unittest.TestCase):
    def test_handoff_after_split_with_offline_node(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "m"), ("m", "z")]
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                key_ranges=ranges,
                replication_factor=3,
            )
            try:
                cluster.stop_node("node_1")
                time.sleep(0.5)

                keys = ["aa", "ab", "ac"]
                for i, k in enumerate(keys):
                    cluster.put(0, k, f"v{i}")
                time.sleep(0.5)

                cluster.split_partition(0, "f")
                time.sleep(0.5)

                cluster.start_node("node_1")
                time.sleep(2)

                for i, k in enumerate(keys):
                    recs = cluster.nodes_by_id["node_1"].client.get(k)
                    val = recs[0][0] if recs else None
                    self.assertEqual(val, f"v{i}")
                    for idx in range(3):
                        self.assertEqual(cluster.get(idx, k), f"v{i}")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
