import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class NodeStartStopTest(unittest.TestCase):
    def test_stop_and_start_node(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=2,
                partition_strategy="hash",
            )
            try:
                cluster.put(0, "k1", "v1")
                time.sleep(0.5)
                cluster.stop_node("node_1")
                time.sleep(0.5)
                self.assertEqual(cluster.get(0, "k1"), "v1")
                cluster.start_node("node_1")
                time.sleep(1)
                cluster.put(0, "k2", "v2")
                time.sleep(0.5)
                self.assertEqual(cluster.get(0, "k2"), "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
