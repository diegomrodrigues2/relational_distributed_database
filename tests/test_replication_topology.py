import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class ReplicationTopologyTest(unittest.TestCase):
    def test_ring_topology_propagates_via_intermediate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            topology = {0: [1], 1: [2], 2: [0]}
            cluster = NodeCluster(base_path=tmpdir, num_nodes=3, topology=topology)
            try:
                ts = int(time.time() * 1000)
                op_id = "node_0:1"
                cluster.nodes[0].client.put(
                    "ring", "v1", timestamp=ts, node_id="node_0", op_id=op_id
                )
                time.sleep(1)
                self.assertEqual(cluster.get(2, "ring"), "v1")

                cluster.nodes[1].client.put(
                    "ring", "dup", timestamp=ts + 1, node_id="node_0", op_id=op_id
                )
                time.sleep(0.5)
                self.assertEqual(cluster.get(2, "ring"), "v1")
            finally:
                cluster.shutdown()

    def test_star_topology_hub_propagation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            topology = {0: [1, 2], 1: [0], 2: [0]}
            cluster = NodeCluster(base_path=tmpdir, num_nodes=3, topology=topology)
            try:
                ts = int(time.time() * 1000)
                op_id = "node_1:1"
                cluster.nodes[1].client.put(
                    "star", "v1", timestamp=ts, node_id="node_1", op_id=op_id
                )
                time.sleep(1)
                self.assertEqual(cluster.get(2, "star"), "v1")

                cluster.nodes[2].client.put(
                    "star", "dup", timestamp=ts + 1, node_id="node_1", op_id=op_id
                )
                time.sleep(0.5)
                self.assertEqual(cluster.get(2, "star"), "v1")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
