import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class ReplicationTopologyTest(unittest.TestCase):
    def test_ring_topology_propagates_via_intermediate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            topology = {0: [1], 1: [2], 2: [0]}
            cluster = NodeCluster(base_path=tmpdir, num_nodes=3, topology=topology)
            try:
                cluster.put(0, "ring", "v1")
                time.sleep(1)
                self.assertEqual(cluster.get(2, "ring"), "v1")

                cluster.put(1, "ring", "dup")
                time.sleep(0.5)
                self.assertEqual(cluster.get(2, "ring"), "dup")
            finally:
                cluster.shutdown()

    def test_star_topology_hub_propagation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            topology = {0: [1, 2], 1: [0], 2: [0]}
            cluster = NodeCluster(base_path=tmpdir, num_nodes=3, topology=topology)
            try:
                cluster.put(1, "star", "v1")
                time.sleep(1)
                self.assertEqual(cluster.get(2, "star"), "v1")

                cluster.put(2, "star", "dup")
                time.sleep(0.5)
                self.assertEqual(cluster.get(2, "star"), "dup")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
