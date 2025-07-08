import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class RestartAfterRebalanceTest(unittest.TestCase):
    @unittest.skip("Data replication after node restart not implemented")
    def test_restart_after_rebalance(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=2,
                key_ranges=[("a", "z")],
            )
            try:
                cluster.split_partition(0, "m")
                cluster.split_partition(1, "t")
                cluster.update_partition_map()
                time.sleep(0.5)

                keys = ["b", "p", "x"]
                for i, k in enumerate(keys):
                    cluster.put(0, k, f"v{i}")
                time.sleep(0.5)

                remove_id = cluster.nodes[-1].node_id
                cluster.remove_node(remove_id)
                time.sleep(1)

                cluster.add_node()
                # give anti-entropy enough time to copy data to the new node
                time.sleep(6)

                for pid, k in enumerate(keys):
                    self.assertEqual(cluster.get(0, k), f"v{pid}")

                for pid in range(cluster.num_partitions):
                    owner = cluster.partition_map.get(pid)
                    self.assertIn(owner, cluster.nodes_by_id)
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
