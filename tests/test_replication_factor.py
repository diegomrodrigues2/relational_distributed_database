import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class ReplicationFactorThreeTest(unittest.TestCase):
    def test_key_replicated_to_exact_three_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=4, replication_factor=3)
            try:
                key = "rf3:key"
                cluster.put(0, key, "v1")

                expected_nodes = set(cluster.ring.get_preference_list(key, 3))
                found_nodes = set()
                for node in cluster.nodes:
                    recs = node.client.get(key)
                    val = recs[0][0] if recs else None
                    if val == "v1":
                        found_nodes.add(node.node_id)
                self.assertEqual(found_nodes, expected_nodes)
            finally:
                cluster.shutdown()

    def test_write_fails_when_replica_offline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=4, replication_factor=3)
            try:
                key = "offline:key"
                pref_nodes = cluster.ring.get_preference_list(key, 3)
                offline_id = pref_nodes[1]
                cluster.nodes_by_id[offline_id].stop()
                time.sleep(0.5)
                cluster.put(0, key, "v2")
                time.sleep(1)
                for nid in pref_nodes:
                    if nid == offline_id:
                        continue
                    recs = cluster.nodes_by_id[nid].client.get(key)
                    self.assertEqual(recs[0][0], "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
