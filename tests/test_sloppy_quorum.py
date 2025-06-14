import os
import sys
import tempfile
import time
import json
import multiprocessing
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster, ClusterNode
from replica.grpc_server import run_server
from replica.client import GRPCReplicaClient


class SloppyQuorumTest(unittest.TestCase):
    def test_write_with_hinted_substitute(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=4, replication_factor=3)
            try:
                key = "sloppy:hint"
                pref = cluster.ring.get_preference_list(key, 3)
                offline_id = pref[1]
                cluster.nodes_by_id[offline_id].stop()
                time.sleep(4)

                cluster.put(0, key, "v1")
                time.sleep(1)

                val = cluster.get(0, key)
                self.assertEqual(val, "v1")

                holder = None
                for node in cluster.nodes:
                    hints_file = os.path.join(tmpdir, node.node_id, "hints.json")
                    if not os.path.exists(hints_file):
                        continue
                    with open(hints_file, "r", encoding="utf-8") as f:
                        hints = json.load(f)
                    if hints.get(offline_id):
                        holder = node
                        break
                self.assertIsNotNone(holder)

                idx = int(offline_id.split("_")[1])
                db_path = os.path.join(tmpdir, offline_id)
                peers = [("localhost", 9000 + i, f"node_{i}") for i in range(4)]
                p = multiprocessing.Process(
                    target=run_server,
                    args=(
                        db_path,
                        "localhost",
                        9000 + idx,
                        offline_id,
                        peers,
                        cluster.ring,
                        cluster.replication_factor,
                        cluster.write_quorum,
                        cluster.read_quorum,
                    ),
                    kwargs={"consistency_mode": cluster.consistency_mode},
                    daemon=True,
                )
                p.start()
                time.sleep(1)
                client = GRPCReplicaClient("localhost", 9000 + idx)
                new_node = ClusterNode(offline_id, "localhost", 9000 + idx, p, client)
                cluster.nodes[idx] = new_node
                cluster.nodes_by_id[offline_id] = new_node

                time.sleep(2)
                with open(os.path.join(tmpdir, holder.node_id, "hints.json"), "r", encoding="utf-8") as f:
                    hints_after = json.load(f)
                self.assertFalse(hints_after.get(offline_id))
                self.assertEqual(cluster.nodes_by_id[offline_id].client.get(key)[0], "v1")
            finally:
                cluster.shutdown()

    def test_sloppy_quorum_read(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=4, replication_factor=3)
            try:
                key = "sloppy:read"
                pref = cluster.ring.get_preference_list(key, 3)
                down1 = pref[1]
                cluster.nodes_by_id[down1].stop()
                time.sleep(4)

                cluster.put(0, key, "v1")
                time.sleep(1)

                down2 = pref[2]
                cluster.nodes_by_id[down2].stop()
                time.sleep(0.5)

                val = cluster.get(0, key)
                self.assertEqual(val, "v1")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
