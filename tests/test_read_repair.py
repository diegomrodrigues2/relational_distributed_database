import os
import sys
import tempfile
import time
import multiprocessing
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster, ClusterNode
from database.replication.replica.grpc_server import run_server
from database.replication.replica.client import GRPCReplicaClient


class ReadRepairTest(unittest.TestCase):
    def test_read_repair_updates_stale_node(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=3)
            try:
                key = "rr:key"
                cluster.put(0, key, "v1")
                pref = cluster.ring.get_preference_list(key, 3)
                stale_id = pref[2]
                cluster.nodes_by_id[stale_id].stop()
                time.sleep(0.5)
                cluster.put(0, key, "v2")
                time.sleep(0.5)

                idx = int(stale_id.split("_")[1])
                db_path = os.path.join(tmpdir, stale_id)
                peers = [("localhost", 9000 + i, f"node_{i}") for i in range(3)]
                p = multiprocessing.Process(
                    target=run_server,
                    args=(
                        db_path,
                        "localhost",
                        9000 + idx,
                        stale_id,
                        peers,
                        cluster.ring,
                        cluster.partition_map,
                        cluster.replication_factor,
                        cluster.write_quorum,
                        cluster.read_quorum,
                    ),
                    kwargs={"consistency_mode": cluster.consistency_mode},
                    daemon=True,
                )
                p.start()
                time.sleep(0.5)
                client = GRPCReplicaClient("localhost", 9000 + idx)
                new_node = ClusterNode(stale_id, "localhost", 9000 + idx, p, client)
                cluster.nodes[idx] = new_node
                cluster.nodes_by_id[stale_id] = new_node

                val = cluster.get(0, key)
                self.assertEqual(val, "v2")
                time.sleep(1)
                recs = cluster.nodes_by_id[stale_id].client.get(key)
                self.assertEqual(recs[0][0], "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
