import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import grpc
from database.replication import NodeCluster
from database.replication.replica import metadata_pb2_grpc, replication_pb2


class RegistryNodeChangesTest(unittest.TestCase):
    def test_registry_reports_node_changes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                key_ranges=[("a", "m"), ("m", "z")],
                start_router=True,
                use_registry=True,
            )
            try:
                # give the gRPC services a moment to start before issuing
                # requests. slower machines may require a slightly longer
                # pause for the underlying servers to bind to their ports.
                time.sleep(1.0)
                channel = grpc.insecure_channel("localhost:9100")
                stub = metadata_pb2_grpc.MetadataServiceStub(channel)

                # write some keys through the router
                keys = ["ka", "kb", "kc"]
                for k in keys:
                    pid = cluster.get_partition_id(k)
                    owner = cluster.get_partition_map()[pid]
                    cluster.router_client.put(k, f"v-{k}", node_id=owner)
                time.sleep(0.5)

                for k in keys:
                    recs = cluster.router_client.get(k)
                    self.assertTrue(recs and recs[0][0] == f"v-{k}")

                # add a node to the cluster
                new_node = cluster.add_node()
                time.sleep(1.0)
                state = stub.GetClusterState(replication_pb2.Empty())
                node_ids = {n.node_id for n in state.nodes}
                self.assertEqual(node_ids, set(cluster.nodes_by_id.keys()))

                for k in keys:
                    recs = cluster.router_client.get(k)
                    self.assertTrue(recs and recs[0][0] == f"v-{k}")

                # remove one of the old nodes
                old_id = "node_0"
                cluster.remove_node(old_id)
                cluster.update_partition_map()
                time.sleep(1.0)

                state = stub.GetClusterState(replication_pb2.Empty())
                node_ids = {n.node_id for n in state.nodes}
                self.assertEqual(node_ids, set(cluster.nodes_by_id.keys()))

                for k in keys:
                    recs = cluster.router_client.get(k)
                    self.assertTrue(recs and recs[0][0] == f"v-{k}")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
