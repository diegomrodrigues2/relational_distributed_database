import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import grpc
from replication import NodeCluster
from driver import Driver
from replica import metadata_pb2_grpc, replication_pb2


class RegistryIntegrationTest(unittest.TestCase):
    def test_registry_updates_router_and_driver(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                key_ranges=[("a", "m"), ("m", "z")],
                start_router=True,
                use_registry=True,
            )
            try:
                time.sleep(0.5)
                channel = grpc.insecure_channel("localhost:9100")
                stub = metadata_pb2_grpc.MetadataServiceStub(channel)
                state = stub.GetClusterState(replication_pb2.Empty())
                self.assertEqual(len(state.nodes), 2)

                driver = Driver(cluster, registry_host="localhost", registry_port=9100)
                time.sleep(0.5)
                cluster.put(0, "ga", "v1")
                time.sleep(0.5)
                self.assertEqual(driver.get("u1", "ga"), "v1")

                cluster.split_partition(0, "g")
                new_map = cluster.update_partition_map()
                time.sleep(1.0)

                key2 = "ha"
                pid2 = cluster.get_partition_id(key2)
                owner2 = new_map[pid2]
                cluster.router_client.put(key2, "v2", node_id=owner2)
                time.sleep(0.5)
                recs2 = cluster.router_client.get(key2)
                self.assertTrue(recs2 and recs2[0][0] == "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
