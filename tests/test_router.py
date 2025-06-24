import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster
from replica import replication_pb2


class RouterMapUpdateTest(unittest.TestCase):
    def test_partition_map_update_via_router(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "m"), ("m", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges, start_router=True)
            try:
                key = "ga"
                pid = cluster.get_partition_id(key)
                owner = cluster.get_partition_map()[pid]
                cluster.router_client.put(key, "v1", node_id=owner)
                time.sleep(0.5)

                recs_router = cluster.router_client.get(key)
                self.assertTrue(recs_router and recs_router[0][0] == "v1")

                cluster.split_partition(0, "g")
                new_map = cluster.update_partition_map()
                time.sleep(0.5)

                req = replication_pb2.PartitionMap(items=new_map)
                cluster.router_client.stub.UpdatePartitionMap(req)
                time.sleep(0.2)

                key2 = "ha"
                pid2 = cluster.get_partition_id(key2)
                owner2 = cluster.get_partition_map()[pid2]
                cluster.router_client.put(key2, "v2", node_id=owner2)
                time.sleep(0.5)

                recs_router = cluster.router_client.get(key2)
                self.assertTrue(recs_router and recs_router[0][0] == "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
