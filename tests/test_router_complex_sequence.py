import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster
from database.replication.replica import replication_pb2


class RouterComplexSequenceTest(unittest.TestCase):
    def test_router_handles_topology_changes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=2,
                replication_factor=1,
                partition_strategy="hash",
                start_router=True,
                num_partitions=2,
            )
            try:
                cluster.update_partition_map()
                time.sleep(0.2)
                key1 = "k1"
                cluster.router_client.put(key1, "v1")
                time.sleep(0.5)
                recs1 = cluster.router_client.get(key1)
                self.assertTrue(recs1 and recs1[0][0] == "v1")

                cluster.split_partition(0)
                new_map = cluster.update_partition_map()
                time.sleep(0.5)
                req = replication_pb2.PartitionMap(items=new_map)
                cluster.router_client.stub.UpdatePartitionMap(req)
                time.sleep(0.2)

                key2 = "k2"
                cluster.router_client.put(key2, "v2")
                time.sleep(0.5)
                recs2 = cluster.router_client.get(key2)
                self.assertTrue(recs2 and recs2[0][0] == "v2")

                cluster.remove_node("node_1")
                new_map = cluster.update_partition_map()
                time.sleep(0.5)
                req = replication_pb2.PartitionMap(items=new_map)
                cluster.router_client.stub.UpdatePartitionMap(req)
                time.sleep(0.2)

                recs1 = cluster.router_client.get(key1)
                recs2 = cluster.router_client.get(key2)
                self.assertTrue(recs1 and recs1[0][0] == "v1")
                self.assertTrue(recs2 and recs2[0][0] == "v2")

                cluster.add_node()
                new_map = cluster.update_partition_map()
                time.sleep(0.5)
                req = replication_pb2.PartitionMap(items=new_map)
                cluster.router_client.stub.UpdatePartitionMap(req)
                time.sleep(0.2)

                key3 = "k3"
                cluster.router_client.put(key3, "v3")
                time.sleep(0.5)
                recs3 = cluster.router_client.get(key3)
                self.assertTrue(recs3 and recs3[0][0] == "v3")

                recs1 = cluster.router_client.get(key1)
                recs2 = cluster.router_client.get(key2)
                self.assertTrue(recs1 and recs1[0][0] == "v1")
                self.assertTrue(recs2 and recs2[0][0] == "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
