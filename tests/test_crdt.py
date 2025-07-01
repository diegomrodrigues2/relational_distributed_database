import tempfile
import json
import unittest

from database.utils.crdt import GCounter, ORSet
from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.replication.replica import replication_pb2


class GCounterUnitTest(unittest.TestCase):
    def test_merge(self):
        a = GCounter("A")
        b = GCounter("B")
        a.apply(2)
        b.apply(3)
        a.merge(b)
        b.merge(a)
        self.assertEqual(a.value, 5)
        self.assertEqual(b.value, 5)


class ReplicaServiceCRDTTest(unittest.TestCase):
    def test_gcounter_replication(self):
        with tempfile.TemporaryDirectory() as dir_a, tempfile.TemporaryDirectory() as dir_b:
            cfg = {"c": "gcounter"}
            node_a = NodeServer(db_path=dir_a, node_id="A", peers=[], crdt_config=cfg, consistency_mode="crdt")
            node_b = NodeServer(db_path=dir_b, node_id="B", peers=[], crdt_config=cfg, consistency_mode="crdt")
            service_a = ReplicaService(node_a)
            service_b = ReplicaService(node_b)

            node_a.apply_crdt("c", 1)
            op_id, (k, v, ts) = list(node_a.replication_log.items())[-1]
            req = replication_pb2.KeyValue(key=k, value=v, timestamp=ts, node_id="A", op_id=op_id)
            service_b.Put(req, None)
            self.assertEqual(node_b.crdts["c"].value, 1)

            node_b.apply_crdt("c", 2)
            op_id_b, (k2, v2, ts2) = list(node_b.replication_log.items())[-1]
            req2 = replication_pb2.KeyValue(key=k2, value=v2, timestamp=ts2, node_id="B", op_id=op_id_b)
            service_a.Put(req2, None)

            self.assertEqual(node_a.crdts["c"].value, 3)
            self.assertEqual(node_b.crdts["c"].value, 3)


if __name__ == "__main__":
    unittest.main()
