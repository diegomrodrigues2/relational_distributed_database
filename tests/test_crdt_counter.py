import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.replication.replica import replication_pb2


class CRDTCounterConcurrentTest(unittest.TestCase):
    def test_concurrent_increments(self):
        with tempfile.TemporaryDirectory() as dir_a, tempfile.TemporaryDirectory() as dir_b:
            cfg = {"cnt": "gcounter"}
            node_a = NodeServer(db_path=dir_a, node_id="A", peers=[], crdt_config=cfg, consistency_mode="crdt")
            node_b = NodeServer(db_path=dir_b, node_id="B", peers=[], crdt_config=cfg, consistency_mode="crdt")
            service_a = ReplicaService(node_a)
            service_b = ReplicaService(node_b)

            node_a.apply_crdt("cnt", 1)
            node_b.apply_crdt("cnt", 1)

            op_id_a, (k_a, v_a, ts_a) = list(node_a.replication_log.items())[-1]
            req_a = replication_pb2.KeyValue(key=k_a, value=v_a, timestamp=ts_a, node_id="A", op_id=op_id_a)
            service_b.Put(req_a, None)

            op_id_b, (k_b, v_b, ts_b) = list(node_b.replication_log.items())[-1]
            req_b = replication_pb2.KeyValue(key=k_b, value=v_b, timestamp=ts_b, node_id="B", op_id=op_id_b)
            service_a.Put(req_b, None)

            self.assertEqual(node_a.crdts["cnt"].value, 2)
            self.assertEqual(node_b.crdts["cnt"].value, 2)

            node_a.db.close()
            node_b.db.close()


if __name__ == "__main__":
    unittest.main()
