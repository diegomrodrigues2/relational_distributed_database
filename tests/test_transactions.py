import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.replication.replica import replication_pb2


class TransactionTest(unittest.TestCase):
    def test_commit_and_abort(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            # start transaction and buffer operations
            tx_id = service.BeginTransaction(replication_pb2.Empty(), None).id
            service.Put(
                replication_pb2.KeyValue(key="k1", value="v1", timestamp=1, tx_id=tx_id),
                None,
            )
            node.db.put("d", "val", timestamp=1)
            service.Delete(
                replication_pb2.KeyRequest(key="d", timestamp=2, tx_id=tx_id), None
            )

            # nothing applied yet
            self.assertIsNone(node.db.get("k1"))
            self.assertEqual(node.db.get("d"), "val")

            service.CommitTransaction(
                replication_pb2.TransactionControl(tx_id=tx_id), None
            )

            self.assertEqual(node.db.get("k1"), "v1")
            self.assertIsNone(node.db.get("d"))

            # abort should discard pending ops
            tx2 = service.BeginTransaction(replication_pb2.Empty(), None).id
            service.Put(
                replication_pb2.KeyValue(key="k2", value="v2", timestamp=3, tx_id=tx2),
                None,
            )
            service.AbortTransaction(
                replication_pb2.TransactionControl(tx_id=tx2), None
            )
            self.assertIsNone(node.db.get("k2"))

            node.db.close()


if __name__ == "__main__":
    unittest.main()
