import os
import tempfile
import unittest

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.replication.replica import replication_pb2

class IndexManagerTest(unittest.TestCase):
    def test_put_updates_index(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, index_fields=["name"])
            service = ReplicaService(node)

            req = replication_pb2.KeyValue(key="k1", value='{"name": "alice"}', timestamp=1)
            service.Put(req, None)
            self.assertIn("k1", node.index_manager.query("name", "alice"))
            node.db.close()

    def test_delete_removes_index(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, index_fields=["name"])
            service = ReplicaService(node)

            service.Put(replication_pb2.KeyValue(key="k1", value='{"name": "bob"}', timestamp=1), None)
            service.Delete(replication_pb2.KeyRequest(key="k1", timestamp=2), None)
            self.assertEqual(node.index_manager.query("name", "bob"), [])
            node.db.close()

if __name__ == "__main__":
    unittest.main()
