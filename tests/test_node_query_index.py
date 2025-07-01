import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.replication.replica import replication_pb2


class NodeQueryIndexTest(unittest.TestCase):
    def test_query_returns_matching_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, index_fields=["name"])
            service = ReplicaService(node)

            service.Put(replication_pb2.KeyValue(key="k1", value='{"name": "alice"}', timestamp=1), None)
            service.Put(replication_pb2.KeyValue(key="k2", value='{"name": "bob"}', timestamp=2), None)
            service.Put(replication_pb2.KeyValue(key="k3", value='{"name": "alice"}', timestamp=3), None)

            result = sorted(node.query_index("name", "alice"))
            self.assertEqual(result, ["k1", "k3"])
            node.db.close()

    def test_query_empty_after_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, index_fields=["name"])
            service = ReplicaService(node)

            service.Put(replication_pb2.KeyValue(key="k1", value='{"name": "alice"}', timestamp=1), None)
            service.Delete(replication_pb2.KeyRequest(key="k1", timestamp=2), None)

            self.assertEqual(node.query_index("name", "alice"), [])
            node.db.close()


if __name__ == "__main__":
    unittest.main()
