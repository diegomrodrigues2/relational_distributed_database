import os
import sys
import tempfile
import json
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replica.grpc_server import NodeServer, ReplicaService
from replica import replication_pb2


class ReplicaListByIndexTest(unittest.TestCase):
    def test_service_returns_indexed_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, index_fields=["name"])
            service = ReplicaService(node)

            service.Put(replication_pb2.KeyValue(key="k1", value=json.dumps({"name": "alice"}), timestamp=1), None)
            service.Put(replication_pb2.KeyValue(key="k2", value=json.dumps({"name": "bob"}), timestamp=2), None)

            req = replication_pb2.IndexQuery(field="name", value="alice")
            resp = service.ListByIndex(req, None)
            self.assertEqual(list(resp.keys), ["k1"])
            node.db.close()


if __name__ == "__main__":
    unittest.main()
