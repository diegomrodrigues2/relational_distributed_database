import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.replication.replica import replication_pb2


class NodeCacheTest(unittest.TestCase):
    def test_cache_updates_after_put(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, cache_size=2)
            service = ReplicaService(node)

            req1 = replication_pb2.KeyValue(key="k", value="v1", timestamp=1)
            service.Put(req1, None)
            resp1 = service.Get(replication_pb2.KeyRequest(key="k", timestamp=0), None)
            self.assertEqual(resp1.values[0].value, "v1")

            req2 = replication_pb2.KeyValue(key="k", value="v2", timestamp=2)
            service.Put(req2, None)
            resp2 = service.Get(replication_pb2.KeyRequest(key="k", timestamp=0), None)
            self.assertEqual(resp2.values[0].value, "v2")
            node.db.close()

    def test_cache_invalidated_on_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, cache_size=2)
            service = ReplicaService(node)

            service.Put(replication_pb2.KeyValue(key="k", value="v1", timestamp=1), None)
            service.Get(replication_pb2.KeyRequest(key="k", timestamp=0), None)
            service.Delete(replication_pb2.KeyRequest(key="k", timestamp=2), None)
            resp = service.Get(replication_pb2.KeyRequest(key="k", timestamp=0), None)
            self.assertEqual(len(resp.values), 0)
            node.db.close()


if __name__ == "__main__":
    unittest.main()
