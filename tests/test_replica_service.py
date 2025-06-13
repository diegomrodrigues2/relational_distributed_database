import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replica.grpc_server import NodeServer, ReplicaService
from replica import replication_pb2


class ReplicaServiceTimestampTest(unittest.TestCase):
    def test_put_delete_respects_timestamps(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            # existing value with newer timestamp
            node.db.put("k", "v1", timestamp=10)

            # update with older timestamp should be ignored
            req_old = replication_pb2.KeyValue(key="k", value="old", timestamp=5)
            service.Put(req_old, None)
            self.assertEqual(node.db.get("k"), "v1")

            # update with newer timestamp should overwrite
            req_new = replication_pb2.KeyValue(key="k", value="new", timestamp=20)
            service.Put(req_new, None)
            self.assertEqual(node.db.get("k"), "new")

            # prepare record for delete tests
            node.db.put("d", "val", timestamp=15)

            # node clock small -> delete ignored
            node.clock.time = 0
            service.Delete(replication_pb2.KeyRequest(key="d"), None)
            self.assertEqual(node.db.get("d"), "val")

            # node clock high -> delete applied
            node.clock.time = 100
            service.Delete(replication_pb2.KeyRequest(key="d"), None)
            self.assertIsNone(node.db.get("d"))

            node.db.close()


if __name__ == "__main__":
    unittest.main()
