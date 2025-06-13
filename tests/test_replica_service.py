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
            service.Delete(replication_pb2.KeyRequest(key="d", timestamp=0, node_id="test"), None)
            self.assertEqual(node.db.get("d"), "val")

            # node clock high -> delete applied
            node.clock.time = 100
            service.Delete(replication_pb2.KeyRequest(key="d", timestamp=200, node_id="test"), None)
            self.assertIsNone(node.db.get("d"))

            node.db.close()


class ReplicaServiceSequenceTest(unittest.TestCase):
    def test_sequence_number_prevents_duplicates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            req1 = replication_pb2.KeyValue(
                key="k",
                value="v1",
                timestamp=10,
                node_id="node_b",
                op_id="node_b:1",
            )
            service.Put(req1, None)
            self.assertEqual(node.db.get("k"), "v1")

            # duplicate with same sequence should be ignored
            dup = replication_pb2.KeyValue(
                key="k",
                value="v2",
                timestamp=20,
                node_id="node_b",
                op_id="node_b:1",
            )
            service.Put(dup, None)
            self.assertEqual(node.db.get("k"), "v1")

            # higher sequence should be applied
            req2 = replication_pb2.KeyValue(
                key="k",
                value="v2",
                timestamp=20,
                node_id="node_b",
                op_id="node_b:2",
            )
            service.Put(req2, None)
            self.assertEqual(node.db.get("k"), "v2")

            node.db.close()


if __name__ == "__main__":
    unittest.main()
