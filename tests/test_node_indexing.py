import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.replication.replica import replication_pb2


class NodeIndexingTest(unittest.TestCase):
    def test_index_insert_update_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, index_fields=["age"])
            service = ReplicaService(node)

            service.Put(replication_pb2.KeyValue(key="u1", value='{"age": 30}', timestamp=1), None)
            service.Put(replication_pb2.KeyValue(key="u2", value='{"age": 40}', timestamp=2), None)

            self.assertEqual(sorted(node.query_index("age", 30)), ["u1"])
            self.assertEqual(sorted(node.query_index("age", 40)), ["u2"])

            service.Put(replication_pb2.KeyValue(key="u1", value='{"age": 31}', timestamp=3), None)

            self.assertEqual(node.query_index("age", 30), [])
            self.assertEqual(sorted(node.query_index("age", 31)), ["u1"])

            service.Delete(replication_pb2.KeyRequest(key="u2", timestamp=4), None)
            self.assertEqual(node.query_index("age", 40), [])

            node.db.close()

    def test_index_rebuild_after_restart(self):
        """Ensure index persists when NodeServer is recreated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, index_fields=["age"])
            service = ReplicaService(node)

            service.Put(
                replication_pb2.KeyValue(key="u1", value='{"age": 20}', timestamp=1),
                None,
            )
            service.Put(
                replication_pb2.KeyValue(key="u2", value='{"age": 25}', timestamp=2),
                None,
            )

            service.Put(
                replication_pb2.KeyValue(key="u1", value='{"age": 22}', timestamp=3),
                None,
            )
            service.Delete(replication_pb2.KeyRequest(key="u2", timestamp=4), None)

            # Before closing, verify current index state
            self.assertEqual(sorted(node.query_index("age", 22)), ["u1"])
            self.assertEqual(node.query_index("age", 20), [])
            self.assertEqual(node.query_index("age", 25), [])

            node.db.close()

            # Recreate node pointing to the same directory
            node = NodeServer(db_path=tmpdir, index_fields=["age"])

            # Index should be rebuilt from on-disk data
            self.assertEqual(sorted(node.query_index("age", 22)), ["u1"])
            self.assertEqual(node.query_index("age", 20), [])
            self.assertEqual(node.query_index("age", 25), [])

            node.db.close()


if __name__ == "__main__":
    unittest.main()
