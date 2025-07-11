import json
import tempfile
import unittest

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.replication.replica import replication_pb2

class IndexWalAtomicTest(unittest.TestCase):
    def test_recovery_after_crash_between_wal_and_memtable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, index_fields=["name"])
            service = ReplicaService(node)

            service.Put(
                replication_pb2.KeyValue(key="u1", value=json.dumps({"name": "alice"}), timestamp=1),
                None,
            )

            original_put = node.db.put
            def failing_put(*args, **kwargs):
                raise RuntimeError("fail")
            node.db.put = failing_put
            with self.assertRaises(RuntimeError):
                service.Put(
                    replication_pb2.KeyValue(key="u1", value=json.dumps({"name": "bob"}), timestamp=2),
                    None,
                )
            node.db.put = original_put

            node2 = NodeServer(db_path=tmpdir, index_fields=["name"])
            self.assertEqual(sorted(node2.query_index("name", "bob")), ["u1"])
            self.assertEqual(node2.query_index("name", "alice"), [])
            node2.db.close()

if __name__ == "__main__":
    unittest.main()
