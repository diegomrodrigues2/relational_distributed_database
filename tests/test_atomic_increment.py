import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.replication.replica import replication_pb2


class AtomicIncrementTest(unittest.TestCase):
    def test_concurrent_increment(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            def inc():
                req = replication_pb2.IncrementRequest(key="cnt", amount=1)
                service.Increment(req, None)

            threads = [threading.Thread(target=inc) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(node.db.get("cnt"), "10")
            node.db.close()


if __name__ == "__main__":
    unittest.main()
