import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.replication.replica import replication_pb2


class AtomicTransferTest(unittest.TestCase):
    def test_concurrent_transfer(self):
        """Concurrent transfers should execute atomically."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            node.db.put("from", "10", timestamp=1)
            node.db.put("to", "0", timestamp=1)

            def do_transfer():
                req = replication_pb2.TransferRequest(
                    from_key="from", to_key="to", amount=1
                )
                service.Transfer(req, None)

            threads = [threading.Thread(target=do_transfer) for _ in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(node.db.get("from"), "5")
            self.assertEqual(node.db.get("to"), "5")

            node.db.close()


if __name__ == "__main__":
    unittest.main()

