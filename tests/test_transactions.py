import os
import sys
import tempfile
import unittest
import threading
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.replication.replica import replication_pb2


class TransactionTest(unittest.TestCase):
    def test_commit_and_abort(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            # start transaction and buffer operations
            tx_id = service.BeginTransaction(replication_pb2.Empty(), None).id
            service.Put(
                replication_pb2.KeyValue(
                    key="k1", value="v1", timestamp=1, tx_id=tx_id
                ),
                None,
            )
            node.db.put("d", "val", timestamp=1)
            service.Delete(
                replication_pb2.KeyRequest(key="d", timestamp=2, tx_id=tx_id), None
            )

            # nothing applied yet
            self.assertIsNone(node.db.get("k1"))
            self.assertEqual(node.db.get("d"), "val")

            service.CommitTransaction(
                replication_pb2.TransactionControl(tx_id=tx_id), None
            )

            self.assertEqual(node.db.get("k1"), "v1")
            self.assertIsNone(node.db.get("d"))

            # abort should discard pending ops
            tx2 = service.BeginTransaction(replication_pb2.Empty(), None).id
            service.Put(
                replication_pb2.KeyValue(key="k2", value="v2", timestamp=3, tx_id=tx2),
                None,
            )
            service.AbortTransaction(
                replication_pb2.TransactionControl(tx_id=tx2), None
            )
            self.assertIsNone(node.db.get("k2"))

            node.db.close()

    def test_concurrent_transaction_ops(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            tx_id = service.BeginTransaction(replication_pb2.Empty(), None).id

            def do_put():
                service.Put(
                    replication_pb2.KeyValue(
                        key="k",
                        value="v",
                        timestamp=1,
                        tx_id=tx_id,
                    ),
                    None,
                )

            def do_del():
                service.Delete(
                    replication_pb2.KeyRequest(
                        key="d",
                        timestamp=1,
                        tx_id=tx_id,
                    ),
                    None,
                )

            threads = [threading.Thread(target=do_put) for _ in range(5)] + [
                threading.Thread(target=do_del) for _ in range(5)
            ]

            for t in threads:
                t.start()
            for t in threads:
                t.join()

            with node._tx_lock:
                self.assertEqual(len(node.active_transactions[tx_id]), 10)

            service.CommitTransaction(
                replication_pb2.TransactionControl(tx_id=tx_id), None
            )

            self.assertEqual(node.db.get("k"), "v")
            self.assertIsNone(node.db.get("d"))

            node.db.close()

    def test_dirty_read_prevention(self):
        """Ensure reads ignore uncommitted changes from other transactions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            # existing committed value
            node.db.put("k", "v0", timestamp=1)

            tx1 = service.BeginTransaction(replication_pb2.Empty(), None).id
            service.Delete(
                replication_pb2.KeyRequest(key="k", timestamp=2, tx_id=tx1), None
            )

            # another transaction should still see committed value
            resp = service.Get(replication_pb2.KeyRequest(key="k", tx_id=""), None)
            self.assertEqual(len(resp.values), 1)
            self.assertEqual(resp.values[0].value, "v0")

            tx2 = service.BeginTransaction(replication_pb2.Empty(), None).id
            resp2 = service.Get(replication_pb2.KeyRequest(key="k", tx_id=tx2), None)
            self.assertEqual(len(resp2.values), 1)
            self.assertEqual(resp2.values[0].value, "v0")

            service.CommitTransaction(
                replication_pb2.TransactionControl(tx_id=tx1), None
            )
            resp3 = service.Get(replication_pb2.KeyRequest(key="k", tx_id=""), None)
            self.assertEqual(len(resp3.values), 0)

            node.db.close()

    def test_get_for_update_locking(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            node.db.put("k", "v0", timestamp=1)

            tx1 = service.BeginTransaction(replication_pb2.Empty(), None).id
            tx2 = service.BeginTransaction(replication_pb2.Empty(), None).id

            service.GetForUpdate(replication_pb2.KeyRequest(key="k", tx_id=tx1), None)

            with self.assertRaises(RuntimeError):
                service.GetForUpdate(
                    replication_pb2.KeyRequest(key="k", tx_id=tx2), None
                )

            service.CommitTransaction(
                replication_pb2.TransactionControl(tx_id=tx1), None
            )

            service.GetForUpdate(replication_pb2.KeyRequest(key="k", tx_id=tx2), None)
            service.AbortTransaction(
                replication_pb2.TransactionControl(tx_id=tx2), None
            )

            node.db.close()

    def test_prevent_write_skew_using_get_for_update(self):
        """Ensure application invariant holds using row locks."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            node.db.put("doc1", "on", timestamp=1)
            node.db.put("doc2", "on", timestamp=1)

            start = threading.Barrier(2)

            def leave(doc_key):
                start.wait()
                tx_id = service.BeginTransaction(replication_pb2.Empty(), None).id

                while True:
                    try:
                        service.GetForUpdate(
                            replication_pb2.KeyRequest(key="doc1", tx_id=tx_id),
                            None,
                        )
                        service.GetForUpdate(
                            replication_pb2.KeyRequest(key="doc2", tx_id=tx_id),
                            None,
                        )
                        break
                    except RuntimeError:
                        service.AbortTransaction(
                            replication_pb2.TransactionControl(tx_id=tx_id), None
                        )
                        time.sleep(0.01)
                        tx_id = service.BeginTransaction(
                            replication_pb2.Empty(), None
                        ).id

                resp1 = service.Get(
                    replication_pb2.KeyRequest(key="doc1", tx_id=tx_id), None
                )
                resp2 = service.Get(
                    replication_pb2.KeyRequest(key="doc2", tx_id=tx_id), None
                )
                count = (1 if resp1.values else 0) + (1 if resp2.values else 0)
                if count > 1:
                    service.Delete(
                        replication_pb2.KeyRequest(
                            key=doc_key, timestamp=2, tx_id=tx_id
                        ),
                        None,
                    )
                    service.CommitTransaction(
                        replication_pb2.TransactionControl(tx_id=tx_id), None
                    )
                else:
                    service.AbortTransaction(
                        replication_pb2.TransactionControl(tx_id=tx_id), None
                    )

            t1 = threading.Thread(target=leave, args=("doc1",))
            t2 = threading.Thread(target=leave, args=("doc2",))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            remaining = 0
            if node.db.get("doc1") is not None:
                remaining += 1
            if node.db.get("doc2") is not None:
                remaining += 1

            self.assertEqual(remaining, 1)

            node.db.close()


if __name__ == "__main__":
    unittest.main()
