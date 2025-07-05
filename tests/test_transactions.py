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
                self.assertEqual(len(node.active_transactions[tx_id]["ops"]), 10)

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
            with self.assertRaises(RuntimeError):
                service.Get(replication_pb2.KeyRequest(key="k", tx_id=tx2), None)

            service.CommitTransaction(
                replication_pb2.TransactionControl(tx_id=tx1), None
            )
            resp3 = service.Get(replication_pb2.KeyRequest(key="k", tx_id=""), None)
            self.assertEqual(len(resp3.values), 0)

            node.db.close()

    def test_no_dirty_reads(self):
        """A transaction should not read uncommitted writes from others."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            # initial committed value
            node.db.put("key", "old", timestamp=1)

            tx1 = service.BeginTransaction(replication_pb2.Empty(), None).id
            service.Put(
                replication_pb2.KeyValue(key="key", value="new_value", timestamp=2, tx_id=tx1),
                None,
            )

            tx2 = service.BeginTransaction(replication_pb2.Empty(), None).id
            with self.assertRaises(RuntimeError):
                service.Get(replication_pb2.KeyRequest(key="key", tx_id=tx2), None)

            service.CommitTransaction(
                replication_pb2.TransactionControl(tx_id=tx1), None
            )
            resp2 = service.Get(replication_pb2.KeyRequest(key="key", tx_id=tx2), None)
            self.assertEqual(len(resp2.values), 1)
            self.assertEqual(resp2.values[0].value, "new_value")

            node.db.close()

    def test_list_transactions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            tx1 = service.BeginTransaction(replication_pb2.Empty(), None).id
            tx2 = service.BeginTransaction(replication_pb2.Empty(), None).id

            resp = service.ListTransactions(replication_pb2.Empty(), None)
            ids = set(resp.tx_ids)
            self.assertIn(tx1, ids)
            self.assertIn(tx2, ids)
            self.assertEqual(len(ids), 2)

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

    def test_write_skew_with_lock_row(self):
        """Prevent write skew by materializing conflicts with a lock row."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            node.db.put("doc1", "on", timestamp=1)
            node.db.put("doc2", "on", timestamp=1)
            node.db.put("shift_locks:1234", "unlocked", timestamp=1)

            start = threading.Barrier(2)

            def leave(doc_key):
                start.wait()
                tx_id = service.BeginTransaction(replication_pb2.Empty(), None).id

                while True:
                    try:
                        service.GetForUpdate(
                            replication_pb2.KeyRequest(
                                key="shift_locks:1234", tx_id=tx_id
                            ),
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

    def test_write_skew_with_get_for_update(self):
        """Ensure write skew is avoided by acquiring row locks."""
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

            # Business invariant: at least one doctor must remain on call.
            self.assertEqual(remaining, 1)

            node.db.close()

    def test_atomic_increment_concurrent_clients(self):
        """Concurrent increments should not result in lost updates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            def worker():
                for _ in range(100):
                    service.Increment(
                        replication_pb2.IncrementRequest(key="cnt", amount=1),
                        None,
                    )

            t1 = threading.Thread(target=worker)
            t2 = threading.Thread(target=worker)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            self.assertEqual(node.db.get("cnt"), str(200))

            node.db.close()

    def test_lost_update_without_get_for_update(self):
        """Concurrent read-modify-write without locks loses an update."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            node.db.put("cnt", "0", timestamp=1)

            barrier = threading.Barrier(2)

            def worker():
                barrier.wait()
                resp = service.Get(
                    replication_pb2.KeyRequest(key="cnt", tx_id=""), None
                )
                val = int(resp.values[0].value) if resp.values else 0
                barrier.wait()
                ts = node.clock.tick()
                service.Put(
                    replication_pb2.KeyValue(
                        key="cnt", value=str(val + 1), timestamp=ts
                    ),
                    None,
                )

            t1 = threading.Thread(target=worker)
            t2 = threading.Thread(target=worker)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            self.assertEqual(node.db.get("cnt"), "1")

            node.db.close()

    def test_get_for_update_prevents_lost_update(self):
        """Explicit locking with GetForUpdate prevents lost updates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            node.db.put("cnt", "0", timestamp=1)

            start = threading.Barrier(2)

            def worker():
                start.wait()
                tx_id = service.BeginTransaction(replication_pb2.Empty(), None).id
                while True:
                    try:
                        resp = service.GetForUpdate(
                            replication_pb2.KeyRequest(key="cnt", tx_id=tx_id),
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

                val = int(resp.values[0].value) if resp.values else 0
                ts = node.clock.tick()
                service.Put(
                    replication_pb2.KeyValue(
                        key="cnt",
                        value=str(val + 1),
                        timestamp=ts,
                        tx_id=tx_id,
                    ),
                    None,
                )
                service.CommitTransaction(
                    replication_pb2.TransactionControl(tx_id=tx_id), None
                )

            t1 = threading.Thread(target=worker)
            t2 = threading.Thread(target=worker)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            self.assertEqual(node.db.get("cnt"), "2")

            node.db.close()

    def test_snapshot_isolation_detects_lost_update(self):
        """Snapshot isolation should abort conflicting updates automatically."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            service = ReplicaService(node)

            node.db.put("cnt", "0", timestamp=1)

            tx1 = service.BeginTransaction(replication_pb2.Empty(), None).id
            tx2 = service.BeginTransaction(replication_pb2.Empty(), None).id

            r1 = service.Get(replication_pb2.KeyRequest(key="cnt", tx_id=tx1), None)
            r2 = service.Get(replication_pb2.KeyRequest(key="cnt", tx_id=tx2), None)
            v1 = int(r1.values[0].value) if r1.values else 0
            v2 = int(r2.values[0].value) if r2.values else 0

            ts1 = node.clock.tick()
            with self.assertRaises(RuntimeError):
                service.Put(
                    replication_pb2.KeyValue(key="cnt", value=str(v1 + 1), timestamp=ts1, tx_id=tx1),
                    None,
                )
            ts2 = node.clock.tick()
            with self.assertRaises(RuntimeError):
                service.Put(
                    replication_pb2.KeyValue(key="cnt", value=str(v2 + 1), timestamp=ts2, tx_id=tx2),
                    None,
                )

            service.AbortTransaction(replication_pb2.TransactionControl(tx_id=tx1), None)
            service.AbortTransaction(replication_pb2.TransactionControl(tx_id=tx2), None)

            self.assertEqual(node.db.get("cnt"), "0")

            node.db.close()


if __name__ == "__main__":
    unittest.main()
