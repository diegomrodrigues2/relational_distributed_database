import os
import sys
import tempfile
import unittest
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replica.grpc_server import NodeServer, ReplicaService
from replica import replication_pb2
from vector_clock import VectorClock


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


class FetchUpdatesTest(unittest.TestCase):
    def test_fetch_updates_apply_and_return(self):
        with tempfile.TemporaryDirectory() as dir_a, tempfile.TemporaryDirectory() as dir_b:
            node_a = NodeServer(db_path=dir_a, node_id="A")
            node_b = NodeServer(db_path=dir_b, node_id="B")
            service_a = ReplicaService(node_a)

            # Operation from node B to be applied on node A
            op_b = replication_pb2.Operation(
                key="k1",
                value="v1",
                timestamp=1,
                node_id="B",
                op_id="B:1",
                delete=False,
            )

            # Node A has a pending operation to send back
            node_a.replication_log["A:1"] = ("k2", "v2", 2)

            req = replication_pb2.FetchRequest(
                vector=replication_pb2.VersionVector(items={}),
                ops=[op_b],
            )

            resp = service_a.FetchUpdates(req, None)

            self.assertEqual(node_a.db.get("k1"), "v1")
            self.assertEqual(len(resp.ops), 1)
            self.assertEqual(resp.ops[0].key, "k2")
            self.assertEqual(resp.ops[0].op_id, "A:1")

            node_a.db.close()
            node_b.db.close()


class AntiEntropyLoopTest(unittest.TestCase):
    def test_anti_entropy_loop_syncs_nodes(self):
        with tempfile.TemporaryDirectory() as dir_a, tempfile.TemporaryDirectory() as dir_b:
            node_a = NodeServer(db_path=dir_a, node_id="A", peers=[])
            node_b = NodeServer(db_path=dir_b, node_id="B", peers=[])
            service_b = ReplicaService(node_b)

            node_b.replication_log["B:1"] = ("k", "v", 5)

            class FakeClient:
                host = "fake"
                port = 0
                def fetch_updates(self, last_seen, ops=None, segment_hashes=None):
                    vv = replication_pb2.VersionVector(items=last_seen)
                    req = replication_pb2.FetchRequest(vector=vv, ops=ops or [], segment_hashes=segment_hashes or {})
                    return service_b.FetchUpdates(req, None)

                def close(self):
                    pass

            node_a.peer_clients = [FakeClient()]
            node_a.anti_entropy_interval = 0.1
            node_a._start_anti_entropy_thread()

            time.sleep(0.3)
            node_a._anti_entropy_stop.set()
            node_a._anti_entropy_thread.join()

            self.assertEqual(node_a.db.get("k"), "v")

            node_a.db.close()
            node_b.db.close()


if __name__ == "__main__":
    unittest.main()
