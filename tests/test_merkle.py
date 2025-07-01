import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.lsm.lsm_db import SimpleLSMDB
from database.utils.merkle import merkle_root, compute_segment_hashes, build_merkle_tree
import json
from database.replication.replica.grpc_server import ReplicaService, NodeServer
from database.replication.replica import replication_pb2


class MerkleUtilsTest(unittest.TestCase):
    def test_compute_hashes_after_flush(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = SimpleLSMDB(db_path=tmpdir, max_memtable_size=2)
            db.put("a", "1")
            db.recalc_merkle()
            before = db.segment_hashes["memtable"]
            db.put("b", "2")  # triggers flush
            seg_names = [os.path.basename(p) for _, p, _ in db.sstable_manager.sstable_segments]
            self.assertEqual(len(seg_names), 1)
            hashes = db.segment_hashes
            self.assertNotEqual(before, hashes["memtable"])
            seg_items = [
                (k, v)
                for k, v, _vc in db.get_segment_items(seg_names[0])
                if v != "__TOMBSTONE__"
            ]
            expected = merkle_root(seg_items)
            self.assertEqual(hashes[seg_names[0]], expected)
            db.close()


class FetchUpdatesMerkleTest(unittest.TestCase):
    def test_matching_segment_hashes_skip_transfer(self):
        with tempfile.TemporaryDirectory() as dir_a, tempfile.TemporaryDirectory() as dir_b:
            node_a = NodeServer(db_path=dir_a, node_id="A")
            node_b = NodeServer(db_path=dir_b, node_id="B")
            service_b = ReplicaService(node_b)

            node_a.db.put("k", "v")
            node_b.db.put("k", "v")
            node_a.db.recalc_merkle()
            node_b.db.recalc_merkle()

            trees = []
            for seg in node_a.db.segment_hashes:
                items = [
                    (k, v)
                    for k, v, _vc in node_a.db.get_segment_items(seg)
                    if v != "__TOMBSTONE__"
                ]
                root = build_merkle_tree(items)
                trees.append(replication_pb2.SegmentTree(segment=seg, root=root.to_proto()))
            req = replication_pb2.FetchRequest(
                vector=replication_pb2.VersionVector(items={}),
                ops=[],
                segment_hashes=node_a.db.segment_hashes,
                trees=trees,
            )
            resp = service_b.FetchUpdates(req, None)
            self.assertEqual(len(resp.ops), 0)

            node_a.db.close()
            node_b.db.close()

    def test_different_segment_hashes_send_diffs(self):
        with tempfile.TemporaryDirectory() as dir_a, tempfile.TemporaryDirectory() as dir_b:
            node_a = NodeServer(db_path=dir_a, node_id="A")
            node_b = NodeServer(db_path=dir_b, node_id="B")
            service_b = ReplicaService(node_b)

            node_a.db.put("k1", "v1")
            node_b.db.put("k1", "v1")
            node_b.db.put("k2", "v2")
            node_a.db.recalc_merkle()
            node_b.db.recalc_merkle()

            trees = []
            for seg in node_a.db.segment_hashes:
                items = [
                    (k, v)
                    for k, v, _vc in node_a.db.get_segment_items(seg)
                    if v != "__TOMBSTONE__"
                ]
                root = build_merkle_tree(items)
                trees.append(replication_pb2.SegmentTree(segment=seg, root=root.to_proto()))
            req = replication_pb2.FetchRequest(
                vector=replication_pb2.VersionVector(items={}),
                ops=[],
                segment_hashes=node_a.db.segment_hashes,
                trees=trees,
            )
            resp = service_b.FetchUpdates(req, None)
            keys = sorted(op.key for op in resp.ops)
            self.assertEqual(keys, ["k2"])

            node_a.db.close()
            node_b.db.close()


if __name__ == "__main__":
    unittest.main()
