import os
import sys
import base64

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import types

dummy_rep = types.ModuleType("database.replication")
dummy_rep.NodeCluster = object
dummy_rep.ClusterNode = object
dummy_rep.__path__ = []
sub = types.ModuleType("database.replication.replica")
sub.replication_pb2 = types.ModuleType("database.replication.replica.replication_pb2")
sys.modules.setdefault("database.replication.replica", sub)
sys.modules.setdefault("database.replication", dummy_rep)

from database.lsm.lsm_db import SimpleLSMDB
from database.sql.execution import MergingIterator, SeqScanNode, IndexScanNode
from database.clustering.index_manager import IndexManager
import time
from database.sql.ast import Column, Literal, BinOp
from database.sql.serialization import RowSerializer


def _enc(row: dict) -> str:
    return base64.b64encode(RowSerializer.dumps(row)).decode("ascii")


def _setup_db(tmp_path) -> SimpleLSMDB:
    db = SimpleLSMDB(db_path=tmp_path, max_memtable_size=10)
    return db


def test_merging_iterator(tmp_path):
    db = _setup_db(tmp_path)
    db.put("t||1", _enc({"id": 1}))
    db.put("t||3", _enc({"id": 3}))
    db._flush_memtable_to_sstable()
    db.put("t||2", _enc({"id": 2}))
    db.put("t||4", _enc({"id": 4}))

    iters = []
    prefix = "t||"
    iters.append([i for i in db.get_segment_items("memtable") if i[0].startswith(prefix)])
    for _, path, _ in db.sstable_manager.sstable_segments:
        seg_id = os.path.basename(path)
        iters.append([i for i in db.get_segment_items(seg_id) if i[0].startswith(prefix)])

    merge = MergingIterator(*iters)
    keys = [k for k, _ in merge]
    assert keys == ["t||1", "t||2", "t||3", "t||4"]
    db.close()


def _load_rows(db: SimpleLSMDB, table: str) -> list[dict]:
    node = SeqScanNode(db, table)
    return list(node.execute())


def test_seq_scan_no_filter(tmp_path):
    db = _setup_db(tmp_path)
    db.put("users||1", _enc({"id": 1, "age": 20}))
    db.put("users||3", _enc({"id": 3, "age": 30}))
    db._flush_memtable_to_sstable()
    db.put("users||2", _enc({"id": 2, "age": 25}))
    db.put("users||4", _enc({"id": 4, "age": 40}))

    rows = _load_rows(db, "users")
    ids = sorted(r["id"] for r in rows)
    assert ids == [1, 2, 3, 4]
    db.close()


def test_seq_scan_with_filter(tmp_path):
    db = _setup_db(tmp_path)
    db.put("users||1", _enc({"id": 1, "age": 20}))
    db.put("users||3", _enc({"id": 3, "age": 30}))
    db._flush_memtable_to_sstable()
    db.put("users||2", _enc({"id": 2, "age": 25}))
    db.put("users||4", _enc({"id": 4, "age": 40}))

    expr = BinOp(left=Column("age"), op="GTE", right=Literal(30))
    node = SeqScanNode(db, "users", where_clause=expr)
    rows = list(node.execute())
    ids = sorted(r["id"] for r in rows)
    assert ids == [3, 4]
    db.close()


def test_index_scan_matches_seq_scan(tmp_path):
    db = _setup_db(tmp_path)
    index = IndexManager(["age"])
    db.put("users||1", _enc({"id": 1, "age": 20}))
    index.add_record("users||1", _enc({"id": 1, "age": 20}))
    db.put("users||3", _enc({"id": 3, "age": 30}))
    index.add_record("users||3", _enc({"id": 3, "age": 30}))
    db._flush_memtable_to_sstable()
    db.put("users||2", _enc({"id": 2, "age": 25}))
    index.add_record("users||2", _enc({"id": 2, "age": 25}))
    db.put("users||4", _enc({"id": 4, "age": 40}))
    index.add_record("users||4", _enc({"id": 4, "age": 40}))

    expr = BinOp(left=Column("age"), op="EQ", right=Literal(30))
    seq = SeqScanNode(db, "users", where_clause=expr)
    idx = IndexScanNode(db, index, "users", "age", 30)

    seq_rows = sorted(list(seq.execute()), key=lambda r: r["id"])
    idx_rows = sorted(list(idx.execute()), key=lambda r: r["id"])
    assert idx_rows == seq_rows
    db.close()


def test_index_scan_faster_than_seq_scan(tmp_path):
    db = SimpleLSMDB(db_path=tmp_path, max_memtable_size=10000)
    index = IndexManager(["age"])

    for i in range(10000):
        row = {"id": i, "age": i}
        val = _enc(row)
        key = f"users||{i}"
        db.put(key, val)
        index.add_record(key, val)
    db._flush_memtable_to_sstable()

    expr = BinOp(left=Column("age"), op="EQ", right=Literal(9999))
    seq = SeqScanNode(db, "users", where_clause=expr)
    start = time.perf_counter()
    seq_rows = list(seq.execute())
    seq_time = time.perf_counter() - start

    idx = IndexScanNode(db, index, "users", "age", 9999)
    start = time.perf_counter()
    idx_rows = list(idx.execute())
    idx_time = time.perf_counter() - start

    assert idx_rows == seq_rows
    assert idx_time < seq_time / 10
    db.close()

