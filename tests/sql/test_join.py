from __future__ import annotations

import os
import sys
import base64
import time
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Stub out replication modules
dummy_rep = types.ModuleType("database.replication")
dummy_rep.NodeCluster = object
dummy_rep.ClusterNode = object
dummy_rep.__path__ = []
sub = types.ModuleType("database.replication.replica")
sub.replication_pb2 = types.ModuleType("database.replication.replica.replication_pb2")
sys.modules.setdefault("database.replication.replica", sub)
sys.modules.setdefault("database.replication", dummy_rep)

from database.sql.parser import parse_sql
from database.sql.planner import QueryPlanner
from database.sql.execution import SeqScanNode
from database.sql.metadata import ColumnDefinition, TableSchema, CatalogManager
from database.lsm.lsm_db import SimpleLSMDB
from database.sql.serialization import RowSerializer


def _enc(row: dict) -> str:
    return base64.b64encode(RowSerializer.dumps(row)).decode("ascii")


class DummyNode:
    def __init__(self, db):
        self.db = db
        self.replication_log = {}

    def next_op_id(self):
        return "n1:1"

    def save_replication_log(self):
        pass

    def replicate(self, *args, **kwargs):
        pass


def _setup_db(tmp_path) -> tuple[SimpleLSMDB, CatalogManager]:
    db = SimpleLSMDB(db_path=tmp_path, max_memtable_size=1000)
    node = DummyNode(db)
    catalog = CatalogManager(node)
    schema_dept = TableSchema(
        name="dept",
        columns=[
            ColumnDefinition("dept_id", "int"),
            ColumnDefinition("dept_name", "string"),
        ],
    )
    schema_student = TableSchema(
        name="student",
        columns=[
            ColumnDefinition("sid", "int"),
            ColumnDefinition("dept_id", "int"),
            ColumnDefinition("sname", "string"),
        ],
    )
    catalog.save_schema(schema_dept)
    catalog.save_schema(schema_student)
    return db, catalog


def test_nested_loop_join_correct(tmp_path):
    db, catalog = _setup_db(tmp_path)
    db.put("dept||1", _enc({"dept_id": 1, "dept_name": "cs"}))
    db.put("dept||2", _enc({"dept_id": 2, "dept_name": "math"}))
    db.put("student||1", _enc({"sid": 1, "dept_id": 1, "sname": "a"}))
    db.put("student||2", _enc({"sid": 2, "dept_id": 2, "sname": "b"}))
    db.put("student||3", _enc({"sid": 3, "dept_id": 1, "sname": "c"}))
    db._flush_memtable_to_sstable()

    planner = QueryPlanner(db, catalog, index_manager=object())
    q = parse_sql("SELECT * FROM student JOIN dept ON student.dept_id = dept.dept_id")
    plan = planner.create_plan(q)
    rows = sorted(list(plan.execute()), key=lambda r: r["sid"])
    names = [r["dept_name"] for r in rows]
    assert names == ["cs", "math", "cs"]
    db.close()


def test_join_empty_result(tmp_path):
    db, catalog = _setup_db(tmp_path)
    db.put("dept||1", _enc({"dept_id": 1, "dept_name": "cs"}))
    db.put("student||1", _enc({"sid": 1, "dept_id": 2, "sname": "a"}))
    db._flush_memtable_to_sstable()

    planner = QueryPlanner(db, catalog, index_manager=object())
    q = parse_sql("SELECT * FROM student JOIN dept ON student.dept_id = dept.dept_id")
    plan = planner.create_plan(q)
    rows = list(plan.execute())
    assert rows == []
    db.close()


def test_batched_join_faster_than_naive(tmp_path):
    db, catalog = _setup_db(tmp_path)
    for i in range(200):
        db.put(f"dept||{i}", _enc({"dept_id": i, "dept_name": f"d{i}"}))
        db.put(f"student||{i}", _enc({"sid": i, "dept_id": i, "sname": f"s{i}"}))
    db._flush_memtable_to_sstable()

    planner = QueryPlanner(db, catalog, index_manager=object())
    q = parse_sql("SELECT * FROM student JOIN dept ON student.dept_id = dept.dept_id")
    plan = planner.create_plan(q)

    start = time.perf_counter()
    batched_rows = list(plan.execute())
    batched_time = time.perf_counter() - start

    def naive():
        res = []
        outer = SeqScanNode(db, "student")
        for o in outer.execute():
            val = o["dept_id"]
            inner = SeqScanNode(db, "dept")
            for irow in inner.execute():
                if irow["dept_id"] == val:
                    res.append({**o, **irow})
        return res

    start = time.perf_counter()
    naive_rows = naive()
    naive_time = time.perf_counter() - start

    assert len(batched_rows) == len(naive_rows)
    assert batched_time < naive_time
    db.close()
