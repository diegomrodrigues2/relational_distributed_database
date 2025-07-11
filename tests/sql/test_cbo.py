import os
import sys
import base64
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# stub replication modules
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
from database.sql.metadata import ColumnDefinition, IndexDefinition, TableSchema, CatalogManager
from database.sql.serialization import RowSerializer
from database.clustering.index_manager import IndexManager
from database.lsm.lsm_db import SimpleLSMDB


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


def _setup(tmp_path, distinct_vals):
    db = SimpleLSMDB(db_path=tmp_path, max_memtable_size=1000)
    node = DummyNode(db)
    catalog = CatalogManager(node)
    index = IndexManager(["val"])
    schema = TableSchema(
        name="users",
        columns=[ColumnDefinition("id", "int"), ColumnDefinition("val", "int")],
        indexes=[IndexDefinition("by_val", ["val"])],
    )
    catalog.save_schema(schema)
    planner = QueryPlanner(db, catalog, index_manager=index)
    for i in range(1000):
        val = i if distinct_vals == 1000 else i % distinct_vals
        row = {"id": i, "val": val}
        enc = _enc(row)
        key = f"users||{i}"
        db.put(key, enc)
        index.add_record(key, enc)
    db._flush_memtable_to_sstable()
    # gather statistics
    planner.create_plan(parse_sql("ANALYZE TABLE users")).execute()
    return planner, db


def test_high_selectivity_uses_index(tmp_path):
    planner, db = _setup(tmp_path, 1000)
    try:
        q = parse_sql("SELECT id FROM users WHERE val = 500")
        plan = planner.create_plan(q)
        from database.sql.execution import IndexScanNode
        assert isinstance(plan, IndexScanNode)
    finally:
        db.close()


def test_low_selectivity_seq_scan(tmp_path):
    planner, db = _setup(tmp_path, 2)
    try:
        q = parse_sql("SELECT id FROM users WHERE val = 1")
        plan = planner.create_plan(q)
        from database.sql.execution import SeqScanNode
        assert isinstance(plan, SeqScanNode)
    finally:
        db.close()
