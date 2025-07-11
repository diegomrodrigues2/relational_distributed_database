import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

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
from database.sql.metadata import (
    ColumnDefinition,
    IndexDefinition,
    TableSchema,
    CatalogManager,
)
from database.sql.execution import SeqScanNode, IndexScanNode
from database.lsm.lsm_db import SimpleLSMDB


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


def test_planner_selects_index(tmp_path):
    db = SimpleLSMDB(db_path=tmp_path)
    node = DummyNode(db)
    catalog = CatalogManager(node)
    schema = TableSchema(
        name="users",
        columns=[
            ColumnDefinition("id", "int"),
            ColumnDefinition("city", "string"),
        ],
        indexes=[IndexDefinition("by_city", ["city"])],
    )
    catalog.save_schema(schema)

    planner = QueryPlanner(db, catalog, index_manager=object())
    query = parse_sql("SELECT id FROM users WHERE city = 'NY'")

    plan = planner.create_plan(query)
    assert isinstance(plan, IndexScanNode)
    db.close()


def test_planner_seq_scan_when_no_index(tmp_path):
    db = SimpleLSMDB(db_path=tmp_path)
    node = DummyNode(db)
    catalog = CatalogManager(node)
    schema = TableSchema(
        name="users",
        columns=[
            ColumnDefinition("id", "int"),
            ColumnDefinition("age", "int"),
        ]
    )
    catalog.save_schema(schema)

    planner = QueryPlanner(db, catalog, index_manager=object())
    query = parse_sql("SELECT id FROM users WHERE age > 30")

    plan = planner.create_plan(query)
    assert isinstance(plan, SeqScanNode)
    db.close()
