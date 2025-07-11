import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from database.sql.planner import QueryPlanner
from database.sql.ast import Column, Literal, BinOp, SelectItem, FromClause, SelectQuery
from database.sql.metadata import ColumnDefinition, IndexDefinition, TableSchema, CatalogManager
from database.lsm.lsm_db import SimpleLSMDB
from database.clustering.index_manager import IndexManager


class DummyNode:
    def __init__(self, db):
        self.db = db


def _planner_with_schema(tmp_path, indexes=None):
    db = SimpleLSMDB(db_path=tmp_path)
    node = DummyNode(db)
    catalog = CatalogManager(node)
    schema = TableSchema(
        name="users",
        columns=[
            ColumnDefinition("id", "int"),
            ColumnDefinition("age", "int"),
            ColumnDefinition("city", "str"),
        ],
        indexes=indexes or [],
    )
    catalog.schemas["users"] = schema
    index_manager = IndexManager([c for idx in (indexes or []) for c in idx.columns])
    planner = QueryPlanner(db, catalog, index_manager)
    return planner, db


def test_plan_selects_index(tmp_path):
    idx = IndexDefinition("by_city", ["city"])
    planner, db = _planner_with_schema(tmp_path, [idx])
    query = SelectQuery(
        select_items=[SelectItem(Column("*"))],
        from_clause=FromClause(table="users"),
        where_clause=BinOp(left=Column("city"), op="EQ", right=Literal("NY")),
    )
    plan = planner.create_plan(query)
    from database.sql.execution import IndexScanNode
    assert isinstance(plan, IndexScanNode)
    db.close()


def test_plan_falls_back_to_seq_scan(tmp_path):
    planner, db = _planner_with_schema(tmp_path)
    query = SelectQuery(
        select_items=[SelectItem(Column("*"))],
        from_clause=FromClause(table="users"),
        where_clause=BinOp(left=Column("age"), op="GT", right=Literal(30)),
    )
    plan = planner.create_plan(query)
    from database.sql.execution import SeqScanNode
    assert isinstance(plan, SeqScanNode)
    db.close()
