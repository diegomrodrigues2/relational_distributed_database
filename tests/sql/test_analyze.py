import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.sql.parser import parse_sql
from database.sql.planner import QueryPlanner
from database.sql.metadata import ColumnDefinition, TableSchema


def _setup(tmp_path):
    node = NodeServer(db_path=tmp_path)
    node.db._start_compaction_async = lambda: None
    service = ReplicaService(node)
    schema = TableSchema(
        name="users",
        columns=[
            ColumnDefinition("id", "int", primary_key=True),
            ColumnDefinition("city", "string"),
        ],
    )
    node.catalog.save_schema(schema)
    planner = QueryPlanner(node.db, node.catalog, node.index_manager, service=service)
    return node, service, planner


def test_analyze_table(tmp_path):
    node, service, planner = _setup(tmp_path)
    try:
        planner.create_plan(parse_sql("INSERT INTO users (id, city) VALUES (1, 'NY')")).execute()
        planner.create_plan(parse_sql("INSERT INTO users (id, city) VALUES (2, 'SF')")).execute()
        planner.create_plan(parse_sql("INSERT INTO users (id, city) VALUES (3, 'NY')")).execute()
        planner.create_plan(parse_sql("ANALYZE TABLE users")).execute()
        tbl = node.catalog.get_table_stats("users")
        col = node.catalog.get_column_stats("users", "city")
        assert tbl is not None and tbl.num_rows == 3
        assert col is not None and col.num_distinct == 2
    finally:
        node.db.close()
