import os
import sys
import json
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from database.replication.replica.grpc_server import NodeServer, ReplicaService
from database.sql.parser import parse_sql
from database.sql.planner import QueryPlanner
from database.sql.metadata import ColumnDefinition, TableSchema
from database.sql.serialization import RowSerializer


def _setup(tmp_path):
    node = NodeServer(db_path=tmp_path)
    node.db._start_compaction_async = lambda: None
    service = ReplicaService(node)
    schema = TableSchema(
        name="users",
        columns=[
            ColumnDefinition("id", "int", primary_key=True),
            ColumnDefinition("name", "string"),
        ],
    )
    node.catalog.save_schema(schema)
    planner = QueryPlanner(node.db, node.catalog, node.index_manager, service=service)
    return node, service, planner


def test_insert_update_delete(tmp_path):
    node, service, planner = _setup(tmp_path)
    try:
        plan = planner.create_plan(parse_sql("INSERT INTO users (id, name) VALUES (1, 'a')"))
        list(plan.execute())

        rows = list(planner.create_plan(parse_sql("SELECT * FROM users")).execute())
        assert rows == [{"id": 1, "name": "a"}]

        planner.create_plan(parse_sql("UPDATE users SET name='b' WHERE id = 1")).execute()
        rows = list(planner.create_plan(parse_sql("SELECT name FROM users WHERE id = 1")).execute())
        assert rows[0]["name"] == "b"

        planner.create_plan(parse_sql("DELETE FROM users WHERE id = 1")).execute()
        rows = list(planner.create_plan(parse_sql("SELECT * FROM users WHERE id = 1")).execute())
        assert rows == []
    finally:
        node.db.close()


def test_delete_writes_tombstone(tmp_path):
    node, service, planner = _setup(tmp_path)
    try:
        planner.create_plan(parse_sql("INSERT INTO users (id, name) VALUES (1, 'a')")).execute()
        node.db._flush_memtable_to_sstable()
        planner.create_plan(parse_sql("DELETE FROM users WHERE id = 1")).execute()
        node.db._flush_memtable_to_sstable()
        seg = node.db.sstable_manager.sstable_segments[-1]
        path = seg[1]
        found = False
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                if data.get("key") == "users||1" and data.get("value") == "__TOMBSTONE__":
                    found = True
                    break
        assert found
    finally:
        node.db.close()
