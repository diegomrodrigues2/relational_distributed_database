import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from database.sql.metadata import ColumnDefinition, IndexDefinition, TableSchema, CatalogManager
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


def test_table_schema_json_roundtrip():
    schema = TableSchema(
        name="users",
        columns=[
            ColumnDefinition("id", "int", primary_key=True),
            ColumnDefinition("name", "str"),
        ],
        indexes=[IndexDefinition("by_name", ["name"])],
    )
    data = schema.to_json()
    loaded = TableSchema.from_json(data)
    assert loaded == schema


def test_save_schema_calls_put(monkeypatch, tmp_path):
    db = SimpleLSMDB(db_path=tmp_path)
    node = DummyNode(db)
    catalog = CatalogManager(node)

    called = {}
    def fake_put(key, value, **kwargs):
        called['key'] = key
        called['value'] = value
    monkeypatch.setattr(node.db, "put", fake_put)

    schema = TableSchema(
        name="items",
        columns=[ColumnDefinition("id", "int", primary_key=True)],
    )
    catalog.save_schema(schema)
    assert called['key'] == "_meta:table:items"
    assert json.loads(called['value'])['name'] == "items"


def test_load_schemas(tmp_path):
    db = SimpleLSMDB(db_path=tmp_path)
    schema1 = TableSchema("t1", [ColumnDefinition("id", "int")])
    schema2 = TableSchema("t2", [ColumnDefinition("id", "int")])
    db.put("_meta:table:t1", schema1.to_json())
    db.put("_meta:table:t2", schema2.to_json())

    node = DummyNode(db)
    catalog = CatalogManager(node)

    assert catalog.get_schema("t1").to_json() == schema1.to_json()
    assert catalog.get_schema("t2").to_json() == schema2.to_json()
