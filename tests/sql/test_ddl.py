import os
import time
import sys
import json
import grpc
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from database.sql.parser import parse_create_table, parse_alter_table
from database.sql.metadata import TableSchema, ColumnDefinition
from database.sql.query_coordinator import QueryCoordinator
from database.clustering.partitioning import compose_key
from database.replication import NodeCluster


def test_parse_create_table_valid():
    schema = parse_create_table("CREATE TABLE users (id INT, name STRING)")
    assert isinstance(schema, TableSchema)
    assert schema.name == "users"
    assert [c.name for c in schema.columns] == ["id", "name"]
    assert [c.data_type for c in schema.columns] == ["int", "string"]


def test_parse_create_table_invalid():
    with pytest.raises(ValueError):
        parse_create_table("CREATE users (id INT)")


def test_execute_create_table(tmp_path):
    cluster = NodeCluster(base_path=tmp_path, num_nodes=1)
    try:
        ddl = "CREATE TABLE users (id INT, name STRING)"
        cluster.nodes[0].client.execute_ddl(ddl)
        time.sleep(0.5)
        assert cluster.get(0, "_meta:table:users") is not None
        tbl_path = os.path.join(tmp_path, "node_0", "users.tbl")
        assert os.path.exists(tbl_path)
    finally:
        cluster.shutdown()


def test_duplicate_table(tmp_path):
    cluster = NodeCluster(base_path=tmp_path, num_nodes=1)
    try:
        ddl = "CREATE TABLE dup (id INT)"
        cluster.nodes[0].client.execute_ddl(ddl)
        time.sleep(0.2)
        with pytest.raises(grpc.RpcError):
            cluster.nodes[0].client.execute_ddl(ddl)
    finally:
        cluster.shutdown()


def test_parse_alter_table():
    table, col = parse_alter_table("ALTER TABLE t ADD COLUMN age INT")
    assert table == "t"
    assert isinstance(col, ColumnDefinition)
    assert col.name == "age"
    assert col.data_type == "int"


def test_alter_table_add_column(tmp_path):
    cluster = NodeCluster(base_path=tmp_path, num_nodes=1)
    try:
        cluster.nodes[0].client.execute_ddl(
            "CREATE TABLE users (id INT PRIMARY KEY, name STRING)"
        )
        time.sleep(0.5)
        cluster.nodes[0].client.execute_ddl(
            "ALTER TABLE users ADD COLUMN age INT"
        )
        time.sleep(0.5)
        val = cluster.get(0, "_meta:table:users")
        schema = TableSchema.from_json(val)
        assert any(c.name == "age" for c in schema.columns)
    finally:
        cluster.shutdown()


def test_alter_table_backward_compat(tmp_path):
    cluster = NodeCluster(base_path=tmp_path, num_nodes=1)
    try:
        cluster.nodes[0].client.execute_ddl(
            "CREATE TABLE users (id INT PRIMARY KEY, name STRING)"
        )
        time.sleep(0.5)
        cluster.nodes[0].client.put(
            compose_key("users", "1", None),
            json.dumps({"id": 1, "name": "a"}),
        )
        time.sleep(0.2)
        cluster.nodes[0].client.execute_ddl(
            "ALTER TABLE users ADD COLUMN age INT"
        )
        time.sleep(0.5)
        cluster.nodes[0].client.put(
            compose_key("users", "2", None),
            json.dumps({"id": 2, "name": "b", "age": 30}),
        )
        time.sleep(0.5)
        qc = QueryCoordinator(cluster.nodes)
        rows = sorted(qc.execute("SELECT * FROM users"), key=lambda r: r["id"])
        assert rows[0].get("age") is None
        assert rows[1].get("age") == 30
    finally:
        cluster.shutdown()
