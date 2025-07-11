import os
import time
import sys
import grpc
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from database.sql.parser import parse_create_table
from database.sql.metadata import TableSchema, ColumnDefinition
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
