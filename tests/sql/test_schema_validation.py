import json
import time
import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from database.sql.metadata import ColumnDefinition, TableSchema
from database.replication.replica.grpc_server import NodeServer
from database.replication.replica.client import GRPCReplicaClient
from database.clustering.partitioning import compose_key
import grpc


def test_table_schema_validate_row():
    schema = TableSchema(
        name="users",
        columns=[
            ColumnDefinition("id", "int", nullable=False),
            ColumnDefinition("name", "string"),
        ],
    )

    # valid row
    schema.validate_row({"id": 1, "name": "a"})

    # missing required column
    with pytest.raises(ValueError):
        schema.validate_row({"name": "a"})

    # wrong type
    with pytest.raises(ValueError):
        schema.validate_row({"id": "x", "name": "a"})

    # extra column
    with pytest.raises(ValueError):
        schema.validate_row({"id": 1, "name": "a", "age": 5})


def test_put_schema_validation(tmp_path):
    node = NodeServer(db_path=tmp_path, port=9120, node_id="A", peers=[])
    node.server.start()
    try:
        client = GRPCReplicaClient("localhost", 9120)
        client.execute_ddl("CREATE TABLE users (id INT PRIMARY KEY, name STRING)")
        time.sleep(0.5)
        key = compose_key("users", "1", None)
        client.put(key, json.dumps({"id": 1, "name": "ok"}))
        time.sleep(0.2)
        assert json.loads(client.get(key)[0][0])["id"] == 1

        with pytest.raises(grpc.RpcError) as exc:
            client.put(compose_key("users", "2", None), json.dumps({"name": "bad"}))
        assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT

        with pytest.raises(grpc.RpcError) as exc:
            client.put(compose_key("users", "3", None), json.dumps({"id": "x", "name": "bad"}))
        assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT

        with pytest.raises(grpc.RpcError) as exc:
            client.put(compose_key("users", "4", None), json.dumps({"id": 4, "name": "b", "age": 10}))
        assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        client.close()
    finally:
        node.server.stop(0).wait()
        node.db.close()
