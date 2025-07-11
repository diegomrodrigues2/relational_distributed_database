import os
import sys
import json
import time
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from database.clustering.partitioning import compose_key
from database.sql.serialization import RowSerializer
from database.replication.replica.grpc_server import NodeServer
from database.replication.replica.client import GRPCReplicaClient


def test_compose_key_with_table():
    assert compose_key("users", "123", "profile") == "users||123|profile"


def test_row_serializer_roundtrip():
    row = {"id": 1, "name": "Alice"}
    data = RowSerializer.dumps(row)
    assert RowSerializer.loads(data) == row


def test_put_get_roundtrip(tmp_path):
    node = NodeServer(db_path=tmp_path, port=9120, node_id="A", peers=[])
    node.server.start()
    try:
        client = GRPCReplicaClient("localhost", 9120)
        row = {"id": 1, "name": "Bob"}
        client.put("user1", json.dumps(row))
        time.sleep(0.1)
        values = client.get("user1")
        client.close()
        assert values
        returned = json.loads(values[0][0])
        assert returned == row
    finally:
        node.server.stop(0).wait()
        node.db.close()
