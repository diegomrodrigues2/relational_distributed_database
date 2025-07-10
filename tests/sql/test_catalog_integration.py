import time
import os
import sys
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from database.sql.metadata import ColumnDefinition, TableSchema
from database.replication import NodeCluster


def test_schema_replication_and_reload(tmp_path):
    cluster = NodeCluster(base_path=tmp_path, num_nodes=2, replication_factor=2)
    schema = TableSchema(
        name="pets",
        columns=[ColumnDefinition("id", "int", primary_key=True)],
    )
    value = schema.to_json()
    try:
        cluster.put(0, "_meta:table:pets", value)
        time.sleep(1.0)
        assert cluster.get(1, "_meta:table:pets") == value
    finally:
        cluster.shutdown()

    import shutil
    import database.replication.replication as repl
    with mock.patch.object(shutil, "rmtree", lambda p: None), \
         mock.patch.object(repl.os, "makedirs", lambda p, exist_ok=False: None):
        cluster2 = NodeCluster(base_path=tmp_path, num_nodes=2, replication_factor=2)
    try:
        time.sleep(1.0)
        assert cluster2.get(0, "_meta:table:pets") == value
        assert cluster2.get(1, "_meta:table:pets") == value
    finally:
        cluster2.shutdown()

