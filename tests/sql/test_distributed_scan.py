import os
import sys
import os
import sys
import time
import base64
import tempfile
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from database.replication import NodeCluster
from database.sql.serialization import RowSerializer
from database.sql.query_coordinator import QueryCoordinator


def _enc(row: dict) -> str:
    return base64.b64encode(RowSerializer.dumps(row)).decode("ascii")


def _key_for_node(cluster, node_idx):
    """Return an ID that maps to the given node index."""
    target_id = cluster.nodes[node_idx].node_id
    for i in range(1000):
        key = f"{node_idx}_{i}"
        pid = cluster.get_partition_id(f"users||{key}")
        if cluster.partition_map[pid] == target_id:
            return key
    raise RuntimeError("no key found")


def test_result_aggregation(tmp_path):
    cluster = NodeCluster(
        base_path=tmp_path,
        num_nodes=3,
        replication_factor=1,
        partition_strategy="hash",
    )
    try:
        for idx, node in enumerate(cluster.nodes):
            rid = _key_for_node(cluster, idx)
            node.client.put(f"users||{rid}", _enc({"id": rid}))
        time.sleep(0.5)
        qc = QueryCoordinator(cluster.nodes)
        rows = qc.execute("SELECT * FROM users")
        assert len(rows) == 3
    finally:
        cluster.shutdown()


def test_parallel_invocation():
    nodes = []
    for _ in range(3):
        client = mock.Mock()
        client.stub.ExecutePlan = mock.Mock(return_value=iter([]))
        node = mock.Mock()
        node.client = client
        nodes.append(node)
    qc = QueryCoordinator(nodes)
    qc.execute("SELECT * FROM t")
    for node in nodes:
        node.client.stub.ExecutePlan.assert_called_once()


