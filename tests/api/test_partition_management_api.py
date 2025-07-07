import os
import sys
import tempfile
import time
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app
from database.replication import NodeCluster


def _setup_range_cluster():
    old_cluster = app.state.cluster
    tmpdir = tempfile.mkdtemp()
    if old_cluster:
        old_cluster.shutdown()
    ranges = [("a", "m"), ("m", "z")]
    app.state.cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
    return app.state.cluster


def _setup_hash_cluster():
    old_cluster = app.state.cluster
    tmpdir = tempfile.mkdtemp()
    if old_cluster:
        old_cluster.shutdown()
    app.state.cluster = NodeCluster(
        base_path=tmpdir,
        num_nodes=2,
        replication_factor=1,
        partition_strategy="hash",
        num_partitions=2,
    )
    return app.state.cluster


def test_split_partition_endpoint_increases_count():
    with TestClient(app) as client:
        cluster = _setup_range_cluster()
        try:
            initial = cluster.num_partitions
            resp = client.post(
                "/cluster/actions/split_partition",
                params={"pid": 0, "split_key": "g"},
            )
            assert resp.status_code == 200
            assert resp.json().get("status") == "ok"
            assert cluster.num_partitions == initial + 1
        finally:
            cluster.shutdown()


def test_split_partition_hash_endpoint_increases_count():
    with TestClient(app) as client:
        cluster = _setup_hash_cluster()
        try:
            initial = cluster.num_partitions
            resp = client.post(
                "/cluster/actions/split_partition",
                params={"pid": 0},
            )
            assert resp.status_code == 200
            assert resp.json().get("status") == "ok"
            assert cluster.num_partitions == initial + 1
        finally:
            cluster.shutdown()


def test_merge_partitions_endpoint_decreases_count():
    with TestClient(app) as client:
        ranges = [("a", "g"), ("g", "n"), ("n", "z")]
        old_cluster = app.state.cluster
        tmpdir = tempfile.mkdtemp()
        if old_cluster:
            old_cluster.shutdown()
        app.state.cluster = NodeCluster(base_path=tmpdir, num_nodes=2, key_ranges=ranges)
        cluster = app.state.cluster
        try:
            initial = cluster.num_partitions
            resp = client.post(
                "/cluster/actions/merge_partitions",
                params={"pid1": 0, "pid2": 1},
            )
            assert resp.status_code == 200
            assert resp.json().get("status") == "ok"
            assert cluster.num_partitions == initial - 1
        finally:
            cluster.shutdown()
