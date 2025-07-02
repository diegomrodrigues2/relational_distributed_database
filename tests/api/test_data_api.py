import os
import sys
import tempfile
import json
import time
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app
from database.replication import NodeCluster


def test_query_index_endpoint():
    with TestClient(app) as client:
        old_cluster = app.state.cluster
        tmpdir = tempfile.mkdtemp()
        old_cluster.shutdown()
        app.state.cluster = NodeCluster(base_path=tmpdir, num_nodes=2, index_fields=["color"])

        resp = client.post(
            "/put/p1",
            params={"value": json.dumps({"color": "red"})},
        )
        assert resp.status_code == 200

        time.sleep(0.5)
        resp = client.get("/data/query_index", params={"field": "color", "value": "red"})
        assert resp.status_code == 200
        result = resp.json()
        assert result.get("keys") == ["p1"]
