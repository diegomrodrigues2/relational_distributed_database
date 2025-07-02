import os
import sys
import time
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_add_and_remove_node():
    with TestClient(app) as client:
        resp = client.get("/cluster/nodes")
        assert resp.status_code == 200
        before_nodes = resp.json().get("nodes", [])
        count_before = len(before_nodes)

        resp = client.post("/cluster/actions/add_node")
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("status") == "ok"
        new_id = data.get("node_id")
        assert new_id

        resp = client.get("/cluster/nodes")
        assert resp.status_code == 200
        after_nodes = resp.json().get("nodes", [])
        assert len(after_nodes) == count_before + 1
        assert any(n["node_id"] == new_id for n in after_nodes)

        resp = client.delete(f"/cluster/actions/remove_node/{new_id}")
        assert resp.status_code == 200
        assert resp.json().get("status") == "ok"

        resp = client.get("/cluster/nodes")
        assert len(resp.json().get("nodes", [])) == count_before


def test_stop_and_start_node():
    with TestClient(app) as client:
        resp = client.get("/cluster/nodes")
        assert resp.status_code == 200
        node_id = resp.json()["nodes"][0]["node_id"]
        cluster = app.state.cluster

        resp = client.post(f"/nodes/{node_id}/stop")
        assert resp.status_code == 200
        assert resp.json().get("status") == "ok"
        time.sleep(0.2)
        assert not cluster.nodes_by_id[node_id].process.is_alive()

        resp = client.post(f"/nodes/{node_id}/start")
        assert resp.status_code == 200
        assert resp.json().get("status") == "ok"
        time.sleep(0.5)
        assert cluster.nodes_by_id[node_id].process.is_alive()
        # verify the restarted node responds
        cluster.nodes_by_id[node_id].client.ping(node_id)

