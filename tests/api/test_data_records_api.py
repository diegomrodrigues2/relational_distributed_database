import os
import sys
import time
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_records_crud_and_scan():
    with TestClient(app) as client:
        data = {"partitionKey": "alpha", "clusteringKey": "a", "value": "v1"}
        resp = client.post("/data/records", json=data)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        time.sleep(0.1)
        resp = client.get("/data/records")
        assert resp.status_code == 200
        records = resp.json().get("records", [])
        assert any(r["partition_key"] == "alpha" and r["clustering_key"] == "a" and r["value"] == "v1" for r in records)

        resp = client.put("/data/records/alpha/a", params={"value": "v2"})
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        time.sleep(0.1)
        resp = client.get("/data/records")
        vals = [r["value"] for r in resp.json().get("records", []) if r["partition_key"] == "alpha" and r["clustering_key"] == "a"]
        assert vals and vals[0] == "v2"

        resp = client.get(
            "/data/records/scan_range",
            params={"partition_key": "alpha", "start_ck": "a", "end_ck": "b"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        items = [(i["clustering_key"], i["value"]) for i in data.get("items", [])]
        assert ("a", "v2") in items

        resp = client.delete("/data/records/alpha/a")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        time.sleep(0.1)
        resp = client.get("/data/records")
        assert not any(r["partition_key"] == "alpha" and r["clustering_key"] == "a" for r in resp.json().get("records", []))
