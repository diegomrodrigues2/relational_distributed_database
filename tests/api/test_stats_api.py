import os
import sys
import time
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from api.main import app


def test_stats_endpoints():
    with TestClient(app) as client:
        resp = client.post('/sql/execute', json={'sql': 'CREATE TABLE users (id INT PRIMARY KEY, city STRING)'})
        assert resp.status_code == 200
        client.post('/put/users||1', params={'value': '{"id":1,"city":"NY"}'})
        client.post('/put/users||2', params={'value': '{"id":2,"city":"SF"}'})
        time.sleep(0.1)
        resp = client.post('/actions/analyze/users')
        assert resp.status_code == 200
        resp = client.get('/stats/table/users')
        assert resp.status_code == 200
        assert resp.json()['num_rows'] == 2
        resp = client.get('/stats/table/users/columns')
        assert resp.status_code == 200
        cols = resp.json().get('columns', [])
        assert any(c['col_name'] == 'city' and c['num_distinct'] == 2 for c in cols)
