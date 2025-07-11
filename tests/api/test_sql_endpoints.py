import os
import sys
import time
import base64

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from api.main import app
from database.sql.serialization import RowSerializer


def _enc(row: dict) -> str:
    return base64.b64encode(RowSerializer.dumps(row)).decode('ascii')


def test_sql_query_and_execute():
    with TestClient(app) as client:
        resp = client.post('/sql/execute', json={'sql': 'CREATE TABLE users (id INT PRIMARY KEY, name STRING)'})
        assert resp.status_code == 200
        cluster = app.state.cluster
        cluster.nodes[0].client.put('users||1', _enc({'id': 1, 'name': 'a'}))
        time.sleep(0.2)
        resp = client.post('/sql/query', json={'sql': 'SELECT * FROM users'})
        assert resp.status_code == 200
        data = resp.json()
        assert data['rows'] and data['rows'][0]['id'] == 1
