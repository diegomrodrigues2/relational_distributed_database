import os
import sys
import time
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from api.main import app


def test_schema_endpoints():
    with TestClient(app) as client:
        resp = client.get('/schema/tables')
        assert resp.status_code == 200
        assert resp.json()['tables'] == []

        ddl = 'CREATE TABLE users (id INT PRIMARY KEY, name STRING)'
        resp = client.post('/sql/execute', json={'sql': ddl})
        assert resp.status_code == 200
        time.sleep(0.5)

        resp = client.get('/schema/tables')
        assert resp.status_code == 200
        tables = resp.json()['tables']
        assert 'users' in tables

        resp = client.get('/schema/tables/users')
        assert resp.status_code == 200
        data = resp.json()
        assert data['name'] == 'users'
        assert any(c['name'] == 'id' for c in data['columns'])

        resp = client.get('/schema/tables/does_not_exist')
        assert resp.status_code == 404
