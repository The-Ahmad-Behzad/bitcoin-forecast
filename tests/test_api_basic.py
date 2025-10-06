import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from backend.app import app

@pytest.fixture
def client():
    app.testing = True
    with app.test_client() as client:
        yield client

def test_ping(client):
    res = client.get("/api/ping")
    assert res.status_code == 200
    assert res.get_json()["status"] == "ok"

def test_historical(client, monkeypatch):
    sample_data = [{"date": "2025-09-01T00:00:00", "close": 101.2}]

    class DummyColl:
        def __init__(self, data):
            self.data = data
        def find(self, *_, **__):
            return self  # allow chaining
        def sort(self, *_, **__):
            return self
        def limit(self, *_):
            return self
        def __iter__(self):
            return iter(self.data)

    class DummyDB:
        def get_collection(self, *_):
            return DummyColl(sample_data)

    # Patch Mongo getter
    monkeypatch.setattr("backend.api.historical.get_db", lambda: DummyDB())

    res = client.get("/api/historical?limit=1")
    assert res.status_code == 200
    j = res.get_json()
    assert j["ticker"] == "BTC-USD"
    assert j["count"] == 1
    assert j["data"][0]["close"] == 101.2
