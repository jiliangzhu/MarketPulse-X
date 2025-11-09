from __future__ import annotations

from fastapi.testclient import TestClient


def test_health_endpoint(client: TestClient):
    resp = client.get("/api/healthz")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"


def test_markets_endpoint(client: TestClient):
    resp = client.get("/api/markets")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert data
    assert "market_id" in data[0]


def test_signals_endpoint(client: TestClient):
    resp = client.get("/api/signals")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


def test_kpi_endpoint(client: TestClient):
    resp = client.get("/api/kpi/daily")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
