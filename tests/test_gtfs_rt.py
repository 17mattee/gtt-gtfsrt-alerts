import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_gtfs_rt_feed_structure():
    """Verifica che l'endpoint restituisca correttamente il codice 200 e il payload protobuf"""
    response = client.get("/gtfs-rt/alerts.pb")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-protobuf"
