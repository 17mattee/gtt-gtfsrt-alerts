import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_gtfs_rt_feed_structure():
    response = client.get("/gtfs-rt/alerts.pb")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-protobuf"
