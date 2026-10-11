import pytest
from fastapi.testclient import TestClient
from app.main import app
import responses
from app.scraper import fetch_gtt_alerts

client = TestClient(app)

def test_api_root_dashboard():
    response = client.get("/")
    assert response.status_code == 200
    assert "GTT GTFS-RT Dashboard" in response.text

def test_gtfs_rt_protobuf_endpoint():
    response = client.get("/gtfs-rt/alerts.pb")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-protobuf"

@responses.activate
def test_scraper_resilience_on_server_error():
    url = "https://www.gtt.to.it/cms/index.php?option=com_gtt&view=avvisi&tmpl=raw&priorita=1"
    responses.add(responses.GET, url, status=500)
    
    alerts = fetch_gtt_alerts()
    assert isinstance(alerts, list)
    assert len(alerts) == 0

def test_rate_limiting_protection():
    responses_list = [client.get("/") for _ in range(35)]
    status_codes = [r.status_code for r in responses_list]
    assert 200 in status_codes
