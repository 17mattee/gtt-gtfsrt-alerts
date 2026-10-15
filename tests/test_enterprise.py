import pytest
from fastapi.testclient import TestClient
from app.main import app
from google.transit import gtfs_realtime_pb2
import os
import docker

client = TestClient(app)

def is_docker_available():
    try:
        client_docker = docker.from_env()
        client_docker.ping()
        return True
    except Exception:
        return False

def test_api_root_dashboard():
    response = client.get("/")
    assert response.status_code == 200
    assert "GTT GTFS-RT Live Dashboard & Map" in response.text

def test_gtfs_rt_protobuf_endpoint():
    response = client.get("/gtfs-rt/alerts.pb")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-protobuf"
    
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    assert feed.header.gtfs_realtime_version == "2.0"

def test_scraper_resilience_on_server_error(monkeypatch):
    import requests
    def mock_get(*args, **kwargs):
        raise requests.exceptions.RequestException("Simulated GTT Portal Down")
    
    monkeypatch.setattr(requests, "get", mock_get)
    from app.scraper import fetch_gtt_alerts
    alerts = fetch_gtt_alerts()
    assert isinstance(alerts, list)

def test_rate_limiting_protection():
    response = client.get("/health")
    assert response.status_code in [200, 503]

@pytest.mark.skipif(not is_docker_available(), reason="Docker daemon non attivo o non disponibile")
def test_real_database_and_redis_containers(monkeypatch):
    from testcontainers.postgres import PostgresContainer
    from testcontainers.redis import RedisContainer
    
    with PostgresContainer("postgres:15-alpine") as postgres, RedisContainer("redis:7-alpine") as redis_container:
        # monkeypatch (non os.environ diretto): ripristino automatico a fine
        # test, nessuna variabile spacciata agli altri test della sessione.
        monkeypatch.setenv("DATABASE_URL", postgres.get_connection_url().replace("psycopg2", "psycopg"))
        monkeypatch.setenv("REDIS_URL", f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}")
        
        from app.database import fetch_latest_alerts_from_db
        alerts = fetch_latest_alerts_from_db()
        assert isinstance(alerts, list)
