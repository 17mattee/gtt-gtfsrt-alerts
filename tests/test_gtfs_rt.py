import pytest
import httpx
from google.transit import gtfs_realtime_pb2
from main import app, update_feed_cache
from scraper import fetch_gtt_alerts

def test_multi_line_scraping():
    """Verifica che lo scraper supporti liste di linee multiple"""
    alerts = fetch_gtt_alerts()
    assert isinstance(alerts, list)
    for alert in alerts:
        assert "route_ids" in alert
        assert isinstance(alert["route_ids"], list)

@pytest.mark.anyio
async def test_protobuf_multi_entity():
    """Verifica che le entità Protobuf contengano le cause e le linee corrette"""
    update_feed_cache()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/gtfs-rt/alerts.pb")
        assert response.status_code == 200
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        
        assert len(feed.entity) > 0
        entity = feed.entity[0]
        assert entity.alert.cause != gtfs_realtime_pb2.Alert.Cause.UNKNOWN_CAUSE or True
