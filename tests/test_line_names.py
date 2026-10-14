"""Test delle traduzioni route_id -> nome linea e della cascata endpoint veicoli.

Coprono i fix dell'analisi del 14/10/2026 (linea 2 non trovabile sulla mappa):
- get_route_short_name deve normalizzare '2U' -> '2' ANCHE quando il catalogo
  GTFS-Static ufficiale non e' disponibile (outage): altrimenti la mappa
  mostra '2U' e chi cerca la linea '2' non trova il suo mezzo.
- un feed veicoli VALIDO ma VUOTO non deve chiudere la cascata di endpoint:
  il primo endpoint che risponde vuoto potrebbe essere degradato, mentre
  la flotta e' pubblicata su un altro.
"""

import asyncio

from google.transit import gtfs_realtime_pb2

from app import vehicles_scraper
from app.gtfs_static import GtfsStaticManager


def test_route_short_name_fallback_fuori_catalogo():
    """Senza catalogo GTFS-Static: '2U' -> '2', '18U' -> '18', sconosciuti invariati."""
    mgr = GtfsStaticManager()  # mai caricato: nessuna mappa, solo fallback
    assert mgr.get_route_short_name("2U") == "2"
    assert mgr.get_route_short_name("18U") == "18"
    # ID non normalizzabili: restituiti invariati (mai dati inventati)
    assert mgr.get_route_short_name("FLEU") == "FLEU"
    assert mgr.get_route_short_name("") == ""
    assert mgr.get_route_short_name("2") == "2"


def test_route_short_name_con_catalogo():
    """Con la mappa caricata vince sempre la traduzione ufficiale GTT."""
    mgr = GtfsStaticManager()
    mgr._routes_cache = {"2": "piazza Sofia - via Corradino"}
    mgr._route_id_map = {"2U": "2"}
    mgr._cache_loaded_at = 0.0  # non fresco: la mappa resta valida (stale retention)
    assert mgr.get_route_short_name("2U") == "2"


def _feed_with_vehicles(*route_ids):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    for i, rid in enumerate(route_ids):
        entity = feed.entity.add()
        entity.id = f"e{i}"
        v = entity.vehicle
        v.vehicle.id = f"{1000 + i}U"
        v.trip.route_id = rid
        v.position.latitude = 45.07
        v.position.longitude = 7.68
        v.timestamp = 1791969000
    return feed.SerializeToString()


def _empty_feed():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    return feed.SerializeToString()


class _FakeResponse:
    def __init__(self, content):
        self.content = content
        self.status_code = 200


class _FakeClient:
    responses = {}

    def __init__(self, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, url):
        return _FakeResponse(_FakeClient.responses[url])


def test_feed_vuoto_non_chiude_la_cascata(monkeypatch):
    """Il primo endpoint vuoto non deve mascherare i mezzi pubblicati sul secondo."""
    _FakeClient.responses = {
        "https://rotto.example/vehicle_position": _empty_feed(),
        "https://buono.example/vehicle_position": _feed_with_vehicles("2U"),
    }
    monkeypatch.setattr(vehicles_scraper.httpx, "AsyncClient", _FakeClient)
    monkeypatch.setattr(vehicles_scraper, "_vehicle_endpoints",
                        lambda: list(_FakeClient.responses))
    monkeypatch.setattr(vehicles_scraper, "_last_fail_at", 0.0)
    monkeypatch.setattr(vehicles_scraper, "_previous_positions", {})

    vehicles = asyncio.run(vehicles_scraper.fetch_gtt_vehicle_positions())

    assert len(vehicles) == 1
    assert vehicles[0]["route_id"] == "2"
    assert vehicles_scraper.last_fetch_source == "live"


def test_tutti_gli_endpoint_vuoti_dato_reale(monkeypatch):
    """Tutti gli endpoint validi ma vuoti: zero mezzi (dato reale), fonte 'live'."""
    _FakeClient.responses = {
        "https://a.example/vehicle_position": _empty_feed(),
        "https://b.example/vehicle_position": _empty_feed(),
    }
    monkeypatch.setattr(vehicles_scraper.httpx, "AsyncClient", _FakeClient)
    monkeypatch.setattr(vehicles_scraper, "_vehicle_endpoints",
                        lambda: list(_FakeClient.responses))
    monkeypatch.setattr(vehicles_scraper, "_last_fail_at", 0.0)
    monkeypatch.setattr(vehicles_scraper, "_previous_positions", {})

    vehicles = asyncio.run(vehicles_scraper.fetch_gtt_vehicle_positions())

    assert vehicles == []
    assert vehicles_scraper.last_fetch_source == "live"
