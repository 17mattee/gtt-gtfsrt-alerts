"""Test di PRECISIONE dell'algoritmo di telemetria flotta.

Ogni test difende un caso osservabile sul feed reale GTT:
- fix ripetuti (stesso timestamp per piu' cicli): la velocita' nota non si perde
- jitter GPS da fermo: 0.0, non velocita' fantasma che mandano in tilt i capolinea
- salti GPS: il fix glitchato non avvelena il calcolo del ciclo successivo
- finestre lunghe: media non rappresentativa -> n/d, mai numeri a caso
- fix fuori ordine: il riferimento piu' fresco resta il migliore
- duplicati nel feed: si tiene il fix PIU' RECENTE
- mezzi senza ID: fallback univoco (N mezzi, non uno)
"""

import asyncio

import pytest
from google.transit import gtfs_realtime_pb2

from app import vehicles_scraper
from app.vehicles_scraper import _real_speed


# Un grado di latitudine ~ 111194 m: utility per spostamenti in metri.
def _lat_offset(meters: float) -> float:
    return meters / 111194.0


def _vehicle(vid="9001U", lat=45.0, lon=7.6, ts=1000):
    return {"vehicle_id": vid, "latitude": lat, "longitude": lon, "timestamp": ts}


def test_primo_avvistamento_senza_velocita():
    vehicles_scraper._previous_positions.clear()
    assert _real_speed(_vehicle()) is None
    vehicles_scraper._previous_positions.clear()


def test_fix_ripetuto_conserva_ultima_velocita():
    """GTT pubblica lo stesso timestamp per 2-3 cicli: la velocita' resta."""
    vehicles_scraper._previous_positions.clear()
    _real_speed(_vehicle(ts=1000))                                   # primo: n/d
    v = _real_speed(_vehicle(lat=45.0 + _lat_offset(30), ts=1003))   # 10 m/s
    assert v == pytest.approx(10.0, abs=0.01)
    # Il feed ripete il fix (stesso timestamp): il valore NOTO deve restare,
    # non tornare 'n/d' (dt <= 0 nel vecchio algoritmo).
    repeat = _real_speed(_vehicle(lat=45.0 + _lat_offset(30), ts=1003))
    assert repeat == pytest.approx(10.0, abs=0.01)
    vehicles_scraper._previous_positions.clear()


def test_jitter_gps_da_fermo_e_zero():
    """Mezzo FERMO: spostamento nel rumore GPS -> 0.0, non 1-4 m/s fantasma."""
    vehicles_scraper._previous_positions.clear()
    _real_speed(_vehicle(ts=1000))
    # ~1.1 m di spostamento (rumore GPS), dt=3s: senza noise floor sarebbero
    # 0.4 m/s ma con jitter reale (5-10m) arrivano punte > 1.5 m/s che
    # manderebbero in tilt il rilevamento 'in sosta al capolinea'.
    speed = _real_speed(_vehicle(lat=45.0 + _lat_offset(1.1), ts=1003))
    assert speed == 0.0
    vehicles_scraper._previous_positions.clear()


def test_salto_gps_non_avvelena_il_ciclo_successivo():
    """Un fix glitchato non diventa il riferimento del calcolo successivo."""
    vehicles_scraper._previous_positions.clear()
    _real_speed(_vehicle(lat=45.0, ts=1000))                          # A
    jump = _real_speed(_vehicle(lat=45.0 + _lat_offset(500), ts=1003))  # salto: ~166 m/s
    assert jump is None  # scartato, si conserva l'ultima velocita' valida
    # Il mezzo riparte da A (non dal punto glitchato): 20m reali in 6s = 3.3 m/s.
    # Se il riferimento fosse stato avvelenato dal salto, la distanza sarebbe
    # ~480m e il calcolo darebbe di nuovo None.
    speed = _real_speed(_vehicle(lat=45.0 + _lat_offset(20), ts=1006))
    assert speed is not None and abs(speed - 3.3) < 0.1
    vehicles_scraper._previous_positions.clear()


def test_finestra_lunga_valore_none():
    """Oltre 300s tra due fix la media non rappresenta la velocita' attuale."""
    vehicles_scraper._previous_positions.clear()
    _real_speed(_vehicle(ts=1000))
    assert _real_speed(_vehicle(lat=45.0 + _lat_offset(3000), ts=1500)) is None
    # Il nuovo fix e' il riferimento: il calcolo riparte da lì.
    speed = _real_speed(_vehicle(lat=45.0 + _lat_offset(3018), ts=1503))
    assert speed is not None and abs(speed - 6.0) < 0.1
    vehicles_scraper._previous_positions.clear()


def test_fix_fuori_ordine_non_sovrascrive_il_riferimento():
    vehicles_scraper._previous_positions.clear()
    _real_speed(_vehicle(lat=45.0, ts=1000))                          # A (buono)
    speed = _real_speed(_vehicle(lat=45.0 + _lat_offset(30), ts=1003))  # B, 10 m/s
    assert speed == 10.0
    # Arriva un fix PIU' VECCHIO (riordino del feed): ignorato.
    old = _real_speed(_vehicle(lat=45.0 + _lat_offset(1000), ts=998))
    assert old == pytest.approx(10.0, abs=0.01)  # resta l'ultima velocita', il riferimento B non si perde
    speed = _real_speed(_vehicle(lat=45.0 + _lat_offset(60), ts=1006))  # da B: 30m/3s
    assert speed == pytest.approx(10.0, abs=0.1)
    vehicles_scraper._previous_positions.clear()


# ---------------------------------------------------------------------
# Test a livello di fetch: deduplicato e ID di fallback
# ---------------------------------------------------------------------

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


def _feed_entity(entity_id, vehicle_id, lat, lon, ts, route_id="2U"):
    feed_entity = gtfs_realtime_pb2.FeedEntity()
    feed_entity.id = entity_id
    if vehicle_id:
        feed_entity.vehicle.vehicle.id = vehicle_id
    feed_entity.vehicle.trip.route_id = route_id
    feed_entity.vehicle.position.latitude = lat
    feed_entity.vehicle.position.longitude = lon
    feed_entity.vehicle.timestamp = ts
    return feed_entity


def _feed(*entities):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    for e in entities:
        feed.entity.add().CopyFrom(e)
    return feed.SerializeToString()


def _run_fetch(monkeypatch, content):
    _FakeClient.responses = {"https://feed.example/vehicle_position": content}
    monkeypatch.setattr(vehicles_scraper.httpx, "AsyncClient", _FakeClient)
    monkeypatch.setattr(vehicles_scraper, "_vehicle_endpoints",
                       lambda: list(_FakeClient.responses))
    monkeypatch.setattr(vehicles_scraper, "_last_fail_at", 0.0)
    monkeypatch.setattr(vehicles_scraper, "_previous_positions", {})
    return asyncio.run(vehicles_scraper.fetch_gtt_vehicle_positions())


def test_deduplicato_tiene_il_fix_piu_recente(monkeypatch):
    """Stesso mezzo pubblicato due volte: vince la posizione piu' recente."""
    vehicles = _run_fetch(
        monkeypatch,
        _feed(
            _feed_entity("e1", "9001U", 45.07, 7.68, 1791969000),       # vecchio
            _feed_entity("e2", "9001U", 45.08, 7.69, 1791969060),       # nuovo
        ),
    )
    assert len(vehicles) == 1
    assert vehicles[0]["timestamp"] == 1791969060
    assert vehicles[0]["latitude"] == pytest.approx(45.08, abs=1e-4)  # float32 del protobuf


def test_mezzi_senza_id_restano_distinti(monkeypatch):
    """L'ID mancante NON deve collassare N mezzi in un solo veicolo."""
    vehicles = _run_fetch(
        monkeypatch,
        _feed(
            _feed_entity("e1", "", 45.07, 7.68, 1791969000),
            _feed_entity("e2", "", 45.08, 7.69, 1791969000),
        ),
    )
    assert len(vehicles) == 2
    ids = {v["vehicle_id"] for v in vehicles}
    assert len(ids) == 2, "i fallback devono essere univoci"
    assert all(i.startswith("GTT-VEH-") for i in ids)
