"""Test di ADERENZA ALLA LINEA.

Ogni mezzo GPS deve stare sul suo percorso ufficiale (shapes.txt, misurato
dal vivo: p50 3m, p99 41m) o su una deviazione attiva. Chi e' oltre i 200m
resta in mappa ma dichiarato fuori_percorso.
"""

import time

from app import gtt_full_sync
from app.gtt_full_sync import (
    OFF_ROUTE_THRESHOLD_M,
    _check_route_adherence,
    _distance_to_polyline_m,
)

# --- Distanza punto-polilinea ---
# ---------------------------------------------------------------------

def _gps_sul_percorso():
    return {"vehicle_id": "9001U", "trip_id": "T1", "route_id": "2",
            "latitude": 45.0, "longitude": 7.615, "timestamp": int(time.time())}


def test_mezzo_sul_percorso_non_flaggato(monkeypatch):
    monkeypatch.setattr(gtt_full_sync.gtfs_manager, "get_trip_shape",
                        lambda tid: [(45.0, 7.6), (45.0, 7.61), (45.0, 7.62)])
    v = _gps_sul_percorso()
    assert _check_route_adherence([v], set()) == 0
    assert "fuori_percorso" not in v


def test_mezzo_fuori_percorso_flaggato(monkeypatch):
    monkeypatch.setattr(gtt_full_sync.gtfs_manager, "get_trip_shape",
                        lambda tid: [(45.0, 7.6), (45.0, 7.61)])
    v = _gps_sul_percorso()
    v["latitude"], v["longitude"] = 45.005, 7.605  # ~555m a nord del percorso
    assert _check_route_adherence([v], set()) == 1
    assert v["fuori_percorso"]["distanza_m"] > 500
    assert v["fuori_percorso"]["deviazione_attiva"] is False


def test_mezzo_fuori_percorso_con_deviazione_attiva(monkeypatch):
    monkeypatch.setattr(gtt_full_sync.gtfs_manager, "get_trip_shape",
                        lambda tid: [(45.0, 7.6), (45.0, 7.61)])
    v = _gps_sul_percorso()
    v["latitude"], v["longitude"] = 45.005, 7.605
    _check_route_adherence([v], {"2"})
    assert v["fuori_percorso"]["deviazione_attiva"] is True, "la linea 2 ha una deviazione attiva"


def test_stimati_e_senza_corsa_saltati(monkeypatch):
    called = []
    monkeypatch.setattr(gtt_full_sync.gtfs_manager, "get_trip_shape",
                        lambda tid: called.append(tid) or [(45.0, 7.6), (45.0, 7.61)])
    v_stimato = _gps_sul_percorso()
    v_stimato["position_source"] = "stimata"
    v_senza_corsa = _gps_sul_percorso()
    v_senza_corsa["trip_id"] = "N/A"
    assert _check_route_adherence([v_stimato, v_senza_corsa], set()) == 0
    assert called == [], "né gli stimati né i senza-corsa richiedono la shape"


def test_soglia_aderenza_misurata():
    """La soglia resta generosa rispetto alla misura reale (p99 41m):
    un mezzo a 100m (GPS urbano scarso, ma plausibile) NON va flaggato."""
    assert OFF_ROUTE_THRESHOLD_M == 200
    shape = [(45.0, 7.6), (45.0, 7.61)]
    assert _distance_to_polyline_m(45.0005, 7.605, shape) < OFF_ROUTE_THRESHOLD_M
