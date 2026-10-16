"""Test delle SOGLIE FUORI PERCORSO PER TIPO e delle DEVIAZIONI NON ANNUNCIATE.

Tram/metro/funicolare viaggiano su rotaia fissa: 75m bastano per dichiararli
fuori percorso (i bus 200m, canyon urbano). E se >= 3 mezzi della STESSA
linea sono fuori percorso senza spiegazione, il sistema DEDUCE una deviazione
che GTT non ha annunciato.
"""

import time

from app import gtt_full_sync
from app.gtt_full_sync import (
    FIXED_TRACK_OFF_ROUTE_M,
    OFF_ROUTE_THRESHOLD_M,
    _detect_unannounced_detours,
)


def test_soglia_per_tipo_tram_rigida_bus_tollerante(monkeypatch):
    """Un mezzo a 100m: per un TRAM e' fuori percorso (rotaia fissa, 75m),
    per un BUS no (200m: canyon urbano e deviazioni d'isolato)."""
    from app.gtt_full_sync import _check_route_adherence
    shape = [(45.0, 7.6), (45.0, 7.61)]
    monkeypatch.setattr(gtt_full_sync.gtfs_manager, "get_trip_shape", lambda tid: shape)
    monkeypatch.setattr(gtt_full_sync.gtfs_manager, "get_route_types",
                        lambda: {"4": 0, "62": 3})  # 4 = tram, 62 = bus
    tram = {"vehicle_id": "8001U", "trip_id": "T1", "route_id": "4",
            "latitude": 45.0 + 100 / 111194, "longitude": 7.605, "timestamp": int(time.time())}
    bus = {"vehicle_id": "9001U", "trip_id": "T2", "route_id": "62",
           "latitude": 45.0 + 100 / 111194, "longitude": 7.605, "timestamp": int(time.time())}
    assert _check_route_adherence([tram, bus], set()) == 1
    assert tram.get("fuori_percorso", {}).get("distanza_m", 0) > FIXED_TRACK_OFF_ROUTE_M
    assert "fuori_percorso" not in bus, "a 100m un bus e' nei limiti del canyon urbano"


def test_deviazione_non_annunciata_rilevata_solo_con_piu_mezzi():
    """Tre mezzi della stessa linea fuori percorso senza spiegazione:
    deviazione NON annunciata. Due mezzi: nessuna pretesa."""
    v = lambda i, route="62": {"vehicle_id": f"900{i}U", "route_id": route,
                               "fuori_percorso": {"distanza_m": 300, "deviazione_attiva": False}}
    tre = [v(1), v(2), v(3)]
    assert _detect_unannounced_detours(tre) == [("62", 3)]
    assert tre[0]["fuori_percorso"]["motivo"] == "deviazione_rilevata"
    assert tre[0]["fuori_percorso"]["mezzi_coinvolti"] == 3

    due = [v(1), v(2)]
    assert _detect_unannounced_detours(due) == []

    annunciata = [v(1), v(2), v(3)]
    for x in annunciata:
        x["fuori_percorso"]["deviazione_attiva"] = True
    assert _detect_unannounced_detours(annunciata) == []

    al_deposito = [v(1), v(2), v(3)]
    for x in al_deposito:
        x["fuori_percorso"]["motivo"] = "rientro_deposito"
    assert _detect_unannounced_detours(al_deposito) == []


def test_soglie_dichiarate_coerenti_con_la_misura():
    """Bus 200m (misura: p99 41m), rotaia 75m: costanti al servizio del dato."""
    assert OFF_ROUTE_THRESHOLD_M == 200
    assert FIXED_TRACK_OFF_ROUTE_M == 75
