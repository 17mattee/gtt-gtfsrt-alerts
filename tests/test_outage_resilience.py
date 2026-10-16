"""Test di INDIPENDENZA DAI CRASH dei feed GTT.

Il caso reale (verificato dal vivo): feed posizioni giu' mentre i trip
updates restano attivi. Senza difese:
- tutte le corse restavano con route_id 'N/A' -> intere linee (es. la 2)
  sparivano dalla lista ritardi
- la mappa e l'elenco mezzi crollavano a zero

Copre:
- risoluzione della linea di ogni corsa dal GTFS-Static (trips.txt), fonte
  indipendente dai feed real-time
- buffer outage: ultima flotta reale nota servita come 'cached', con scadenza
- snapshot su disco: save/restore atomico per il cold start, mai vuoto, mai
  stantio oltre il TTL
"""

import json
import time

from app import gtt_full_sync, vehicles_scraper
from app.gtt_full_sync import (
    OUTAGE_FLEET_TTL_S,
    SNAPSHOT_MAX_AGE_S,
    STALE_FEED_GRACE_SECONDS,
    _remember_good_fleet,
    _resolve_trip_routes,
    _save_snapshot_to_disk,
    _serve_outage_fleet,
    _filter_fresh_positions,
    load_last_good_snapshot,
)


def _catalogo_linea_2(monkeypatch):
    """Catalogo GTFS-Static minimale: corsa 'T-2' e' della linea 2U ('2')."""
    monkeypatch.setattr(gtt_full_sync.gtfs_manager, "_routes_cache", {"2": "piazza Sofia - via Corradino"})
    monkeypatch.setattr(gtt_full_sync.gtfs_manager, "_route_id_map", {"2U": "2"})
    monkeypatch.setattr(gtt_full_sync.gtfs_manager, "_trip_routes", {"T-2": "2U"})
    monkeypatch.setattr(gtt_full_sync.gtfs_manager, "_cache_loaded_at", time.time())


def test_linea_corsa_risolta_dal_gtfs_static_con_feed_posizioni_giu(monkeypatch):
    """Feed posizioni GIU' (zero mezzi): la linea viene da trips.txt, mai 'N/A'."""
    _catalogo_linea_2(monkeypatch)
    tus = [{"trip_id": "T-2", "route_id": "N/A", "vehicle_id": "9712U"}]
    _resolve_trip_routes(tus, [])  # cross-ref impossibile: nessun mezzo
    assert tus[0]["route_id"] == "2", "la corsa della linea 2 deve restare visibile"


def test_cross_ref_posizioni_ha_priorita_sul_statico(monkeypatch):
    """Il feed posizioni (real-time) vince sullo statico quando e' attivo."""
    _catalogo_linea_2(monkeypatch)
    vehicles = [{"vehicle_id": "9712U", "trip_id": "T-2", "route_id": "4"}]
    tus = [{"trip_id": "T-2", "route_id": "N/A", "vehicle_id": "9712U"}]
    _resolve_trip_routes(tus, vehicles)
    assert tus[0]["route_id"] == "4"


def test_buffer_outage_serve_ultima_flotta_reale(monkeypatch):
    """Feed posizioni unavailable: si serve l'ultima flotta nota, marcata dal buffer."""
    monkeypatch.setattr(vehicles_scraper, "last_fetch_source", "unavailable")
    monkeypatch.setattr(gtt_full_sync, "_last_good_fleet",
                        [{"vehicle_id": "9001U", "route_id": "2", "timestamp": int(time.time())}])
    monkeypatch.setattr(gtt_full_sync, "_last_good_fleet_at", time.time())
    served, from_buffer = _serve_outage_fleet([])
    assert from_buffer is True
    assert served[0]["vehicle_id"] == "9001U"


def test_buffer_outage_scade_oltre_ttl(monkeypatch):
    """Oltre il TTL la flotta del buffer non si serve piu': zero onesto, mai finzione."""
    monkeypatch.setattr(vehicles_scraper, "last_fetch_source", "unavailable")
    monkeypatch.setattr(gtt_full_sync, "_last_good_fleet", [{"vehicle_id": "9001U"}])
    monkeypatch.setattr(gtt_full_sync, "_last_good_fleet_at", time.time() - (OUTAGE_FLEET_TTL_S + 1))
    served, from_buffer = _serve_outage_fleet([])
    assert from_buffer is False and served == []


def test_buffer_outage_non_attivo_con_feed_ok(monkeypatch):
    """GTT pubblica davvero zero mezzi (dato reale): NESSUN buffer, zero onesto."""
    monkeypatch.setattr(vehicles_scraper, "last_fetch_source", "live")
    monkeypatch.setattr(gtt_full_sync, "_last_good_fleet", [{"vehicle_id": "9001U"}])
    monkeypatch.setattr(gtt_full_sync, "_last_good_fleet_at", time.time())
    served, from_buffer = _serve_outage_fleet([])
    assert from_buffer is False and served == []


def test_remember_good_fleet_solo_da_feed_reale():
    try:
        _remember_good_fleet([{"vehicle_id": "9001U"}], False)
        assert gtt_full_sync._last_good_fleet[0]["vehicle_id"] == "9001U"
        before = list(gtt_full_sync._last_good_fleet)
        _remember_good_fleet([{"vehicle_id": "ALTRO"}], True)  # dal buffer: non aggiorna
        assert gtt_full_sync._last_good_fleet == before
    finally:
        gtt_full_sync._last_good_fleet = []
        gtt_full_sync._last_good_fleet_at = 0.0


def test_snapshot_salva_e_ripristina(tmp_path, monkeypatch):
    """Roundtrip completo: snapshot buono -> cold start con dati 'cached'."""
    monkeypatch.setattr(gtt_full_sync, "_SNAPSHOT_DIR", tmp_path)
    monkeypatch.setattr(gtt_full_sync, "_SNAPSHOT_FILE", tmp_path / "last_good_snapshot.json")
    monkeypatch.setattr(gtt_full_sync, "_last_snapshot_write_at", 0.0)
    monkeypatch.setattr(gtt_full_sync, "_last_good_fleet", [])
    monkeypatch.setattr(gtt_full_sync, "_last_good_fleet_at", 0.0)

    result = {
        "vehicles": [{"vehicle_id": "9001U", "route_id": "2"}],
        "trip_updates": [],
        "alerts": [],
        "meta": {"last_update": int(time.time()), "vehicles_source": "live"},
    }
    _save_snapshot_to_disk(result)
    snapshot_file = tmp_path / "last_good_snapshot.json"
    assert snapshot_file.exists(), "lo snapshot buono deve finire su disco"

    orig = dict(gtt_full_sync.network_snapshot)
    try:
        assert load_last_good_snapshot() is True
        snap = gtt_full_sync.network_snapshot
        assert snap["vehicles"][0]["vehicle_id"] == "9001U"
        assert snap["meta"]["vehicles_source"] == "cached", "mai spacciato per live"
        assert gtt_full_sync._last_good_fleet[0]["vehicle_id"] == "9001U", "seeda il buffer outage"
    finally:
        gtt_full_sync.network_snapshot.clear()
        gtt_full_sync.network_snapshot.update(orig)
        gtt_full_sync._last_good_fleet = []
        gtt_full_sync._last_good_fleet_at = 0.0


def test_snapshot_flotta_vuota_non_si_salva(tmp_path, monkeypatch):
    monkeypatch.setattr(gtt_full_sync, "_SNAPSHOT_DIR", tmp_path)
    monkeypatch.setattr(gtt_full_sync, "_SNAPSHOT_FILE", tmp_path / "last_good_snapshot.json")
    monkeypatch.setattr(gtt_full_sync, "_last_snapshot_write_at", 0.0)
    _save_snapshot_to_disk({"vehicles": [], "meta": {}})
    assert not (tmp_path / "last_good_snapshot.json").exists()


def test_snapshot_stantio_non_si_ripristina(tmp_path, monkeypatch):
    """Uno snapshot oltre SNAPSHOT_MAX_AGE_S non deve mai tornare a schermo."""
    monkeypatch.setattr(gtt_full_sync, "_SNAPSHOT_FILE", tmp_path / "last_good_snapshot.json")
    stale = {"saved_at": time.time() - (SNAPSHOT_MAX_AGE_S + 60),
             "data": {"vehicles": [{"vehicle_id": "9001U"}], "meta": {}}}
    (tmp_path / "last_good_snapshot.json").write_text(json.dumps(stale), encoding="utf-8")
    assert load_last_good_snapshot() is False


def test_filtro_freschezza_finestra_rilassata_per_feed_stantio():
    """Un fix di 5 minuti col filtro standard e' nascosto; con la finestra
    rilassata del feed stantio di massa resta visibile (UI: 'GPS datato')."""
    now = time.time()
    v5min = {"vehicle_id": "9001U", "latitude": 45.0, "longitude": 7.6,
             "timestamp": now - 300}
    assert _filter_fresh_positions([v5min], now) == []  # default: troppo datato
    kept = _filter_fresh_positions([v5min], now, max_age=STALE_FEED_GRACE_SECONDS)
    assert len(kept) == 1, "feed stantio di massa: il mezzo resta in mappa"
    # il futuro corrotto resta sempre escluso, anche con la finestra rilassata
    vfuturo = dict(v5min, vehicle_id="9002U", timestamp=now + 3600)
    assert _filter_fresh_positions([vfuturo], now, max_age=STALE_FEED_GRACE_SECONDS) == []



def test_buffer_outage_caso_feed_degradato():
    """Feed RAGGIUNGIBILE ma senza posizioni fresche, con centinaia di corse
    attive: la citta' e' piena di bus, la mappa non puo' svuotarsi."""
    vehicles_scraper.last_fetch_source = "live"
    try:
        gtt_full_sync._last_good_fleet = [{"vehicle_id": "9001U", "timestamp": time.time()}]
        gtt_full_sync._last_good_fleet_at = time.time()
        served, from_buffer = _serve_outage_fleet([], trips_active=400)
        assert from_buffer is True and served[0]["vehicle_id"] == "9001U"
        # notte con poco servizio (pocoe corse attive): dato reale, nessun buffer
        served, from_buffer = _serve_outage_fleet([], trips_active=5)
        assert from_buffer is False and served == []
    finally:
        gtt_full_sync._last_good_fleet = []
        gtt_full_sync._last_good_fleet_at = 0.0
        vehicles_scraper.last_fetch_source = "unknown"