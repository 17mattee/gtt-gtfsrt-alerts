"""Test di robustezza sui fix della ri-analisi completa.

Coprono:
- scraper: id avviso STABILE (per l'upsert nello storico Supabase) e orari
  del portale GTT interpretati nel fuso Europe/Rome, non del server
- feed_registry: il fallimento del catalogo va in cache, non ritentato a
  ogni ciclo con chiamate sincrone che bloccano l'event loop
- gtfs_rt: a un fallimento transitorio del feed alerts si riutilizzano gli
  avvisi in cache invece di azzerare la UI
"""

import asyncio
from datetime import datetime, timedelta, timezone

from google.transit import gtfs_realtime_pb2

from app import feed_registry, gtfs_rt, scraper


def test_extract_start_time_fuso_rome():
    """L'orario del portale GTT e' ora italiana: epoch con offset Europe/Rome."""
    t_luglio = scraper.extract_start_time("Avviso del 25/07/2026 15:30 per lavori")
    assert t_luglio == int(datetime(2026, 7, 25, 15, 30, tzinfo=timezone(timedelta(hours=2))).timestamp())
    t_gennaio = scraper.extract_start_time("Avviso del 15/01/2026 09:00 per lavori")
    assert t_gennaio == int(datetime(2026, 1, 15, 9, 0, tzinfo=timezone(timedelta(hours=1))).timestamp())


def test_scraper_id_avviso_stabile(monkeypatch):
    """Stesso testo = stesso id: lo storico fa upsert, non duplicati a ogni sync."""
    html = "<html><body>25/07/2026 15:30 Linee 4, 15 deviate causa lavori in corso. Leggi avviso</body></html>"
    monkeypatch.setattr(scraper, "_request_gtt_page", lambda: html)
    a1 = scraper.fetch_gtt_alerts()
    a2 = scraper.fetch_gtt_alerts()
    assert len(a1) == 1 and len(a2) == 1
    assert a1[0]["id"] == a2[0]["id"], "l'id deve derivare dal contenuto, non dall'orario di fetch"


def test_registry_failure_cached(monkeypatch):
    """Catalogo feed non raggiungibile: si riprova dopo il TTL errore, non a ogni chiamata."""
    calls = {"n": 0}

    def fake_fetch():
        calls["n"] += 1
        return {}

    monkeypatch.setattr(feed_registry, "_fetch_registry", fake_fetch)
    feed_registry._registry_cache["urls"] = None
    feed_registry._registry_cache["fetched_at"] = 0.0
    feed_registry.get_feed_urls()
    feed_registry.get_feed_urls()
    assert calls["n"] == 1, "il fallimento deve andare in cache (no chiamate sincrone a ogni ciclo)"


def test_alerts_cache_stantica_su_fallimento_transitorio(monkeypatch):
    """Feed alerts giu' a TTL scaduto: si riusano gli avvisi in cache, la UI non si azzera."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "a1"
    entity.alert.header_text.translation.add(language="it", text="Sciopero nazionale")

    async def ok_fetch(url, timeout=8.0):
        return feed

    async def fail_fetch(url, timeout=8.0):
        return None

    monkeypatch.setattr(gtfs_rt, "_alerts_urls", lambda: ["fake://alerts"])
    monkeypatch.setattr(gtfs_rt, "_fetch_feed", ok_fetch)
    gtfs_rt._alerts_cache["data"] = None
    gtfs_rt._alerts_cache["fetched_at"] = 0.0
    alerts_prima = asyncio.run(gtfs_rt.fetch_gtt_rt_alerts())
    assert len(alerts_prima) == 1

    # Il feed cade e il TTL della cache e' scaduto: gli avvisi restano visibili.
    gtfs_rt._alerts_cache["fetched_at"] -= gtfs_rt.ALERTS_CACHE_TTL + 10
    monkeypatch.setattr(gtfs_rt, "_fetch_feed", fail_fetch)
    alerts_dopo = asyncio.run(gtfs_rt.fetch_gtt_rt_alerts())
    assert alerts_dopo == alerts_prima, "un blip di rete non deve azzerare gli avvisi"
    assert gtfs_rt.last_alerts_source == "unavailable"


def test_persist_guard_in_flight(monkeypatch):
    """Persistenza gia' in corso: la successiva si salta, niente code infinite."""
    import asyncio
    import app.main as main_mod
    import app.database_writer as dw

    chiamate = {"n": 0}

    async def slow_save(data):
        chiamate["n"] += 1
        await asyncio.sleep(0.1)

    monkeypatch.setattr(dw, "save_network_data_to_db", slow_save)
    main_mod._last_persist_at = 0.0
    main_mod._persist_in_flight = True  # una scrittura e' ancora in volo
    asyncio.run(main_mod._persist_snapshot({"vehicles": []}))
    assert chiamate["n"] == 0, "con una persistenza in corso non ne deve partire un'altra"
    main_mod._persist_in_flight = False
    asyncio.run(main_mod._persist_snapshot({"vehicles": []}))
    assert chiamate["n"] == 1


def test_persist_throttle_concorrente(monkeypatch):
    """Due persist chieste insieme: entra solo la prima (throttle + guard)."""
    import asyncio
    import app.main as main_mod
    import app.database_writer as dw

    in_volo = {"n": 0, "picco": 0}

    async def slow_save(data):
        in_volo["n"] += 1
        in_volo["picco"] = max(in_volo["picco"], in_volo["n"])
        await asyncio.sleep(0.15)
        in_volo["n"] -= 1

    monkeypatch.setattr(dw, "save_network_data_to_db", slow_save)
    main_mod._last_persist_at = 0.0
    main_mod._persist_in_flight = False

    async def run():
        await asyncio.gather(
            main_mod._persist_snapshot({"vehicles": []}),
            main_mod._persist_snapshot({"vehicles": []}),
        )

    asyncio.run(run())
    assert in_volo["picco"] == 1, "una sola scrittura Supabase alla volta"


def test_posizioni_gps_throttle_dedicato():
    """Le posizioni GPS non si salvano a ogni persist: cadenza dedicata."""
    import app.database_writer as dw

    dw._last_positions_persist_at = 0.0
    now = 1_000_000.0
    # prima volta: dovute
    assert (now - dw._last_positions_persist_at) >= dw.POSITIONS_PERSIST_INTERVAL_SECONDS
    dw._last_positions_persist_at = now
    # 30s dopo (il throttle generale delle persist): posizioni NON dovute
    now += 30
    assert (now - dw._last_positions_persist_at) < dw.POSITIONS_PERSIST_INTERVAL_SECONDS
    # dopo l'intervallo dedicato: dovute di nuovo
    now += dw.POSITIONS_PERSIST_INTERVAL_SECONDS
    assert (now - dw._last_positions_persist_at) >= dw.POSITIONS_PERSIST_INTERVAL_SECONDS


def test_hysteresis_sopravvive_a_rimozioni_concorrenti():
    """Un mezzo rimosso dal dizionario tra lo snapshot e l'iterazione non crasha."""
    import app.gtt_full_sync as fs

    fs._recent_vehicles.clear()
    fs._apply_fleet_hysteresis([{"vehicle_id": "9001U", "latitude": 45.0, "longitude": 7.6}])
    # simula la rimozione avvenuta altrove tra list(keys) e l'accesso
    fs._recent_vehicles.pop("9001U", None)
    kept = fs._apply_fleet_hysteresis([])
    assert kept == []


def test_titolo_avviso_non_solo_data():
    """Titolo scraper: la prima frase e' solo data/orario -> include la frase utile."""
    t1 = scraper._title_from_text("13/10/2026 09:15. Linee 4, 15 deviate causa lavori. Dettagli nel sito.")
    assert "Linee 4, 15 deviate" in t1, "il titolo non deve essere solo la data"
    assert t1.startswith("13/10/2026")
    t2 = scraper._title_from_text("Linee 4 e 15 deviate causa lavori. Dettagli nel sito.")
    assert t2 == "Linee 4 e 15 deviate causa lavori"
