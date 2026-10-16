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


def test_avvisi_scraper_stessa_forma_del_feed_rt():
    """Gli avvisi di fallback hanno gli stessi tipi e chiavi del feed ufficiale."""
    import time as _time
    from app.gtt_full_sync import _map_scraped_alert
    from app import gtfs_rt

    raw = {"id": "x1", "title": "Linee 4, 15 deviate", "description": "testo",
           "route_ids": ["4", "15"], "cause": 10, "effect": 4, "start_time": 1790000000}
    alert = _map_scraped_alert(raw, 0)
    # stessi TIPI del feed RT: cause/effect int, non stringhe
    assert isinstance(alert["cause"], int) and alert["cause"] == 10
    assert isinstance(alert["effect"], int) and alert["effect"] == 4
    # category aggregata: le deviazioni del fallback devono essere riconosciute
    assert alert["category"] == "deviazione"
    assert alert["cause_label"] == gtfs_rt.CAUSE_LABELS[10]
    assert alert["effect_label"] == gtfs_rt.EFFECT_LABELS[4]


def test_mappa_inversa_route_id_gtfs():
    """'2' -> '2U': il feed GTFS-RT ricostruito deve usare i route_id di routes.txt."""
    import time as _time
    from app.gtfs_static import gtfs_manager

    orig_map, orig_cache, orig_loaded = gtfs_manager._route_id_map, gtfs_manager._routes_cache, gtfs_manager._cache_loaded_at
    try:
        gtfs_manager._route_id_map = {"2U": "2", "METROU": "M1"}
        gtfs_manager._routes_cache = {"2": "x", "M1": "y"}
        gtfs_manager._cache_loaded_at = _time.time()
        assert gtfs_manager.get_gtfs_route_id("2") == "2U"
        assert gtfs_manager.get_gtfs_route_id("M1") == "METROU"
        # linea ignota: restituita invariata, mai None
        assert gtfs_manager.get_gtfs_route_id("99") == "99"
    finally:
        gtfs_manager._route_id_map, gtfs_manager._routes_cache, gtfs_manager._cache_loaded_at = orig_map, orig_cache, orig_loaded


def test_client_ip_dietro_proxy():
    """Il rate limit deve distinguere gli utenti reali (X-Forwarded-For), non l'IP del LB."""
    from starlette.requests import Request
    from app.main import _client_ip

    scope = {"type": "http", "headers": [(b"x-forwarded-for", b"203.0.113.9, 10.0.0.1")], "client": ("10.20.30.40", 50000)}
    assert _client_ip(Request(scope)) == "203.0.113.9"
    scope_senza_xff = {"type": "http", "headers": [], "client": ("10.20.30.40", 50000)}
    assert _client_ip(Request(scope_senza_xff)) == "10.20.30.40"


def test_raw_alerts_negative_cache_durante_outage(monkeypatch):
    """Outage recente del feed alerts: il proxy non tocca nemmeno la rete."""
    import asyncio
    gtfs_rt._raw_alerts_cache.update({"data": b"", "fetched_at": 0.0})
    gtfs_rt._raw_alerts_fail_at = gtfs_rt.time.time()  # outage appena avvenuto

    def no_urls():
        raise AssertionError("durante l'outage non si devono enumerare gli URL")

    monkeypatch.setattr(gtfs_rt, "_alerts_urls", no_urls)
    assert asyncio.run(gtfs_rt.fetch_raw_alerts_feed()) == b""

    # scaduto il TTL di fallimento si riprova (qui: nessun URL configurato
    # -> fallisce di nuovo e riarma il marker)
    gtfs_rt._raw_alerts_fail_at = gtfs_rt.time.time() - gtfs_rt.RAW_ALERTS_FAIL_TTL - 1
    monkeypatch.setattr(gtfs_rt, "_alerts_urls", lambda: [])
    assert asyncio.run(gtfs_rt.fetch_raw_alerts_feed()) == b""
    assert gtfs_rt._raw_alerts_fail_at > 0
    gtfs_rt._raw_alerts_fail_at = 0.0


def test_niente_di_datato_in_mappa():
    """Politica freschezza: mai fix vecchi (>2 min) o impossibili (dal futuro)."""
    import time as _t
    from app.gtt_full_sync import _filter_fresh_positions, MAX_GPS_AGE_SECONDS, MAX_GPS_FUTURE_SECONDS

    now = 1_000_000.0
    fresco = {"vehicle_id": "A", "timestamp": now - 30}
    datato = {"vehicle_id": "B", "timestamp": now - MAX_GPS_AGE_SECONDS - 1}     # 2 min e 1s fa
    futuro = {"vehicle_id": "C", "timestamp": now + MAX_GPS_FUTURE_SECONDS + 1}  # timestamp corrotto
    senza = {"vehicle_id": "D", "timestamp": None}
    out = _filter_fresh_positions([fresco, datato, futuro, senza], now=now)
    ids = {v["vehicle_id"] for v in out}
    assert ids == {"A"}, f"solo il fix fresco deve restare, rimasti: {ids}"

    # il limite esatto (2 min precisi) resta valido: e' l'eta' tollerata
    limite = {"vehicle_id": "E", "timestamp": now - MAX_GPS_AGE_SECONDS}
    assert _filter_fresh_positions([limite], now=now)[0]["vehicle_id"] == "E"
def test_vehicles_negative_backoff_durante_outage(monkeypatch):
    """Feed posizioni giu': backoff di 30s, niente cascata di timeout a ogni ciclo."""
    import asyncio
    import time as _t
    from app import vehicles_scraper as vs

    vs._last_fail_at = _t.time()  # outage appena avvenuto

    def no_endpoints():
        raise AssertionError("durante il backoff non si devono enumerare gli endpoint")

    monkeypatch.setattr(vs, "_vehicle_endpoints", no_endpoints)
    assert asyncio.run(vs.fetch_gtt_vehicle_positions()) == []
    assert vs.last_fetch_source == "unavailable"

    # scaduto il backoff si riprova: cascata vuota = nuovo fallimento -> marker riarmedato
    vs._last_fail_at = _t.time() - vs.VEHICLES_FAIL_TTL - 1
    monkeypatch.setattr(vs, "_vehicle_endpoints", lambda: [])
    assert asyncio.run(vs.fetch_gtt_vehicle_positions()) == []
    assert vs._last_fail_at > 0
    vs._last_fail_at = 0.0


def test_trip_updates_buffer_su_fallimento_transitorio(monkeypatch):
    """Feed ritardi giu' per un ciclo: buffer 60s con flag 'cached', poi n/d."""
    import asyncio
    from google.transit import gtfs_realtime_pb2

    def _feed():
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        e = feed.entity.add()
        e.id = "t1"
        tu = e.trip_update
        tu.trip.trip_id = "28379006U"
        tu.stop_time_update.add().arrival.delay = 120
        return feed

    async def ok_fetch(url, timeout=8.0):
        return _feed()

    async def fail_fetch(url, timeout=8.0):
        return None

    monkeypatch.setattr(gtfs_rt, "_trip_update_urls", lambda: ["fake://tu"])
    monkeypatch.setattr(gtfs_rt, "_fetch_feed", ok_fetch)
    gtfs_rt._trip_updates_cache["data"] = None
    gtfs_rt._trip_updates_cache["fetched_at"] = 0.0
    prima = asyncio.run(gtfs_rt.fetch_gtt_trip_updates())
    assert len(prima) == 1 and gtfs_rt.last_trip_updates_source == "live"

    # feed giu' MA entro 60s: buffer servito, flag 'cached' (dichiarato, non 'live')
    monkeypatch.setattr(gtfs_rt, "_fetch_feed", fail_fetch)
    seconda = asyncio.run(gtfs_rt.fetch_gtt_trip_updates())
    assert len(seconda) == 1 and gtfs_rt.last_trip_updates_source == "cached"

    # oltre i 60s: n/d onesto, lista vuota
    gtfs_rt._trip_updates_cache["fetched_at"] -= gtfs_rt.TRIP_UPDATES_STALE_TTL + 1
    terza = asyncio.run(gtfs_rt.fetch_gtt_trip_updates())
    assert terza == [] and gtfs_rt.last_trip_updates_source == "unavailable"


def test_catalogo_stale_durante_outage_totale():
    """Outage GTT lungo: il catalogo resta 215 linee (stale), NON crolla a 73."""
    import asyncio
    from app.gtfs_static import gtfs_manager

    gtfs_manager._routes_cache = {"4": "x", "15": "y"}
    gtfs_manager._cache_loaded_at = 0.0  # cache SCADUTA (outage da oltre 1h)
    gtfs_manager._cache_source = "gtfs-static-ufficiale"

    async def fail_download():
        return {}

    gtfs_manager._download_routes = fail_download
    result = asyncio.run(gtfs_manager.load_routes())
    assert result == {"4": "x", "15": "y"}, "deve servire il catalogo stale, non il fallback"
    assert gtfs_manager._cache_loaded_at > 0, "cache ri-datata: niente download a ogni ciclo"
    gtfs_manager._routes_cache = {}
    gtfs_manager._cache_source = "fallback"


def test_news_monitor_filtro_severo():
    """Solo scioperi/mezzi pubblici, solo recenti (<=3 giorni)."""
    import time as _t
    from datetime import datetime, timedelta, timezone
    import app.news_monitor as nm

    adesso = _t.time()
    oggi = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
    vecchia = (datetime.now(timezone.utc) - timedelta(days=10)).strftime("%a, %d %b %Y %H:%M:%S +0000")

    rss = f"""<?xml version="1.0"?><rss><channel>
    <item><title>Sciopero GTT giovedi: autobus e tram fermi</title><link>https://e.it/1</link><pubDate>{oggi}</pubDate><description>Agitazione sindacale dei lavoratori</description></item>
    <item><title>Il ritorno del vinile a Nichelino: mostra-mercato del disco</title><link>https://e.it/2</link><pubDate>{oggi}</pubDate><description>Mostra mercato</description></item>
    <item><title>Donna sevizjata per 12 ore: un arresto</title><link>https://e.it/3</link><pubDate>{oggi}</pubDate><description>Cronaca nera</description></item>
    <item><title>Proteste studentesche all'Istituto Boselli</title><link>https://e.it/4</link><pubDate>{oggi}</pubDate><description>Occupazione scolastica</description></item>
    <item><title>Vecchio sciopero dei tram di 10 giorni fa</title><link>https://e.it/5</link><pubDate>{vecchia}</pubDate><description>Tram fermi</description></item>
    </channel></rss>"""
    relevant = nm._filter_relevant(nm._parse_rss(rss.encode()), now=adesso)
    titoli = [i["titolo"] for i in relevant]
    assert len(titoli) == 1 and "Sciopero GTT" in titoli[0], f"deve restare SOLO lo sciopero recente: {titoli}"
    # il mercato del vinile, la cronaca nera, le proteste scolastiche e le
    # notizie di 10 giorni fa NON c'entrano i mezzi pubblici: escluse.
    assert not any("vinile" in t or "seviz" in t or "studente" in t or "Vecchio" in t for t in titoli)
