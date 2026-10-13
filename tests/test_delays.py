"""Test di correttezza dei tempi e dei ritardi GTT.

Coprono i bug risolti sul feed reale:
- il ritardo attuale di una corsa e' quello della PRIMA fermata pubblicata
  (la prossima fermata), non dell'ultima (previsione di fine corsa);
- TripUpdate.delay, quando presente, ha la precedenza (standard GTFS-RT);
- le corse oltre mezzanotte (orari GTFS 24:00+) vanno ancorate alla
  giornata di servizio PRECEDENTE.
"""

import asyncio
import time
from datetime import datetime, timezone

from google.transit import gtfs_realtime_pb2

from app import gtfs_rt
from app.gtt_full_sync import _predict_next_stop


def _feed_with_trip(stus, trip_id="28371686U", vehicle_id="1389U", trip_delay=None):
    """FeedMessage GTFS-RT con una sola corsa. stus: [(stop_id, arr_delay, arr_time, dep_delay)]."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(time.time())
    entity = feed.entity.add()
    entity.id = "e1"
    tu = entity.trip_update
    tu.trip.trip_id = trip_id
    tu.vehicle.id = vehicle_id
    if trip_delay is not None:
        tu.delay = trip_delay
    for stop_id, arr_delay, arr_time, dep_delay in stus:
        stu = tu.stop_time_update.add()
        stu.stop_id = stop_id
        if arr_delay is not None:
            stu.arrival.delay = arr_delay
        if arr_time is not None:
            stu.arrival.time = arr_time
        if dep_delay is not None:
            stu.departure.delay = dep_delay
    return feed


def _run_trip_updates(monkeypatch, feed):
    monkeypatch.setattr(gtfs_rt, "_trip_update_urls", lambda: ["fake://trip_update"])
    monkeypatch.setattr(gtfs_rt, "_fetch_feed", lambda url, timeout=8.0: _async(feed))
    return asyncio.run(gtfs_rt.fetch_gtt_trip_updates())


async def _async(value):
    return value


def test_delay_e_della_prima_fermata_non_dell_ultima(monkeypatch):
    """Il feed GTT pubblica piu' fermate: il ritardo ATTUALE e' il primo delay.

    Caso reale (corsa 28378604U): prima fermata +154s (in ritardo), ultima -85s
    (previsione di recupero a fine corsa). Il vecchio codice mostrava
    'in anticipo' un mezzo in ritardo.
    """
    feed = _feed_with_trip([
        ("", 154, None, None),
        ("", 110, None, None),
        ("", 26, None, None),
        ("", -30, None, None),
        ("", -85, None, None),
    ])
    updates = _run_trip_updates(monkeypatch, feed)
    assert len(updates) == 1
    assert updates[0]["delay_seconds"] == 154


def test_trip_update_delay_ha_precedenza(monkeypatch):
    """TripUpdate.delay (standard GTFS-RT) vince sui delay delle singole fermate."""
    feed = _feed_with_trip([("", 154, None, None), ("", -85, None, None)], trip_delay=42)
    updates = _run_trip_updates(monkeypatch, feed)
    assert updates[0]["delay_seconds"] == 42


def test_corsa_senza_delay_e_in_orario(monkeypatch):
    """Corsa tracciata senza alcun campo delay = rispetta l'orario (standard GTFS-RT)."""
    now = int(time.time())
    feed = _feed_with_trip([("", None, now + 60, None), ("", None, now + 120, None)])
    updates = _run_trip_updates(monkeypatch, feed)
    assert updates[0]["delay_seconds"] == 0
    # prossima fermata: il primo orario d'arrivo nel futuro
    assert updates[0]["next_stop"]["arrival_ts"] == now + 60


def test_fallback_su_departure_delay(monkeypatch):
    """Fermata senza arrival.delay ma con departure.delay: il delay non va perso."""
    feed = _feed_with_trip([("", None, None, 97)])
    updates = _run_trip_updates(monkeypatch, feed)
    assert updates[0]["delay_seconds"] == 97


def test_prossima_fermata_salta_le_passate(monkeypatch):
    """La prossima fermata e' il primo arrivo NEL FUTURO, mai una fermata passata.

    Il feed GTT pubblica anche fermate appena passate (fino a ~2 min fa):
    oltre la tolleranza di 30s vengono scartate.
    """
    now = int(time.time())
    feed = _feed_with_trip([
        ("", None, now - 120, None),  # fermata gia' passata
        ("", None, now - 45, None),   # passata da troppo: oltre la tolleranza
        ("", None, now + 90, None),   # la vera prossima fermata
        ("", None, now + 200, None),
    ])
    updates = _run_trip_updates(monkeypatch, feed)
    assert updates[0]["next_stop"]["arrival_ts"] == now + 90


def test_previsione_corsa_dopo_mezzanotte():
    """Orari GTFS 24:00+ (verificati nel feed GTT fino a 26:xx) = giornata precedente.

    Sono le 00:30 di Roma: una corsa notturna partita ieri ha la prossima
    fermata programmata alle 24:40 (= 00:40 di oggi). Ancorarla alla mezzanotte
    di oggi la sposterebbe di 24 ore.
    """
    # 2026-10-14 00:30 ora locale di Roma (CEST, +2) = 2026-10-13 22:30 UTC
    now = int(datetime(2026, 10, 13, 22, 30, tzinfo=timezone.utc).timestamp())
    seq = [(86100, "S_2355"), (88800, "S_2440")]  # 23:55 e 24:40 della giornata di ieri
    prediction = _predict_next_stop(seq, delay_seconds=60, now=now)
    assert prediction is not None
    predicted, stop_id = prediction
    assert stop_id == "S_2440"
    # 24:40 di ieri + 60s di ritardo = oggi 00:41 Roma = ieri 22:41 UTC
    expected = int(datetime(2026, 10, 13, 22, 41, tzinfo=timezone.utc).timestamp())
    assert predicted == expected


def test_previsione_giornata_normale():
    """In orario diurno la previsione resta ancorata alla mezzanotte di oggi."""
    # 2026-10-13 12:30 UTC = 14:30 Roma (CEST)
    now = int(datetime(2026, 10, 13, 12, 30, tzinfo=timezone.utc).timestamp())
    seq = [(52200, "S_1430"), (53400, "S_1445")]  # 14:30 e 14:45 di oggi
    prediction = _predict_next_stop(seq, delay_seconds=120, now=now)
    assert prediction is not None
    predicted, stop_id = prediction
    assert stop_id == "S_1430"
    # 14:30 + 2 min di ritardo = 14:32 Roma = 12:32 UTC
    expected = int(datetime(2026, 10, 13, 12, 32, tzinfo=timezone.utc).timestamp())
    assert predicted == expected


def test_previsione_fermata_appena_passata_mostrata_come_zero():
    """Fermata prevista fino a 60s fa: resta 'tra 0 min', non viene scartata."""
    now = int(datetime(2026, 10, 13, 12, 30, tzinfo=timezone.utc).timestamp())
    seq = [(52170, "S_142930")]  # 14:29:30, prevista 30s fa
    prediction = _predict_next_stop(seq, delay_seconds=0, now=now)
    assert prediction is not None
    predicted, stop_id = prediction
    assert stop_id == "S_142930"
    assert predicted == now - 30


def test_delay_derivato_corsa_solo_orari():
    """Corse GTT 'solo-orari': delay = previsione - orario programmato."""
    from datetime import datetime, timezone
    from app.gtt_full_sync import _derive_delay_from_arrival

    # 2026-10-13 12:00 UTC = 14:00 Roma (CEST)
    base_utc = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc).timestamp()
    sched_1410 = 51000  # 14:10 locali
    seq = [(50400, "S_1400"), (sched_1410, "S_1410"), (51600, "S_1430")]

    # previsione d'arrivo alle 14:11:30 Roma (base+690s) -> 90s di ritardo reale
    assert _derive_delay_from_arrival(seq, base_utc + 690) == 90

    # anticipo: arrivo previsto alle 14:08 -> 2 min prima delle 14:10
    assert _derive_delay_from_arrival(seq, base_utc + 480) == -120

    # puntuale: 10s dopo le 14:10
    assert _derive_delay_from_arrival(seq, base_utc + 610) == 10


def test_delay_derivato_oltre_tolleranza_none():
    """Nessuna fermata programmata entro la tolleranza: None, mai un numero a caso."""
    from datetime import datetime, timezone
    from app.gtt_full_sync import _derive_delay_from_arrival

    base_utc = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc).timestamp()
    seq = [(50400, "S_1400"), (51000, "S_1410")]
    # arrivo previsto lontano da ogni orario programmato (>300s)
    assert _derive_delay_from_arrival(seq, base_utc + 3600) is None
    assert _derive_delay_from_arrival([], base_utc) is None
    assert _derive_delay_from_arrival(seq, 0) is None


def test_delay_derivato_corsa_dopo_mezzanotte():
    """Corse notturne 'solo-orari': la derivazione prova anche la mezzanotte di ieri."""
    from datetime import datetime, timezone
    from app.gtt_full_sync import _derive_delay_from_arrival

    # 2026-10-14 00:30 Roma = 2026-10-13 22:30 UTC
    now_utc = datetime(2026, 10, 13, 22, 30, tzinfo=timezone.utc).timestamp()
    seq = [(86100, "S_2355"), (88800, "S_2440")]  # 23:55 e 24:40 di ieri
    # previsione: oggi 00:42 Roma (22:42 UTC) = 2 min dopo le 24:40 di ieri
    arr = datetime(2026, 10, 13, 22, 42, tzinfo=timezone.utc).timestamp()
    derived = _derive_delay_from_arrival(seq, arr)
    assert derived == 120, f"atteso 120s di ritardo, derivato {derived}"
