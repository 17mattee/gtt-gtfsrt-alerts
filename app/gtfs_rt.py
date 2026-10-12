"""
Feed GTFS-Realtime UFFICIALI di GTT Torino (pubblicati come open data su aperTO).

Endpoint documentati (dataset "Feed GTFS Real-Time trasporti GTT", licenza CC BY 4.0):
- https://percorsieorari.gtt.to.it/das_gtfsrt/vehicle_position.aspx
- https://percorsieorari.gtt.to.it/das_gtfsrt/trip_update.aspx
- https://percorsieorari.gtt.to.it/das_gtfsrt/alerts.aspx

Ogni funzione e' asincrona, non bloccante e non solleva MAI eccezioni:
in caso di feed non raggiungibile restituisce una struttura vuota.
"""

import logging
import re
import time

import httpx
from google.transit import gtfs_realtime_pb2

from app.feed_registry import get_feed_urls
from app.gtfs_static import gtfs_manager

logger = logging.getLogger("GTT-Realtime")

GTFS_RT_BASE = "https://percorsieorari.gtt.to.it/das_gtfsrt"
BASE_TRIP_UPDATE_URL = f"{GTFS_RT_BASE}/trip_update.aspx"
BASE_ALERTS_URL = f"{GTFS_RT_BASE}/alerts.aspx"


def _trip_update_urls() -> list:
    """URL trip updates: prima quelli auto-scoperti dal catalogo open-data."""
    discovered = get_feed_urls().get("trip_updates", [])
    seen, ordered = set(), []
    for url in discovered + [BASE_TRIP_UPDATE_URL]:
        if url not in seen:
            seen.add(url)
            ordered.append(url)
    return ordered


def _alerts_urls() -> list:
    """URL service alerts: prima quelli auto-scoperti dal catalogo open-data."""
    discovered = get_feed_urls().get("alerts", [])
    seen, ordered = set(), []
    for url in discovered + [BASE_ALERTS_URL]:
        if url not in seen:
            seen.add(url)
            ordered.append(url)
    return ordered

# Sorgente dell'ultimo fetch riuscito per ogni feed.
last_trip_updates_source = "unknown"
last_alerts_source = "unknown"

# Cache del feed alerts: TTL breve per il super tempo reale, ma abbastanza
# lungo da non sovraccaricare i server GTT con richieste inutili.
ALERTS_CACHE_TTL = 20
_alerts_cache = {"data": None, "fetched_at": 0.0}
_raw_alerts_cache = {"data": b"", "fetched_at": 0.0}

# Traduzione in italiano delle cause GTFS-RT (spec Google Transit).
CAUSE_LABELS = {
    1: "Causa non specificata",
    2: "Altra causa",
    3: "Problema tecnico",
    4: "Sciopero",
    5: "Manifestazione / corteo",
    6: "Incidente",
    7: "Festività",
    8: "Condizioni meteo",
    9: "Manutenzione",
    10: "Lavori / cantiere",
    11: "Attività di polizia",
    12: "Emergenza medica",
    13: "Evento speciale",
}

# Traduzione in italiano degli effetti GTFS-RT.
EFFECT_LABELS = {
    1: "Servizio sospeso su questa linea",
    2: "Servizio ridotto",
    3: "Ritardi significativi",
    4: "Deviazione di percorso",
    5: "Corse aggiuntive",
    6: "Servizio modificato",
    7: "Altro effetto sul servizio",
    8: "Effetto non specificato",
    9: "Fermata spostata",
    10: "Nessun effetto sul servizio",
    11: "Problema di accessibilità",
}

# Categorie aggregate per la Control Room (deviazioni sempre ben visibili).
CATEGORY_BY_EFFECT = {
    4: "deviazione",
    3: "ritardo",
    1: "interruzione",
    9: "fermata_spostata",
    2: "servizio_ridotto",
    5: "potenziamento",
    11: "accessibilita",
}


def _classify_alert(cause: int, effect: int) -> str:
    """Categoria leggibile dell'avviso, pensata per gli operatori della Control Room."""
    category = CATEGORY_BY_EFFECT.get(effect)
    if category:
        return category
    return "informazione"


def _extract_routes_from_text(text: str, max_routes: int = 12) -> list:
    """Estrae le linee citate in un testo ("Linee 39, 1N deviate...").

    GTT spesso non compila informed_entity negli avvisi: le linee coinvolte
    sono indicate solo nel titolo/descrizione. Qui le riconosciamo e le
    validiamo contro il catalogo ufficiale delle 215 linee.
    """
    found = []
    for match in re.finditer(r"\bline[ea]e?\s+([A-Za-z0-9\,\s\/\.]+?)(?=\s+(?:deviat|soppress|sospes|limitat|potenziat|intensificat|modificat|attive|interess|circol|transit)|[.,;:!?)]|$)", text, re.IGNORECASE):
        for token in re.split(r"[,\s]+|\s+e\s+", match.group(1)):
            token = token.strip().strip(".")
            if not token or len(token) > 8:
                continue
            validated = gtfs_manager.validate_route_id(token)
            if validated and validated not in found:
                found.append(validated)
            if len(found) >= max_routes:
                return found
    return found


def _pick_translation(translated_string, language: str = "it") -> str:
    """Estrae la traduzione richiesta (o la prima disponibile) da un TranslatedString."""
    translations = list(translated_string.translation)
    if not translations:
        return ""
    for t in translations:
        if (t.language or "").lower().startswith(language):
            return t.text
    return translations[0].text


async def _fetch_feed(url: str, timeout: float = 8.0):
    """Scarica e deserializza un feed protobuf GTFS-RT. Restituisce FeedMessage o None."""
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.get(url)
        if response.status_code != 200 or not response.content:
            return None
        if b"<html" in response.content[:200].lower():
            return None
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        return feed
    except Exception as exc:
        logger.warning("Feed GTFS-RT non raggiungibile (%s): %s", url, exc)
        return None


async def fetch_raw_alerts_feed() -> bytes:
    """Byte grezzi del feed alerts ufficiale (per il proxy /gtfs-rt/alerts.pb).

    Con cache TTL di 20s: richieste ravvicinate di client diversi non
    generano altrettante chiamate verso i server GTT. Vuoto se offline.
    """
    cached = _raw_alerts_cache["data"]
    if cached and (time.time() - _raw_alerts_cache["fetched_at"]) < ALERTS_CACHE_TTL:
        return cached
    for url in _alerts_urls():
        try:
            async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
                response = await client.get(url)
            if response.status_code == 200 and b"<html" not in response.content[:200].lower():
                _raw_alerts_cache["data"] = response.content
                _raw_alerts_cache["fetched_at"] = time.time()
                return response.content
        except Exception as exc:
            logger.warning("Feed alerts ufficiale non raggiungibile (%s): %s", url, exc)
    return b""


async def fetch_gtt_trip_updates() -> list:
    """Trip Updates reali GTT: ritardi per corsa. Restituisce una lista di dict, mai eccezioni."""
    global last_trip_updates_source
    updates = []
    feed = None
    for url in _trip_update_urls():
        feed = await _fetch_feed(url)
        if feed is not None:
            break

    if feed is not None:
        now = time.time()
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            tu = entity.trip_update
            # RITARDO: si prende l'ultimo StopTimeUpdate con delay = previsione
            # attuale della corsa (verificato sui dati GTT: coincide col ritardo
            # della prossima fermata; le fermate passate portano ritardi vecchi).
            delay = None
            # PROSSIMA FERMATA: primo StopTimeUpdate con orario d'arrivo nel futuro,
            # con stop_id e orario previsto (per la precisione sull'arrivo reale).
            next_stop = None
            for stu in tu.stop_time_update:
                if stu.HasField("arrival") and stu.arrival.HasField("delay"):
                    delay = stu.arrival.delay
                elif stu.HasField("departure") and stu.departure.HasField("delay"):
                    delay = stu.departure.delay
                if next_stop is None:
                    arr_ts = None
                    if stu.HasField("arrival") and stu.arrival.time:
                        arr_ts = stu.arrival.time
                    elif stu.HasField("departure") and stu.departure.time:
                        arr_ts = stu.departure.time
                    if arr_ts and arr_ts >= now - 30:  # tolleranza 30s per dati al limite
                        next_stop = {"stop_id": stu.stop_id, "arrival_ts": arr_ts}
            # Standard GTFS-RT: trip tracciato senza campo delay = corsa che
            # sta rispettando l'orario programmato. Il ritardo e' quindi SEMPRE
            # disponibile per ogni corsa attiva (mai 'n/d').
            if delay is None:
                delay = 0
            route_id = gtfs_manager.get_route_short_name(tu.trip.route_id) if tu.HasField("trip") else ""
            updates.append({
                "trip_id": tu.trip.trip_id if tu.HasField("trip") else entity.id,
                "route_id": route_id or "N/A",
                "vehicle_id": tu.vehicle.id if tu.HasField("vehicle") else None,
                "delay_seconds": delay,
                "next_stop": next_stop,
                "timestamp": tu.timestamp or int(time.time()),
            })
        last_trip_updates_source = "live"
        print(f"🕐 [GTFS-RT] Trip Updates UFFICIALI GTT: {len(updates)} corse con stato di ritardo in tempo reale.")
    else:
        last_trip_updates_source = "unavailable"
        print("⚠️ [GTFS-RT] Feed Trip Updates ufficiale non raggiungibile in questo ciclo.")

    return updates


async def fetch_gtt_rt_alerts() -> list:
    """Service Alerts UFFICIALI GTT (feed GTFS-RT del Comune di Torino).

    Restituisce una lista nel formato della Control Room, mai eccezioni.
    """
    global last_alerts_source

    # Cache valida: nessuna nuova chiamata di rete verso GTT.
    if _alerts_cache["data"] is not None and (time.time() - _alerts_cache["fetched_at"]) < ALERTS_CACHE_TTL:
        logger.info("Cache alerts GTT valida (TTL %ss): riuso %d avvisi.", ALERTS_CACHE_TTL, len(_alerts_cache["data"]))
        return _alerts_cache["data"]

    alerts = []
    feed = None
    for url in _alerts_urls():
        feed = await _fetch_feed(url)
        if feed is not None:
            break

    if feed is not None:
        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue
            alert = entity.alert
            affected = []
            for informed in alert.informed_entity:
                if not informed.route_id:
                    continue
                short = gtfs_manager.get_route_short_name(informed.route_id)
                if short and short not in affected:
                    affected.append(short)
            header = _pick_translation(alert.header_text) or "Avviso di servizio GTT"
            description = _pick_translation(alert.description_text)
            if not affected:
                # GTT indica spesso le linee solo nel testo: le estraiamo e validiamo.
                affected = _extract_routes_from_text(header + " " + description)
            cause = int(alert.cause) if alert.cause else 1
            effect = int(alert.effect) if alert.effect else 8
            alerts.append({
                "alert_id": entity.id or f"gtt-rt-alert-{len(alerts)}",
                "header": header,
                "description": description,
                "affected_routes": affected,
                "cause": cause,
                "effect": effect,
                "cause_label": CAUSE_LABELS.get(cause, "Causa non specificata"),
                "effect_label": EFFECT_LABELS.get(effect, "Effetto non specificato"),
                "category": _classify_alert(cause, effect),
                "start_time": alert.active_period[0].start if alert.active_period else None,
                "end_time": alert.active_period[0].end if alert.active_period and alert.active_period[0].end else None,
                "timestamp": feed.header.timestamp or int(time.time()),
            })
        # Difensivo: se il feed pubblicasse alert_id duplicati, si deduplica.
        seen_ids = set()
        alerts = [a for a in alerts if not (a["alert_id"] in seen_ids or seen_ids.add(a["alert_id"]))]
        last_alerts_source = "live"
        _alerts_cache["data"] = alerts
        _alerts_cache["fetched_at"] = time.time()
        print(f"📢 [GTFS-RT] Service Alerts UFFICIALI GTT ricevuti: {len(alerts)} avvisi in tempo reale.")
    else:
        last_alerts_source = "unavailable"
        print("⚠️ [GTFS-RT] Feed Alerts ufficiale non raggiungibile in questo ciclo.")

    return alerts
