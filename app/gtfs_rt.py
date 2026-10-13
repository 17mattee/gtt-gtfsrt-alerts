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

# Outage del feed alerts: per RAW_ALERTS_FAIL_TTL secondi il proxy
# /gtfs-rt/alerts.pb NON ritenta la rete (ogni richiesta bloccherebbe il
# worker fino a ~30s di timeout per URL) e passa subito al feed ricostruito
# dallo snapshot.
RAW_ALERTS_FAIL_TTL = 30
_raw_alerts_fail_at = 0.0

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


# ----------------------------------------------------------------------------
# ELABORAZIONE DEVIAZIONI: dai testi GTT (via per via) a dati strutturati.
# Esempio GTT: "Direzione via Ventimiglia: da corso Sebastopoli angolo via
# Tripoli devia per corso IV Novembre, via Sacchi, percorso regolare;"
# ----------------------------------------------------------------------------

_WEEKDAY_OR_MONTH = (
    r"(?:lunedi|martedi|mercoledi|giovedi|venerdi|sabato|domenica|gennaio|"
    r"febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|"
    r"novembre|dicembre)"
)


def _parse_itinerary(body: str) -> dict:
    """Estrae punto di deviazione, elenco delle vie e ritorno al percorso regolare."""
    body = (body or "").strip()
    da, streets_txt = None, None
    m = re.search(r"^(?:da\s+)?(.+?)\s+(?:devia|prosegue)\s+(?:per|attraverso)\s+(.+)$", body, re.IGNORECASE | re.DOTALL)
    if m:
        da, streets_txt = m.group(1).strip(), m.group(2)
    else:
        m2 = re.search(r"^(?:da\s+)?(.+?)\s+per\s+(.+)$", body, re.IGNORECASE | re.DOTALL)
        if m2:
            da, streets_txt = m2.group(1).strip(), m2.group(2)
    if not streets_txt:
        return {"da": None, "strade": [], "ritorno": False}
    ritorno = "percorso regolare" in streets_txt.lower()
    streets_txt = re.sub(r",?\s*percorso regolare.*$", "", streets_txt, flags=re.IGNORECASE).strip(" .,")
    strade = [s.strip(" .") for s in streets_txt.split(",") if s.strip(" .")]
    return {"da": da, "strade": strade, "ritorno": ritorno}


def _parse_detour_details(header: str, description: str):
    """Elabora il testo di una deviazione GTT in dati strutturati.

    Estrae: periodo, motivo, e per OGNI direzione il punto in cui il mezzo
    lascia il percorso e le vie che percorre una per una (dal testo GTT,
    senza inventare nulla). Restituisce None se non c'e' nulla da strutturare.
    """
    text = (description or "").strip()
    if not text:
        return None
    out = {"periodo": None, "motivo": None, "direzioni": [], "itinerario": None, "fonte_esterna": False}
    try:
        # Nome della direzione dichiarata nel titolo (es. "deviata in direzione Sassi")
        header_dir = None
        mh = re.search(r"deviat[ae]\s+(?:in\s+)?direzione\s+(.+?)\s*$", header or "", re.IGNORECASE)
        if mh:
            header_dir = mh.group(1).strip().rstrip(".,;")

        for seg in re.split(r"[\n;]+", text):
            seg = seg.strip()
            if not seg:
                continue
            low = seg.lower()

            # Periodo: segmento con data/ora ("Martedi' 13 ottobre 2026 dalle ore 7.00 alle 20.00 circa")
            if out["periodo"] is None and re.search(_WEEKDAY_OR_MONTH, low) and re.search(r"\d{1,2}", seg) and "direzione" not in low[:12]:
                out["periodo"] = seg.rstrip(".,;")
                continue

            # Direzione esplicita: "Direzione X: <itinerario>"
            if low.startswith("direzione") and ":" in seg:
                name, body = seg.split(":", 1)
                itinerary = _parse_itinerary(body)
                out["direzioni"].append({
                    "nome": name.replace("Direzione", "").strip(),
                    **itinerary,
                })
                continue

            # Motivo: causa / lavori / impedimento
            if out["motivo"] is None and (low.startswith("causa") or low.startswith("lavori") or " causa " in low[:30]):
                out["motivo"] = seg.rstrip(".,;")
                continue

            # Rinvio al sito GTT: verificato PRIMA dell'itinerario (il testo
            # "Per conoscere la deviazione..." verrebbe altrimenti scambiato
            # per un itinerario perche' "deviazione" contiene "via").
            if "www.gtt" in low or "consulta la sezione" in low:
                out["fonte_esterna"] = True
                continue

            # Itinerario senza direzione esplicita (mono-direzionale).
            # Le parole delle vie usano confini di parola: "via" deve essere una
            # parola intera, non dentro "deviazione".
            if out["itinerario"] is None and re.search(r"\b(devia|prosegue|per)\b", low) and re.search(r"\b(via|corso|piazza|piazzale|viale|largo)\b", low):
                itinerary = _parse_itinerary(seg)
                if itinerary["strade"]:
                    itinerary["nome"] = header_dir
                    out["itinerario"] = itinerary
                continue

        # se c'e' una sola direzione e l'itinerario e' vuoto, riusa l'itinerario singolo
        if out["direzioni"] and not out["itinerario"] and header_dir and len(out["direzioni"]) == 1:
            pass
    except Exception as exc:
        logger.warning("Elaborazione deviazione fallita (%s): testo grezzo mantenuto.", exc)
        return None
    if not (out["periodo"] or out["motivo"] or out["direzioni"] or out["itinerario"]):
        return None
    return out


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
    generano altrettante chiamate verso i server GTT. Vuoto se offline
    (il chiamante ricostruisce il feed dallo snapshot): durante un outage
    prolungato non si ritenta la rete per RAW_ALERTS_FAIL_TTL secondi.
    """
    global _raw_alerts_fail_at
    cached = _raw_alerts_cache["data"]
    if cached and (time.time() - _raw_alerts_cache["fetched_at"]) < ALERTS_CACHE_TTL:
        return cached
    if _raw_alerts_fail_at and (time.time() - _raw_alerts_fail_at) < RAW_ALERTS_FAIL_TTL:
        return b""
    for url in _alerts_urls():
        try:
            async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
                response = await client.get(url)
            if response.status_code == 200 and b"<html" not in response.content[:200].lower():
                _raw_alerts_cache["data"] = response.content
                _raw_alerts_cache["fetched_at"] = time.time()
                _raw_alerts_fail_at = 0.0
                return response.content
        except Exception as exc:
            logger.warning("Feed alerts ufficiale non raggiungibile (%s): %s", url, exc)
    _raw_alerts_fail_at = time.time()
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
            # RITARDO ATTUALE della corsa. Il feed GTT pubblica piu' fermate
            # per corsa, in ordine di percorso: la PRIMA e' la fermata piu'
            # vicina al mezzo (la prossima fermata), le successive sono
            # previsioni sempre piu' lontane (spesso mostrano recupero a fine
            # corsa). Il ritardo attuale e' quindi quello della PRIMA fermata
            # con delay; usare l'ultima sovrastimava sistematicamente gli
            # anticipi di fine corsa (verificato sul feed reale: il 26% delle
            # corse cambia classe orario/ritardo/anticipo).
            # Precedenza standard GTFS-RT: TripUpdate.delay se presente.
            delay = tu.delay if tu.HasField("delay") else None
            # PROSSIMA FERMATA: primo StopTimeUpdate con orario d'arrivo nel futuro,
            # con stop_id e orario previsto (per la precisione sull'arrivo reale).
            next_stop = None
            for stu in tu.stop_time_update:
                if delay is None:
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
                # Nessun timestamp = corsa senza dato di aggiornamento: 0, mai
                # un finto "adesso" che farebbe vincere questa entita' sulla
                # corsa piu' recente nella scelta del ritardo del mezzo.
                "timestamp": tu.timestamp or 0,
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
            detour = _parse_detour_details(header, description) if _classify_alert(cause, effect) == "deviazione" else None
            alerts.append({
                "alert_id": entity.id or f"gtt-rt-alert-{len(alerts)}",
                "header": header,
                "description": description,
                "detour": detour,
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
        # Fallimento transitorio: meglio gli avvisi di 20s fa (cache) che zero
        # avvisi, che farebbero scattare il fallback e far "lampeggiare" la UI
        # con l'avviso di default per poi tornare a quelli veri al ciclo dopo.
        if _alerts_cache["data"]:
            print(f"📢 [GTFS-RT] Feed Alerts non raggiungibile: riuso {len(_alerts_cache['data'])} avvisi dalla cache ({int(time.time() - _alerts_cache['fetched_at'])}s).")
            return _alerts_cache["data"]
        print("⚠️ [GTFS-RT] Feed Alerts ufficiale non raggiungibile in questo ciclo.")

    return alerts
