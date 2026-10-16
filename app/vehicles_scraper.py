"""
Telemetria GPS in tempo reale della flotta GTT Torino (GTFS-RT Vehicle Positions).

Caratteristiche:
- Prova in sequenza piu' endpoint GTFS-RT (GTT) con httpx asincrono e timeout aggressivo.
- Parsing Protocol Buffers con validazione bounding-box sull'area di Torino.
- DATI VERI SOLAMENTE: se GTT non pubblica posizioni, non vengono mostrati mezzi
  inventati ma un insieme vuoto con stato esplicito. Mai dati di fantasia.
- La velocita' non e' trasmessa da GTT: viene calcolata dagli spostamenti GPS reali.
"""

import asyncio
import logging
import math
import time

import httpx
from google.transit import gtfs_realtime_pb2

from app.feed_registry import get_feed_urls
from app.gtfs_static import gtfs_manager

logger = logging.getLogger("GTT-Vehicles")

# Endpoint di base; in testa vengono aggiunti quelli auto-scoperti dal catalogo
# open-data (a prova di futuro: se GTT sposta gli URL, il sistema si adatta).
BASE_GTFS_RT_VEHICLE_ENDPOINTS = [
    "https://percorsieorari.gtt.to.it/das_gtfsrt/vehicle_position.aspx",
    "https://www.gtt.to.it/gtfs-rt/vehicle_positions.pb",
    "https://www.gtt.to.it/gtfs-rt/vehiclepositions.pb",
]


def _vehicle_endpoints() -> list:
    """URL posizioni veicoli: prima quelli auto-scoperti, poi quelli noti."""
    discovered = get_feed_urls().get("vehicle_positions", [])
    seen, ordered = set(), []
    for url in discovered + BASE_GTFS_RT_VEHICLE_ENDPOINTS:
        if url not in seen:
            seen.add(url)
            ordered.append(url)
    return ordered

# Bounding box di validita' dell'intera rete GTT (citta' metropolitana di Torino + extraurbano).
TURIN_BBOX = {"lat_min": 44.0, "lat_max": 46.0, "lon_min": 6.0, "lon_max": 9.0}

# Sorgente dell'ultimo fetch: "live" (feed ufficiale GTT) oppure "unavailable".
last_fetch_source = "unknown"

# Outage del feed posizioni (server GTT giu', verificato dal vivo): dopo un
# fallimento TOTALE della cascata non si ritenta per 30s, altrimenti ogni ciclo
# di sync (~3s) brucia ~30s di timeout HTTP e rallenta ritardi e avvisi.
VEHICLES_FAIL_TTL = 30
_last_fail_at = 0.0

# Il feed veicoli di GTT NON trasmette il campo speed: lo calcoliamo dalla
# distanza tra posizioni GPS consecutive (dato reale, mai inventato).
# Ogni voce tiene anche l'ultima velocita' valida: un fix ripetuto (stesso
# timestamp per 2-3 cicli, frequente nei feed GTT) non deve far perdere il
# valore noto.
_previous_positions: dict = {}  # vehicle_id -> (lat, lon, timestamp, ultima_velocita)
MAX_REALISTIC_SPEED_MS = 30.0  # ~108 km/h: soglia anti-GPS-jitter
# Spostamento sotto questa soglia = dentro il rumore GPS (precisione urbana
# ~5-15m): il mezzo e' FERMO, si riporta 0.0 invece di velocita' fantasma da
# 1-4 m/s che manderebbero in tilt il rilevamento capolinea (soglia 1.5 m/s).
SPEED_NOISE_FLOOR_M = 5.0
# Oltre questa finestra tra due fix la media distanza/tempo non rappresenta
# piu' la velocita' ATTUALE del mezzo: nessun valore (n/d), mai un numero
# a caso. Il mezzo resta tracciato in memoria per la stessa durata, cosi'
# un lampeggio GPS di un ciclo non azzera il calcolo.
SPEED_MAX_WINDOW_S = 300.0
POSITIONS_MEMORY_TTL_S = 300.0  # coerenza con SPEED_MAX_WINDOW_S


def _haversine_meters(lat1, lon1, lat2, lon2) -> float:
    """Distanza reale in metri tra due punti GPS (formula di haversine)."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _real_speed(vehicle: dict):
    """Velocita' reale in m/s derivata dagli spostamenti GPS tra cicli consecutivi.

    Restituisce None se il mezzo e' comparso per la prima volta (o dopo una
    finestra troppo lunga): in quel caso la UI mostra 'n/d' invece di
    inventare uno zero.

    Regole di precisione (ognuna difende un caso osservabile sul feed GTT):
    - fix RIPETUTO (stesso timestamp, tipico di GTT per 2-3 cicli): nessuna
      informazione di movimento nuova, si conserva l'ultima velocita' nota
      invece di perderla con un 'n/d';
    - fix FUORI ORDINE (piu' vecchio del riferimento in memoria): ignorato,
      il riferimento piu' fresco resta il migliore per il calcolo successivo;
    - spostamento sotto il rumore GPS: FERMO (0.0), non velocita' fantasma;
    - SALTO GPS (oltre ~108 km/h): il fix glitchato NON diventa il nuovo
      riferimento, altrimenti avvelenerebbe anche il ciclo successivo; si
      conserva l'ultima velocita' valida;
    - finestra oltre SPEED_MAX_WINDOW_S: media non rappresentativa, n/d.
    """
    vid = vehicle.get("vehicle_id")
    lat, lon = vehicle.get("latitude"), vehicle.get("longitude")
    ts = vehicle.get("timestamp") or int(time.time())
    prev = _previous_positions.get(vid)

    if prev is None:
        _previous_positions[vid] = (lat, lon, ts, None)
        return None

    prev_lat, prev_lon, prev_ts, last_speed = prev
    if ts <= prev_ts:
        # Fix ripetuto o fuori ordine: il riferimento precedente (piu' fresco)
        # resta in memoria e la velocita' nota non si perde.
        return last_speed

    dt = ts - prev_ts
    if dt > SPEED_MAX_WINDOW_S:
        # Finestra troppo lunga: la media non dice nulla della velocita' di ADESSO.
        _previous_positions[vid] = (lat, lon, ts, None)
        return None

    dist = _haversine_meters(prev_lat, prev_lon, lat, lon)
    if dist < SPEED_NOISE_FLOOR_M:
        speed = 0.0  # spostamento nel rumore GPS: mezzo fermo
    else:
        speed = dist / dt
        if speed > MAX_REALISTIC_SPEED_MS:
            # Salto GPS: il fix NON aggiorna il riferimento (non deve
            # avvelenare il prossimo calcolo); resta l'ultima velocita' valida.
            return last_speed
    _previous_positions[vid] = (lat, lon, ts, speed)
    return round(speed, 1)


async def fetch_gtt_vehicle_positions() -> list:
    """Posizioni REALI della flotta GTT dal feed ufficiale.

    Politica dati: nessuna flotta simulata. Se GTT non pubblica posizioni,
    viene restituita una lista vuota e lo stato diventa 'unavailable':
    la Control Room mostra la verita', mai mezzi inventati.
    Durante un outage la cascata non viene ritentata per VEHICLES_FAIL_TTL
    (backoff): l'isteresi di gtt_full_sync mantiene la flotta in ultima
    posizione nota, marcata come datata.
    """
    global last_fetch_source, _last_fail_at
    if _last_fail_at and (time.time() - _last_fail_at) < VEHICLES_FAIL_TTL:
        last_fetch_source = "unavailable"
        return []
    vehicles = []
    valid_but_empty = False
    print("🔍 [SCRAPER] Avvio telemetria flotta GTT (GTFS-RT Vehicle Positions)...")

    # La scoperta degli URL usa httpx SINCRONO in cache: a scadenza TTL la
    # chiamata gira in un thread dedicato per non bloccare l'event loop
    # (refresher + WebSocket) per l'intero timeout HTTP.
    urls = await asyncio.to_thread(_vehicle_endpoints)

    for url in urls:
        try:
            async with httpx.AsyncClient(timeout=6.0, follow_redirects=True) as client:
                response = await client.get(url)
            print(f"🔍 [SCRAPER] Endpoint {url} -> HTTP {response.status_code}")

            if response.status_code != 200 or not response.content:
                continue
            if b"<html" in response.content[:200].lower():
                print(f"⚠️ [SCRAPER] {url} ha restituito HTML, non protobuf: salto.")
                continue

            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)

            for entity in feed.entity:
                if entity.HasField("vehicle"):
                    v = entity.vehicle
                    lat = v.position.latitude if v.HasField("position") else 0.0
                    lon = v.position.longitude if v.HasField("position") else 0.0
                    if not (TURIN_BBOX["lat_min"] <= lat <= TURIN_BBOX["lat_max"]):
                        continue
                    if not (TURIN_BBOX["lon_min"] <= lon <= TURIN_BBOX["lon_max"]):
                        continue
                    route_id = v.trip.route_id if v.HasField("trip") else ""
                    # ID univoco DI FALLBACK: l'ID mancante NON deve collassare
                    # tutti i mezzi anonimi in un solo "GTT-BUS" (sarebbe un
                    # mezzo invece di N). Si usa l'id dell'entita' del feed o,
                    # in ultima istanza, un progressivo del ciclo.
                    fallback_id = f"GTT-VEH-{entity.id}" if entity.id else f"GTT-VEH-{len(vehicles)}"
                    vehicle = {
                        "vehicle_id": v.vehicle.id or fallback_id,
                        "trip_id": v.trip.trip_id if v.HasField("trip") else "N/A",
                        # route_id GTFS (es. "2U") tradotto nel nome linea reale (es. "2")
                        "route_id": gtfs_manager.get_route_short_name(route_id) or "N/A",
                        "latitude": lat,
                        "longitude": lon,
                        "bearing": v.position.bearing if (v.HasField("position") and v.position.HasField("bearing")) else None,
                        # GTT non trasmette speed: valore reale calcolato dagli spostamenti GPS
                        "speed": None,
                        "timestamp": v.timestamp or int(time.time()),
                    }
                    feed_speed = v.position.speed if (v.HasField("position") and v.position.HasField("speed") and v.position.speed > 0) else None
                    computed = _real_speed(vehicle)
                    vehicle["speed"] = computed if computed is not None else feed_speed
                    vehicles.append(vehicle)

            # Difensivo: se il feed pubblicasse lo stesso mezzo due volte, si
            # deduplica tenendo il fix PIU' RECENTE (il primo esemplare
            # potrebbe essere la posizione del ciclo precedente).
            latest: dict = {}
            for v in vehicles:
                vid = v["vehicle_id"]
                if vid not in latest or (v.get("timestamp") or 0) > (latest[vid].get("timestamp") or 0):
                    latest[vid] = v
            vehicles = list(latest.values())

            # Feed raggiunto: mostra ESATTAMENTE quello che GTT pubblica,
            # anche se in questo momento sono pochissimi mezzi.
            last_fetch_source = "live"
            _last_fail_at = 0.0
            if vehicles:
                # Memoria posizioni con TTL: un mezzo che lampeggia (assente
                # per un ciclo GPS) NON perde il riferimento, cosi' alla
                # ricomparsa la velocita' si calcola dal fix precedente e non
                # riparte da 'n/d'. Il TTL coincide con la finestra massima di
                # calcolo della velocita'.
                now = time.time()
                for stale_id, entry in list(_previous_positions.items()):
                    if now - entry[2] > POSITIONS_MEMORY_TTL_S:
                        _previous_positions.pop(stale_id, None)
                with_speed = sum(1 for v in vehicles if v.get("speed") is not None)
                print(f"✅ [SCRAPER] Feed UFFICIALE GTT: {len(vehicles)} mezzi reali pubblicati e tracciati (velocita' reale calcolata per {with_speed}).")
                return vehicles
            # Feed VALIDO ma VUOTO: puo' essere un endpoint degradato (che
            # risponde ma non e' quello giusto) oppure GTT che in questo
            # istante non pubblica mezzi. NON si accetta lo zero dal primo
            # endpoint: si prova il successivo, cosi' un endpoint rotto non
            # puo' mascherare la flotta pubblicata su un altro.
            valid_but_empty = True
            vehicles = []
            print(f"⚠️ [SCRAPER] {url} ha risposto un feed valido ma senza veicoli: provo l'endpoint successivo.")
            continue

        except Exception as exc:
            print(f"⚠️ [SCRAPER] Endpoint {url} non raggiungibile: {exc}")
            continue

    # Tutti gli endpoint validi sono vuoti: GTT pubblica davvero zero mezzi
    # in questo istante (dato reale mostrato come tale, fonte 'live').
    if valid_but_empty:
        print("⚠️ [SCRAPER] Feed GTT raggiungibile ma nessun veicolo pubblicato in questo istante: mostro zero mezzi (dato reale).")
        return []
    # Nessun endpoint raggiungibile: lista vuota, MAI mezzi inventati.
    last_fetch_source = "unavailable"
    _last_fail_at = time.time()
    print("🛑 [SCRAPER] Feed posizioni GTT non raggiungibile: nessun dato pubblicato, nessun mezzo simulato (politica dati reali).")
    return []

