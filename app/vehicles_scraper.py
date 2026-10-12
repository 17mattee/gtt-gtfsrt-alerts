"""
Telemetria GPS in tempo reale della flotta GTT Torino (GTFS-RT Vehicle Positions).

Caratteristiche:
- Prova in sequenza piu' endpoint GTFS-RT (GTT) con httpx asincrono e timeout aggressivo.
- Parsing Protocol Buffers con validazione bounding-box sull'area di Torino.
- DATI VERI SOLAMENTE: se GTT non pubblica posizioni, non vengono mostrati mezzi
  inventati ma un insieme vuoto con stato esplicito. Mai dati di fantasia.
- La velocita' non e' trasmessa da GTT: viene calcolata dagli spostamenti GPS reali.
"""

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

# Il feed veicoli di GTT NON trasmette il campo speed: lo calcoliamo dalla
# distanza tra posizioni GPS consecutive (dato reale, mai inventato).
_previous_positions: dict = {}  # vehicle_id -> (lat, lon, timestamp)
MAX_REALISTIC_SPEED_MS = 30.0  # ~108 km/h: soglia anti-GPS-jitter


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

    Restituisce None se il mezzo e' comparso per la prima volta: in quel caso
    la UI mostra 'n/d' invece di inventare uno zero.
    """
    vid = vehicle.get("vehicle_id")
    lat, lon = vehicle.get("latitude"), vehicle.get("longitude")
    ts = vehicle.get("timestamp") or int(time.time())
    prev = _previous_positions.get(vid)

    _previous_positions[vid] = (lat, lon, ts)

    if prev is None:
        return None

    prev_lat, prev_lon, prev_ts = prev
    dt = ts - prev_ts
    if dt <= 0:
        return None
    dist = _haversine_meters(prev_lat, prev_lon, lat, lon)
    speed = dist / dt
    if speed > MAX_REALISTIC_SPEED_MS:
        return None  # salto GPS: dato inaffidabile, meglio nessun valore
    return round(speed, 1)


async def fetch_gtt_vehicle_positions() -> list:
    """Posizioni REALI della flotta GTT dal feed ufficiale.

    Politica dati: nessuna flotta simulata. Se GTT non pubblica posizioni,
    viene restituita una lista vuota e lo stato diventa 'unavailable':
    la Control Room mostra la verita', mai mezzi inventati.
    """
    global last_fetch_source
    vehicles = []
    print("🔍 [SCRAPER] Avvio telemetria flotta GTT (GTFS-RT Vehicle Positions)...")

    for url in _vehicle_endpoints():
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
                    vehicle = {
                        "vehicle_id": v.vehicle.id or "GTT-BUS",
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

            # Difensivo: se il feed pubblicasse lo stesso mezzo due volte, si deduplica.
            seen_ids = set()
            deduped = []
            for v in vehicles:
                if v["vehicle_id"] not in seen_ids:
                    seen_ids.add(v["vehicle_id"])
                    deduped.append(v)
            vehicles = deduped

            # Feed raggiunto: mostra ESATTAMENTE quello che GTT pubblica,
            # anche se in questo momento sono pochissimi mezzi.
            last_fetch_source = "live"
            if vehicles:
                current_ids = {v["vehicle_id"] for v in vehicles}
                for stale in list(set(_previous_positions) - current_ids):
                    _previous_positions.pop(stale, None)
                with_speed = sum(1 for v in vehicles if v.get("speed") is not None)
                print(f"✅ [SCRAPER] Feed UFFICIALE GTT: {len(vehicles)} mezzi reali pubblicati e tracciati (velocita' reale calcolata per {with_speed}).")
            else:
                print("⚠️ [SCRAPER] Feed GTT raggiungibile ma nessun veicolo pubblicato in questo istante: mostro zero mezzi (dato reale).")
            return vehicles

        except Exception as exc:
            print(f"⚠️ [SCRAPER] Endpoint {url} non raggiungibile: {exc}")
            continue

    # Nessun endpoint raggiungibile: lista vuota, MAI mezzi inventati.
    last_fetch_source = "unavailable"
    print("🛑 [SCRAPER] Feed posizioni GTT non raggiungibile: nessun dato pubblicato, nessun mezzo simulato (politica dati reali).")
    return []

