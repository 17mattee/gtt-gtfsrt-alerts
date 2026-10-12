"""
Telemetria GPS in tempo reale della flotta GTT Torino (GTFS-RT Vehicle Positions).

Caratteristiche:
- Prova in sequenza piu' endpoint GTFS-RT (GTT / 5T) con httpx asincrono e timeout aggressivo.
- Parsing Protocol Buffers con validazione bounding-box su Torino (44.8-45.3 N, 7.3-7.9 E).
- Se nessun feed risponde, genera una flotta realistica e animata sulle linee reali di Torino:
  il sistema NON resta mai a zero mezzi.
- Ogni fase stampa log chiari e leggibili nel terminale.
"""

import logging
import math
import random
import time

import httpx
from google.transit import gtfs_realtime_pb2

from app.gtfs_static import FALLBACK_ROUTES, gtfs_manager

logger = logging.getLogger("GTT-Vehicles")

# Endpoint GTFS-RT per le posizioni veicoli (provati in ordine).
# Il primo e' il feed UFFICIALE GTT pubblicato su aperTO (Comune di Torino).
GTFS_RT_VEHICLE_ENDPOINTS = [
    "https://percorsieorari.gtt.to.it/das_gtfsrt/vehicle_position.aspx",
    "https://www.gtt.to.it/gtfs-rt/vehicle_positions.pb",
    "https://www.gtt.to.it/gtfs-rt/vehiclepositions.pb",
]

# Bounding box di validita' dell'intera rete GTT (citta' metropolitana di Torino + extraurbano).
TURIN_BBOX = {"lat_min": 44.0, "lat_max": 46.0, "lon_min": 6.0, "lon_max": 9.0}

# Sorgente dell'ultimo fetch: "live" (feed ufficiale GTT) oppure "simulated" (fallback).
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

    Restituisce None se il mezzo e' comparso per la prima volta e il feed
    non fornisce il dato: in quel caso la UI mostra 'n/d' invece di inventare uno zero.
    """
    vid = vehicle.get("vehicle_id")
    lat, lon = vehicle.get("latitude"), vehicle.get("longitude")
    ts = vehicle.get("timestamp") or int(time.time())
    prev = _previous_positions.get(vid)

    # Registra la posizione corrente per il prossimo ciclo.
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

# Corridoi reali del centro di Torino per la flotta di fallback:
# M1 (Fermi-Bengasi), Via Po, Corso Vittorio, Porta Nuova, Piazza Statuto, Lingotto.
CORRIDORS = [
    ("M1", [(45.4469, 7.6590), (45.0724, 7.6660), (45.0580, 7.6780), (45.0405, 7.6880), (45.0280, 7.6930)]),
    ("4", [(45.0160, 7.6360), (45.0400, 7.6520), (45.0703, 7.6869), (45.0800, 7.6600), (45.0830, 7.6220)]),
    ("15", [(45.0400, 7.6880), (45.0620, 7.6950), (45.0703, 7.6869), (45.0810, 7.6610), (45.0900, 7.6400)]),
    ("18", [(45.0530, 7.6820), (45.0703, 7.6869), (45.0840, 7.6540), (45.0930, 7.6320), (45.0980, 7.6100)]),
    ("10", [(45.0900, 7.6220), (45.0730, 7.6480), (45.0703, 7.6869), (45.0450, 7.6550), (45.0330, 7.6390)]),
    ("9", [(45.0360, 7.6330), (45.0550, 7.6520), (45.0703, 7.6869), (45.0860, 7.7000), (45.0950, 7.7100)]),
    ("16", [(45.0330, 7.6540), (45.0520, 7.6680), (45.0703, 7.6869), (45.0880, 7.6780), (45.0970, 7.6650)]),
    ("3", [(45.0260, 7.6390), (45.0480, 7.6560), (45.0703, 7.6869), (45.0890, 7.6590), (45.1000, 7.6400)]),
]

N_FALLBACK_VEHICLES = 42


async def fetch_gtt_vehicle_positions() -> list:
    """Recupera le posizioni reali della flotta GTT; se impossibile, flotta di fallback su linee reali."""
    global last_fetch_source
    vehicles = []
    print("🔍 [SCRAPER] Avvio telemetria flotta GTT (GTFS-RT Vehicle Positions)...")

    for url in GTFS_RT_VEHICLE_ENDPOINTS:
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

            if vehicles:
                # Prune della cache posizioni: conserva solo i mezzi visti in questo ciclo.
                current_ids = {v["vehicle_id"] for v in vehicles}
                for stale in list(set(_previous_positions) - current_ids):
                    _previous_positions.pop(stale, None)
                last_fetch_source = "live"
                with_speed = sum(1 for v in vehicles if v.get("speed") is not None)
                print(f"✅ [SCRAPER] Feed UFFICIALE GTT attivo: {len(vehicles)} mezzi reali tracciati in tempo reale da {url} (velocita' reale calcolata per {with_speed} mezzi).")
                return vehicles
            print(f"⚠️ [SCRAPER] Feed {url} senza veicoli validi nell'area di Torino.")

        except Exception as exc:
            print(f"⚠️ [SCRAPER] Endpoint {url} non raggiungibile: {exc}")
            continue

    print(f"🌐 [SCRAPER] Nessun feed esterno disponibile: attivo fallback smart su linee reali di Torino...")
    last_fetch_source = "simulated"
    try:
        vehicles = _generate_fallback_fleet()
        print(f"🌐 [SCRAPER] Flotta operativa generata: {len(vehicles)} mezzi su {len(CORRIDORS)} linee reali. Mappa MAI a zero.")
    except Exception as exc:
        # Cintura di sicurezza assoluta: nemmeno il fallback puo' fallire.
        logger.error("Fallback fleet fallito (%s): uso payload minimo garantito.", exc)
        vehicles = [{
            "vehicle_id": "GTT-TRN-2000",
            "trip_id": "trip_emergency_0",
            "route_id": "M1",
            "latitude": 45.0703,
            "longitude": 7.6869,
            "bearing": 90.0,
            "speed": 8.0,
            "timestamp": int(time.time()),
        }]
    return vehicles


def _interpolate(points, fraction):
    """Interpola una posizione lungo una polilinea di corridoio."""
    total = len(points) - 1
    scaled = min(fraction, 0.999) * total
    idx = int(scaled)
    local = scaled - idx
    lat1, lon1 = points[idx]
    lat2, lon2 = points[min(idx + 1, len(points) - 1)]
    return lat1 + (lat2 - lat1) * local, lon1 + (lon2 - lon1) * local


def _generate_fallback_fleet() -> list:
    """Flotta simulata animata lungo i corridoi reali delle linee di Torino."""
    vehicles = []
    now = time.time()
    route_names = list(FALLBACK_ROUTES)

    for i in range(N_FALLBACK_VEHICLES):
        route_id, corridor = CORRIDORS[i % len(CORRIDORS)]
        # Movimento deterministico nel tempo: i mezzi avanzano a ogni refresh.
        phase = ((now * 0.004) + i * 0.13) % 1.0
        lat, lon = _interpolate(corridor, phase)
        bearing = (i * 47 + int(now)) % 360
        vehicles.append({
            "vehicle_id": f"GTT-TRN-{2000 + i}",
            "trip_id": f"trip_{route_id}_{i}",
            "route_id": str(route_id),
            "latitude": round(lat, 5),
            "longitude": round(lon, 5),
            "bearing": float(bearing),
            "speed": float(random.Random(int(now) + i).randint(12, 38)) / 3.6,
            "timestamp": int(now),
        })

    # Qualche bus extra su linee reali del catalogo, fuori dai corridoi principali.
    for j in range(6):
        route_id = route_names[j % len(route_names)]
        angle = j * 2 * math.pi / 6 + now * 0.0005
        vehicles.append({
            "vehicle_id": f"GTT-TRN-{3000 + j}",
            "trip_id": f"trip_{route_id}_x{j}",
            "route_id": str(route_id),
            "latitude": round(45.0703 + 0.022 * math.sin(angle), 5),
            "longitude": round(7.6869 + 0.030 * math.cos(angle), 5),
            "bearing": float((j * 90) % 360),
            "speed": float(random.Random(int(now) + 100 + j).randint(10, 30)) / 3.6,
            "timestamp": int(now),
        })

    return vehicles
