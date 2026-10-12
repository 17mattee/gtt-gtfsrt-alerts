import time
import logging
import threading
import json
import os
import redis
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from google.transit import gtfs_realtime_pb2
from app.gtfs_static import gtfs_manager
from app.scraper import fetch_gtt_alerts
from app.database import save_alerts_to_db, fetch_latest_alerts_from_db
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from prometheus_fastapi_instrumentator import Instrumentator
import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("GTT-Main")
limiter = Limiter(key_func=get_remote_address)

redis_url = os.getenv("REDIS_URL")
redis_client = redis.Redis.from_url(redis_url, decode_responses=True) if redis_url else None

LOCAL_FALLBACK_CACHE = []
cache_lock = threading.Lock()
REDIS_CACHE_KEY = "gtt_rt_alerts_cache"

def set_cached_alerts(alerts: list):
    global LOCAL_FALLBACK_CACHE
    if redis_client:
        try:
            redis_client.set(REDIS_CACHE_KEY, json.dumps(alerts))
            return
        except Exception as e:
            logger.warning(f"Errore scrittura Redis: {e}")
    with cache_lock:
        LOCAL_FALLBACK_CACHE = alerts

def get_cached_alerts() -> list:
    global LOCAL_FALLBACK_CACHE
    if redis_client:
        try:
            data = redis_client.get(REDIS_CACHE_KEY)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.warning(f"Errore lettura Redis: {e}")
    with cache_lock:
        return list(LOCAL_FALLBACK_CACHE)

@asynccontextmanager
async def lifespan(app: FastAPI):
    gtfs_manager._ensure_data_loaded()
    initial_alerts = fetch_gtt_alerts()
    if initial_alerts:
        set_cached_alerts(initial_alerts)
        save_alerts_to_db(initial_alerts)
    else:
        fallback_alerts = fetch_latest_alerts_from_db()
        if fallback_alerts:
            set_cached_alerts(fallback_alerts)
    scheduler = BackgroundScheduler()
    scheduler.add_job(job_update_alerts, "interval", minutes=2)
    scheduler.start()
    yield
    scheduler.shutdown()

def job_update_alerts():
    alerts = fetch_gtt_alerts()
    if alerts:
        set_cached_alerts(alerts)
        save_alerts_to_db(alerts)

app = FastAPI(title="GTT Turin GTFS-RT Middleware", version="2.0.0", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=500)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
Instrumentator().instrument(app).expose(app, endpoint="/metrics")

@app.get("/", response_class=HTMLResponse)
@limiter.limit("30/minute")
def dashboard(request: Request):
    current_alerts = get_cached_alerts()
    count = len(current_alerts)
    html = f"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8">
<title>GTT GTFS-RT Live Dashboard & Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<style>
body {{ font-family: sans-serif; background: #0f172a; color: #f8fafc; padding: 30px; margin: 0; }}
.container {{ max-width: 1000px; margin: auto; background: #1e293b; padding: 30px; border-radius: 12px; }}
h1 {{ color: #38bdf8; }}
.badge {{ background: #22c55e; color: white; padding: 4px 10px; border-radius: 20px; font-size: 12px; }}
.endpoint {{ background: #0f172a; padding: 12px; border-radius: 6px; margin-top: 15px; font-family: monospace; color: #38bdf8; }}
#map {{ height: 350px; border-radius: 8px; margin-top: 20px; }}
</style>
</head>
<body>
<div class="container">
<h1>GTT GTFS-RT Synthesizer <span class="badge">V2.0 LIVE MAP</span></h1>
<p>Middleware di conversione avvisi GTT Torino in GTFS-Realtime (Protobuf).</p>
<div class="endpoint">GET <a href="/gtfs-rt/alerts.pb">/gtfs-rt/alerts.pb</a></div>
<div class="endpoint">GET <a href="/health">/health</a></div>
<div class="endpoint">GET <a href="/metrics">/metrics</a></div>
<p>Avvisi attivi in cache: <strong>{count}</strong></p>
<div id="map"></div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
var map = L.map("map").setView([45.0703, 7.6869], 13);
L.tileLayer("https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{ maxZoom: 19 }}).addTo(map);
L.marker([45.0703, 7.6869]).addTo(map).bindPopup("<b>GTT Torino Hub Centrale</b>").openPopup();
</script>
</body>
</html>"""
    return HTMLResponse(content=html)

@app.get("/health")
def health_check():
    db_status = "disconnected"
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        try:
            with psycopg.connect(database_url, connect_timeout=2) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            db_status = "connected"
        except Exception:
            db_status = "error"

    redis_status = "disabled"
    if redis_client:
        try:
            redis_client.ping()
            redis_status = "connected"
        except Exception:
            redis_status = "error"

    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "database": db_status,
            "redis": redis_status,
            "cached_alerts": len(get_cached_alerts()),
            "timestamp": int(time.time())
        }
    )

@app.get("/gtfs-rt/alerts.pb")
@limiter.limit("60/minute")
def get_gtfs_rt_alerts(request: Request):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(time.time())
    for item in get_cached_alerts():
        entity = feed.entity.add()
        entity.id = item["id"]
        entity.is_deleted = False
        alert = entity.alert
        alert.cause = item["cause"]
        alert.effect = item["effect"]
        active_period = alert.active_period.add()
        active_period.start = item["start_time"]
        for r_id in item["route_ids"]:
            informed_entity = alert.informed_entity.add()
            informed_entity.route_id = r_id
        ht = alert.header_text.translation.add()
        ht.text = item["title"]
        ht.language = "it"
        dt = alert.description_text.translation.add()
        dt.text = item["description"]
        dt.language = "it"
    return Response(content=feed.SerializeToString(), media_type="application/x-protobuf")

@app.websocket("/ws/control-room")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Invio immediato garantito (dati di flotta Torino)
        vehicles = [
            {"vehicle_id": "GTT-Tram-3", "trip_id": "trip_01", "route_id": "3", "latitude": 45.0703, "longitude": 7.6869, "bearing": 90.0, "speed": 15.0, "timestamp": 1718000000},
            {"vehicle_id": "GTT-Bus-4", "trip_id": "trip_02", "route_id": "4", "latitude": 45.0650, "longitude": 7.6800, "bearing": 180.0, "speed": 22.0, "timestamp": 1718000000},
            {"vehicle_id": "GTT-Bus-58", "trip_id": "trip_03", "route_id": "58/", "latitude": 45.0750, "longitude": 7.6600, "bearing": 45.0, "speed": 18.0, "timestamp": 1718000000}
        ]
        await websocket.send_json({"type": "full_network_update", "data": full_data})

        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=2.0)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "full_network_update", "data": full_data})
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        manager.disconnect(websocket)
