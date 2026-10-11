import time
import logging
import threading
from fastapi import FastAPI, Request
from fastapi.responses import Response, HTMLResponse
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from google.transit import gtfs_realtime_pb2
from app.gtfs_static import gtfs_manager
from app.scraper import fetch_gtt_alerts
from app.database import save_alerts_to_db
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Configurazione Logging Strutturato
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("GTT-Main")

limiter = Limiter(key_func=get_remote_address)

ALERTS_DATA_CACHE = []
cache_lock = threading.Lock()  # Thread Lock per prevenire race conditions

@asynccontextmanager
async def lifespan(app: FastAPI):
    gtfs_manager._ensure_data_loaded()
    
    global ALERTS_DATA_CACHE
    logger.info("Primo avvio scraping GTT...")
    initial_alerts = fetch_gtt_alerts()
    if initial_alerts:
        with cache_lock:
            ALERTS_DATA_CACHE = initial_alerts
        save_alerts_to_db(initial_alerts)

    scheduler = BackgroundScheduler()
    scheduler.add_job(job_update_alerts, 'interval', minutes=2)
    scheduler.start()
    
    yield
    
    scheduler.shutdown()

def job_update_alerts():
    global ALERTS_DATA_CACHE
    logger.info("Aggiornamento periodico feed GTFS-RT...")
    alerts = fetch_gtt_alerts()
    if alerts:
        with cache_lock:
            ALERTS_DATA_CACHE = alerts
        save_alerts_to_db(alerts)
        logger.info(f"Feed aggiornato con {len(alerts)} avvisi.")

app = FastAPI(
    title="GTT Turin GTFS-RT Middleware",
    description="Middleware enterprise ad alta affidabilità con Gzip, Thread Locking e Storico Supabase.",
    version="1.1.0",
    lifespan=lifespan
)

# Abilitazione compressione Gzip per ridurre la latenza di rete
app.add_middleware(GZipMiddleware, minimum_size=500)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.get("/", response_class=HTMLResponse)
@limiter.limit("30/minute")
def dashboard(request: Request):
    with cache_lock:
        cache_len = len(ALERTS_DATA_CACHE)

    html_content = f"""
    <!DOCTYPE html>
    <html lang="it">
    <head>
        <meta charset="UTF-8">
        <title>GTT GTFS-RT Dashboard</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #f8fafc; padding: 40px; margin: 0; }}
            .container {{ max-width: 800px; margin: auto; background: #1e293b; padding: 30px; border-radius: 12px; box-shadow: 0 10px 25px rgba(0,0,0,0.3); }}
            h1 {{ color: #38bdf8; margin-top: 0; }}
            .badge {{ background: #22c55e; color: white; padding: 4px 10px; border-radius: 20px; font-size: 12px; font-weight: bold; }}
            .endpoint {{ background: #0f172a; padding: 12px; border-radius: 6px; margin-top: 15px; font-family: monospace; color: #38bdf8; }}
            ul {{ padding-left: 20px; }}
            li {{ margin-bottom: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>GTT GTFS-RT Middleware <span class="badge">OPTIMIZED & SECURED</span></h1>
            <p>Sistema di traduzione in tempo reale degli avvisi GTT Torino nello standard internazionale GTFS-Realtime.</p>
            
            <h3>Endpoint Protobuf Ufficiale:</h3>
            <div class="endpoint">GET <a href="/gtfs-rt/alerts.pb" style="color: #38bdf8; text-decoration: none;">/gtfs-rt/alerts.pb</a></div>
            
            <h3>Stato Attuale:</h3>
            <p>Avvisi attivi processati in memoria: <strong>{cache_len}</strong></p>
            
            <h3>Ottimizzazioni Architetturali:</h3>
            <ul>
                <li><strong>Gzip Compression:</strong> Attiva (Payload alleggeriti)</li>
                <li><strong>Thread Safety (Locking):</strong> Attivo (Zero Race Conditions)</li>
                <li><strong>Rate Limiting & Retries:</strong> Attivi e protetti</li>
                <li><strong>Persistence:</strong> Storico PostgreSQL permanente su Supabase</li>
            </ul>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/gtfs-rt/alerts.pb")
@limiter.limit("60/minute")
def get_gtfs_rt_alerts(request: Request):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(time.time())

    with cache_lock:
        current_alerts = list(ALERTS_DATA_CACHE)

    for item in current_alerts:
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

        header_text = alert.header_text.translation.add()
        header_text.text = item["title"]
        header_text.language = "it"

        description_text = alert.description_text.translation.add()
        description_text.text = item["description"]
        description_text.language = "it"

    binary_data = feed.SerializeToString()
    return Response(content=binary_data, media_type="application/x-protobuf")
