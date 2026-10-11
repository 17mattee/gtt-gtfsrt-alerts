import time
from fastapi import FastAPI, Request
from fastapi.responses import Response, HTMLResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from google.transit import gtfs_realtime_pb2
from gtfs_static import gtfs_manager
from scraper import fetch_gtt_alerts
from database import save_alerts_to_db
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Configurazione del Limiter per prevenire abusi / attacchi DoS
limiter = Limiter(key_func=get_remote_address)

ALERTS_DATA_CACHE = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inizializzazione dati statici GTFS
    gtfs_manager._ensure_data_loaded()
    
    # Primo scraping immediato all'avvio
    global ALERTS_DATA_CACHE
    print("[Scheduler] Primo avvio scraping GTT...")
    initial_alerts = fetch_gtt_alerts()
    if initial_alerts:
        ALERTS_DATA_CACHE = initial_alerts
        save_alerts_to_db(initial_alerts)

    # Scheduler in background (aggiornamento ogni 2 minuti)
    scheduler = BackgroundScheduler()
    scheduler.add_job(job_update_alerts, 'interval', minutes=2)
    scheduler.start()
    
    yield
    
    scheduler.shutdown()

def job_update_alerts():
    global ALERTS_DATA_CACHE
    print("[Scheduler] Aggiornamento feed GTFS-RT...")
    alerts = fetch_gtt_alerts()
    if alerts:
        ALERTS_DATA_CACHE = alerts
        save_alerts_to_db(alerts)
        print(f"[Scheduler] Feed aggiornato con {len(alerts)} avvisi.")

app = FastAPI(
    title="GTT Turin GTFS-RT Middleware",
    description="Middleware enterprise ad alta affidabilità per la conversione degli avvisi GTT in formato GTFS-Realtime Protobuf.",
    version="1.0.0",
    lifespan=lifespan
)

# Registrazione handler per il Rate Limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.get("/", response_class=HTMLResponse)
@limiter.limit("30/minute")
def dashboard(request: Request):
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
            <h1>GTT GTFS-RT Middleware <span class="badge">ONLINE & SECURED</span></h1>
            <p>Sistema di traduzione in tempo reale degli avvisi di infomobilità GTT Torino nello standard internazionale GTFS-Realtime.</p>
            
            <h3>Endpoint Protobuf Ufficiale:</h3>
            <div class="endpoint">GET <a href="/gtfs-rt/alerts.pb" style="color: #38bdf8; text-decoration: none;">/gtfs-rt/alerts.pb</a></div>
            
            <h3>Stato Attuale:</h3>
            <p>Avvisi attivi processati in memoria: <strong>{len(ALERTS_DATA_CACHE)}</strong></p>
            
            <h3>Architettura di Sicurezza:</h3>
            <ul>
                <li><strong>Rate Limiting:</strong> Attivo (Protezione Anti-DoS)</li>
                <li><strong>Retry Policies:</strong> Attive su Scraper e Supabase</li>
                <li><strong>Persistence:</strong> Storico PostgreSQL permanente</li>
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

    for item in ALERTS_DATA_CACHE:
        entity = feed.entity.add()
        entity.id = item["id"]
        entity.is_deleted = False

        alert = entity.alert
        alert.cause = item["cause"]
        alert.effect = item["effect"]

        # Intervallo di validità
        active_period = alert.active_period.add()
        active_period.start = item["start_time"]

        # Linee impattate
        for r_id in item["route_ids"]:
            informed_entity = alert.informed_entity.add()
            informed_entity.route_id = r_id

        # Testi descrittivi (Header e Descrizione)
        header_text = alert.header_text.translation.add()
        header_text.text = item["title"]
        header_text.language = "it"

        description_text = alert.description_text.translation.add()
        description_text.text = item["description"]
        description_text.language = "it"

    binary_data = feed.SerializeToString()
    return Response(content=binary_data, media_type="application/x-protobuf")
