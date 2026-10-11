import os
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler
from google.transit import gtfs_realtime_pb2
from scraper import fetch_gtt_alerts
from gtfs_static import gtfs_manager

FEED_CACHE = b""
ALERTS_DATA_CACHE = []

def update_feed_cache():
    global FEED_CACHE, ALERTS_DATA_CACHE
    print("[Scheduler] Aggiornamento feed GTFS-RT...")
    alerts = fetch_gtt_alerts()
    ALERTS_DATA_CACHE = alerts
    
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(time.time())
    
    for alert_data in alerts:
        entity = feed.entity.add()
        entity.id = alert_data["id"]
        
        # Scrive l'orario di inizio nel Protobuf (Active Period)
        if "start_time" in alert_data:
            active_period = entity.alert.active_period.add()
            active_period.start = alert_data["start_time"]
        
        for route_id in alert_data["route_ids"]:
            selector = entity.alert.informed_entity.add()
            selector.route_id = route_id
            
        if not alert_data["route_ids"]:
            selector = entity.alert.informed_entity.add()
            selector.agency_id = "GTT"
            
        entity.alert.cause = alert_data["cause"]
        entity.alert.effect = alert_data["effect"]
        
        header = entity.alert.header_text.translation.add()
        header.text = alert_data["title"]
        header.language = "it"
        
        desc = entity.alert.description_text.translation.add()
        desc.text = alert_data["description"]
        desc.language = "it"
        
    FEED_CACHE = feed.SerializeToString()
    print(f"[Scheduler] Feed aggiornato con {len(alerts)} avvisi.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    gtfs_manager._ensure_data_loaded()
    update_feed_cache()
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_feed_cache, 'interval', minutes=2)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan, title="GTT GTFS-RT Alerts")

@app.get("/gtfs-rt/alerts.pb")
def get_alerts_pb():
    return Response(content=FEED_CACHE, media_type="application/x-protobuf")

@app.get("/", response_class=HTMLResponse)
def dashboard():
    html = "<html><head><title>Dashboard GTT GTFS-RT</title><meta charset='utf-8'><style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif; margin:40px; background:#f4f4f9;} .alert{background:white; border:1px solid #ddd; padding:20px; margin-bottom:15px; border-radius:10px; box-shadow:0 2px 4px rgba(0,0,0,0.05);} .badge{background:#007bff; color:white; padding:4px 10px; border-radius:6px; font-weight:bold; font-size:14px; margin-right:5px;} .date{color:#666; font-size:13px; margin-bottom:10px;}</style></head><body>"
    html += "<h1>🚌 Monitoraggio Avvisi GTT (GTFS-RT)</h1>"
    html += f"<p style='color:#666;'>Ultimo aggiornamento: {time.strftime('%H:%M:%S')}</p><hr>"
    
    if not ALERTS_DATA_CACHE:
        html += "<p>Nessun avviso attivo sulla rete.</p>"
    else:
        for a in ALERTS_DATA_CACHE:
            start_str = time.strftime('%d/%m/%Y %H:%M', time.localtime(a['start_time'])) if a.get('start_time') else 'Ora'
            html += f"<div class='alert'><h3>{a['title']}</h3>"
            html += f"<div class='date'>Attivo da: {start_str}</div>"
            if a['route_ids']:
                html += "<p><strong>Linee in deviazione:</strong> "
                for r in a['route_ids']:
                    html += f"<span class='badge'>{r}</span>"
                html += "</p>"
            else:
                html += "<p><strong>Linee in deviazione:</strong> <span class='badge' style='background:#6c757d;'>Generale / Tutte</span></p>"
            html += f"<p>{a['description']}</p></div>"
    
    html += "</body></html>"
    return html
