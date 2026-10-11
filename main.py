import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response
from google.transit import gtfs_realtime_pb2
from apscheduler.schedulers.background import BackgroundScheduler
from scraper import fetch_gtt_alerts

CACHED_PROTOBUF = b""
scheduler = BackgroundScheduler()

def update_feed_cache():
    global CACHED_PROTOBUF
    
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(time.time())

    raw_alerts = fetch_gtt_alerts()

    for item in raw_alerts:
        entity = feed.entity.add()
        entity.id = item["id"]
        entity.is_deleted = False

        alert = entity.alert
        active_period = alert.active_period.add()
        active_period.start = int(time.time())

        for r_id in item.get("route_ids", []):
            informed = alert.informed_entity.add()
            informed.route_id = str(r_id)

        alert.cause = item.get("cause", gtfs_realtime_pb2.Alert.Cause.UNKNOWN_CAUSE)
        alert.effect = item.get("effect", gtfs_realtime_pb2.Alert.Effect.UNKNOWN_EFFECT)

        header_text = alert.header_text.translation.add()
        header_text.text = item["title"]
        header_text.language = "it"

        description_text = alert.description_text.translation.add()
        description_text.text = item["description"]
        description_text.language = "it"

    CACHED_PROTOBUF = feed.SerializeToString()

@asynccontextmanager
async def lifespan(app: FastAPI):
    update_feed_cache()
    scheduler.add_job(update_feed_cache, 'interval', minutes=5)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(
    title="GTT GTFS-RT Service Alerts Synthesizer",
    description="Engine open source enterprise per la conversione avvisi GTT in GTFS-RT Protobuf.",
    lifespan=lifespan
)

@app.get("/gtfs-rt/alerts.pb", response_class=Response)
def get_service_alerts():
    return Response(
        content=CACHED_PROTOBUF,
        media_type="application/x-protobuf",
        headers={"Content-Disposition": "inline; filename=alerts.pb"}
    )

@app.get("/")
def health_check():
    return {"status": "online", "feed_endpoint": "/gtfs-rt/alerts.pb", "cache_size_bytes": len(CACHED_PROTOBUF)}
