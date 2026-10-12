import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from google.transit import gtfs_realtime_pb2

from app.gtt_full_sync import fetch_all_gtt_data, get_network_snapshot
from app.websocket_manager import manager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("GTT-Main")


async def _persist_snapshot(data: dict) -> None:
    """Persistenza opzionale su Supabase (no-op se non configurato). Mai bloccante, mai fatale."""
    try:
        from app.database_writer import save_network_data_to_db
        # save_network_data_to_db è async ma usa il client sync Supabase:
        # la eseguiamo in un thread dedicato per non bloccare l'event loop.
        await asyncio.to_thread(asyncio.run, save_network_data_to_db(data))
    except Exception as exc:
        logger.warning("Persistenza su database saltata: %s", exc)


async def _background_refresher() -> None:
    """Cuore del real-time: UN solo ciclo di fetch condiviso da tutti i client.

    Scarica i feed ufficiali GTT in parallelo ogni ~2 secondi e aggiorna lo
    snapshot globale. N client WebSocket = comunque 1 sola serie di fetch:
    la latenza resta costante e i server GTT non vengono sovraccaricati.
    """
    while True:
        started = time.time()
        try:
            payload = await fetch_all_gtt_data()
            asyncio.create_task(_persist_snapshot(payload))
            # fetch_all_gtt_data aggiorna già lo snapshot globale
            logger.info(
                "♻️ [REFRESHER] Snapshot rinnovato in %.1fs -> %s mezzi, %s avvisi, %s corse.",
                time.time() - started,
                len(payload.get("vehicles", [])),
                len(payload.get("alerts", [])),
                len(payload.get("trip_updates", [])),
            )
        except Exception as exc:
            logger.error("♻️ [REFRESHER] Ciclo di refresh fallito (%s): uso l'ultimo snapshot valido.", exc)

        # Pausa minima: il fetch parallelo dura ~2-3s, quindi il dato fresco
        # arriva tipicamente ogni ~3-4 secondi, in scia con i feed GTT.
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(application: FastAPI):
    refresher = asyncio.create_task(_background_refresher())
    logger.info("🚀 Avvio GTT Enterprise Middleware: refresher real-time attivo.")
    yield
    refresher.cancel()
    logger.info("🛑 Arresto GTT Enterprise Middleware.")


app = FastAPI(
    title="GTT Torino GTFS-RT Enterprise Middleware",
    version="2.2.0",
    lifespan=lifespan,
)

# Monta la cartella statica per servire la UI
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def get_dashboard():
    return FileResponse("static/index.html")


@app.get("/health")
async def health_check():
    snapshot = get_network_snapshot()
    return {
        "status": "healthy",
        "city": "Turin",
        "agency": "GTT",
        "database": bool(os.getenv("DATABASE_URL") or os.getenv("SUPABASE_URL")),
        "cached_alerts": len(snapshot.get("alerts", [])),
        "vehicles": len(snapshot.get("vehicles", [])),
        "routes": len(snapshot.get("routes_catalog", {})),
        "trip_updates": len(snapshot.get("trip_updates", [])),
        "last_update": snapshot.get("meta", {}).get("last_update"),
        "timestamp": int(time.time()),
    }


@app.get("/gtfs-rt/alerts.pb")
async def gtfs_rt_alerts_feed():
    """Feed GTFS-Realtime (Service Alerts) in formato Protocol Buffers standard.

    Proxy trasparente del feed UFFICIALE GTT quando raggiungibile;
    altrimenti ricostruisce il feed dallo snapshot locale.
    """
    try:
        from app.gtfs_rt import fetch_raw_alerts_feed
        raw = await fetch_raw_alerts_feed()
        if raw:
            logger.info("Servito feed alerts UFFICIALE GTT (%d bytes).", len(raw))
            return Response(content=raw, media_type="application/x-protobuf")
    except Exception as exc:
        logger.warning("Proxy feed alerts ufficiale fallito (%s): uso snapshot locale.", exc)

    alerts = get_network_snapshot().get("alerts", [])

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(time.time())

    for alert in alerts:
        entity = feed.entity.add()
        entity.id = str(alert.get("alert_id", "gtt-alert"))
        informed = entity.alert.informed_entity
        for route_id in alert.get("affected_routes", []):
            informed.add().route_id = str(route_id)
        entity.alert.header_text.translation.add(
            language="it", text=str(alert.get("header", "Avviso di servizio GTT"))
        )
        entity.alert.description_text.translation.add(
            language="it", text=str(alert.get("description") or alert.get("header", ""))
        )

    return Response(content=feed.SerializeToString(), media_type="application/x-protobuf")


def _fallback_payload() -> dict:
    """Payload di emergenza: garantisce che il WebSocket non invii MAI nulla di vuoto."""
    try:
        snapshot = get_network_snapshot()
        return {
            "vehicles": snapshot.get("vehicles") or [],
            "trip_updates": snapshot.get("trip_updates") or [],
            "alerts": snapshot.get("alerts") or [],
            "routes_catalog": snapshot.get("routes_catalog") or {},
            "meta": snapshot.get("meta") or {"last_update": int(time.time())},
        }
    except Exception:
        return {
            "vehicles": [], "trip_updates": [], "alerts": [],
            "routes_catalog": {}, "meta": {"last_update": int(time.time())},
        }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    print(f"🔌 [WS] Control Room collegata. Client totali: {len(manager.active_connections)}")

    try:
        cycle = 0
        while True:
            cycle += 1

            # Cold start: se il refresher non ha ancora riempito lo snapshot,
            # si attende il primo sync reale per il primo messaggio.
            payload = get_network_snapshot()
            if not payload.get("vehicles"):
                try:
                    payload = await fetch_all_gtt_data()
                except Exception as exc:
                    logger.error("Primo sync fallito (%s): payload di emergenza.", exc)
                    payload = _fallback_payload()

            try:
                await websocket.send_json({"type": "full_network_update", "data": payload})
                meta = payload.get("meta", {})
                age = int(time.time()) - meta.get("last_update", 0) if meta.get("last_update") else 0
                print(
                    f"📡 [WS] Ciclo {cycle} -> {len(payload.get('vehicles', []))} mezzi, "
                    f"{len(payload.get('alerts', []))} avvisi, {len(payload.get('trip_updates', []))} corse "
                    f"(dati vecchi di {age}s, sorgente flotta: {meta.get('vehicles_source', 'n/d')})."
                )
            except WebSocketDisconnect:
                raise
            except Exception as exc:
                logger.info("Client WebSocket non piu' raggiungibile: %s", exc)
                break

            # Push frequente dello snapshot: il refresher in background lo mantiene fresco.
            await asyncio.sleep(2)

    except WebSocketDisconnect:
        logger.info("Client WebSocket disconnesso correttamente.")
    except Exception as exc:
        logger.error("Loop WebSocket interrotto per errore imprevisto: %s", exc)
    finally:
        manager.disconnect(websocket)
        print(f"🔌 [WS] Control Room scollegata. Client rimasti: {len(manager.active_connections)}")
