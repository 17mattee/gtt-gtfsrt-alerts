import asyncio
import hashlib
import json
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
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


_last_persist_at = 0.0
PERSIST_INTERVAL_SECONDS = 30


async def _persist_snapshot(data: dict) -> None:
    """Persistenza opzionale su Supabase (no-op se non configurato).

    Throttata a una scrittura ogni 30s: la rete pubblica ~440 posizioni per
    ciclo e senza freno riempirebbe il database (milioni di righe al giorno).
    Mai bloccante, mai fatale.
    """
    global _last_persist_at
    now = time.time()
    if now - _last_persist_at < PERSIST_INTERVAL_SECONDS:
        return
    _last_persist_at = now
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

# --- Sicurezza: header di risposta + CSP contro XSS/clickjacking/injection ---
# NB: il jolly *.tile.openstreetmap.org NON copre il dominio esatto: entrambi
# vanno elencati, altrimenti Safari/Chrome bloccano le tiles della mappa.
_CSP = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline' https://unpkg.com; "
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://unpkg.com; "
    "font-src https://fonts.gstatic.com; "
    "img-src 'self' data: https://tile.openstreetmap.org https://*.tile.openstreetmap.org "
    "https://services.arcgisonline.com https://*.arcgisonline.com; "
    "connect-src 'self' ws: wss:"
)


@app.middleware("http")
async def security_headers_middleware(request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = _CSP
    return response


# Host allowlist opzionale: attiva SOLO se imposti ALLOWED_HOSTS (protegge da
# attacchi Host-header). Di default resta aperto per consentire i test in LAN
# (es. iPhone che punta all'IP del Mac). Esempio:
#   ALLOWED_HOSTS="127.0.0.1,localhost,gtt-gtfsrt-alerts.onrender.com"
_allowed_hosts = os.getenv("ALLOWED_HOSTS")
if _allowed_hosts:
    from starlette.middleware.trustedhost import TrustedHostMiddleware
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=_allowed_hosts.split(","))


# --- Rate limit semplice sul proxy GTT (protegge i feed GTT dall'abuso) ---
_rate_buckets: dict = {}
RATE_LIMIT_PER_MINUTE = 60


def _rate_limited(client_ip: str) -> bool:
    now = time.time()
    window = _rate_buckets.setdefault(client_ip, [])
    # Anti memory-leak: se il dizionario cresce troppo (attacco da molti IP),
    # ripulisce le finestre scadute di tutti.
    if len(_rate_buckets) > 5000:
        for ip in list(_rate_buckets):
            bucket = _rate_buckets[ip]
            bucket[:] = [t for t in bucket if now - t < 60]
            if not bucket:
                _rate_buckets.pop(ip, None)
    window[:] = [t for t in window if now - t < 60]
    if len(window) >= RATE_LIMIT_PER_MINUTE:
        return True
    window.append(now)
    return False


@app.api_route("/", methods=["GET", "HEAD"])
async def get_dashboard():
    return FileResponse("static/index.html")


@app.api_route("/health", methods=["GET", "HEAD"])
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
async def gtfs_rt_alerts_feed(request: Request):
    """Feed GTFS-Realtime (Service Alerts) in formato Protocol Buffers standard.

    Proxy trasparente del feed UFFICIALE GTT quando raggiungibile;
    altrimenti ricostruisce il feed dallo snapshot locale.
    Con rate limit per non far abusare dei feed GTT tramite il nostro proxy.
    """
    client_ip = request.client.host if request.client else "unknown"
    if _rate_limited(client_ip):
        return Response(status_code=429, content=b"Too Many Requests")
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

    static_signature = ""
    first_message = True
    try:
        cycle = 0
        while True:
            cycle += 1

            # Cold start: solo al PRIMO messaggio si attende un sync reale;
            # se GTT pubblica zero mezzi, si mostra zero (dato reale).
            payload = get_network_snapshot()
            if cycle == 1 and not payload.get("vehicles"):
                try:
                    payload = await fetch_all_gtt_data()
                except Exception as exc:
                    logger.error("Primo sync fallito (%s): payload di emergenza.", exc)
                    payload = _fallback_payload()

            # Ottimizzazione traffico: le parti DINAMICHE (mezzi, corse, meta)
            # si inviano a ogni ciclo; le parti STATICHE (avvisi, catalogo,
            # tipi/colori, ~60KB) solo quando cambiano davvero. Da ~7MB/min
            # si scende a ~2MB/min per client.
            dynamic = {
                "vehicles": payload.get("vehicles", []),
                "trip_updates": payload.get("trip_updates", []),
                "meta": payload.get("meta", {}),
            }
            static_part = {
                "alerts": payload.get("alerts", []),
                "routes_catalog": payload.get("routes_catalog", {}),
                "route_types": payload.get("route_types", {}),
                "route_colors": payload.get("route_colors", {}),
                "route_text_colors": payload.get("route_text_colors", {}),
            }
            try:
                sig = hashlib.md5(json.dumps(static_part, sort_keys=True).encode("utf-8")).hexdigest()
            except Exception:
                sig = str(len(static_part.get("alerts", [])))
            if first_message or sig != static_signature:
                dynamic["static"] = static_part
                static_signature = sig
            first_message = False

            try:
                await websocket.send_json({"type": "full_network_update", "data": dynamic})
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
