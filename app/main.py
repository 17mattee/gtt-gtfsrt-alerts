import asyncio
import hashlib
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from google.transit import gtfs_realtime_pb2

from app.gtt_full_sync import fetch_all_gtt_data, get_network_snapshot, load_last_good_snapshot
from app.websocket_manager import manager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("GTT-Main")


_last_persist_at = 0.0
PERSIST_INTERVAL_SECONDS = 30
_persist_in_flight = False
_background_tasks: set = set()


async def _persist_snapshot(data: dict) -> None:
    """Persistenza opzionale su Supabase (no-op se non configurato).

    Throttata a una scrittura ogni 30s: la rete pubblica ~440 posizioni per
    ciclo e senza freno riempirebbe il database (milioni di righe al giorno).
    Mai bloccante, mai fatale.

    GUARDIA IN-FLIGHT: se la scrittura precedente e' ancora in corsa (DB
    lento o project Supabase in pausa), questa viene saltata invece di
    accumulare task (e i loro payload) in coda senza limite.
    """
    global _last_persist_at, _persist_in_flight
    if _persist_in_flight:
        return
    now = time.time()
    if now - _last_persist_at < PERSIST_INTERVAL_SECONDS:
        return
    _last_persist_at = now
    _persist_in_flight = True
    try:
        from app.database_writer import save_network_data_to_db
        # save_network_data_to_db è async ma usa il client sync Supabase:
        # la eseguiamo in un thread dedicato per non bloccare l'event loop.
        await asyncio.to_thread(asyncio.run, save_network_data_to_db(data))
    except Exception as exc:
        logger.warning("Persistenza su database saltata: %s", exc)
    finally:
        _persist_in_flight = False


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
            # Riferimento mantenuto al task di persistenza (pattern asyncio:
            # un task senza riferimenti puo' essere raccolto dal GC a meta'
            # esecuzione) con pulizia automatica a completamento.
            task = asyncio.create_task(_persist_snapshot(payload))
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)
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
    # Anti-crash GTT: se il processo riparte mentre i feed sono giu', il cold
    # start parte con l'ultimo snapshot buono dal disco invece che a zero.
    await asyncio.to_thread(load_last_good_snapshot)
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

# Cartella statica risolta in modo ASSOLUTO dal file sorgente: l'avvio da una
# working directory diversa dalla root del repo (es. `uvicorn app.main:app`
# da una subdir) non deve rompere dashboard ne' montaggio /static.
BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"

# Monta la cartella statica per servire la UI
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --- Sicurezza: header di risposta + CSP contro XSS/clickjacking/injection ---
# Leaflet e' self-hosted in /static/vendor: NESSUNA origine di terze parti
# e' piu' necessaria per script e stili. La source map di Leaflet e' same-origin
# e il suo fetch resta consentito da connect-src 'self' (nessun warning console).
_CSP_BASE = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline'; "
    "style-src 'self' 'unsafe-inline'; "
    "img-src 'self' data: https://tile.openstreetmap.org https://*.tile.openstreetmap.org "
    "https://services.arcgisonline.com https://*.arcgisonline.com; "
    "connect-src 'self'"
)


def _csp_for_request(request: Request) -> str:
    """CSP con WebSocket consentito SOLO verso l'host che ha servito la pagina.

    `ws: wss:` aperti consentirebbero esfiltrazione verso qualunque server:
    si restringe all'host della richiesta (sanificato: un Host header ostile
    non deve finire nell'header, ne' iniettarvi CRLF).
    """
    host = (request.headers.get("host") or "").strip()
    if re.fullmatch(r"[A-Za-z0-9.\-]+(?::\d{1,5})?", host):
        return f"{_CSP_BASE} ws://{host} wss://{host}"
    return f"{_CSP_BASE} ws: wss:"  # Host assente/invalido (es. accesso diretto via IP senza Host)


@app.middleware("http")
async def security_headers_middleware(request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = _csp_for_request(request)
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
    return FileResponse(str(STATIC_DIR / "index.html"))


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


def _client_ip(request: Request) -> str:
    """IP reale del client: dietro il proxy di Render, request.client.host e'
    l'IP del load balancer (uguale per TUTTI gli utenti): senza X-Forwarded-For
    il rate limit diventerebbe collettivo e throttlerebbe utenti legittimi."""
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


@app.get("/gtfs-rt/alerts.pb")
async def gtfs_rt_alerts_feed(request: Request):
    """Feed GTFS-Realtime (Service Alerts) in formato Protocol Buffers standard.

    Proxy trasparente del feed UFFICIALE GTT quando raggiungibile;
    altrimenti ricostruisce il feed dallo snapshot locale.
    Con rate limit per non far abusare dei feed GTT tramite il nostro proxy.
    """
    if _rate_limited(_client_ip(request)):
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
        # Contratto GTFS-RT: informed_entity.route_id deve riferirsi al
        # route_id di routes.txt (es. '2U'), non al nome linea visibile ('2').
        from app.gtfs_static import gtfs_manager
        for route_id in alert.get("affected_routes", []):
            informed.add().route_id = str(gtfs_manager.get_gtfs_route_id(route_id) or route_id)
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


# Tetto alle connessioni WebSocket simultanee: sul piano free (RAM e banda
# limitate) un crawler o un client impazzito con centinaia di tab aperte
# satura memoria e banda e degrada il servizio per tutti. Oltre il limite
# la connessione viene rifiutata con close code 1013 (Try Again Later):
# il client la riapre da solo col backoff esistente.
MAX_WS_CLIENTS = 40


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    if len(manager.active_connections) > MAX_WS_CLIENTS:
        logger.warning("WS: limite di %d client raggiunto, connessione rifiutata (protezione flood).", MAX_WS_CLIENTS)
        manager.disconnect(websocket)
        try:
            await websocket.close(code=1013, reason="Limite client raggiunto, riconnessione automatica tra poco")
        except Exception:
            pass
        return
    print(f"🔌 [WS] Control Room collegata. Client totali: {len(manager.active_connections)}")

    static_signature = ""
    first_message = True
    try:
        cycle = 0
        while True:
            cycle += 1

            # Cold start: al PRIMO messaggio si attende un sync reale SOLO se
            # lo snapshot e' davvero vecchio. Di notte GTT puo' pubblicare
            # zero mezzi (dato reale mostrato come tale): senza questa guardia
            # OGNI nuovo client rimetteva in moto una sync completa, e col
            # lock di serializzazione N client = N sync in coda verso GTT.
            payload = get_network_snapshot()
            if cycle == 1 and not payload.get("vehicles"):
                meta = payload.get("meta") or {}
                if (time.time() - (meta.get("last_update") or 0)) > 15:
                    try:
                        # Timeout difensivo: con un feed GTT lento/giu' la sync
                        # completa puo' metterci ~30-60s. Il primo messaggio NON
                        # deve restare appeso ('Caricamento flotta...' eterno):
                        # oltre 25s si invia lo snapshot disponibile e i cicli
                        # successivi (ogni 2s) portano i dati freschi.
                        payload = await asyncio.wait_for(fetch_all_gtt_data(), timeout=25)
                    except asyncio.TimeoutError:
                        logger.info("⚠️ [WS] Cold start: sync oltre 25s, invio lo snapshot disponibile (i dati freschi arrivano coi cicli successivi).")
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
                "scioperi": payload.get("scioperi", {"ufficiali": [], "notizie": []}),
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
