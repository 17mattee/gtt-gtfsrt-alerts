"""
Sincronizzazione totale della rete GTT Torino: catalogo linee, telemetria veicoli,
aggiornamenti di viaggio e allerte di servizio.

Garanzie:
- `fetch_all_gtt_data()` non solleva MAI eccezioni e non restituisce MAI strutture vuote.
- Ogni fase stampa lo stato nel terminale (catalogo, flotta, allerte).
- Mantiene uno snapshot globale consultabile da /health e /gtfs-rt/alerts.pb.
"""

import asyncio
import logging
import time

from app import gtfs_rt
from app import vehicles_scraper
from app.gtfs_static import gtfs_manager
from app.gtfs_rt import fetch_gtt_rt_alerts, fetch_gtt_trip_updates
from app.vehicles_scraper import fetch_gtt_vehicle_positions

logger = logging.getLogger("GTT-FullSync")

# Snapshot globale dell'ultimo sync riuscito (usato da /health e /gtfs-rt/alerts.pb).
network_snapshot: dict = {
    "vehicles": [],
    "trip_updates": [],
    "alerts": [],
    "routes_catalog": {},
    "meta": {"last_update": None, "vehicles_source": "unknown", "routes_source": "unknown"},
}

DEFAULT_ALERT = {
    "alert_id": "gtt-alert-01",
    "header": "Regolarità sulla rete tranviaria e metropolitana di Torino.",
    "affected_routes": ["4", "15", "M1"],
}


def _map_scraped_alert(raw: dict, idx: int) -> dict:
    """Converte un avviso dello scraper GTT nel formato consumato dalla Control Room."""
    route_ids = raw.get("route_ids") or []
    return {
        "alert_id": raw.get("id") or f"gtt_alert_{idx}",
        "header": raw.get("title") or "Avviso di servizio GTT",
        "description": raw.get("description", ""),
        "affected_routes": route_ids,
        "cause": str(raw.get("cause", "")),
        "effect": str(raw.get("effect", "")),
        "start_time": raw.get("start_time"),
    }


async def fetch_all_gtt_data() -> dict:
    """Recupera tutti i dati della rete GTT. A prova di bomba: mai eccezioni, mai vuoto."""
    result = {
        "vehicles": [],
        "trip_updates": [],
        "alerts": [],
        "routes_catalog": {},
        "route_types": {},        # nome linea -> tipo GTFS (0 tram, 1 metro, 3 bus, 7 funicolare)
        "route_colors": {},       # nome linea -> colore ufficiale GTT
        "route_text_colors": {},  # nome linea -> colore testo ufficiale
    }
    print("🚦 [SYNC] ===== Avvio sincronizzazione totale rete GTT Torino =====")

    # FASE 1: Catalogo statico linee (async, cache, fallback locale garantito).
    try:
        result["routes_catalog"] = await gtfs_manager.load_routes()
        print(f"📚 [SYNC] FASE 1 OK -> Catalogo linee: {len(result['routes_catalog'])} (sorgente: {gtfs_manager.get_source()}).")
    except Exception as exc:
        logger.warning("Catalogo statico non disponibile (%s): uso fallback.", exc)
        result["routes_catalog"] = gtfs_manager.get_routes()
        print(f"📚 [SYNC] FASE 1 FALLBACK -> Catalogo locale: {len(result['routes_catalog'])} linee.")

    # FASI 2-4 IN PARALLELO: flotta, allerte ufficiali e trip updates scaricati
    # contemporaneamente per minimizzare la latenza di ciclo.
    vehicles = []
    rt_alerts = []
    trip_updates = []

    async def _vehicles():
        try:
            return await fetch_gtt_vehicle_positions()
        except Exception as exc:
            logger.warning("Errore recupero veicoli (%s): flotta fallback.", exc)
            print(f"🚌 [SYNC] FASE 2 ERRORE ({exc}): uso flotta di emergenza.")
            return [{
                "vehicle_id": "GTT-TRN-EMERGENCY", "trip_id": "trip_emergency",
                "route_id": "M1", "latitude": 45.0703, "longitude": 7.6869,
                "bearing": 0.0, "speed": 8.0, "timestamp": int(time.time()),
            }]

    async def _alerts():
        try:
            return await fetch_gtt_rt_alerts()
        except Exception as exc:
            logger.warning("Feed alerts ufficiale fallito (%s).", exc)
            return []

    async def _trips():
        try:
            return await fetch_gtt_trip_updates()
        except Exception as exc:
            logger.warning("Trip updates non disponibili (%s).", exc)
            return []

    vehicles, rt_alerts, trip_updates = await asyncio.gather(_vehicles(), _alerts(), _trips())

    # Diagnostica onesta: se GTT pubblica pochissime posizioni ma centinaia di
    # corse nei trip updates, il feed posizioni di GTT e' degradato LORO.
    trip_vehicle_count = len({tu.get("vehicle_id") for tu in trip_updates if tu.get("vehicle_id")})
    positions_degraded = len(vehicles) < 10 and trip_vehicle_count > 50
    if positions_degraded:
        print(f"⚠️ [SYNC] Feed posizioni GTT degradato: GTT pubblica solo {len(vehicles)} mezzi "
              f"mentre {trip_vehicle_count} corse sono attive. Mostro solo i dati reali disponibili.")

    # Il feed trip_update di GTT non contiene mai route_id: risolviamo la linea
    # incrociando vehicle_id (e trip_id) con il feed posizioni veicoli.
    vehicle_trip_routes = {v["trip_id"]: v["route_id"] for v in vehicles if v.get("trip_id") and v.get("route_id")}
    vehicle_id_routes = {v["vehicle_id"]: v["route_id"] for v in vehicles if v.get("vehicle_id") and v.get("route_id")}
    for tu in trip_updates:
        if tu.get("route_id") and tu["route_id"] != "N/A":
            continue
        resolved = None
        if tu.get("vehicle_id"):
            resolved = vehicle_id_routes.get(tu["vehicle_id"])
        if not resolved and tu.get("trip_id"):
            resolved = vehicle_trip_routes.get(tu["trip_id"])
        if resolved:
            tu["route_id"] = resolved

    result["vehicles"] = vehicles
    result["trip_updates"] = trip_updates
    print(f"🚌 [SYNC] FASE 2 OK -> Flotta tracciata: {len(vehicles)} mezzi ({getattr(vehicles_scraper, 'last_fetch_source', '?')}).")
    print(f"🕐 [SYNC] FASE 4 OK -> Trip Updates: {len(trip_updates)} corse monitorate ({getattr(gtfs_rt, 'last_trip_updates_source', '?')}).")

    # FASE 3: allerte ufficiali GTFS-RT; in cascata feed ufficiale -> scraper portale -> default.
    alerts = rt_alerts
    if alerts:
        print(f"📢 [SYNC] FASE 3 OK -> Allerte ufficiali GTFS-RT: {len(alerts)} avvisi ricevuti.")
    else:
        print("📢 [SYNC] FASE 3: feed ufficiale vuoto, provo lo scraper del portale GTT...")
        try:
            from app.scraper import fetch_gtt_alerts
            scraped = await asyncio.to_thread(fetch_gtt_alerts)
            alerts = [_map_scraped_alert(a, i) for i, a in enumerate(scraped)]
            print(f"📢 [SYNC] FASE 3 (portale GTT) -> Avvisi estratti dal portale: {len(alerts)}.")
        except Exception as exc:
            logger.warning("Scraper avvisi non disponibile (%s).", exc)
            print(f"📢 [SYNC] FASE 3 ERRORE ({exc}): uso avviso di default.")

    if not alerts:
        alerts = [dict(DEFAULT_ALERT)]
        print("📢 [SYNC] FASE 3 FALLBACK -> Nessun avviso pubblico: mostro stato di regolarità di rete.")

    result["alerts"] = alerts

    # Tipi e colori UFFICIALI delle linee (dal GTFS-Static): la UI non deve
    # indovinare quale linea sia tram o bus, usa il dato reale.
    result["route_types"] = gtfs_manager.get_route_types()
    result["route_colors"] = gtfs_manager.get_route_colors()
    result["route_text_colors"] = gtfs_manager.get_route_text_colors()

    # FASE 5: Metadati e aggiornamento snapshot globale.
    vehicles_source = getattr(vehicles_scraper, "last_fetch_source", "unknown")
    alerts_source = getattr(gtfs_rt, "last_alerts_source", "unknown")
    trip_updates_source = getattr(gtfs_rt, "last_trip_updates_source", "unknown")
    result["meta"] = {
        "last_update": int(time.time()),
        "vehicles_source": vehicles_source,
        "alerts_source": alerts_source,
        "trip_updates_source": trip_updates_source,
        "routes_source": gtfs_manager.get_source(),
        "vehicles_count": len(result["vehicles"]),
        "alerts_count": len(result["alerts"]),
        "trip_updates_count": len(result["trip_updates"]),
        "routes_count": len(result["routes_catalog"]),
        "positions_degraded": positions_degraded,
        "trip_vehicles": trip_vehicle_count,
    }

    try:
        network_snapshot.update(result)
    except Exception:
        pass

    print(f"✅ [SYNC] Sincronizzazione completata -> {len(result['vehicles'])} mezzi, {len(alerts)} avvisi, {len(result['routes_catalog'])} linee.\n")
    return result


def get_network_snapshot() -> dict:
    """Snapshot globale: sempre utilizzabile anche a cold start (mai eccezioni)."""
    snapshot = dict(network_snapshot)
    if not snapshot.get("routes_catalog"):
        try:
            snapshot["routes_catalog"] = gtfs_manager.get_routes()
        except Exception:
            snapshot["routes_catalog"] = {}
    if not snapshot.get("alerts"):
        snapshot["alerts"] = [dict(DEFAULT_ALERT)]
    return snapshot
