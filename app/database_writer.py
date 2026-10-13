"""
Persistenza relazionale della rete GTT su Supabase (PostgreSQL).

Schema di riferimento: sql/schema.sql
- gtt_routes: catalogo linee (upsert per route_id)
- gtt_vehicles: mezzi noti con ultimo avvistamento (upsert per vehicle_id)
- gtt_vehicle_positions: time-series delle posizioni GPS (insert)
- gtt_alerts_history: storico avvisi/deviazioni (upsert per alert_id)

Tutto è no-op se SUPABASE_URL e SUPABASE_KEY non sono configurati,
e nessun errore di persistenza può mai compromettere il servizio real-time.
"""

import logging
import os
import time
from datetime import datetime, timezone

from supabase import Client, create_client

logger = logging.getLogger("GTT-Database")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# THROTTLE DEDICATO per le posizioni GPS (time-series): e' di gran lunga la
# tabella piu' voluminosa. Con ~490 mezzi in strada, salvare le posizioni a
# ogni ciclo di persistenza (30s) genera ~1,4 milioni di righe AL GIORNO
# (~300 MB/giorno tra righe e indici): il free tier Supabase (500 MB) si
# satura in circa un giorno e mezzo e il project va in pausa.
# Di default si salva una posizione ogni 10 minuti per mezzo (~70k righe/giorno,
# ~15 MB/giorno): lo storico resta utile per le analisi e il database respira.
# Cadenza diversa se serve piu' granularita': POSITIONS_PERSIST_SECONDS=300.
POSITIONS_PERSIST_INTERVAL_SECONDS = int(os.getenv("POSITIONS_PERSIST_SECONDS", "600"))
_last_positions_persist_at = 0.0

supabase: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        logger.warning(f"Impossibile inizializzare il client Supabase: {e}")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _batched(seq, size=500):
    """Supabase accetta batch limitati: suddividiamo gli insert grandi."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


async def save_network_data_to_db(data: dict):
    """Salva in modo asincrono i dati completi della rete GTT su Supabase."""
    if not supabase:
        logger.debug("Supabase non configurato: persistenza saltata.")
        return

    saved = []

    # 1. Catalogo linee
    routes = data.get("routes_catalog", {})
    if routes:
        try:
            rows = [
                {
                    "route_id": str(rid),
                    "route_short_name": str(rid),
                    "route_long_name": str(rname or ""),
                    "updated_at": _now_iso(),
                }
                for rid, rname in routes.items()
            ]
            for batch in _batched(rows):
                supabase.table("gtt_routes").upsert(batch, on_conflict="route_id").execute()
            saved.append(f"{len(rows)} linee")
        except Exception as exc:
            logger.error(f"Errore upsert gtt_routes: {exc}")

    # 2. Mezzi (upsert leggero a ogni persist) + 3. posizioni GPS
    #    (time-series: salvate con il throttle dedicato, vedi sopra).
    vehicles = data.get("vehicles", [])
    if vehicles:
        vehicle_rows = []
        position_rows = []
        seen = set()
        positions_due = False
        global _last_positions_persist_at
        now = time.time()
        if now - _last_positions_persist_at >= POSITIONS_PERSIST_INTERVAL_SECONDS:
            _last_positions_persist_at = now
            positions_due = True
        for v in vehicles:
            v_id = v.get("vehicle_id")
            if not v_id or v_id in seen:
                continue
            seen.add(v_id)
            vehicle_rows.append({"vehicle_id": v_id, "agency_id": "GTT", "last_seen": _now_iso()})
            if positions_due:
                position_rows.append({
                    "vehicle_id": v_id,
                    "trip_id": v.get("trip_id"),
                    "route_id": v.get("route_id") if v.get("route_id") != "N/A" else None,
                    "latitude": v.get("latitude"),
                    "longitude": v.get("longitude"),
                    "bearing": v.get("bearing"),
                    "speed": v.get("speed"),
                    "timestamp": v.get("timestamp"),
                    "recorded_at": _now_iso(),
                })
        try:
            for batch in _batched(vehicle_rows):
                supabase.table("gtt_vehicles").upsert(batch, on_conflict="vehicle_id").execute()
            if position_rows:
                for batch in _batched(position_rows):
                    supabase.table("gtt_vehicle_positions").insert(batch).execute()
                saved.append(f"{len(vehicle_rows)} mezzi e {len(position_rows)} posizioni GPS")
            else:
                saved.append(f"{len(vehicle_rows)} mezzi")
        except Exception as exc:
            logger.error(f"Errore salvataggio veicoli/posizioni: {exc}")

    # 4. Storico avvisi e deviazioni (deduplicati per alert_id)
    alerts = data.get("alerts", [])
    if alerts:
        try:
            rows = []
            for al in alerts:
                alert_id = al.get("alert_id") or str(al.get("header") or "avviso")[:60]
                start_time = al.get("start_time")
                rows.append({
                    "alert_id": str(alert_id),
                    "title": al.get("header") or al.get("title") or "Avviso di servizio GTT",
                    "description": al.get("description") or "",
                    "route_ids": al.get("affected_routes") or al.get("route_ids") or [],
                    "cause": al.get("cause_label") or str(al.get("cause") or ""),
                    "effect": al.get("effect_label") or str(al.get("effect") or ""),
                    "category": al.get("category") or "informazione",
                    "start_time": datetime.fromtimestamp(start_time, tz=timezone.utc).isoformat() if start_time else None,
                })
            for batch in _batched(rows, 200):
                supabase.table("gtt_alerts_history").upsert(batch, on_conflict="alert_id").execute()
            saved.append(f"{len(rows)} avvisi")
        except Exception as exc:
            logger.error(f"Errore salvataggio avvisi: {exc}")

    if saved:
        logger.info(f"✅ Persistenza Supabase completata: {', '.join(saved)}.")
