import os
import logging
from supabase import create_client, Client

logger = logging.getLogger("GTT-Database")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        logger.warning(f"Impossibile inizializzare il client Supabase: {e}")

async def save_network_data_to_db(data: dict):
    """
    Salva in modo asincrono i dati della rete GTT nel database relazionale Supabase.
    """
    if not supabase:
        return

    try:
        routes = data.get("routes_catalog", {})
        if routes:
            route_rows = [{"route_id": str(rid), "route_short_name": str(rid), "route_long_name": rname} for rid, rname in routes.items()]
            supabase.table("gtt_routes").upsert(route_rows, on_conflict="route_id").execute()

        vehicles = data.get("vehicles", [])
        if vehicles:
            vehicle_entries = []
            position_entries = []
            
            for v in vehicles:
                v_id = v.get("vehicle_id")
                if not v_id:
                    continue
                vehicle_entries.append({"vehicle_id": v_id})
                position_entries.append({
                    "vehicle_id": v_id,
                    "trip_id": v.get("trip_id"),
                    "route_id": v.get("route_id") if v.get("route_id") != "N/A" else None,
                    "latitude": v.get("latitude"),
                    "longitude": v.get("longitude"),
                    "bearing": v.get("bearing"),
                    "speed": v.get("speed"),
                    "timestamp": v.get("timestamp")
                })

            supabase.table("gtt_vehicles").upsert(vehicle_entries, on_conflict="vehicle_id").execute()
            supabase.table("gtt_vehicle_positions").insert(position_entries).execute()

        alerts = data.get("alerts", [])
        if alerts:
            alert_entries = []
            for al in alerts:
                alert_entries.append({
                    "alert_id": al.get("alert_id"),
                    "title": al.get("header"),
                    "route_ids": al.get("affected_routes", [])
                })
            supabase.table("gtt_alerts_history").upsert(alert_entries, on_conflict="alert_id").execute()

        logger.info("Dati della rete GTT salvati con successo su Supabase.")
    except Exception as e:
        logger.error(f"Errore durante la persistenza su database: {e}")
