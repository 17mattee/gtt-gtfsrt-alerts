import os
import psycopg2
from datetime import datetime

def save_alerts_to_db(alerts: list):
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("[Database] URL di connessione non trovato. Salvataggio storico saltato.")
        return

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        for alert in alerts:
            start_dt = datetime.fromtimestamp(alert["start_time"])
            
            cur.execute(
                """
                INSERT INTO gtt_alerts_history (alert_id, title, description, route_ids, cause, effect, start_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    alert["id"],
                    alert["title"],
                    alert["description"],
                    alert["route_ids"],
                    str(alert["cause"]),
                    str(alert["effect"]),
                    start_dt
                )
            )
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"[Database] Salvati con successo {len(alerts)} avvisi nello storico Supabase.")
    except Exception as e:
        print(f"[Database] Errore durante il salvataggio su Supabase: {e}")
