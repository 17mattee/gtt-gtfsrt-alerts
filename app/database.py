import os
import logging
import psycopg
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger("GTT-Database")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((psycopg.OperationalError, psycopg.InterfaceError)),
    reraise=True
)
def save_alerts_to_db(alerts: list):
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.warning("URL di connessione non trovato. Salvataggio storico saltato.")
        return

    try:
        with psycopg.connect(database_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
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
        logger.info(f"Salvati con successo {len(alerts)} avvisi nello storico Supabase.")
    except Exception as e:
        logger.error(f"Errore durante il salvataggio su Supabase: {e}")
        raise
