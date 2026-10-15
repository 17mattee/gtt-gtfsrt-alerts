import os
import logging
import time
import psycopg
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger("GTT-Database")

# Stesse etichette italiane del resto del middleware: gtt_alerts_history e'
# scritta anche da database_writer.py con le LABEL testuali, non con gli int.
# Scrivere qui gli int creerebbe formati misti nella stessa colonna.
from app.gtfs_rt import CAUSE_LABELS, EFFECT_LABELS


def _cause_from_db(value) -> int:
    """Ricostruisce il cause int GTFS-RT dal valore a DB (int, cifra o label)."""
    if value is None:
        return 1
    if isinstance(value, int):
        return value
    s = str(value)
    if s.isdigit():
        return int(s)
    for code, label in CAUSE_LABELS.items():
        if label == s:
            return code
    return 1


def _effect_from_db(value) -> int:
    """Ricostruisce l'effect int GTFS-RT dal valore a DB (int, cifra o label)."""
    if value is None:
        return 8
    if isinstance(value, int):
        return value
    s = str(value)
    if s.isdigit():
        return int(s)
    for code, label in EFFECT_LABELS.items():
        if label == s:
            return code
    return 8


def _epoch_utc_or_none(value):
    """epoch -> datetime UTC esplicito; None se assente/non numerico.

    Senza tz verrebbe interpretato col fuso del server (UTC su Render),
    spostando lo storico di 1-2 ore. Un valore mancante o non valido non
    deve far perdere l'intero batch di avvisi.
    """
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    return None

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
                    start_dt = _epoch_utc_or_none(alert.get("start_time"))
                    cause_code = alert.get("cause") if isinstance(alert.get("cause"), int) else 1
                    effect_code = alert.get("effect") if isinstance(alert.get("effect"), int) else 8
                    cur.execute(
                        """
                        INSERT INTO gtt_alerts_history (alert_id, title, description, route_ids, cause, effect, start_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            alert.get("id") or alert.get("alert_id") or "gtt-alert",
                            alert.get("title") or alert.get("header") or "Avviso di servizio GTT",
                            alert.get("description") or "",
                            alert.get("route_ids") or alert.get("affected_routes") or [],
                            # Label testuali, conformi a database_writer.py (colonne TEXT)
                            CAUSE_LABELS.get(cause_code, "Causa non specificata"),
                            EFFECT_LABELS.get(effect_code, "Effetto non specificato"),
                            start_dt
                        )
                    )
                conn.commit()
        logger.info(f"Salvati con successo {len(alerts)} avvisi nello storico Supabase.")
    except Exception as e:
        logger.error(f"Errore durante il salvataggio su Supabase: {e}")
        raise

def fetch_latest_alerts_from_db(limit: int = 50):
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return []

    alerts = []
    try:
        with psycopg.connect(database_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT alert_id, title, description, route_ids, cause, effect, start_time
                    FROM gtt_alerts_history
                    ORDER BY start_time DESC
                    LIMIT %s
                    """,
                    (limit,)
                )
                rows = cur.fetchall()
                for row in rows:
                    alert_id, title, description, route_ids, cause, effect, start_time = row
                    alerts.append({
                        "id": alert_id,
                        "title": title,
                        "description": description,
                        "route_ids": route_ids if isinstance(route_ids, list) else [],
                        "cause": _cause_from_db(cause),
                        "effect": _effect_from_db(effect),
                        "start_time": int(start_time.timestamp()) if isinstance(start_time, datetime) else int(time.time())
                    })
        logger.info(f"Recuperati {len(alerts)} avvisi di backup dal database Supabase (Cold Start).")
    except Exception as e:
        logger.error(f"Impossibile recuperare lo storico da Supabase durante il cold start: {e}")
    
    return alerts
