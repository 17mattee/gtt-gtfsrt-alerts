import os
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import time
import logging
from google.transit import gtfs_realtime_pb2
from app.gtfs_static import gtfs_manager
from app.notifier import send_telegram_alert
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger("GTT-Scraper")
GTT_RAW_URL = "https://www.gtt.to.it/cms/index.php?option=com_gtt&view=avvisi&tmpl=raw&priorita=1"

def parse_cause(text: str):
    text_lower = text.lower()
    if any(k in text_lower for k in ["sciopero", "agitazione sindacale", "fermata sindacale"]):
        return gtfs_realtime_pb2.Alert.Cause.STRIKE
    elif any(k in text_lower for k in ["corteo", "manifestazione", "cortei", "sfilata"]):
        return gtfs_realtime_pb2.Alert.Cause.DEMONSTRATION
    elif any(k in text_lower for k in ["lavori", "cantier", "manutenzione", "risanamento binari", "scavi"]):
        return gtfs_realtime_pb2.Alert.Cause.CONSTRUCTION
    elif any(k in text_lower for k in ["concerto", "partita", "stadio", "evento", "mercato", "fiera", "maratona"]):
        return gtfs_realtime_pb2.Alert.Cause.OTHER_CAUSE
    elif any(k in text_lower for k in ["incidente", "guasto", "ostacolo"]):
        return gtfs_realtime_pb2.Alert.Cause.ACCIDENT
    return gtfs_realtime_pb2.Alert.Cause.UNKNOWN_CAUSE

def parse_effect(text: str):
    text_lower = text.lower()
    if any(k in text_lower for k in ["deviat", "deviazione", "modifica percorso", "percorso alternativo"]):
        return gtfs_realtime_pb2.Alert.Effect.DETOUR
    elif any(k in text_lower for k in ["soppress", "sospes", "interrotto", "limitato", "fuori servizio"]):
        return gtfs_realtime_pb2.Alert.Effect.NO_SERVICE
    elif any(k in text_lower for k in ["intensificaz", "potenziament", "corse bis", "maggiori transiti"]):
        return gtfs_realtime_pb2.Alert.Effect.ADDITIONAL_SERVICE
    elif any(k in text_lower for k in ["rallentament", "ritardi", "code"]):
        return gtfs_realtime_pb2.Alert.Effect.SIGNIFICANT_DELAYS
    return gtfs_realtime_pb2.Alert.Effect.UNKNOWN_EFFECT

def extract_start_time(text: str):
    match = re.search(r"(\d{2}/\d{2}/\d{4})\s(\d{2}:\d{2})", text)
    if match:
        try:
            dt = datetime.strptime(match.group(0), "%d/%m/%Y %H:%M")
            return int(dt.timestamp())
        except:
            pass
    return int(time.time())

def extract_route_ids(text: str):
    route_ids = []
    lines_match = re.search(r"Line[ea]e?\s+([A-Za-z0-9\s,\/\-\.]+?)(?=\s+deviat|\s+soppress|\s+potenziat|\s+intensificat|\s+Causa|\.|$)", text, re.IGNORECASE)
    
    if lines_match:
        raw_lines = lines_match.group(1)
        tokens = re.split(r",|\se\s|\/|\s-\s", raw_lines)
        for tok in tokens:
            clean_tok = tok.strip().strip(".").upper()
            if clean_tok and len(clean_tok) < 10:
                validated = gtfs_manager.validate_route_id(clean_tok)
                if validated and validated not in route_ids:
                    route_ids.append(validated)
                    
    if not route_ids:
        potential_lines = re.findall(r"\b(?:linea|linee)\s+([A-Za-z0-9]+)\b", text, re.IGNORECASE)
        for pl in potential_lines:
            validated = gtfs_manager.validate_route_id(pl.upper())
            if validated and validated not in route_ids:
                route_ids.append(validated)

    return route_ids

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=8),
    retry=retry_if_exception_type((requests.exceptions.RequestException, ConnectionError)),
    reraise=True
)
def _request_gtt_page():
    headers = {"User-Agent": "Mozilla/5.0 (Compatible; GTT-GTFS-RT-Middleware/1.3; +https://github.com/17mattee/gtt-gtfsrt-alerts)"}
    response = requests.get(GTT_RAW_URL, headers=headers, timeout=10)
    response.raise_for_status()
    return response.text

def fetch_gtt_alerts():
    alerts = []
    try:
        html_content = _request_gtt_page()
        soup = BeautifulSoup(html_content, "html.parser")
        raw_full_text = soup.get_text(separator=" ", strip=True)
        
        blocks = re.split(r"(?=\d{2}/\d{2}/\d{4}\s\d{2}:\d{2})", raw_full_text)
        raw_texts = [b.replace("Leggi avviso", "").strip() for b in blocks if len(b.strip()) > 20]
        unique_texts = list(dict.fromkeys(raw_texts))
        
        for idx, text in enumerate(unique_texts):
            route_ids = extract_route_ids(text)
            title_match = text.split(".")[0] if "." in text else text[:60]
            
            alerts.append({
                "id": f"gtt_alert_pro_{idx + 1}_{int(time.time())}",
                "title": title_match.strip(),
                "description": text.strip(),
                "route_ids": route_ids,
                "cause": parse_cause(text),
                "effect": parse_effect(text),
                "start_time": extract_start_time(text)
            })
            
        # Controllo critico di integrità: se la pagina GTT risponde ma non estrae alcun blocco, il layout potrebbe essere cambiato
        if len(unique_texts) == 0 and len(html_content) > 500:
            logger.warning("Attenzione: La pagina GTT è stata scaricata ma il parser non ha estratto alcun avviso. Possibile cambio di layout HTML.")
            send_telegram_alert("⚠️ **GTT Scraper Warning**: La pagina GTT è raggiungibile ma non ha prodotto avvisi. Verificare eventuale cambio di layout HTML.")

    except Exception as e:
        error_msg = f"Errore critico nello scraper GTT ottimizzato dopo i retry: {e}"
        logger.error(error_msg)
        send_telegram_alert(error_msg)

    return alerts
