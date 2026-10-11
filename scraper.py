import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import time
from google.transit import gtfs_realtime_pb2
from gtfs_static import gtfs_manager
from notifier import send_telegram_alert
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

GTT_RAW_URL = "https://www.gtt.to.it/cms/index.php?option=com_gtt&view=avvisi&tmpl=raw&priorita=1"

def parse_cause(text: str):
    text_lower = text.lower()
    if "sciopero" in text_lower:
        return gtfs_realtime_pb2.Alert.Cause.STRIKE
    elif "corteo" in text_lower or "manifestazione" in text_lower:
        return gtfs_realtime_pb2.Alert.Cause.DEMONSTRATION
    elif "lavori" in text_lower or "cantier" in text_lower:
        return gtfs_realtime_pb2.Alert.Cause.CONSTRUCTION
    elif "concert" in text_lower or "partit" in text_lower or "podistic" in text_lower:
        return gtfs_realtime_pb2.Alert.Cause.OTHER_CAUSE
    return gtfs_realtime_pb2.Alert.Cause.UNKNOWN_CAUSE

def parse_effect(text: str):
    text_lower = text.lower()
    if "deviat" in text_lower:
        return gtfs_realtime_pb2.Alert.Effect.DETOUR
    elif "soppress" in text_lower or "sospes" in text_lower:
        return gtfs_realtime_pb2.Alert.Effect.NO_SERVICE
    elif "intensificaz" in text_lower or "potenziament" in text_lower:
        return gtfs_realtime_pb2.Alert.Effect.ADDITIONAL_SERVICE
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

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=8),
    retry=retry_if_exception_type((requests.exceptions.RequestException, ConnectionError)),
    reraise=True
)
def _request_gtt_page():
    headers = {"User-Agent": "Mozilla/5.0 (Compatible; GTT-GTFS-RT-Middleware/1.0)"}
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
        raw_texts = [b.replace("Leggi avviso", "").strip() for b in blocks if len(b.strip()) > 30]
        unique_texts = list(dict.fromkeys(raw_texts))
        
        for idx, text in enumerate(unique_texts):
            lines_match = re.search(r"Line[ea]e?\s+([A-Za-z0-9\s,\/\-]+?)(?=\s+deviat|\s+soppress|\s+potenziat|\s+intensificat|\s+Causa|\.|$)", text, re.IGNORECASE)
            route_ids = []
            
            if lines_match:
                raw_lines = lines_match.group(1)
                tokens = re.split(r",|\se\s|\/|\s-\s", raw_lines)
                for tok in tokens:
                    clean_tok = tok.strip()
                    if clean_tok:
                        validated = gtfs_manager.validate_route_id(clean_tok)
                        if validated and validated not in route_ids:
                            route_ids.append(validated)

            title_match = text.split(".")[0] if "." in text else text[:60]
            
            alerts.append({
                "id": f"gtt_alert_raw_{idx + 1}",
                "title": title_match.strip(),
                "description": text.strip(),
                "route_ids": route_ids,
                "cause": parse_cause(text),
                "effect": parse_effect(text),
                "start_time": extract_start_time(text)
            })

    except Exception as e:
        error_msg = f"Errore critico nello scraper GTT dopo i retry: {e}"
        print(f"[Scraper] {error_msg}")
        send_telegram_alert(error_msg)

    return alerts
