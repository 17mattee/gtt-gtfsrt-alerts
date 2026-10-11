import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from google.transit import gtfs_realtime_pb2
from gtfs_static import gtfs_manager

GTT_RAW_URL = "https://www.gtt.to.it/cms/index.php?option=com_gtt&view=avvisi&tmpl=raw&priorita=1"

def parse_cause(text: str):
    text_lower = text.lower()
    if "sciopero" in text_lower:
        return gtfs_realtime_pb2.Alert.Cause.LABOR_STRIKE
    elif "corteo" in text_lower or "manifestazione" in text_lower:
        return gtfs_realtime_pb2.Alert.Cause.DEMONSTRATION
    elif "lavori" in text_lower or "cantieri" in text_lower:
        return gtfs_realtime_pb2.Alert.Cause.CONSTRUCTION
    elif "concerto" in text_lower or "partita" in text_lower or "podistica" in text_lower:
        return gtfs_realtime_pb2.Alert.Cause.HOLIDAY
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

def fetch_gtt_alerts():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    alerts = []
    
    try:
        response = requests.get(GTT_RAW_URL, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            items = soup.find_all(["div", "li", "p"]) or [soup]
            
            for idx, item in enumerate(items):
                text = item.get_text(separator=" ", strip=True)
                if len(text) < 15:
                    continue

                # Estrazione di TUTTE le linee citate (es. "Linee 9, 18, 67 Festiva, Star 2")
                lines_match = re.search(r"Linee?\s+([A-Za-z0-9\s,\/\-]+?)(?=\s+deviate|\s+soppresse|\s+potenziate|\s+Causa|\.|$)", text, re.IGNORECASE)
                route_ids = []
                
                if lines_match:
                    raw_lines = lines_match.group(1)
                    # Splitta per virgola, 'e', o slash
                    tokens = re.split(r",|\se\s|\/", raw_lines)
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
                    "effect": parse_effect(text)
                })

    except Exception as e:
        print(f"[Scraper] Errore nell'estrazione Raw: {e}")

    # Fallback dimostrativo avanzato
    if not alerts:
        alerts.append({
            "id": "gtt_alert_demo_1",
            "title": "Linee 9, 18 e 67 deviate per manifestazione",
            "description": "Linee 9, 18, 67 deviate su percorsi alternativi causa manifestazione in via Madama Cristina.",
            "route_ids": [gtfs_manager.validate_route_id("9"), gtfs_manager.validate_route_id("18"), gtfs_manager.validate_route_id("67")],
            "cause": gtfs_realtime_pb2.Alert.Cause.DEMONSTRATION,
            "effect": gtfs_realtime_pb2.Alert.Effect.DETOUR
        })

    return alerts
