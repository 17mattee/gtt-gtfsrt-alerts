import os
import io
import zipfile
import csv
import logging
import requests

logger = logging.getLogger("GTT-Static")

GTFS_STATIC_URLS = [
    "https://www.gtt.to.it/open_data/gtt_gtfs.zip",
    "https://apidatadocs.avmspa.it/opendata/gtt_gtfs.zip"
]

def get_gtfs_routes() -> dict:
    routes = {}
    for url in GTFS_STATIC_URLS:
        try:
            response = requests.get(url, timeout=8)
            if response.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    if "routes.txt" in z.namelist():
                        with z.open("routes.txt") as f:
                            content = f.read().decode("utf-8-sig")
                            reader = csv.DictReader(content.splitlines())
                            for row in reader:
                                route_id = row.get("route_short_name") or row.get("route_id")
                                route_long = row.get("route_long_name", "")
                                if route_id:
                                    routes[route_id] = route_long
                if routes:
                    return routes
        except Exception as e:
            logger.warning(f"Mirror GTFS Static fallito ({url}): {e}")
            continue
            
    # Fallback di sicurezza essenziale se i server esterni non rispondono
    return {
        "M1": "Metropolitana - Fermi / Bengasi",
        "3": "Tram - Corso Tortona / Vallette",
        "4": "Tram - Strada Cattane / Drosso",
        "9": "Tram - Piazza Stampalia / Torino Esposizioni",
        "15": "Tram - Sassi / Via Senatore Ghedini"
    }

# Alias di compatibilità per evitare qualsiasi errore di importazione
gtfs_manager = get_gtfs_routes
