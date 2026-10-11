import os
import zipfile
import requests
import pandas as pd

GTFS_ZIP_URL = "http://opendata.5t.torino.it/gtfs/gtfs_torino.zip"
DATA_DIR = "gtfs_data"

class GTFSStaticManager:
    def __init__(self):
        self.routes_df = None
        self.stops_df = None
        self._ensure_data_loaded()

    def _ensure_data_loaded(self):
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
            
        zip_path = os.path.join(DATA_DIR, "gtfs.zip")
        
        if not os.path.exists(zip_path):
            try:
                print("[GTFS Static] Scaricamento dataset GTFS da 5T Torino...")
                resp = requests.get(GTFS_ZIP_URL, timeout=15)
                if resp.status_code == 200:
                    with open(zip_path, "wb") as f:
                        f.write(resp.content)
                    
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(DATA_DIR)
                    print("[GTFS Static] Dataset estratto con successo.")
            except Exception as e:
                print(f"[GTFS Static] Download fallito: {e}. Modalità isolata attiva.")

        routes_path = os.path.join(DATA_DIR, "routes.txt")
        stops_path = os.path.join(DATA_DIR, "stops.txt")
        
        if os.path.exists(routes_path):
            self.routes_df = pd.read_csv(routes_path, dtype=str)
        if os.path.exists(stops_path):
            self.stops_df = pd.read_csv(stops_path, dtype=str)

    def validate_route_id(self, route_short_name: str) -> str:
        if self.routes_df is None or not route_short_name:
            return route_short_name

        matched = self.routes_df[self.routes_df['route_short_name'].str.upper() == route_short_name.upper()]
        if not matched.empty:
            return str(matched.iloc[0]['route_id'])
        
        return route_short_name

    def find_stop_ids_by_street(self, street_name: str) -> list:
        """Cerca gli stop_id del dataset 5T corrispondenti a una via o corso"""
        if self.stops_df is None or not street_name or len(street_name) < 4:
            return []

        matched = self.stops_df[self.stops_df['stop_name'].str.contains(street_name, case=False, na=False)]
        if not matched.empty:
            return matched['stop_id'].tolist()[:5]  # Ritorna fino a 5 fermate rilevanti
        return []

gtfs_manager = GTFSStaticManager()
