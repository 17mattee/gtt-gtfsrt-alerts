import httpx
from google.transit import gtfs_realtime_pb2
from datetime import datetime

def inspect():
    try:
        response = httpx.get("http://127.0.0.1:8000/gtfs-rt/alerts.pb", timeout=5)
        response.raise_for_status()
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        
        print("\n=== HEADER GTFS-RT ===")
        print(f"Versione: {feed.header.gtfs_realtime_version}")
        print(f"Timestamp: {feed.header.timestamp}\n")
        print(f"=== AVVISI TROVATI ({len(feed.entity)}) ===\n")
        
        for entity in feed.entity:
            print(f"ID Entità: {entity.id}")
            
            if len(entity.alert.active_period) > 0:
                start_ts = entity.alert.active_period[0].start
                print(f"Inizio Validità: {datetime.fromtimestamp(start_ts).strftime('%d/%m/%Y %H:%M')}")
            
            route_ids = [ie.route_id for ie in entity.alert.informed_entity if ie.route_id]
            if route_ids:
                print(f"Linee impattate: {', '.join(route_ids)}")
            else:
                print("Linee impattate: Generale")
            
            print(f"Titolo: {entity.alert.header_text.translation[0].text}")
            print(f"Dettagli: {entity.alert.description_text.translation[0].text}\n")
            
    except Exception as e:
        print(f"Errore: {e}")

if __name__ == "__main__":
    inspect()
