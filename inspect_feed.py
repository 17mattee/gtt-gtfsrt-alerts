import requests
from google.transit import gtfs_realtime_pb2

URL = "http://127.0.0.1:8000/gtfs-rt/alerts.pb"

def inspect():
    try:
        response = requests.get(URL, timeout=5)
        if response.status_code != 200:
            print(f"Errore server: {response.status_code}")
            return

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        print("=== HEADER GTFS-RT ===")
        print(f"Versione: {feed.header.gtfs_realtime_version}")
        print(f"Timestamp: {feed.header.timestamp}")
        print(f"\n=== AVVISI TROVATI ({len(feed.entity)}) ===")

        for entity in feed.entity:
            print(f"\nID Entità: {entity.id}")
            alert = entity.alert
            
            lines = [e.route_id for e in alert.informed_entity if e.route_id]
            print(f"Linee impattate: {', '.join(lines) if lines else 'Generale'}")
            
            if alert.header_text.translation:
                print(f"Titolo: {alert.header_text.translation[0].text}")
            if alert.description_text.translation:
                print(f"Dettagli: {alert.description_text.translation[0].text}")

    except Exception as e:
        print(f"Errore durante l'ispezione: {e}")

if __name__ == "__main__":
    inspect()
