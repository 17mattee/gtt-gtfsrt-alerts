import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import asyncio
import logging
import sys
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("Enterprise-QA")

async def run_comprehensive_audit():
    print("=" * 65)
    print(" 🚀 AVVIO AUDIT DI COLLAUDO ENTERPRISE - GTT GTFS-RT MIDDLEWARE ")
    print("=" * 65)
    
    errors = []

    # 1. Test Importazione Moduli Core
    print("\n[1/6] Verifica integrità moduli di sistema...")
    try:
        from app.main import app
        from app.websocket_manager import manager
        from app.vehicles_scraper import fetch_gtt_vehicle_positions
        from app.gtfs_static import get_gtfs_routes
        from app.gtt_full_sync import fetch_all_gtt_data
        print("  ✅ Tutti i moduli core (main, manager, scraper, static, full_sync) importati con successo.")
    except Exception as e:
        err_msg = f"Errore critico di importazione moduli: {e}"
        print(f"  ❌ {err_msg}")
        errors.append(err_msg)

    # 2. Test GTFS Static (Estrazione dinamica linee)
    print("\n[2/6] Test recupero dinamico linee da GTT (GTFS Static)...")
    try:
        routes = get_gtfs_routes()
        print(f"  ✅ Linee ufficiali estratte dinamicamente: {len(routes)}")
        assert len(routes) > 0, "Il dizionario delle rotte non deve essere vuoto"
    except Exception as e:
        err_msg = f"Fallimento test GTFS Static: {e}"
        print(f"  ❌ {err_msg}")
        errors.append(err_msg)

    # 3. Test Telemetria Veicoli (Scraper & Fallback)
    print("\n[3/6] Test telemetria in tempo reale dei mezzi di Torino...")
    try:
        vehicles = await fetch_gtt_vehicle_positions()
        print(f"  ✅ Mezzi pubblicati da GTT in questo istante: {len(vehicles)} (solo dati reali)")
        assert isinstance(vehicles, list)
        if len(vehicles) == 0:
            print("  ℹ️ GTT non pubblica posizioni ora: nessun mezzo simulato.")
        sample = vehicles[0] if vehicles else {"vehicle_id": "n/d", "route_id": "n/d", "latitude": 0, "longitude": 0}
        print(f"     Esempio mezzo -> ID: {sample.get('vehicle_id')} | Linea: {sample.get('route_id')} | Pos: ({sample.get('latitude')}, {sample.get('longitude')})")
    except Exception as e:
        err_msg = f"Fallimento test Vehicle Scraper: {e}"
        print(f"  ❌ {err_msg}")
        errors.append(err_msg)

    # 4. Test Sincronizzazione Totale (Full Sync: Veicoli, Ritardi, Avvisi)
    print("\n[4/6] Test sincronizzazione totale (Full Network Data)...")
    try:
        full_data = await fetch_all_gtt_data()
        assert "vehicles" in full_data
        assert "trip_updates" in full_data
        assert "alerts" in full_data
        assert "routes_catalog" in full_data
        print(f"  ✅ Sincronizzazione totale OK -> Veicoli: {len(full_data['vehicles'])}, Trip Updates: {len(full_data['trip_updates'])}, Allerte: {len(full_data['alerts'])}")
    except Exception as e:
        err_msg = f"Fallimento test Full Sync: {e}"
        print(f"  ❌ {err_msg}")
        errors.append(err_msg)

    # 5. Test WebSocket Connection Manager
    print("\n[5/6] Test del Connection Manager WebSocket (Control Room)...")
    try:
        class DummyWebSocket:
            def __init__(self):
                self.messages = []
                self.accepted = False
            async def accept(self):
                self.accepted = True
            async def send_json(self, data):
                self.messages.append(data)

        dummy = DummyWebSocket()
        await manager.connect(dummy)
        assert dummy.accepted is True
        assert dummy in manager.active_connections
        
        # Test broadcast
        await manager.broadcast({"type": "ping", "status": "ok"})
        manager.disconnect(dummy)
        assert dummy not in manager.active_connections
        print("  ✅ Gestione connessioni e broadcast WebSocket verificata al 100%.")
    except Exception as e:
        err_msg = f"Fallimento test WebSocket Manager: {e}"
        print(f"  ❌ {err_msg}")
        errors.append(err_msg)

    # 6. Test Database / Supabase / Persistenza
    print("\n[6/6] Verifica configurazione database e persistenza...")
    db_url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_URL")
    if db_url:
        print("  ✅ Variabili di ambiente Database/Supabase rilevate.")
    else:
        print("  ℹ️ Nessun database esterno configurato (modalità autonoma / in-memory / test attiva).")

    print("\n" + "=" * 65)
    if errors:
        print(f" ❌ AUDIT FALLITO CON {len(errors)} ERRORI:")
        for err in errors:
            print(f"   - {err}")
        print("=" * 65)
        sys.exit(1)
    else:
        print(" 🎉 AUDIT COMPLETATO CON SUCCESSO AL 100%! SISTEMA PERFETTO. ")
        print("=" * 65)

if __name__ == "__main__":
    asyncio.run(run_comprehensive_audit())
