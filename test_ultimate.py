import asyncio
import sys
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Ultimate-QA")

async def run_ultimate_test():
    print("=" * 70)
    print(" 🌟 AVVIO TEST SUPREMO DI COLLAUDO ENTERPRISE - GTT TORINO ")
    print("=" * 70)
    
    passed_checks = 0
    total_checks = 5

    # Check 1: Integrità Moduli e FastAPI App
    print("\n[Check 1/5] Verifica moduli core e FastAPI...")
    try:
        from app.main import app
        from app.websocket_manager import manager
        from app.vehicles_scraper import fetch_gtt_vehicle_positions
        from app.gtfs_static import get_gtfs_routes
        from app.gtt_full_sync import fetch_all_gtt_data
        assert app is not None
        assert manager is not None
        print("  ✅ Tutti i moduli core caricati correttamente.")
        passed_checks += 1
    except Exception as e:
        print(f"  ❌ Errore modulo: {e}")

    # Check 2: GTFS Static & Estrazione Linee Torino
    print("\n[Check 2/5] Test catalogo linee GTFS Static (Torino)...")
    try:
        routes = get_gtfs_routes()
        print(f"  ✅ Linee ufficiali rilevate: {len(routes)}")
        assert len(routes) > 0
        passed_checks += 1
    except Exception as e:
        print(f"  ❌ Errore GTFS Static: {e}")

    # Check 3: Telemetria Veicoli & Fallback di Resilienza
    print("\n[Check 3/5] Test telemetria flotta & resilienza multi-endpoint...")
    try:
        vehicles = await fetch_gtt_vehicle_positions()
        print(f"  ✅ Mezzi tracciati dal sistema: {len(vehicles)}")
        assert isinstance(vehicles, list)
        assert len(vehicles) > 0
        passed_checks += 1
    except Exception as e:
        print(f"  ❌ Errore Scraper Veicoli: {e}")

    # Check 4: Sincronizzazione Totale (Full Sync: Veicoli + TripUpdates + Alerts)
    print("\n[Check 4/5] Test sincronizzazione totale (Full Network Data)...")
    try:
        full_data = await fetch_all_gtt_data()
        assert "vehicles" in full_data
        assert "trip_updates" in full_data
        assert "alerts" in full_data
        assert "routes_catalog" in full_data
        print(f"  ✅ Full Sync OK -> Veicoli: {len(full_data['vehicles'])}, Avvisi: {len(full_data['alerts'])}")
        passed_checks += 1
    except Exception as e:
        print(f"  ❌ Errore Full Sync: {e}")

    # Check 5: WebSocket Control Room Manager
    print("\n[Check 5/5] Test Connection Manager WebSocket (Control Room)...")
    try:
        class MockSocket:
            def __init__(self):
                self.accepted = False
                self.sent = []
            async def accept(self):
                self.accepted = True
            async def send_json(self, data):
                self.sent.append(data)

        sock = MockSocket()
        await manager.connect(sock)
        assert sock.accepted is True
        await manager.broadcast({"status": "test_ping"})
        manager.disconnect(sock)
        assert sock not in manager.active_connections
        print("  ✅ Gestione WebSocket verificata al 100%.")
        passed_checks += 1
    except Exception as e:
        print(f"  ❌ Errore WebSocket Manager: {e}")

    print("\n" + "=" * 70)
    print(f" RISULTATO FINALE: {passed_checks}/{total_checks} VERIFICHE SUPERATE CON SUCCESSO.")
    print("=" * 70)
    
    if passed_checks == total_checks:
        print(" 🎉 IL SISTEMA È PERFETTO, PRONTO E STABILE AL 100%! ")
        sys.exit(0)
    else:
        print(" ⚠️ ALCUNI TEST HANNO FALLITO.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(run_ultimate_test())
