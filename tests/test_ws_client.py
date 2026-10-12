import pytest
import asyncio
from app.websocket_manager import manager
from app.vehicles_scraper import fetch_gtt_vehicle_positions

@pytest.mark.anyio
async def test_websocket_manager_logic():
    print("🧪 Avvio test unitario asincrono della Control Room WebSocket...")
    
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
    print("  -> Connessione WebSocket al Manager: OK")

    vehicles = await fetch_gtt_vehicle_positions()
    print(f"  -> Telemetria flotta Torino caricata: {len(vehicles)} mezzi attivi.")

    await manager.broadcast({"type": "vehicles_update", "data": vehicles})
    assert len(dummy.messages) > 0
    print("  -> Broadcast dei dati in tempo reale: OK")

    manager.disconnect(dummy)
    assert dummy not in manager.active_connections
    print("  -> Disconnessione pulita dal Manager: OK")
    print("🎉 Test unitario WebSocket completato con successo al 100%!")

if __name__ == "__main__":
    asyncio.run(test_websocket_manager_logic())
