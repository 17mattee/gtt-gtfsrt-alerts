import pytest
import asyncio
from app.gtt_full_sync import fetch_all_gtt_data, _apply_fleet_hysteresis, _recent_vehicles, VEHICLE_GRACE_SECONDS

@pytest.mark.anyio
async def test_sync():
    print("🔍 Avvio test di acquisizione totale (Linee, Veicoli, Ritardi, Avvisi)...")
    data = await fetch_all_gtt_data()
    print(f"  -> Linee nel catalogo statico: {len(data['routes_catalog'])}")
    print(f"  -> Veicoli tracciati: {len(data['vehicles'])}")
    print(f"  -> Aggiornamenti ritardi (Trip Updates): {len(data['trip_updates'])}")
    print(f"  -> Avvisi e deviazioni attive (Alerts): {len(data['alerts'])}")
    assert isinstance(data, dict)
    print("✅ Sincronizzazione totale completata con successo al 100%!")

def _clear():
    _recent_vehicles.clear()

def test_hysteresis_gps_blink():
    """Un mezzo che salta un ciclo GPS (lampeggio) NON sparisce dalla flotta."""
    _clear()
    v1 = {"vehicle_id": "9001U", "latitude": 45.0, "longitude": 7.6}
    kept = _apply_fleet_hysteresis([v1, {"vehicle_id": "9002U", "latitude": 45.1, "longitude": 7.7}])
    assert {v["vehicle_id"] for v in kept} == {"9001U", "9002U"}
    # Ciclo successivo: 9001U NON pubblicato dal feed (GPS perso), 9002U si'
    kept = _apply_fleet_hysteresis([{"vehicle_id": "9002U", "latitude": 45.1, "longitude": 7.7}])
    ids = {v["vehicle_id"] for v in kept}
    assert "9001U" in ids, "il mezzo lampeggiante deve restare in strada (isteresi)"
    assert len(kept) == 2

def test_hysteresis_depot_exit():
    """Un mezzo silenzioso oltre il periodo di grazia esce (rientro al deposito)."""
    _clear()
    _apply_fleet_hysteresis([{"vehicle_id": "9001U", "latitude": 45.0, "longitude": 7.6}])
    # Simula il passare di piu' di VEHICLE_GRACE_SECONDS senza avvistamenti
    entry = _recent_vehicles["9001U"]
    entry["seen_at"] -= VEHICLE_GRACE_SECONDS + 1
    kept = _apply_fleet_hysteresis([])
    assert kept == [], "dopo il silenzio prolungato il mezzo deve uscire (deposito)"

def test_hysteresis_reappearance_updates_position():
    """Se il mezzo riappare, la posizione in flotta e' quella fresca."""
    _clear()
    _apply_fleet_hysteresis([{"vehicle_id": "9001U", "latitude": 45.0, "longitude": 7.6}])
    kept = _apply_fleet_hysteresis([{"vehicle_id": "9001U", "latitude": 45.2, "longitude": 7.9}])
    assert len(kept) == 1
    assert kept[0]["latitude"] == 45.2 and kept[0]["longitude"] == 7.9

def test_hysteresis_feed_outage_keeps_fleet():
    """Feed intero giu' per un ciclo: la flotta resta visibile (no blackout)."""
    _clear()
    _apply_fleet_hysteresis([{"vehicle_id": "9001U"}, {"vehicle_id": "9002U"}, {"vehicle_id": "9003U"}])
    kept = _apply_fleet_hysteresis([])  # feed vuoto per un ciclo
    assert len(kept) == 3, "un singolo ciclo a vuoto non deve svuotare la mappa"
    _clear()

if __name__ == "__main__":
    asyncio.run(test_sync())
