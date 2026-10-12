import asyncio
from app.gtt_full_sync import fetch_all_gtt_data

async def test_sync():
    print("🔍 Avvio test di acquisizione totale (Linee, Veicoli, Ritardi, Avvisi)...")
    data = await fetch_all_gtt_data()
    print(f"  -> Linee nel catalogo statico: {len(data['routes_catalog'])}")
    print(f"  -> Veicoli tracciati: {len(data['vehicles'])}")
    print(f"  -> Aggiornamenti ritardi (Trip Updates): {len(data['trip_updates'])}")
    print(f"  -> Avvisi e deviazioni attive (Alerts): {len(data['alerts'])}")
    print("✅ Sincronizzazione totale completata con successo al 100%!")

if __name__ == "__main__":
    asyncio.run(test_sync())
