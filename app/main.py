import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.websocket_manager import manager
from app.gtt_full_sync import fetch_all_gtt_data

app = FastAPI(title="GTT Torino GTFS-RT Enterprise Middleware", version="2.0.0")

# Monta la cartella statica per servire la UI
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def get_dashboard():
    return FileResponse("static/index.html")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "city": "Turin", "agency": "GTT"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Recupera tutti i dati aggiornati (veicoli, ritardi, allerte)
            full_data = await fetch_all_gtt_data()
            await websocket.send_json({
                "type": "full_network_update",
                "data": full_data
            })
            await asyncio.sleep(5) # Aggiornamento live ogni 5 secondi
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)
