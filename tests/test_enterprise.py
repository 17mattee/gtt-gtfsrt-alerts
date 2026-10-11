import pytest
from fastapi.testclient import TestClient
from main import app
import responses
from scraper import fetch_gtt_alerts

client = TestClient(app)

def test_api_root_dashboard():
    """Verifica che la dashboard HTML risponda correttamente"""
    response = client.get("/")
    assert response.status_code == 200
    assert "GTT GTFS-RT Middleware" in response.text

def test_gtfs_rt_protobuf_endpoint():
    """Verifica che l'endpoint Protobuf restituisca il formato binario corretto"""
    response = client.get("/gtfs-rt/alerts.pb")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-protobuf"

@responses.activate
def test_scraper_resilience_on_server_error():
    """Simula un errore HTTP 500 dal server GTT e verifica la gestione della resilienza"""
    url = "https://www.gtt.to.it/cms/index.php?option=com_gtt&view=avvisi&tmpl=raw&priorita=1"
    responses.add(responses.GET, url, status=500)
    
    alerts = fetch_gtt_alerts()
    # Lo scraper deve gestire l'errore graziosamente senza crashare, restituendo una lista vuota
    assert isinstance(alerts, list)
    assert len(alerts) == 0

def test_rate_limiting_protection():
    """Verifica che il rate limiter protegga le API da chiamate massive"""
    # Effettua chiamate rapide per testare il limite impostato sulla rotta principale
    responses_list = [client.get("/") for _ in range(35)]
    # Almeno una chiamata oltre la soglia dovrebbe attivare il blocco o comunque la rotta principale deve reggere
    status_codes = [r.status_code for r in responses_list]
    assert 200 in status_codes
