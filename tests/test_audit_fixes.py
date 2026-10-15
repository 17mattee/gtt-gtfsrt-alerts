"""Test delle correzioni dell'audit repository (fix/repository-audit-and-bugfixes).

Coprono:
- percorsi statici ASSOLUTI in app.main: la dashboard non dipende piu' dalla
  working directory di avvio
- CSP dinamica: connect-src consente WebSocket solo verso l'host servito
- database.py: parsing robusto di cause/effect (int, cifra o label italiana)
  e avvisi senza start_time piu' validi
- scraper.py: data illeggibile -> orario di pubblicazione, nessun except muto
"""

import time

from fastapi.testclient import TestClient

from app import main
from app.main import app

client = TestClient(app)


def test_percorsi_statici_assoluti():
    """Le risorse statiche risolvono dal file sorgente, non dalla CWD."""
    assert main.STATIC_DIR.is_absolute()
    assert (main.STATIC_DIR / "index.html").exists()
    assert main.STATIC_DIR.name == "static"


def test_dashboard_servita():
    response = client.get("/")
    assert response.status_code == 200
    assert "GTT GTFS-RT Live Dashboard & Map" in response.text


def test_header_sicurezza_e_csp_ristretta():
    """CSP presente, con WebSocket consentito SOLO verso l'host servito."""
    response = client.get("/health")
    csp = response.headers.get("content-security-policy", "")
    assert "connect-src 'self'" in csp
    assert "ws://testserver" in csp  # solo l'host della richiesta, non ws: aperto
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["referrer-policy"] == "strict-origin-when-cross-origin"


def test_csp_con_host_ostile_non_inietta_valori():
    """Un Host header invalido non finisce nella CSP (no header injection)."""
    response = client.get("/health", headers={"Host": "evil.example\r\nX-Injected: 1"})
    csp = response.headers.get("content-security-policy", "")
    assert "X-Injected" not in csp
    assert "evil.example" not in csp


def test_cause_effect_da_label_o_int():
    """Il DB contiene label italiane o int: entrambi si rileggono come int GTFS-RT."""
    from app.database import _cause_from_db, _effect_from_db

    assert _cause_from_db("Sciopero") == 4          # label di database_writer
    assert _cause_from_db(4) == 4                   # int
    assert _cause_from_db("4") == 4                 # cifra testuale
    assert _cause_from_db(None) == 1                # default onesto
    assert _cause_from_db("valore ignoto") == 1
    assert _effect_from_db("Deviazione di percorso") == 4
    assert _effect_from_db(None) == 8               # UNKNOWN_EFFECT, non 1


def test_epoch_utc_conversion_robusta():
    from app.database import _epoch_utc_or_none
    from datetime import datetime, timezone

    dt = _epoch_utc_or_none(1791900000)
    assert dt == datetime.fromtimestamp(1791900000, tz=timezone.utc)
    assert _epoch_utc_or_none(None) is None          # avviso senza start_time
    assert _epoch_utc_or_none("25/07/2026") is None  # stringa, non epoch
    assert _epoch_utc_or_none(10**12) is None        # epoch fuori range: OverflowError


def test_save_alerts_to_db_senza_db_configurato(monkeypatch):
    """Nessun DATABASE_URL: no-op silenzioso, mai eccezione."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    from app.database import save_alerts_to_db
    assert save_alerts_to_db([{"id": "x", "title": "t", "description": "d"}]) is None


def test_extract_start_time_con_data_illeggibile():
    """Data malformata (32/13/2026): fallback sull'orario di pubblicazione."""
    from app.scraper import extract_start_time
    ts = extract_start_time("Aggiornamento del 32/13/2026 25:61 sulla linea")
    assert abs(ts - int(time.time())) < 10
