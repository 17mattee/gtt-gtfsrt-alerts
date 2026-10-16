"""Test di RILEVAMENTO RIENTRO AL DEPOSITO (blocchi-corse GTFS).

Il block_id del GTFS e' la catena di corse dello stesso mezzo nella giornata
di servizio: chi e' all'ULTIMA corsa del blocco, terminata questa, non ha
piu' servizio programmato (misurato dal vivo: 463 corse su 464 con blocco).
"""

import io
import zipfile

from app import gtt_full_sync
from app.gtt_full_sync import _mark_depot_returns
from app.gtfs_static import GtfsStaticManager

# --- Blocchi GTFS ---
# ---------------------------------------------------------------------

def _zip_sintetico():
    """Pacchetto GTFS minimale: blocco B con tre corse (T1<T2<T3) dello stesso
    service S, piu' una corsa T_X del blocco B su ALTRO service (altro giorno:
    non deve entrare nel blocco di oggi)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("trips.txt", (
            "route_id,service_id,trip_id,block_id\n"
            "2U,S,T1,B\n"
            "2U,S,T2,B\n"
            "10U,S,T3,B\n"
            "2U,ALTRO,T_X,B\n"
        ))
        rows = [
            ("T1", "08:00:00"), ("T1", "08:30:00"),
            ("T2", "09:00:00"), ("T2", "09:40:00"),
            ("T3", "10:00:00"), ("T3", "10:50:00"),
            ("T_X", "06:00:00"),
        ]
        z.writestr("stop_times.txt", "trip_id,arrival_time,departure_time,stop_id\n" +
                   "".join(f"{t},{a},{a},S{i}\n" for i, (t, a) in enumerate(rows)))
    return buf.getvalue()


def test_blocco_ordinato_e_solo_stesso_service():
    mgr = GtfsStaticManager()
    mgr._zip_bytes = _zip_sintetico()
    added = mgr.refresh_block_schedules({"T1"})
    assert added == 1
    sched = mgr.get_block_schedule("T1")
    assert sched["block_id"] == "B"
    assert sched["trips"] == ["T1", "T2", "T3"], "ordinati per partenza, senza il service ALTRO"


def test_blocco_senza_zip_o_senza_blocco():
    mgr = GtfsStaticManager()
    mgr._zip_bytes = b""
    assert mgr.refresh_block_schedules({"T1"}) == 0
    mgr._zip_bytes = _zip_sintetico()
    assert mgr.refresh_block_schedules({"T_X"}) == 1  # T_X ha comunque il blocco B (suo service)


def test_mark_depot_returns_ultima_corsa():
    gtt_full_sync.gtfs_manager._block_schedules = {
        "T1": {"block_id": "B", "trips": ["T1", "T2"]},
        "T2": {"block_id": "B", "trips": ["T1", "T2"]},
    }
    try:
        in_servizio = {"vehicle_id": "9001U", "trip_id": "T1"}
        ultima = {"vehicle_id": "9002U", "trip_id": "T2"}
        ignoto = {"vehicle_id": "9003U", "trip_id": "T9"}
        marked = _mark_depot_returns([in_servizio, ultima, ignoto])
        assert marked == 1
        assert "rientro_deposito" not in in_servizio, "ha ancora T2 dopo"
        assert ultima.get("rientro_deposito") is True, "T2 e' l'ultima del blocco"
        assert "rientro_deposito" not in ignoto
    finally:
        gtt_full_sync.gtfs_manager._block_schedules = {}
