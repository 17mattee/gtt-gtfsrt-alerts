"""Test delle STIME DI POSIZIONE LUNGO LA SHAPE UFFICIALE.

Un mezzo stimato (corsa attiva ma GPS non pubblicato) avanza sulla
GEOMETRIA UFFICIALE della linea (shapes.txt), non in linea d'aria tra le
fermate: a meta' cammino di un percorso a L sta sull'angolo della via, non
dentro i palazzi. Verifica dal vivo: 7 stimati a 0.0m dalla shape, il resto
al capolinea di partenza o con errore <= 8.5m.
"""

import math
import time

from app import gtt_full_sync
from app.gtt_full_sync import (
    _nearest_shape_index,
    _point_along_shape,
)


def test_punto_lungo_shape_a_meta_strada_segue_la_curva():
    """Percorso a L con gambe uguali in METRI (1 grado lon a 45 gradi lat
    vale ~0.707 di un grado lat): a meta' del cammino la stima sta
    sull'ANGOLO, NON sul punto medio in linea d'aria (nei palazzi)."""
    d_lon = 0.001 / math.cos(math.radians(45.07))
    shape = [(45.0, 7.6), (45.0, 7.6 + d_lon), (45.0 + 0.001, 7.6 + d_lon)]
    lat, lon = _point_along_shape(shape, 0, 2, 0.5)
    assert abs(lon - (7.6 + d_lon)) < 1e-6, "meta' del cammino = all'angolo"
    assert abs(lat - 45.0) < 1e-6


def test_nearest_shape_index():
    shape = [(45.0, 7.6), (45.0, 7.61), (45.0, 7.62)]
    assert _nearest_shape_index(shape, 45.0, 7.615) == 1
    assert _nearest_shape_index([], 45.0, 7.6) is None


def test_stima_mezzo_senza_gps_segue_la_shape(monkeypatch):
    """Un mezzo stimato a meta' tra due fermate di un percorso a L sta sulla
    shape (all'angolo), non sulla retta che taglia i palazzi."""
    from app.gtt_full_sync import _estimate_unpublished_vehicles, _rome_offset_seconds
    now = time.time()
    off = _rome_offset_seconds(now)
    sod = int((now + off) % 86400)
    seq = [(sod - 100, "S0"), (sod + 100, "S1")]
    stops = {"S0": {"name": "Fermata 1 - VIA PARTENZA", "lat": 45.0, "lon": 7.6},
             "S1": {"name": "Fermata 2 - VIA ARRIVO", "lat": 45.0 + 0.001, "lon": 7.6 + 0.001}}
    d_lon = 0.001 / math.cos(math.radians(45.07))
    shape = [(45.0, 7.6), (45.0, 7.6 + d_lon), (45.0 + 0.001, 7.6 + d_lon)]
    monkeypatch.setattr(gtt_full_sync.gtfs_manager, "get_trip_shape", lambda tid: shape)
    tu = {"vehicle_id": "9001U", "trip_id": "T1", "route_id": "4",
          "delay_seconds": 0, "next_stop": None}
    est = _estimate_unpublished_vehicles([tu], [], {"T1": seq}, stops)
    assert len(est) == 1
    assert abs(est[0]["longitude"] - (7.6 + d_lon)) < 1e-4, "segue la curva vera della via"
    assert abs(est[0]["latitude"] - 45.0) < 1e-4, "meta' cammino = angolo, non retta nei palazzi"
