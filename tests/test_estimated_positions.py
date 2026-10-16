"""Test di POSIZIONE STIMATA per i mezzi con corsa attiva ma GPS non pubblicato.

Il caso reale (misurato dal vivo): ~2% dei mezzi con corsa attiva non compare
mai nel feed posizioni (GPS guasto o non trasmesso): il passeggero vede il
bus per strada ma la mappa no. La stima usa solo dati ufficiali (corsa attiva
nel feed trip updates + percorso programmato GTFS-Static + ritardo reale) e
viene dichiarata 'stimata' in ogni punto della UI.
"""

import time

from app.gtt_full_sync import (
    _estimate_unpublished_vehicles,
    _rome_offset_seconds,
)


def _seq_attorno_ad_orario(now, offsetti):
    """Sequenza programmata i cui orari (secondi dal mezzogiorno locale di
    Roma) stanno a 'offsetti' secondi da adesso: base per la previsione."""
    off = _rome_offset_seconds(now)
    sod = int((now + off) % 86400)
    return [(sod + o, f"S{i}") for i, o in enumerate(offsetti)]


def _stops(coords):
    return {f"S{i}": {"name": f"Fermata {100+i} - VIA PROVA {i}", "lat": lat, "lon": lon}
            for i, (lat, lon) in enumerate(coords)}


def test_stima_mezzo_con_corsa_attiva_senza_gps():
    """Mezzo con corsa attiva e ritardo reale, GPS mai pubblicato: posizione
    stimata a META' del segmento tra fermata precedente e prossima fermata."""
    now = time.time()
    # prossima fermata S1 prevista a +100s, la precedente S0 e' a -100s:
    # segmento 200s, ne restano 100 -> frazione 0.5 (punto medio).
    seq = _seq_attorno_ad_orario(now, [-100, 100])
    stops = _stops([(45.0, 7.6), (45.0 + 0.01, 7.7)])
    tu = {"vehicle_id": "9001U", "trip_id": "T1", "route_id": "2",
          "delay_seconds": 0, "next_stop": None, "headsign": "P.ZZA SOFIA"}

    estimated = _estimate_unpublished_vehicles([tu], [], {"T1": seq}, stops)

    assert len(estimated) == 1
    est = estimated[0]
    assert est["position_source"] == "stimata", "mai confondibile con un fix GPS"
    assert est["verifica"] == "corsa_attiva"
    assert est["vehicle_id"] == "9001U" and est["route_id"] == "2"
    # punto medio tra le due fermate (tolleranza: microscopici ritardi di
    # esecuzione tra il 'now' del test e quello interno della funzione)
    assert abs(est["latitude"] - (45.0 + 0.005)) < 0.002
    assert abs(est["longitude"] - (7.6 + 0.05)) < 0.002
    assert est["next_stop"]["stop_id"] == "S1"
    assert est["next_stop"]["minutes"] in (1, 2)
    assert est["delay_seconds"] == 0


def test_stima_non_duplica_i_mezzi_con_gps():
    """Un mezzo che GTT pubblica col GPS non viene stimato: nessun doppione."""
    now = time.time()
    seq = _seq_attorno_ad_orario(now, [-100, 100])
    stops = _stops([(45.0, 7.6), (45.01, 7.7)])
    tu = {"vehicle_id": "9001U", "trip_id": "T1", "route_id": "2",
          "delay_seconds": 0, "next_stop": None}
    vehicles = [{"vehicle_id": "9001U", "latitude": 45.05, "longitude": 7.65,
                 "timestamp": int(now), "route_id": "2", "trip_id": "T1"}]
    assert _estimate_unpublished_vehicles([tu], vehicles, {"T1": seq}, stops) == []


def test_stima_corsa_non_ancora_partita_salta():
    """Prossima fermata prevista oltre i 15 minuti: corsa non partita, la
    posizione sarebbe fantasia: nessuna stima."""
    now = time.time()
    seq = _seq_attorno_ad_orario(now, [2000, 2100])
    stops = _stops([(45.0, 7.6), (45.01, 7.7)])
    tu = {"vehicle_id": "9001U", "trip_id": "T1", "route_id": "2",
          "delay_seconds": 0, "next_stop": None}
    assert _estimate_unpublished_vehicles([tu], [], {"T1": seq}, stops) == []


def test_stima_in_attesa_alla_partenza():
    """Corsa appena partita (o in attesa): la stima e' il capolinea di partenza."""
    now = time.time()
    seq = _seq_attorno_ad_orario(now, [100, 300, 500])  # prossima fermata: S0
    stops = _stops([(45.0, 7.6), (45.01, 7.7), (45.02, 7.8)])
    tu = {"vehicle_id": "9002U", "trip_id": "T2", "route_id": "4",
          "delay_seconds": 0, "next_stop": None}
    estimated = _estimate_unpublished_vehicles([tu], [], {"T2": seq}, stops)
    assert len(estimated) == 1
    assert abs(estimated[0]["latitude"] - 45.0) < 1e-6, "in attesa alla prima fermata"
    assert estimated[0]["next_stop"]["stop_id"] == "S0"


def test_stima_senza_sequenza_o_coordinate_salta():
    now = time.time()
    tu = {"vehicle_id": "9001U", "trip_id": "T1", "route_id": "2",
          "delay_seconds": 0, "next_stop": None}
    assert _estimate_unpublished_vehicles([tu], [], {"T1": []}, {}) == []  # nessuna sequenza
    seq = _seq_attorno_ad_orario(now, [-100, 100])
    stops_senza_coord = {"S0": {"name": "x"}, "S1": {"name": "y"}}
    assert _estimate_unpublished_vehicles([tu], [], {"T1": seq}, stops_senza_coord) == []



