from app.scraper import parse_cause, parse_effect, extract_start_time
from google.transit import gtfs_realtime_pb2

def test_parse_cause_logic():
    assert parse_cause("Avviso di sciopero generale") == gtfs_realtime_pb2.Alert.Cause.STRIKE
    assert parse_cause("Lavori in corso sulla linea") == gtfs_realtime_pb2.Alert.Cause.CONSTRUCTION
    assert parse_cause("Corteo in centro") == gtfs_realtime_pb2.Alert.Cause.DEMONSTRATION
    assert parse_cause("Evento sconosciuto") == gtfs_realtime_pb2.Alert.Cause.UNKNOWN_CAUSE

def test_parse_effect_logic():
    assert parse_effect("La linea 4 è deviata") == gtfs_realtime_pb2.Alert.Effect.DETOUR
    assert parse_effect("Servizio soppresso oggi") == gtfs_realtime_pb2.Alert.Effect.NO_SERVICE
    assert parse_effect("Potenziamento corse") == gtfs_realtime_pb2.Alert.Effect.ADDITIONAL_SERVICE

def test_extract_start_time_regex():
    text = "Aggiornamento del 15/10/2026 08:30 sulla linea"
    timestamp = extract_start_time(text)
    assert isinstance(timestamp, int)
    assert timestamp > 0
