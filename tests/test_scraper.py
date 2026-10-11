import pytest
from scraper import parse_cause, parse_effect, extract_start_time
from google.transit import gtfs_realtime_pb2
from datetime import datetime

def test_parse_cause_logic():
    """Verifica che l'algoritmo associ correttamente la causa GTFS-RT partendo dal testo GTT"""
    assert parse_cause("Sciopero nazionale del trasporto") == gtfs_realtime_pb2.Alert.Cause.STRIKE
    assert parse_cause("Deviazione causa cantieri in piazza") == gtfs_realtime_pb2.Alert.Cause.CONSTRUCTION
    assert parse_cause("Corteo studentesco in centro") == gtfs_realtime_pb2.Alert.Cause.DEMONSTRATION
    assert parse_cause("Modifiche per concerto musicale") == gtfs_realtime_pb2.Alert.Cause.OTHER_CAUSE
    assert parse_cause("Incidente non specificato") == gtfs_realtime_pb2.Alert.Cause.UNKNOWN_CAUSE

def test_parse_effect_logic():
    """Verifica che l'algoritmo riconosca l'impatto sul servizio"""
    assert parse_effect("Le linee 4 e 10 saranno deviate") == gtfs_realtime_pb2.Alert.Effect.DETOUR
    assert parse_effect("Fermata soppressa in via Roma") == gtfs_realtime_pb2.Alert.Effect.NO_SERVICE
    assert parse_effect("Potenziamento della linea festiva") == gtfs_realtime_pb2.Alert.Effect.ADDITIONAL_SERVICE

def test_extract_start_time_regex():
    """Verifica che il motore regex estragga il timestamp UNIX corretto dalla stringa italiana"""
    text_with_date = "Titolo generico 11/10/2026 09:00 - Deviazione linea 4"
    timestamp = extract_start_time(text_with_date)
    dt = datetime.fromtimestamp(timestamp)
    
    assert dt.year == 2026
    assert dt.month == 10
    assert dt.day == 11
    assert dt.hour == 9
    assert dt.minute == 0
