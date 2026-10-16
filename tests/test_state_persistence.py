"""Test dello STATO DI SISTEMA PERSISTENTE e dei TTL configurabili.

Lo snapshot dell'ultimo sync buono vive anche su Supabase (tabella
gtt_system_state): il disco del free tier di Render e' effimero tra i deploy,
cosi' il cold start durante un outage GTT parte comunque con i dati reali,
dal piu' fresco tra disco e cloud. I TTL sono configurabili via env.
"""

import json
import time

from app import gtt_full_sync


class _FakeSupabaseTable:
    def __init__(self, store):
        self.store = store

    def upsert(self, row, on_conflict=None):
        self.store[row["key"]] = row["payload"]
        return self

    def select(self, *a):
        return self

    def eq(self, key, value):
        self._key = value
        return self

    def limit(self, n):
        return self

    def execute(self):
        payload = self.store.get(getattr(self, "_key", ""))
        return type("Res", (), {"data": [{"payload": payload}] if payload is not None else []})()


def test_stato_supabase_salva_e_carica(monkeypatch):
    """Roundtrip con un client Supabase finto: upsert su gtt_system_state."""
    from app import database_writer
    store = {}
    fake = type("FakeClient", (), {"table": lambda self, name: _FakeSupabaseTable(store)})()
    monkeypatch.setattr(database_writer, "supabase", fake)
    payload = {"saved_at": time.time(), "data": {"vehicles": [{"vehicle_id": "9001U"}]}}
    assert database_writer.save_state_to_supabase(database_writer.SYSTEM_STATE_KEY, payload) is True
    loaded = database_writer.load_state_from_supabase(database_writer.SYSTEM_STATE_KEY)
    assert loaded == payload


def test_stato_supabase_non_configurato_noop(monkeypatch):
    from app import database_writer
    monkeypatch.setattr(database_writer, "supabase", None)
    assert database_writer.save_state_to_supabase("k", {"a": 1}) is False
    assert database_writer.load_state_from_supabase("k") is None


def test_restore_sceglie_il_piu_fresco_tra_disco_e_supabase(tmp_path, monkeypatch):
    """Il cloud piu' fresco del disco vince (caso deploy: disco vuoto/vecchio)."""
    monkeypatch.setattr(gtt_full_sync, "_SNAPSHOT_DIR", tmp_path)
    monkeypatch.setattr(gtt_full_sync, "_SNAPSHOT_FILE", tmp_path / "snap.json")
    now = time.time()

    old = {"saved_at": now - 600, "data": {"vehicles": [{"vehicle_id": "VECCHIO"}], "meta": {"vehicles_source": "live"}}}
    (tmp_path / "snap.json").write_text(json.dumps(old), encoding="utf-8")
    fresh = {"saved_at": now - 10, "data": {"vehicles": [{"vehicle_id": "FRESCO"}], "meta": {"vehicles_source": "live"}}}
    monkeypatch.setattr("app.database_writer.load_state_from_supabase", lambda key: fresh)

    orig = dict(gtt_full_sync.network_snapshot)
    try:
        assert gtt_full_sync.load_last_good_snapshot() is True
        vids = {v["vehicle_id"] for v in gtt_full_sync.network_snapshot["vehicles"]}
        assert vids == {"FRESCO"}, "il payload cloud piu' fresco deve vincere"
        assert gtt_full_sync.network_snapshot["meta"]["vehicles_source"] == "cached"
        disk = json.loads((tmp_path / "snap.json").read_text(encoding="utf-8"))
        assert disk["data"]["vehicles"][0]["vehicle_id"] == "FRESCO", "riscritto su disco"
    finally:
        gtt_full_sync.network_snapshot.clear()
        gtt_full_sync.network_snapshot.update(orig)
        gtt_full_sync._last_good_fleet = []
        gtt_full_sync._last_good_fleet_at = 0.0


def test_restore_cloud_mancante_cade_sul_disco(tmp_path, monkeypatch):
    monkeypatch.setattr(gtt_full_sync, "_SNAPSHOT_DIR", tmp_path)
    monkeypatch.setattr(gtt_full_sync, "_SNAPSHOT_FILE", tmp_path / "snap.json")
    now = time.time()
    disk_payload = {"saved_at": now - 30, "data": {"vehicles": [{"vehicle_id": "DISCO"}], "meta": {}}}
    (tmp_path / "snap.json").write_text(json.dumps(disk_payload), encoding="utf-8")
    monkeypatch.setattr("app.database_writer.load_state_from_supabase", lambda key: None)
    orig = dict(gtt_full_sync.network_snapshot)
    try:
        assert gtt_full_sync.load_last_good_snapshot() is True
        assert gtt_full_sync.network_snapshot["vehicles"][0]["vehicle_id"] == "DISCO"
    finally:
        gtt_full_sync.network_snapshot.clear()
        gtt_full_sync.network_snapshot.update(orig)
        gtt_full_sync._last_good_fleet = []
        gtt_full_sync._last_good_fleet_at = 0.0


def test_ttl_configurabili_da_env(monkeypatch):
    """I TTL sono configurabili: il free tier di Render non decide piu' per noi."""
    monkeypatch.setenv("OUTAGE_FLEET_TTL_S", "1800")
    monkeypatch.setenv("SNAPSHOT_MAX_AGE_S", "3600")
    import importlib
    from app import gtt_full_sync as mod
    importlib.reload(mod)
    try:
        assert mod.OUTAGE_FLEET_TTL_S == 1800.0
        assert mod.SNAPSHOT_MAX_AGE_S == 3600.0
    finally:
        monkeypatch.delenv("OUTAGE_FLEET_TTL_S")
        monkeypatch.delenv("SNAPSHOT_MAX_AGE_S")
        importlib.reload(mod)
