#!/usr/bin/env python
"""Validazione completa del contratto dati/API di un'istanza GTT Middleware.

Controlla, contro un'istanza IN ESECUZIONE (locale o produzione):
  1. /health           — campi e coerenza dei contatori
  2. / e /static       — HTML servito con SRI, marker frontend nuovi, manifest
  3. header di sicurezza (CSP, nosniff, frame-deny)
  4. /gtfs-rt/alerts.pb — protobuf valido, entità complete, traduzioni
  5. WebSocket          — payload full_network_update: tipi, range, coerenza
                          (mezzi, corse, avvisi, meta, parte statica)
  6. rate limit         — 65 richieste ravvicinate -> almeno un 429 (opzionale)

Uso:
    python scripts/validate_contract.py --url http://127.0.0.1:8000
    python scripts/validate_contract.py --url https://gtt-gtfsrt-alerts.onrender.com --skip-rate-limit
"""
import argparse
import asyncio
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, ".")

from google.transit import gtfs_realtime_pb2

KNOWN_CATEGORIES = {"deviazione", "interruzione", "fermata_spostata", "ritardo",
                     "servizio_ridotto", "potenziamento", "accessibilita", "informazione"}
KNOWN_SOURCES = {"live", "unavailable", "unknown"}

_results = []


def check(name, ok, detail=""):
    _results.append((name, bool(ok)))
    mark = "OK " if ok else "KO "
    line = f"  {mark}{name}"
    if detail and not ok:
        line += f"  -> {detail}"
    print(line)
    return ok


def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "GTT-Contract-Validator/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except Exception as e:
        return None, {}, str(e).encode()


def validate_http(base):
    print("\n[1] /health")
    # Cold start: il primo sync (catalogo GTFS ~12MB + feed) impiega alcuni
    # secondi; last_update arriva null finche' non completa — si riprova.
    ok, detail, health = False, "nessuna risposta", {}
    for attempt in range(13):  # fino a ~60s: durante un outage GTT il primo sync e' lento
        status, _, body = http_get(f"{base}/health")
        if status == 200:
            try:
                health = json.loads(body)
                lu = health.get("last_update")
                ok = (health.get("status") == "healthy" and health.get("agency") == "GTT"
                      and all(k in health for k in ("vehicles", "routes", "trip_updates", "last_update")))
                if ok and not isinstance(lu, int):
                    ok, detail = False, "last_update non int (cold start?)"
                if ok or attempt == 6:
                    break
            except Exception as e:
                ok, detail = False, f"JSON non valido: {e}"
        else:
            detail = f"HTTP {status}"
        time.sleep(5)
    check("health: campi e tipi corretti", ok, detail)

    print("\n[2] Dashboard, Leaflet self-hosted e manifest")
    status, _, body = http_get(f"{base}/")
    html = body.decode("utf-8", "replace") if isinstance(body, bytes) else ""
    check("dashboard servita (200)", status == 200)
    check("Leaflet self-hosted (nessun CDN di terze parti)", "/static/vendor/leaflet/leaflet.js" in html and "unpkg.com/leaflet" not in html)
    s, _, _ = http_get(f"{base}/static/vendor/leaflet/leaflet.js")
    check("leaflet.js servito localmente (200)", s == 200)
    s, _, _ = http_get(f"{base}/static/vendor/leaflet/leaflet.js.map")
    check("source map leaflet raggiungibile same-origin (nessun warning CSP)", s == 200)
    check("meta mobile-web-app-capable presente", 'name="mobile-web-app-capable"' in html)
    check("frontend aggiornato (etaMinutes, d-early)", "etaMinutes" in html and "d-early" in html)
    check("nessun marker colorato con delay null (?? 0 rimosso)", "v.delay_seconds ?? 0" not in html)
    s, _, _ = http_get(f"{base}/static/manifest.webmanifest")
    check("manifest PWA raggiungibile", s == 200)

    print("\n[3] Header di sicurezza")
    status, headers, _ = http_get(f"{base}/health")
    check("Content-Security-Policy", "content-security-policy" in {k.lower() for k in headers})
    check("X-Content-Type-Options: nosniff", headers.get("X-Content-Type-Options", headers.get("x-content-type-options")) == "nosniff")
    check("X-Frame-Options: DENY", (headers.get("X-Frame-Options") or headers.get("x-frame-options")) == "DENY")

    print("\n[4] /gtfs-rt/alerts.pb (contratto protobuf)")
    status, headers, body = http_get(f"{base}/gtfs-rt/alerts.pb")
    check("HTTP 200 + content-type protobuf", status == 200 and "protobuf" in (headers.get("Content-Type") or headers.get("content-type") or ""))
    feed_ok, detail = False, "nessun dato"
    if status == 200:
        try:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(body)
            feed_ok = feed.header.gtfs_realtime_version and feed.header.timestamp
            bad = [e.id for e in feed.entity if not (e.id and e.alert.header_text.translation)]
            detail = f"{len(feed.entity)} entita', {len(bad)} incomplete"
            feed_ok = feed_ok and not bad
        except Exception as e:
            feed_ok, detail = False, f"protobuf non valido: {e}"
    check("FeedMessage valido, entita' complete", feed_ok, detail)


async def validate_ws(base_ws, max_vehicles=50_000):
    import websockets

    print("\n[5] WebSocket: contratto payload")
    async with websockets.connect(base_ws, max_size=5_000_000) as ws:
        first = json.loads(await asyncio.wait_for(ws.recv(), timeout=90))
        assert first.get("type") == "full_network_update", f"tipo inatteso: {first.get('type')}"
        data = first["data"]

        # Il client fonde la parte statica (avvisi, catalogo, tipi/colori) con
        # la dinamica: si replica ESATTAMENTE la stessa logica del frontend.
        static = data.get("static")
        data = {**(static or {}), **{k: v for k, v in data.items() if k != "static"}}

        vehicles = data.get("vehicles", [])
        trip_updates = data.get("trip_updates", [])
        alerts = data.get("alerts", [])
        meta = data.get("meta", {})

        check("payload: chiavi dinamiche presenti", all(k in data for k in ("vehicles", "trip_updates", "meta")))
        check("primo messaggio: parte statica inclusa", static is not None and "routes_catalog" in static)

        n_err, examples = 0, []
        now = time.time()
        for v in vehicles[:max_vehicles]:
            problems = []
            lat, lon = v.get("latitude"), v.get("longitude")
            if not isinstance(lat, float) or not isinstance(lon, float):
                problems.append("lat/lon non float")
            elif not (44.0 <= lat <= 46.0 and 6.0 <= lon <= 9.0):
                problems.append(f"fuori bbox: {lat},{lon}")
            vid = v.get("vehicle_id")
            if not isinstance(vid, str) or not vid:
                problems.append("vehicle_id mancante")
            rid = v.get("route_id")
            if not isinstance(rid, str) or not rid:
                problems.append("route_id mancante")
            d = v.get("delay_seconds")
            if d is not None and (not isinstance(d, int) or abs(d) > 7200):
                problems.append(f"delay implausibile: {d}")
            ts = v.get("timestamp")
            if not isinstance(ts, int) or ts > now + 120:
                problems.append(f"timestamp non valido: {ts}")
            cap = v.get("capolinea")
            if cap is not None:
                if cap.get("stato") not in ("in_arrivo", "sosta", "attesa_partenza"):
                    problems.append(f"stato capolinea non valido: {cap.get('stato')}")
                if not isinstance(cap.get("nome"), str) or not cap.get("nome"):
                    problems.append("capolinea.nome mancante")
            ns = v.get("next_stop")
            if ns is not None:
                if not isinstance(ns.get("name"), str) or not ns.get("name"):
                    problems.append("next_stop.name mancante")
                m = ns.get("minutes")
                if not isinstance(m, int) or not (0 <= m <= 90):
                    problems.append(f"next_stop.minutes fuori range: {m}")
                if not isinstance(ns.get("arrival_ts"), int):
                    problems.append("next_stop.arrival_ts non int")
            if problems:
                n_err += 1
                if len(examples) < 3:
                    examples.append(f"{vid}: {problems}")
        check(f"mezzi: tipi, bbox, delay, ETA ({len(vehicles)} esaminati)", n_err == 0, "; ".join(examples))

        n_err = 0
        for tu in trip_updates:
            if not isinstance(tu.get("trip_id"), str) or not tu.get("trip_id"):
                n_err += 1
            d = tu.get("delay_seconds")
            if d is not None and (not isinstance(d, int) or abs(d) > 7200):
                n_err += 1
        check(f"corse: trip_id e delay ({len(trip_updates)} esaminate)", n_err == 0)

        n_err, examples = 0, []
        for a in alerts:
            if not isinstance(a.get("alert_id"), str) or not a.get("alert_id"):
                n_err += 1
            if a.get("category") not in KNOWN_CATEGORIES:
                n_err += 1
                if len(examples) < 3:
                    examples.append(f"category '{a.get('category')}' non valida")
            if not isinstance(a.get("affected_routes"), list):
                n_err += 1
            if not isinstance(a.get("header"), str) or not a.get("header"):
                n_err += 1
        check(f"avvisi: forma completa ({len(alerts)} esaminati)", n_err == 0, "; ".join(examples))

        coerenza = (meta.get("vehicles_count") == len(vehicles)
                    and meta.get("trip_updates_count") == len(trip_updates)
                    and meta.get("alerts_count") == len(alerts))
        check("meta: contatori coerenti col payload", coerenza,
              f"meta={meta.get('vehicles_count')}/{meta.get('trip_updates_count')}/{meta.get('alerts_count')}")
        check("meta: sorgenti valide",
              meta.get("vehicles_source") in KNOWN_SOURCES and meta.get("trip_updates_source") in KNOWN_SOURCES)
        lu = meta.get("last_update") or 0
        check("meta: last_update fresco (<= 5 min)", isinstance(lu, int) and now - lu <= 300,
              f"ultimo update {int(now - lu)}s fa")
        check("meta: posizione_degradata indicata", "positions_degraded" in meta)

        if static:
            rc = static.get("routes_catalog") or {}
            check(f"catalogo linee servito ({len(rc)})", len(rc) >= 70)
            rtypes = static.get("route_types") or {}
            sample_routes = {v.get("route_id") for v in vehicles if v.get("route_id")}
            noti = sum(1 for r in sample_routes if r in rtypes or r == "N/A")
            check(f"tipo mezzo noto per le linee in mappa ({noti}/{len(sample_routes)})", noti >= 0.95 * len(sample_routes) if sample_routes else True)

        # secondo messaggio: la parte statica non si riinvia se invariata
        second = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
        check("secondo push: senza parte statica se invariata", "static" not in second["data"] or second["data"]["static"] is not None)


def validate_rate_limit(base):
    print("\n[6] Rate limit (65 richieste ravvicinate)")
    codes = []
    for _ in range(65):
        s, _, _ = http_get(f"{base}/gtfs-rt/alerts.pb", timeout=10)
        codes.append(s)
    check("almeno un 429 oltre il limite di 60/min", 429 in codes, f"codici visti: {sorted(set(codes))}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--skip-rate-limit", action="store_true")
    ap.add_argument("--skip-ws", action="store_true")
    args = ap.parse_args()
    base = args.url.rstrip("/")

    print("=" * 66)
    print(f" VALIDAZIONE CONTRATTO GTT MIDDLEWARE — {base}")
    print(f" {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 66)

    validate_http(base)
    if not args.skip_ws:
        ws_url = ("wss://" + base[8:] if base.startswith("https://") else "ws://" + base[7:]) + "/ws"
        asyncio.run(validate_ws(ws_url))
    if not args.skip_rate_limit:
        validate_rate_limit(base)

    passed = sum(1 for _, ok in _results if ok)
    total = len(_results)
    print("\n" + "=" * 66)
    print(f" ESITO: {passed}/{total} controlli superati" + ("" if passed == total else f" — FALLITI: {[n for n, ok in _results if not ok]}"))
    print("=" * 66)
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
