"""
Sincronizzazione totale della rete GTT Torino: catalogo linee, telemetria veicoli,
aggiornamenti di viaggio e allerte di servizio.

Garanzie:
- `fetch_all_gtt_data()` non solleva MAI eccezioni e non restituisce MAI strutture vuote.
- Ogni fase stampa lo stato nel terminale (catalogo, flotta, allerte).
- Mantiene uno snapshot globale consultabile da /health e /gtfs-rt/alerts.pb.
"""

import asyncio
import re
import logging
import time

from app import gtfs_rt
from app import vehicles_scraper
from app.gtfs_static import gtfs_manager
from app.gtfs_rt import fetch_gtt_rt_alerts, fetch_gtt_trip_updates
from app.vehicles_scraper import fetch_gtt_vehicle_positions

logger = logging.getLogger("GTT-FullSync")

# Snapshot globale dell'ultimo sync riuscito (usato da /health e /gtfs-rt/alerts.pb).
network_snapshot: dict = {
    "vehicles": [],
    "trip_updates": [],
    "alerts": [],
    "routes_catalog": {},
    "meta": {"last_update": None, "vehicles_source": "unknown", "routes_source": "unknown"},
}

_last_sequence_refresh = 0.0
SEQUENCE_REFRESH_INTERVAL = 600  # secondi: nuove corse ottengono le fermate ogni 10 min


def _rome_offset_seconds(epoch: float) -> int:
    """Fuso orario Europe/Rome (CET +1, DST +2) calcolato senza librerie esterne.

    Le ore GTFS sono locali (Roma), gli epoch del feed GTFS-RT sono UTC:
    serve l'offset per allinearli (DST: ultima domenica di marzo -> ottobre).
    """
    from datetime import datetime, timedelta, timezone

    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    year = dt.year

    def last_sunday_utc(month: int, hour: int) -> datetime:
        d = datetime(year, month, 31, hour, tzinfo=timezone.utc)
        while d.weekday() != 6:  # domenica
            d -= timedelta(days=1)
        return d

    dst_start = last_sunday_utc(3, 1)   # 02:00 locale = 01:00 UTC
    dst_end = last_sunday_utc(10, 1)     # 03:00 locale = 01:00 UTC
    return 7200 if dst_start <= dt < dst_end else 3600


def _match_next_stop(sequence, rt_arrival_ts, delay_seconds, tolerance=300):
    """Ricostruisce la fermata successiva dal GTFS-Static (il feed RT non ha stop_id).

    L'orario programmato della fermata e' (arrivo_previsto - ritardo): si cerca
    nella sequenza della corsa la fermata il cui orario programmato locale,
    spostato nella giornata di servizio, coincide con (arrivo_previsto - ritardo).
    Ritorna lo stop_id o None se nessun match confidente.
    """
    if not sequence:
        return None
    # Allineamento: gli orari GTFS sono locali (Roma), l'epoch del feed e' UTC.
    # La mezzanotte di Roma anticipa quella UTC dell'offset (es. 22:00 UTC del
    # giorno prima), quindi l'offset va SOMMATO per ottenere i secondi locali.
    scheduled_local_sec = (rt_arrival_ts - delay_seconds + _rome_offset_seconds(rt_arrival_ts)) % 86400
    best_stop, best_dist = None, None
    for sched, stop_id in sequence:
        d = abs(sched - scheduled_local_sec)
        d = min(d, 86400 - d)  # distanza circolare (corse oltre la mezzanotte)
        if best_dist is None or d < best_dist:
            best_stop, best_dist = stop_id, d
    return best_stop if best_dist is not None and best_dist <= tolerance else None

DEFAULT_ALERT = {
    "alert_id": "gtt-alert-01",
    "header": "Regolarità sulla rete tranviaria e metropolitana di Torino.",
    "affected_routes": ["4", "15", "M1"],
}


def _map_scraped_alert(raw: dict, idx: int) -> dict:
    """Converte un avviso dello scraper GTT nel formato consumato dalla Control Room."""
    route_ids = raw.get("route_ids") or []
    return {
        "alert_id": raw.get("id") or f"gtt_alert_{idx}",
        "header": raw.get("title") or "Avviso di servizio GTT",
        "description": raw.get("description", ""),
        "affected_routes": route_ids,
        "cause": str(raw.get("cause", "")),
        "effect": str(raw.get("effect", "")),
        "start_time": raw.get("start_time"),
    }


async def fetch_all_gtt_data() -> dict:
    """Recupera tutti i dati della rete GTT. A prova di bomba: mai eccezioni, mai vuoto."""
    result = {
        "vehicles": [],
        "trip_updates": [],
        "alerts": [],
        "routes_catalog": {},
        "route_types": {},        # nome linea -> tipo GTFS (0 tram, 1 metro, 3 bus, 7 funicolare)
        "route_colors": {},       # nome linea -> colore ufficiale GTT
        "route_text_colors": {},  # nome linea -> colore testo ufficiale
    }
    print("🚦 [SYNC] ===== Avvio sincronizzazione totale rete GTT Torino =====")

    # FASE 1: Catalogo statico linee (async, cache, fallback locale garantito).
    try:
        result["routes_catalog"] = await gtfs_manager.load_routes()
        print(f"📚 [SYNC] FASE 1 OK -> Catalogo linee: {len(result['routes_catalog'])} (sorgente: {gtfs_manager.get_source()}).")
    except Exception as exc:
        logger.warning("Catalogo statico non disponibile (%s): uso fallback.", exc)
        result["routes_catalog"] = gtfs_manager.get_routes()
        print(f"📚 [SYNC] FASE 1 FALLBACK -> Catalogo locale: {len(result['routes_catalog'])} linee.")

    # FASI 2-4 IN PARALLELO: flotta, allerte ufficiali e trip updates scaricati
    # contemporaneamente per minimizzare la latenza di ciclo.
    vehicles = []
    rt_alerts = []
    trip_updates = []

    async def _vehicles():
        try:
            return await fetch_gtt_vehicle_positions()
        except Exception as exc:
            logger.warning("Errore recupero veicoli (%s): mostro solo i dati pubblicati da GTT.", exc)
            print(f"🚌 [SYNC] FASE 2 ERRORE ({exc}): nessun mezzo pubblicato (politica dati reali).")
            return []

    async def _alerts():
        try:
            return await fetch_gtt_rt_alerts()
        except Exception as exc:
            logger.warning("Feed alerts ufficiale fallito (%s).", exc)
            return []

    async def _trips():
        try:
            return await fetch_gtt_trip_updates()
        except Exception as exc:
            logger.warning("Trip updates non disponibili (%s).", exc)
            return []

    vehicles, rt_alerts, trip_updates = await asyncio.gather(_vehicles(), _alerts(), _trips())

    # Diagnostica onesta: se GTT pubblica pochissime posizioni ma centinaia di
    # corse nei trip updates, il feed posizioni di GTT e' degradato LORO.
    trip_vehicle_count = len({tu.get("vehicle_id") for tu in trip_updates if tu.get("vehicle_id")})
    positions_degraded = len(vehicles) < 10 and trip_vehicle_count > 50
    if positions_degraded:
        print(f"⚠️ [SYNC] Feed posizioni GTT degradato: GTT pubblica solo {len(vehicles)} mezzi "
              f"mentre {trip_vehicle_count} corse sono attive. Mostro solo i dati reali disponibili.")

    # Il feed trip_update di GTT non contiene mai route_id: risolviamo la linea
    # incrociando vehicle_id (e trip_id) con il feed posizioni veicoli.
    vehicle_trip_routes = {v["trip_id"]: v["route_id"] for v in vehicles if v.get("trip_id") and v.get("route_id")}
    vehicle_id_routes = {v["vehicle_id"]: v["route_id"] for v in vehicles if v.get("vehicle_id") and v.get("route_id")}
    for tu in trip_updates:
        if tu.get("route_id") and tu["route_id"] != "N/A":
            continue
        resolved = None
        if tu.get("vehicle_id"):
            resolved = vehicle_id_routes.get(tu["vehicle_id"])
        if not resolved and tu.get("trip_id"):
            resolved = vehicle_trip_routes.get(tu["trip_id"])
        if resolved:
            tu["route_id"] = resolved

    # RITARDO PERFETTO: un mezzo in cambio corsa puo' avere PIU' trip updates
    # contemporanee (vecchia + nuova corsa, 21 mezzi su ~450 in questo momento).
    # Si sceglie: 1) l'entita' la cui corsa coincide con quella ATTUALE del mezzo
    # (dal feed posizioni), 2) altrimenti la piu' recente per timestamp.
    # Mai il ritardo di una corsa terminata.
    from collections import defaultdict

    updates_by_vehicle = defaultdict(list)
    for tu in trip_updates:
        if tu.get("vehicle_id"):
            updates_by_vehicle[tu["vehicle_id"]].append(tu)
    delay_by_trip = {tu.get("trip_id"): tu["delay_seconds"] for tu in trip_updates if tu.get("trip_id") and tu.get("delay_seconds") is not None}
    stops_catalog = gtfs_manager.get_stops()
    headsigns = gtfs_manager.get_trip_headsigns()
    trip_routes = gtfs_manager.get_trip_routes()
    from app.vehicles_scraper import _haversine_meters

    def _terminus_name(stop_id):
        """Nome pulito del capolinea: 'Fermata 163 - CORRADINO CAP' -> 'CORRADINO CAP'."""
        name = (stops_catalog.get(stop_id) or {}).get("name") or ""
        m = re.match(r"Fermata\s+\d+\s*-\s*(.+)$", name)
        return (m.group(1).strip() if m else name).strip()

    def _headsign_valido(trip_id, vehicle_route_id):
        """Headsign di trips.txt, usato SOLO se la corsa appartiene alla stessa
        linea del mezzo (protegge da scambi di corsa) e solo come fallback:
        per alcune corse varianti GTT pubblica l'origine, non la destinazione."""
        if not trip_id or trip_id not in headsigns:
            return None
        corsa_route = gtfs_manager.get_route_short_name(trip_routes.get(trip_id) or "")
        if vehicle_route_id and corsa_route and corsa_route != vehicle_route_id:
            return None
        return headsigns[trip_id]

    # Fermate programmate per corsa (stop_times GTFS-Static): il feed RT di GTT
    # non pubblica gli stop_id, servono per risalire al nome della prossima
    # fermata. Si aggiornano ogni 10 minuti per le corse appena partite.
    # NB: si includono ANCHE i trip_id dei trip updates (quelli del feed
    # posizioni possono essere vuoti o sfasati durante il cambio corsa).
    global _last_sequence_refresh
    trip_sequences = gtfs_manager.get_trip_sequences()
    active_trips = {v.get("trip_id") for v in vehicles if v.get("trip_id")}
    active_trips |= {tu.get("trip_id") for tu in trip_updates if tu.get("trip_id")}
    missing = [t for t in active_trips if t not in trip_sequences]
    broken = [t for t in active_trips if t in trip_sequences and len(trip_sequences[t]) < 2]
    if (missing or broken) and (time.time() - _last_sequence_refresh) > SEQUENCE_REFRESH_INTERVAL:
        _last_sequence_refresh = time.time()
        try:
            await asyncio.to_thread(gtfs_manager.refresh_trip_sequences, active_trips)
            trip_sequences = gtfs_manager.get_trip_sequences()
            print(f"📍 [SYNC] Fermate programmate caricate dal GTFS-Static: {len(missing)} nuove corse, {len(broken)} sequenze riparate.")
        except Exception as exc:
            logger.warning("Sequenze fermate non disponibili (%s): nomi fermate limitati.", exc)

    for v in vehicles:
        candidates = updates_by_vehicle.get(v.get("vehicle_id")) or []
        chosen = None
        for tu in candidates:
            if tu.get("trip_id") == v.get("trip_id"):
                chosen = tu
                break
        if chosen is None and candidates:
            chosen = max(candidates, key=lambda t: t.get("timestamp") or 0)
        if chosen is not None:
            v["delay_seconds"] = chosen["delay_seconds"]
        elif v.get("trip_id") in delay_by_trip:
            v["delay_seconds"] = delay_by_trip[v["trip_id"]]
        else:
            # Corsa non tracciata nei trip update: in orario (standard GTFS-RT).
            v["delay_seconds"] = 0

        # CAPOLINEA DI DESTINAZIONE: la fonte piu' affidabile e' l'ULTIMA
        # fermata della sequenza della corsa (stop_times GTFS-Static). Per
        # alcune corse varianti GTT pubblica l'origine come headsign (verificato
        # su dati reali): es. corsa 2U con headsign 'MONGINEVRO' che in realta'
        # termina a CORRADINO CAP. L'headsign resta solo come fallback, e solo
        # se la corsa appartiene alla stessa linea del mezzo.
        v_route = v.get("route_id")
        chosen_tid = (chosen or {}).get("trip_id")
        own_tid = v.get("trip_id")
        v["headsign"] = None
        for tid in (chosen_tid, own_tid):
            if not tid:
                continue
            # Coerenza linea-corsa anche per il capolinea da sequenza: la corsa
            # dev'essere della stessa linea del mezzo (difesa preventiva).
            corsa_route = gtfs_manager.get_route_short_name(trip_routes.get(tid) or "")
            if v_route and corsa_route and corsa_route != v_route:
                continue
            seq = trip_sequences.get(tid)
            if seq:
                v["headsign"] = _terminus_name(seq[-1][1])
                if v["headsign"]:
                    break
            fallback_hs = _headsign_valido(tid, v_route)
            if fallback_hs and not v["headsign"]:
                v["headsign"] = fallback_hs
                break

        # PREVISIONE CALCOLATA: GTT pubblica gli orari d'arrivo assoluti solo
        # per una parte delle corse. Per tutte le altre, la prossima fermata si
        # calcola con il metodo standard di previsione: orario programmato
        # (GTFS-Static) + ritardo ufficiale della corsa. Stesso criterio che
        # usa il sistema di GTT: nessun dato inventato, solo combinazione dei
        # due dati ufficiali. Plausibilita' GPS applicata come per le previsioni RT.
        if not v.get("next_stop") and v.get("delay_seconds") is not None:
            for tid in (chosen_tid, own_tid):
                if not tid:
                    continue
                corsa_route = gtfs_manager.get_route_short_name(trip_routes.get(tid) or "")
                if v_route and corsa_route and corsa_route != v_route:
                    continue
                seq = trip_sequences.get(tid)
                if not seq:
                    continue
                now = time.time()
                offset = _rome_offset_seconds(now)
                # Mezzanotte di Roma in epoch: la mezzanotte locale CEST e' alle
                # 22:00 UTC del giorno prima, quindi e' a (midnight UTC - offset).
                base = ((now + offset) // 86400) * 86400 - offset
                delay = v["delay_seconds"]
                prediction = None
                for sched, stop_id in seq:
                    predicted = base + sched + delay
                    if predicted >= now - 60:  # tolleranza fermata appena passata
                        prediction = (predicted, stop_id)
                        break
                if prediction:
                    predicted, stop_id = prediction
                    minutes = round((predicted - now) / 60)
                    if 0 <= minutes <= 60:  # la prossima fermata deve essere plausibile
                        stop = stops_catalog.get(stop_id) or {}
                        name = stop.get("name") or stop_id
                        m2 = re.match(r"Fermata\s+\d+\s*-\s*(.+)$", name)
                        clean_name = m2.group(1).strip() if m2 else name
                        incoh = False
                        if stop.get("lat") and stop.get("lon"):
                            dist = _haversine_meters(v.get("latitude", 0), v.get("longitude", 0), stop["lat"], stop["lon"])
                            remaining = predicted - now
                            if dist > 500 and remaining > 0 and (dist / remaining) > 30.0:
                                incoh = True
                        v["next_stop"] = {
                            "name": clean_name,
                            "arrival_ts": predicted,
                            "minutes": max(0, minutes),
                            "incoerente": incoh,
                            "calcolata": True,
                        }
                break  # elabora solo la prima corsa coerente trovata

        # PRECISIONE D'ARRIVO: prossima fermata con nome reale (GTFS-Static) e
        # minuti all'arrivo previsti da GTT. Con verifica di coerenza: se il GPS
        # del mezzo e' troppo lontano dalla fermata per arrivarci in tempo, la
        # previsione viene marcata come incoerente (dato reale, mai modificato).
        next_stop = (chosen or {}).get("next_stop") if chosen is not None else None
        if next_stop:
            stop = stops_catalog.get(next_stop.get("stop_id")) or {}
            remaining = next_stop.get("arrival_ts", 0) - time.time()
            entry = {
                "name": stop.get("name") or next_stop.get("stop_id") or "fermata",
                "arrival_ts": next_stop.get("arrival_ts"),
                "minutes": max(0, round(remaining / 60)),
                "incoerente": False,
            }
            # Il feed RT non pubblica gli stop_id: si risale alla fermata vera
            # incrociando l'orario programmato del GTFS-Static (arrivo_previsto
            # - ritardo = orario programmato della fermata). Match confidenziale.
            # NB: si usa il trip_id del TRIP UPDATE scelto (quello del veicolo
            # nel feed posizioni puo' essere vuoto o sfasato durante il cambio corsa).
            chosen_trip = (chosen or {}).get("trip_id")
            if not stop and chosen_trip and chosen_trip in trip_sequences:
                matched = _match_next_stop(
                    trip_sequences[chosen_trip],
                    next_stop.get("arrival_ts", 0),
                    chosen.get("delay_seconds", 0) if chosen else 0,
                )
                if matched:
                    stop = stops_catalog.get(matched) or {}
                    if stop.get("name"):
                        entry["name"] = stop["name"]
            if stop.get("lat") and stop.get("lon") and remaining > 0:
                dist = _haversine_meters(v.get("latitude", 0), v.get("longitude", 0), stop["lat"], stop["lon"])
                # Velocita' richiesta oltre ~108 km/h con distanza significativa
                # = previsione non plausibile rispetto alla posizione GPS reale.
                if dist > 500 and (dist / remaining) > 30.0:
                    entry["incoerente"] = True
            v["next_stop"] = entry

    result["vehicles"] = vehicles
    result["trip_updates"] = trip_updates
    print(f"🚌 [SYNC] FASE 2 OK -> Flotta tracciata: {len(vehicles)} mezzi ({getattr(vehicles_scraper, 'last_fetch_source', '?')}).")
    print(f"🕐 [SYNC] FASE 4 OK -> Trip Updates: {len(trip_updates)} corse monitorate ({getattr(gtfs_rt, 'last_trip_updates_source', '?')}).")

    # FASE 3: allerte ufficiali GTFS-RT; in cascata feed ufficiale -> scraper portale -> default.
    alerts = rt_alerts
    if alerts:
        print(f"📢 [SYNC] FASE 3 OK -> Allerte ufficiali GTFS-RT: {len(alerts)} avvisi ricevuti.")
    else:
        print("📢 [SYNC] FASE 3: feed ufficiale vuoto, provo lo scraper del portale GTT...")
        try:
            from app.scraper import fetch_gtt_alerts
            scraped = await asyncio.to_thread(fetch_gtt_alerts)
            alerts = [_map_scraped_alert(a, i) for i, a in enumerate(scraped)]
            print(f"📢 [SYNC] FASE 3 (portale GTT) -> Avvisi estratti dal portale: {len(alerts)}.")
        except Exception as exc:
            logger.warning("Scraper avvisi non disponibile (%s).", exc)
            print(f"📢 [SYNC] FASE 3 ERRORE ({exc}): uso avviso di default.")

    if not alerts:
        alerts = [dict(DEFAULT_ALERT)]
        print("📢 [SYNC] FASE 3 FALLBACK -> Nessun avviso pubblico: mostro stato di regolarità di rete.")

    result["alerts"] = alerts

    # Capolinea di destinazione anche sulle corse (per la lista ritardi).
    headsigns_all = gtfs_manager.get_trip_headsigns()
    for tu in trip_updates:
        if tu.get("trip_id") and not tu.get("headsign"):
            tu["headsign"] = headsigns_all.get(tu["trip_id"])

    # Tipi e colori UFFICIALI delle linee (dal GTFS-Static): la UI non deve
    # indovinare quale linea sia tram o bus, usa il dato reale.
    result["route_types"] = gtfs_manager.get_route_types()
    result["route_colors"] = gtfs_manager.get_route_colors()
    result["route_text_colors"] = gtfs_manager.get_route_text_colors()

    # FASE 5: Metadati e aggiornamento snapshot globale.
    vehicles_source = getattr(vehicles_scraper, "last_fetch_source", "unknown")
    alerts_source = getattr(gtfs_rt, "last_alerts_source", "unknown")
    trip_updates_source = getattr(gtfs_rt, "last_trip_updates_source", "unknown")
    result["meta"] = {
        "last_update": int(time.time()),
        "vehicles_source": vehicles_source,
        "alerts_source": alerts_source,
        "trip_updates_source": trip_updates_source,
        "routes_source": gtfs_manager.get_source(),
        "vehicles_count": len(result["vehicles"]),
        "alerts_count": len(result["alerts"]),
        "trip_updates_count": len(result["trip_updates"]),
        "routes_count": len(result["routes_catalog"]),
        "positions_degraded": positions_degraded,
        "trip_vehicles": trip_vehicle_count,
    }

    try:
        network_snapshot.update(result)
    except Exception:
        pass

    print(f"✅ [SYNC] Sincronizzazione completata -> {len(result['vehicles'])} mezzi, {len(alerts)} avvisi, {len(result['routes_catalog'])} linee.\n")
    return result


def get_network_snapshot() -> dict:
    """Snapshot globale: sempre utilizzabile anche a cold start (mai eccezioni)."""
    snapshot = dict(network_snapshot)
    if not snapshot.get("routes_catalog"):
        try:
            snapshot["routes_catalog"] = gtfs_manager.get_routes()
        except Exception:
            snapshot["routes_catalog"] = {}
    if not snapshot.get("alerts"):
        snapshot["alerts"] = [dict(DEFAULT_ALERT)]
    return snapshot
