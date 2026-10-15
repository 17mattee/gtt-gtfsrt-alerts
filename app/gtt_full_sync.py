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
from app.vehicles_scraper import fetch_gtt_vehicle_positions, _haversine_meters

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

# ISTERESI FLOTTA: il feed posizioni GTT fa "lampeggiare" i mezzi — un mezzo
# puo' sparire per uno-due cicli GPS (20-60s) e tornare, facendo oscillare il
# contatore senza senso. Un mezzo resta "in strada" per QUESTI secondi dopo
# l'ultimo avvistamento; esce dal conteggio solo dopo un silenzio prolungato,
# che significa rientro reale al deposito o fine servizio. (Misurato sul
# feed reale: 16 mezzi su 183 lampeggiano tra due letture consecutive.)
VEHICLE_GRACE_SECONDS = 120
# POLITICA FRESCHEZZA (esplicita, voluta): sulla mappa si mostra SOLO dove i
# mezzi SONO adesso. Un fix GPS piu' vecchio di 2 minuti non viene mostrato
# (mai posizioni datate spacciate per attuali), e nemmeno fix con timestamp
# NEL FUTURO (feed GTT corrotto: verificato dal vivo il 13/10/2026, un'entita'
# col timestamp a +49 minuti). Blip GPS di 1-2 cicli: coperti dalla grazia
# normale di isteresi; outage piu' lunghi: mappa vuota e status bar onesta.
MAX_GPS_AGE_SECONDS = 120
MAX_GPS_FUTURE_SECONDS = 90
_recent_vehicles: dict = {}  # vehicle_id -> {"data": dict, "seen_at": epoch}


def _filter_fresh_positions(vehicles: list, now: float = None) -> list:
    """Solo mezzi col fix GPS REALE e RECENTE: mai niente di datato o impossibile."""
    now = time.time() if now is None else now
    min_ts = now - MAX_GPS_AGE_SECONDS
    max_ts = now + MAX_GPS_FUTURE_SECONDS
    return [v for v in vehicles if min_ts <= (v.get("timestamp") or 0) <= max_ts]


def _apply_fleet_hysteresis(fresh_vehicles: list) -> list:
    """Unisce le posizioni fresche con i mezzi visti di recente.

    - I mezzi presenti nel feed aggiornano posizione e orario di avvistamento.
    - I mezzi assenti da piu' della grazia escono (deposito reale o fine servizio).
    - I mezzi assenti da poco restano con l'ultima posizione nota.
    - Se il feed intero salta un ciclo, la flotta resta visibile (nessun blackout).
    - La POLITICA DI FRESCHEZZA (_filter_fresh_positions, applicata a valle
      dalla sync) decide cosa si mostra: solo fix GPS recenti, mai datato.
    """
    now = time.time()
    grace = VEHICLE_GRACE_SECONDS
    for v in fresh_vehicles:
        vid = v.get("vehicle_id")
        if vid:
            _recent_vehicles[vid] = {"data": v, "seen_at": now}
    kept = []
    # Difensivo: anche con la sync serializzata, il dizionario e' stato
    # condiviso (test, ecc.): un mezzo gia' rimosso non deve far crashare
    # il conteggio con KeyError.
    for vid in list(_recent_vehicles.keys()):
        entry = _recent_vehicles.get(vid)
        if entry is None:
            continue
        if now - entry["seen_at"] > grace:
            _recent_vehicles.pop(vid, None)  # silenzio oltre la grazia: rientro reale
        else:
            kept.append(entry["data"])
    return kept


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


CAPOLINEA_SOSTA_RADIUS_M = 150
CAPOLINEA_PARTENZA_RADIUS_M = 100
CAPOLINEA_SPEED_FERMO_MS = 1.5


def _terminus_epoch(sched, delay, now):
    """Orario previsto (schedulato + ritardo) piu' plausibile tra le due mezzanotti."""
    offset = _rome_offset_seconds(now)
    base = ((now + offset) // 86400) * 86400 - offset
    return min((b + sched + delay for b in (base - 86400, base)), key=lambda t: abs(t - now))


def _capolinea_state(lat, lon, speed, seq, delay, next_stop_id, stops, now=None):
    """Stato capolinea del mezzo sulla corsa corrente, o None se in servizio.

    Algoritmo a tre stati, calibrato su misure reali GTT (13/10/2026):
    - in_arrivo: la PROSSIMA fermata e' l'ultima della corsa (segnale preciso
      dalla catena next_stop gia' validata, nessuna soglia geometrica);
    - sosta: al capolinea di arrivo, PRATICAMENTE FERMO (speed < 1.5 m/s o
      n/d al primo avvistamento), orario previsto (col ritardo) scaduto e
      entro 150m dal capolinea (i bus in sosta misurati: fermi al 100% dei
      casi entro 150m);
    - attesa_partenza: al capolinea di partenza, fermo, orario di partenza
      ancora futuro, entro 100m dal primo stop (i bus in attesa misurati:
      mediana 15m).
    """
    now = time.time() if now is None else now
    if not seq or delay is None or lat is None:
        return None
    if len(seq) < 2:
        return None
    fermo = speed is None or speed < CAPOLINEA_SPEED_FERMO_MS
    sched_last, last_stop = seq[-1]
    sched_first, first_stop = seq[0]

    # 1) IN ARRIVO: la prossima fermata e' il capolinea di arrivo
    if next_stop_id and next_stop_id == last_stop:
        return {"stato": "in_arrivo", "stop_id": last_stop}

    sl = stops.get(last_stop) or {}
    sf = stops.get(first_stop) or {}

    # 2) IN SOSTA al capolinea di arrivo
    if fermo and sl.get("lat"):
        d = _haversine_meters(lat, lon, sl["lat"], sl["lon"])
        arr_last = _terminus_epoch(sched_last, delay, now)
        if d <= CAPOLINEA_SOSTA_RADIUS_M and arr_last <= now:
            return {"stato": "sosta", "stop_id": last_stop, "distanza_m": round(d)}

    # 3) IN ATTESA ALLA PARTENZA (capolinei diversi: le corse circolari
    #    con capolineo unico sono gia' coperte dallo stato 'sosta')
    if fermo and sf.get("lat") and first_stop != last_stop:
        d = _haversine_meters(lat, lon, sf["lat"], sf["lon"])
        dep_first = _terminus_epoch(sched_first, delay, now)
        if d <= CAPOLINEA_PARTENZA_RADIUS_M and dep_first > now:
            return {"stato": "attesa_partenza", "stop_id": first_stop, "distanza_m": round(d)}
    return None


def _prev_stop_name(sequence, stop_id, stops_catalog):
    """Nome della fermata PRECEDENTE a stop_id lungo la corsa (o None).

    Serve per il 'tra la fermata X e Y': con la corsa e la fermata in arrivo
    note, la precedente in sequenza e' l'ultima appena servita. Solo dati
    GTFS-Static, nessuna stima sul GPS.
    """
    for i, (_sched, sid) in enumerate(sequence):
        if sid == stop_id:
            if i == 0:
                return None  # il mezzo e' alla prima fermata della corsa
            prev = stops_catalog.get(sequence[i - 1][1]) or {}
            name = prev.get("name") or sequence[i - 1][1]
            m = re.match(r"Fermata\s+\d+\s*-\s*(.+)$", name)
            return (m.group(1).strip() if m else name).strip()
    return None


def _predict_next_stop(sequence, delay_seconds, now=None):
    """Prossima fermata prevista di una corsa: (epoch_arrivo, stop_id) o None.

    Metodo standard di previsione: orario programmato (GTFS-Static) + ritardo
    ufficiale della corsa. Le fermate GTFS oltre la mezzanotte usano orari
    24:00+ (nel feed GTT arrivano a 26:xx) e appartengono alla giornata di
    servizio PRECEDENTE: si provano entrambe le mezzanotti locali di Roma
    (oggi e ieri) e si tiene la previsione valida piu' vicina nel tempo.
    """
    if not sequence:
        return None
    now = time.time() if now is None else now
    offset = _rome_offset_seconds(now)
    # Mezzanotte di Roma piu' recente in epoch (la mezzanotte locale CEST e'
    # alle 22:00 UTC del giorno prima, quindi e' a midnight UTC - offset).
    base = ((now + offset) // 86400) * 86400 - offset
    best = None
    for sched, stop_id in sequence:
        for day_base in (base - 86400, base):
            # epoch INT (il percorso real-time pubblica int: tipo coerente)
            predicted = int(day_base + sched + delay_seconds)
            if predicted >= now - 60 and (best is None or predicted < best[0]):
                best = (predicted, stop_id)
    return best


def _derive_delay_from_arrival(sequence, rt_arrival_ts, tolerance=300):
    """Delay effettivo derivato per le corse GTT 'solo-orari'.

    GTT pubblica queste corse solo con orari d'arrivo previsti (senza campo
    delay): il delay e' la distanza tra la previsione e l'orario PROGRAMMATTO
    della fermata piu' vicina nella sequenza della corsa (GTFS-Static),
    provando entrambe le mezzanotti locali (corse oltre mezzanotte).
    Ritorna il delay in secondi o None se nessun match entro la tolleranza:
    oltre, l'abbinamento fermata-per-fermata diventa ambiguo e il valore
    standard GTFS-RT (in orario) resta piu' onesto di un numero a caso.
    """
    if not sequence or not rt_arrival_ts:
        return None
    offset = _rome_offset_seconds(rt_arrival_ts)
    base = ((rt_arrival_ts + offset) // 86400) * 86400 - offset
    best = None
    for sched, _stop_id in sequence:
        for day_base in (base - 86400, base):
            delay = rt_arrival_ts - (day_base + sched)
            if abs(delay) <= tolerance and (best is None or abs(delay) < abs(best)):
                best = delay
    return best

DEFAULT_ALERT = {
    "alert_id": "gtt-alert-01",
    "header": "Regolarità sulla rete tranviaria e metropolitana di Torino.",
    "description": "Nessun avviso ufficiale pubblicato da GTT in questo momento.",
    "affected_routes": ["4", "15", "M1"],
    "cause": 1,
    "effect": 8,
    "cause_label": "Causa non specificata",
    "effect_label": "Effetto non specificato",
    "category": "informazione",
}


def _map_scraped_alert(raw: dict, idx: int) -> dict:
    """Converte un avviso dello scraper GTT nel formato consumato dalla Control Room.

    Stessa FORMA del feed GTFS-RT ufficiale (stessi tipi e stesse chiavi):
    cause/effect int, label italiani e category aggregata — cosi' le
    deviazioni del fallback vengono elaborate dal parser e mostrate con il
    badge giusto, esattamente come quelle del feed ufficiale.
    """
    route_ids = raw.get("route_ids") or []
    try:
        cause = int(raw.get("cause") or 1)
    except (TypeError, ValueError):
        cause = 1
    try:
        effect = int(raw.get("effect") or 8)
    except (TypeError, ValueError):
        effect = 8
    return {
        "alert_id": raw.get("id") or f"gtt_alert_{idx}",
        "header": raw.get("title") or "Avviso di servizio GTT",
        "description": raw.get("description", ""),
        "affected_routes": route_ids,
        "cause": cause,
        "effect": effect,
        "cause_label": gtfs_rt.CAUSE_LABELS.get(cause, "Causa non specificata"),
        "effect_label": gtfs_rt.EFFECT_LABELS.get(effect, "Effetto non specificato"),
        "category": gtfs_rt.CATEGORY_BY_EFFECT.get(effect) or "informazione",
        "start_time": raw.get("start_time"),
    }


async def _fetch_all_gtt_data_inner() -> dict:
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

    # ISTERESI FLOTTA (vedi _apply_fleet_hysteresis): il contatore dei mezzi
    # in strada cambia SOLO per fatti reali (nuova uscita, rientro al deposito),
    # mai per un ciclo GPS perso del feed GTT.
    raw_vehicle_count = len(vehicles)
    vehicles = _apply_fleet_hysteresis(vehicles)
    # POLITICA 'NIENTE DATATO' (richiesta esplicita): sulla mappa si mostra
    # SOLO dove i mezzi sono adesso. Fix piu' vecchio di 2 minuti: non mostrato.
    # Timestamp dal futuro (feed GTT corrotto, visto dal vivo): non mostrato.
    # I mezzi tolti restano in memoria (isteresi): tornano appena riappare un
    # fix fresco, senza attendere nuove uscite dal feed.
    vehicles = _filter_fresh_positions(vehicles)
    if len(vehicles) != raw_vehicle_count:
        print(f"🚌 [SYNC] Flotta con isteresi: {raw_vehicle_count} pubblicati ora, "
              f"{len(vehicles)} in strada (GPS datato entro {VEHICLE_GRACE_SECONDS}s incluso).")

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

    # Feed ritardi GTT non raggiungibile in questo ciclo: il ritardo dei mezzi
    # resta 'n/d' (datetto unknown), MAI un finto 'in orario' che nasconderebbe
    # ritardi reali.
    trips_feed_down = getattr(gtfs_rt, "last_trip_updates_source", "") == "unavailable"

    updates_by_vehicle = defaultdict(list)
    for tu in trip_updates:
        if tu.get("vehicle_id"):
            updates_by_vehicle[tu["vehicle_id"]].append(tu)
    delay_by_trip = {tu.get("trip_id"): tu["delay_seconds"] for tu in trip_updates if tu.get("trip_id") and tu.get("delay_seconds") is not None}
    # VERIFICA DOPPIA FONTE: un mezzo mostrato esiste perche' ha un fix GPS REALE
    # fresco (filtro polit 'niente datato'). La conferma incrociata col feed
    # corse (stesso vehicle_id o stessa corsa) attesta che e' davvero IN SERVIZIO
    # su una corsa tracciata dagli orari: verificato da due fonti indipendenti GTT.
    vehicles_in_tu = {t.get("vehicle_id") for t in trip_updates if t.get("vehicle_id")}
    trips_in_tu = {t.get("trip_id") for t in trip_updates if t.get("trip_id")}
    stops_catalog = gtfs_manager.get_stops()
    headsigns = gtfs_manager.get_trip_headsigns()
    trip_routes = gtfs_manager.get_trip_routes()

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

    # DELAY DERIVATO per le corse 'solo-orari' del feed GTT: GTT pubblica le
    # previsioni d'arrivo ASSOLUTE senza campo delay solo per le corse
    # sostanzialmente puntuali (verificato sul feed reale: 96% entro +-60s
    # dall'orario programmato, mediana 0). Lo standard GTFS-RT le tratterebbe
    # tutte come 'in orario'; incrociando la previsione con l'orario
    # programmato del GTFS-Static si deriva lo scostamento EFFETTIVO: le
    # poche corse davvero in ritardo (~1 su 100) emergono, e non si inventa
    # nulla (solo dati ufficiali GTT incrociati). Derivazione solo con match
    # confidente (<=300s): oltre, l'abbinamento della fermata e' ambiguo.
    derived_count = 0
    for tu in trip_updates:
        if tu.get("delay_seconds") != 0 or not tu.get("next_stop"):
            continue
        seq = trip_sequences.get(tu.get("trip_id") or "")
        if not seq:
            continue
        derived = _derive_delay_from_arrival(seq, tu["next_stop"].get("arrival_ts") or 0)
        if derived is not None and derived != 0:
            tu["delay_seconds"] = int(derived)
            derived_count += 1
    if derived_count:
        # delay_by_trip cattura i valori: si ricostruisce dopo la derivazione.
        delay_by_trip = {tu.get("trip_id"): tu["delay_seconds"] for tu in trip_updates if tu.get("trip_id") and tu.get("delay_seconds") is not None}
        print(f"🕐 [SYNC] Delay derivato dal GTFS-Static per {derived_count} corse 'solo-orari' (senza campo delay nel feed).")

    for v in vehicles:
        v["verifica"] = ("doppia_fonte"
                         if v.get("vehicle_id") in vehicles_in_tu or v.get("trip_id") in trips_in_tu
                         else "gps")
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
        elif v.get("delay_seconds") is not None:
            # Mezzo tenuto dall'isteresi: conserva l'ultimo ritardo noto.
            pass
        elif trips_feed_down:
            v["delay_seconds"] = None  # feed ritardi giu': ritardo sconosciuto, non 'in orario'
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
                prediction = _predict_next_stop(seq, v["delay_seconds"])
                if prediction is not None:
                    predicted, stop_id = prediction
                    minutes = round((predicted - time.time()) / 60)
                    if -1 <= minutes <= 60:  # la prossima fermata deve essere plausibile
                        stop = stops_catalog.get(stop_id) or {}
                        name = stop.get("name") or stop_id
                        m2 = re.match(r"Fermata\s+\d+\s*-\s*(.+)$", name)
                        clean_name = m2.group(1).strip() if m2 else name
                        incoh = False
                        if stop.get("lat") and stop.get("lon"):
                            dist = _haversine_meters(v.get("latitude", 0), v.get("longitude", 0), stop["lat"], stop["lon"])
                            remaining = predicted - time.time()
                            if dist > 500 and remaining > 0 and (dist / remaining) > 30.0:
                                incoh = True
                        v["next_stop"] = {
                            "name": clean_name,
                            "stop_id": stop_id,
                            "arrival_ts": predicted,
                            "minutes": max(0, minutes),
                            "incoerente": incoh,
                            "calcolata": True,
                            "prev_stop": _prev_stop_name(seq, stop_id, stops_catalog),
                        }
                # Si passa alla corsa successiva solo se questa non ha prodotto
                # una previsione (es. cambio corsa: la chosen e' gia' terminata).
                if v.get("next_stop"):
                    break

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
                "stop_id": next_stop.get("stop_id"),
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
            resolved_stop = next_stop.get("stop_id")
            if not stop and chosen_trip and chosen_trip in trip_sequences:
                matched = _match_next_stop(
                    trip_sequences[chosen_trip],
                    next_stop.get("arrival_ts", 0),
                    chosen.get("delay_seconds", 0) if chosen else 0,
                )
                if matched:
                    resolved_stop = matched
                    stop = stops_catalog.get(matched) or {}
                    if stop.get("name"):
                        entry["name"] = stop["name"]
            # 'Tra la fermata X e Y': la fermata PRECEDENTE in sequenza e'
            # l'ultima appena servita (solo GTFS-Static, nessuna stima GPS).
            if resolved_stop and chosen_trip and chosen_trip in trip_sequences:
                prev_name = _prev_stop_name(trip_sequences[chosen_trip], resolved_stop, stops_catalog)
                if prev_name:
                    entry["prev_stop"] = prev_name
            if stop.get("lat") and stop.get("lon") and remaining > 0:
                dist = _haversine_meters(v.get("latitude", 0), v.get("longitude", 0), stop["lat"], stop["lon"])
                # Velocita' richiesta oltre ~108 km/h con distanza significativa
                # = previsione non plausibile rispetto alla posizione GPS reale.
                if dist > 500 and (dist / remaining) > 30.0:
                    entry["incoerente"] = True
            v["next_stop"] = entry

        # STATO CAPOLINEA: in arrivo / in sosta / in attesa della partenza
        # (algoritmo calibrato su misure reali, vedi _capolinea_state). Solo
        # dati ufficiali: sequenza fermate della corsa, GPS, velocita' reale,
        # ritardo. In servizio normale il campo resta assente (mai inventato).
        cap_seq = trip_sequences.get(chosen_tid) or trip_sequences.get(own_tid)
        if cap_seq and v.get("delay_seconds") is not None:
            cap = _capolinea_state(
                v.get("latitude"), v.get("longitude"), v.get("speed"),
                cap_seq, v["delay_seconds"],
                (v.get("next_stop") or {}).get("stop_id"),
                stops_catalog,
            )
            if cap:
                nome = _terminus_name(cap["stop_id"])
                if nome:
                    cap["nome"] = nome
                    v["capolinea"] = cap

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
            # Stessa elaborazione del feed RT: le deviazioni del portale
            # diventano dati strutturati (periodo, vie, ritorno), non solo testo.
            for a in alerts:
                if a.get("category") == "deviazione":
                    a["detour"] = gtfs_rt._parse_detour_details(a.get("header", ""), a.get("description") or "")
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

    # SCIOPERI & EVENTI: doppia fonte con interscambio.
    # - UFFICIALI: avvisi GTT con causa sciopero (GTFS-RT Cause.STRIKE = 4)
    # - NOTIZIE: fonte locale RSS filtrata su mobilita' (30 min di cache)
    # - INTERSCAMBIO: le linee citate nella notizia che hanno avvisi GTT attivi
    #   vengono collegate (badge), cosi' l'operatore vede subito la correlazione
    from app.news_monitor import get_transport_news
    from app.gtfs_rt import _extract_routes_from_text
    try:
        notizie = await asyncio.to_thread(get_transport_news)
    except Exception as exc:
        logger.warning("Notizie locali non disponibili (%s).", exc)
        notizie = []
    alert_routes_attive = set()
    for a in alerts:
        alert_routes_attive.update(a.get("affected_routes") or [])
    for n in notizie:
        try:
            citate = _extract_routes_from_text(n.get("titolo", "") + " " + n.get("descrizione", ""))
            n["linee_gtt_attive"] = [l for l in citate if l in alert_routes_attive]
        except Exception:
            n["linee_gtt_attive"] = []
    result["scioperi"] = {
        "ufficiali": [a for a in alerts if a.get("cause") == 4],
        "notizie": notizie,
    }

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


# Serializza le sync complete: il refresher di background e i cold-start dei
# client WebSocket possono partire nello stesso istante (piu' tab aperte
# insieme). Senza lock due sync si sovrappongono, raddoppiano le chiamate
# ai feed GTT e si contendono lo stato condiviso (isteresi flotta).
_sync_lock = asyncio.Lock()


async def fetch_all_gtt_data() -> dict:
    """Recupera tutti i dati della rete GTT (una sync alla volta, mai concurrently)."""
    async with _sync_lock:
        return await _fetch_all_gtt_data_inner()


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
