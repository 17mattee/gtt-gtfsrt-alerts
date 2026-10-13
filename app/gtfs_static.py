"""
Catalogo statico GTFS della rete GTT Torino.

Caratteristiche:
- Download asincrono (httpx) del pacchetto GTFS-Static ufficiale con cache in memoria (TTL 1 ora).
- Zero chiamate di rete sui percorsi sincroni: `get_gtfs_routes()` non blocca MAI l'event loop.
- Fallback garantito su oltre 60 linee reali della rete di Torino (Metropolitana, Tram, Bus).
- `gtfs_manager` e' un oggetto con `validate_route_id()`, usato dallo scraper degli avvisi.
"""

import asyncio
import csv
import io
import logging
import re
import time
import zipfile

import httpx

logger = logging.getLogger("GTT-Static")

GTFS_STATIC_URLS = [
    "https://www.gtt.to.it/open_data/gtt_gtfs.zip",
    "https://apidatadocs.avmspa.it/opendata/gtt_gtfs.zip",
]

CACHE_TTL_SECONDS = 3600

# Fallback di sicurezza: linee reali della rete GTT di Torino.
# Usato SOLO se i server GTT non rispondono. Mai vuoto, mai bloccante.
FALLBACK_ROUTES = {
    "M1": "Metropolitana - Fermi / Bengasi",
    "3": "Tram - Corso Tortona / Vallette",
    "4": "Tram - Strada Cattane / Drosso",
    "6": "Bus - Piazza Hermada / Piazza Carlo Felice",
    "7": "Tram Storici - Circolare Piazza Castello / Porta Nuova",
    "9": "Tram - Piazza Stampalia / Torino Esposizioni",
    "10": "Tram - Piazza Statuto / Vallette",
    "15": "Tram - Sassi / Via Senatore Ghedini",
    "16": "Tram - Piazza Sabotino / Piazza Risorgimento",
    "18": "Bus - Piazza Massaua / Via Lombardia",
    "1": "Bus - Piazza Carlo Emanuele II / Via Gaidano",
    "2": "Bus - Piazza Sofia / Via Pio VII",
    "5": "Bus - Piazza Carlo Emanuele II / Corso Grosseto",
    "11": "Bus - Piazza Sofia / Via Pio VII",
    "12": "Bus - Corso Sebastopoli / Via Reni",
    "13": "Bus - Piazza Campanella / Via Pianezza",
    "14": "Bus - Piazza Risorgimento / Corso IV Marzo",
    "17": "Bus - Piazza Sofia / Via Salvador Allende",
    "19": "Bus - Piazza Carlo Felice / Corso IV Marzo",
    "20": "Bus - Piazza Carlo Felice / Corso Salvucci",
    "24": "Bus - Piazza Carlo Felice / Corso Maroncelli",
    "26": "Bus - Piazza Massaua / Corso Fruttero",
    "27": "Bus - Piazza Massaua / Corso Vercelli",
    "29": "Bus - Piazza Massaua / Via Leinì",
    "30": "Bus - Piazza Statuto / Corso Grosseto",
    "31": "Bus - Piazza Statuto / Via Pianezza",
    "32": "Bus - Piazza Statuto / Via Turina",
    "33": "Bus - Piazza Statuto / Corso Vercelli",
    "34": "Bus - Piazza Statuto / Corso Maroncelli",
    "35": "Bus - Piazza Statuto / Via Fossata",
    "36": "Bus - Piazza Statuto / Via Pio VII",
    "37": "Bus - Piazza Carlo Felice / Corso Salvucci",
    "38": "Bus - Piazza Bernini / Via Attivissimo",
    "39": "Bus - Piazza Bernini / Corso Vercelli",
    "41": "Bus - Piazza Vittorio Veneto / Corso Racconigi",
    "42": "Bus - Piazza Vittorio Veneto / Via Artom",
    "43": "Bus - Piazza Vittorio Veneto / Corso IV Marzo",
    "44": "Bus - Piazza Vittorio Veneto / Corso Salvucci",
    "45": "Bus - Piazza Vittorio Veneto / Corso Grosseto",
    "46": "Bus - Piazza Vittorio Veneto / Via Pianezza",
    "47": "Bus - Piazza Vittorio Veneto / Via Reni",
    "48": "Bus - Piazza Vittorio Veneto / Corso Frecchia",
    "49": "Bus - Piazza Vittorio Veneto / Corso Tazzoli",
    "50": "Bus - Piazza Vittorio Veneto / Corso Salvo D'Acquisto",
    "51": "Bus - Piazza Vittorio Veneto / Corso Marx",
    "52": "Bus - Piazza Vittorio Veneto / Via Chivasso",
    "53": "Bus - Piazza Vittorio Veneto / Corso Costigliole",
    "54": "Bus - Piazza Vittorio Veneto / Via Pallanza",
    "55": "Bus - Piazza Vittorio Veneto / Via Del Maillard",
    "56": "Bus - Piazza Vittorio Veneto / Corso Unione Sovietica",
    "57": "Bus - Piazza Vittorio Veneto / Via Pio VII",
    "58": "Bus - Piazza Vittorio Veneto / Via Leinì",
    "59": "Bus - Piazza Vittorio Veneto / Corso Gaetano Mosca",
    "60": "Bus - Piazza Vittorio Veneto / Via Francia",
    "61": "Bus - Piazza Vittorio Veneto / Corso Gino Olivetti",
    "62": "Bus - Piazza Vittorio Veneto / Via Botticelli",
    "63": "Bus - Piazza Vittorio Veneto / Corso Tazzoli",
    "64": "Bus - Piazza Vittorio Veneto / Via Allamegliano",
    "65": "Bus - Piazza Vittorio Veneto / Corso Salvucci",
    "66": "Bus - Piazza Vittorio Veneto / Via Pergolesi",
    "67": "Bus - Piazza Vittorio Veneto / Corso Vercelli",
    "68": "Bus - Piazza Vittorio Veneto / Via Frazione Gerbole",
    "69": "Bus - Piazza Vittorio Veneto / Via Airauda",
    "70": "Bus - Piazza Vittorio Veneto / Corso Maroncelli",
    "71": "Bus - Piazza Vittorio Veneto / Via Allamegliano",
    "72": "Bus - Piazza Vittorio Veneto / Corso Gino Olivetti",
    "73": "Bus - Piazza Vittorio Veneto / Via Dolin",
    "74": "Bus - Piazza Vittorio Veneto / Via Vico",
    "75": "Bus - Piazza Vittorio Veneto / Via Attivissimo",
    "76": "Bus - Piazza Vittorio Veneto / Via Pergolesi",
    "N": "Navetta - Servizio interportuale Torino Dora",
    "STAR1": "Star 1 - Corso Francia / Via Pianezza",
    "STAR2": "Star 2 - Piazza Carlo Felice / Via Reni",
}


class GtfsStaticManager:
    """Gestore del catalogo linee GTT con cache, fallback e validazione ID linea."""

    def __init__(self):
        self._routes_cache: dict = {}
        self._route_id_map: dict = {}   # route_id GTFS (es. "2U") -> route_short_name (es. "2")
        self._route_types: dict = {}    # short_name -> route_type GTFS (0 tram, 1 metro, 3 bus, 7 funicolare)
        self._route_colors: dict = {}  # short_name -> "#RRGGBB" (colori ufficiali GTT)
        self._route_text_colors: dict = {}  # short_name -> "#RRGGBB"
        self._stops_cache: dict = {}    # stop_id -> {"name", "lat", "lon"} (fermate reali)
        self._trip_headsigns: dict = {} # trip_id -> headsign GTT (a volte e' l'ORIGINE, non affidabile)
        self._trip_routes: dict = {}   # trip_id -> route_id GTFS (per coerenza linea-corsa)
        self._zip_bytes: bytes = b""    # pacchetto GTFS-Static in cache (per stop_times)
        self._trip_sequences: dict = {} # trip_id -> [(secondi_dalla_mezzanotte, stop_id), ...]
        self._cache_loaded_at: float = 0.0
        self._cache_source: str = "fallback"
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Percorsi sincroni: NON eseguono mai chiamate di rete.
    # ------------------------------------------------------------------
    def get_routes(self) -> dict:
        """Copia del catalogo corrente (cache se valida, altrimenti fallback locale)."""
        if self._routes_cache and self._is_cache_fresh():
            return dict(self._routes_cache)
        return dict(FALLBACK_ROUTES)

    def get_source(self) -> str:
        return self._cache_source if self._is_cache_fresh() else "fallback"

    def validate_route_id(self, route_id: str):
        """Valida un ID linea contro il catalogo (cache + fallback). Restituisce l'ID canonico o None."""
        if not route_id:
            return None
        candidate = str(route_id).strip().upper()
        if not candidate:
            return None
        known = set(FALLBACK_ROUTES) | set(self._routes_cache) | set(self._route_id_map)
        for known_id in known:
            if str(known_id).strip().upper() == candidate:
                return known_id
        return None

    def get_route_short_name(self, route_id: str) -> str:
        """Traduce un route_id GTFS (es. '2U', 'METROU') nel nome linea visibile (es. '2', 'M1').

        Se l'ID e' gia' un nome linea o non e' mappabile, viene restituito invariato.
        """
        rid = str(route_id or "").strip()
        if not rid:
            return rid
        if self._is_cache_fresh() and rid in self._route_id_map:
            return self._route_id_map[rid]
        return rid

    def get_gtfs_route_id(self, short_name: str) -> str:
        """Nome linea visibile (es. '2') -> route_id GTFS (es. '2U').

        Inverso di get_route_short_name: serve per i feed GTFS-RT, dove
        informed_entity.route_id deve riferirsi al route_id di routes.txt.
        Restituisce il nome linea invariato se non mappabile (mai None).
        """
        target = str(short_name or "").strip()
        if target and self._is_cache_fresh():
            for route_id, short in self._route_id_map.items():
                if short == target:
                    return route_id
        return target

    def get_route_types(self) -> dict:
        """Tipo di mezzo per linea dai dati UFFICIALI GTT (route_type GTFS).

        0 = tram, 1 = metro, 3 = bus, 7 = funicolare. Fallback locale
        se il catalogo non e' stato ancora scaricato: la linea 6 e' un BUS
        (dato reale GTT), i tram di Torino sono 3, 4, 7, 8, 9, 10, 15, 16, 18.
        """
        if self._is_cache_fresh() and self._route_types:
            return dict(self._route_types)
        fallback = dict.fromkeys(FALLBACK_ROUTES, 3)
        # Tram UFFICIALI della rete GTT attuale (route_type=0 del GTFS):
        # la linea 8 e la 18 sono BUS, non tram. Fonte: routes.txt GTT.
        for tram in ["3", "4", "7", "9", "10", "13", "15", "16", "16 CD", "16 CS"]:
            fallback[tram] = 0
        fallback["M1"] = 1
        return fallback

    def get_route_colors(self) -> dict:
        """Colori ufficiali delle linee GTT (route_color del GTFS-Static)."""
        if self._is_cache_fresh() and self._route_colors:
            return dict(self._route_colors)
        return {}

    def get_route_text_colors(self) -> dict:
        """Colori del testo ufficiali per badge linea (route_text_color GTFS)."""
        if self._is_cache_fresh() and self._route_text_colors:
            return dict(self._route_text_colors)
        return {}

    def get_stops(self) -> dict:
        """Fermate reali della rete GTT: stop_id -> {"name", "lat", "lon"}.

        Serve per mostrare la PROSSIMA FERMATA di ogni mezzo col nome vero,
        presa dal GTFS-Static ufficiale. Mai dati inventati.
        """
        if self._is_cache_fresh() and self._stops_cache:
            return self._stops_cache
        return {}

    def get_trip_sequences(self) -> dict:
        """Sequenze fermate per corsa: trip_id -> [(secondi, stop_id), ...]."""
        return self._trip_sequences

    def get_trip_headsigns(self) -> dict:
        """Headsign per corsa (trips.txt GTT). ATTENZIONE: per alcune corse
        varianti GTT pubblica l'origine anziche' la destinazione: usare
        preferibilmente l'ultima fermata della sequenza (get_trip_sequences)."""
        if self._is_cache_fresh() and self._trip_headsigns:
            return self._trip_headsigns
        return {}

    def get_trip_routes(self) -> dict:
        """Riferimento linea per corsa: trip_id -> route_id GTFS (es. '2U')."""
        if self._is_cache_fresh() and self._trip_routes:
            return self._trip_routes
        return {}

    def refresh_trip_sequences(self, trip_ids) -> int:
        """Costruisce/aggiorna le sequenze fermate SOLO per le corse richieste.

        Il feed GTFS-RT di GTT non pubblica gli stop_id: si risale alla fermata
        successiva incrociando gli orari programmati (stop_times.txt) con
        l'orario previsto dal feed real-time. Stream dal pacchetto in cache:
        mai dati inventati, solo incrocio di dati ufficiali GTT.
        Da eseguire in un thread (circa 2-4s per passaggio).
        """
        wanted = {str(t) for t in trip_ids if t}
        if not wanted or not self._zip_bytes:
            return 0
        added = 0
        # stop_times.txt NON e' contiguo per corsa: le righe di una corsa sono
        # sparse nel file. Si ricostruiscono da zero TUTTE le corse richieste in
        # un unico passaggio, accumulando ogni riga della stessa corsa ovunque
        # compaia. Ogni passaggio e' autogiustificante (nessuno stato corrotto
        # puo' sopravvivere).
        in_pass = {}
        try:
            with zipfile.ZipFile(io.BytesIO(self._zip_bytes)) as z:
                if "stop_times.txt" not in z.namelist():
                    return 0
                text = io.TextIOWrapper(z.open("stop_times.txt"), encoding="utf-8-sig")
                reader = csv.DictReader(text)
                for row in reader:
                    tid = (row.get("trip_id") or "").strip()
                    if tid not in wanted:
                        continue
                    arr = (row.get("arrival_time") or "").strip()
                    stop_id = (row.get("stop_id") or "").strip()
                    if not arr or not stop_id:
                        continue
                    try:
                        hh, mm, ss = (int(p) for p in arr.split(":"))
                        sched = hh * 3600 + mm * 60 + ss
                    except (ValueError, TypeError):
                        continue
                    in_pass.setdefault(tid, []).append((sched, stop_id))
                    added += 1
        except Exception as exc:
            logger.warning("stop_times.txt non leggibile (%s): fermate previste non disponibili.", exc)
            return 0
        # Pubblica le sequenze ricostruite, ORDINATE per orario programamato:
        # lungo una corsa gli orari crescono sempre, quindi l'ultima fermata
        # della sequenza ordinata e' il capolinea di destinazione (il file
        # stop_times di GTT non e' ordinato nemmeno internamente per corsa).
        for lst in in_pass.values():
            lst.sort(key=lambda item: item[0])
        self._trip_sequences.update(in_pass)
        if len(self._trip_sequences) > 3000:
            self._trip_sequences = {k: v for k, v in self._trip_sequences.items() if k in wanted}
        return added

    # ------------------------------------------------------------------
    # Percorso asincrono: scarica il GTFS-Static ufficiale con cache TTL.
    # ------------------------------------------------------------------
    def _is_cache_fresh(self) -> bool:
        return (time.time() - self._cache_loaded_at) < CACHE_TTL_SECONDS and bool(self._routes_cache)

    async def load_routes(self, force: bool = False) -> dict:
        """Catalogo linee: cache fresca, download remoto o fallback. Mai eccezioni, mai vuoto."""
        if not force and self._is_cache_fresh():
            logger.info("📚 [CATALOGO] Cache valida (%s linee, sorgente=%s)", len(self._routes_cache), self._cache_source)
            return dict(self._routes_cache)

        async with self._lock:
            if self._is_cache_fresh():
                return dict(self._routes_cache)

            routes = await self._download_routes()
            if routes:
                self._routes_cache = routes["short_names"]
                self._route_id_map = routes["route_id_map"]
                self._route_types = routes.get("route_types", {})
                self._route_colors = routes.get("route_colors", {})
                self._route_text_colors = routes.get("route_text_colors", {})
                self._stops_cache = routes.get("stops", {})
                self._trip_headsigns = routes.get("trip_headsigns", {})
                self._trip_routes = routes.get("trip_routes", {})
                self._zip_bytes = routes.get("zip_bytes", b"")
                self._cache_loaded_at = time.time()
                self._cache_source = "gtfs-static-ufficiale"
                print(f"✅ [CATALOGO] GTFS-Static ufficiale scaricato: {len(self._routes_cache)} linee reali della rete GTT catalogate.")
                return dict(self._routes_cache)

            # Refresh fallito ma catalogo GIÀ scaricato in passato: si serve lo
            # STALE (le 215 linee restano valide per giorni — il GTFS cambia
            # raramente) invece di crollare al fallback di 73 linee dopo il
            # TTL di un'ora. Il catalogo viene ri-datato: nuovo tentativo di
            # refresh tra un'ora, senza scaricare a ogni ciclo durante un
            # outage. E' cio' che tiene la Control Room indipendente anche
            # quando GTT sta avendo un outage totale (verificato dal vivo
            # il 13/10/2026: feed GTFS-RT rimossi con HTTP 404).
            if self._routes_cache:
                self._cache_loaded_at = time.time()
                print(f"🌐 [CATALOGO] Refresh non riuscito: mantengo il catalogo STALE di {len(self._routes_cache)} linee (riprovo tra {CACHE_TTL_SECONDS // 3600}h).")
                return dict(self._routes_cache)

            print(f"🌐 [CATALOGO] Server GTT non raggiungibili e nessun catalogo mai scaricato: fallback locale con {len(FALLBACK_ROUTES)} linee reali di Torino.")
            self._cache_source = "fallback"
            return dict(FALLBACK_ROUTES)

    async def _download_routes(self) -> dict:
        # Mirror noti + URL auto-scoperto dal catalogo open-data (a prova di futuro).
        from app.feed_registry import get_feed_urls
        mirrors = []
        for url in get_feed_urls().get("static", []) + GTFS_STATIC_URLS:
            if url not in mirrors:
                mirrors.append(url)
        for url in mirrors:
            try:
                async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
                    response = await client.get(url)
                if response.status_code != 200:
                    print(f"⚠️ [CATALOGO] Mirror {url} ha risposto {response.status_code}, provo il successivo...")
                    continue

                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    if "routes.txt" not in z.namelist():
                        print(f"⚠️ [CATALOGO] routes.txt assente nel pacchetto di {url}.")
                        continue
                    content = z.read("routes.txt").decode("utf-8-sig")
                    # Capolinea di destinazione per ogni corsa (trips.txt):
                    # e' la vera indicazione di direzione verso il terminale.
                    trip_headsigns = {}
                    trip_routes = {}
                    if "trips.txt" in z.namelist():
                        try:
                            trips_content = z.read("trips.txt").decode("utf-8-sig")
                            for trow in csv.DictReader(trips_content.splitlines()):
                                tid = (trow.get("trip_id") or "").strip()
                                headsign = (trow.get("trip_headsign") or "").strip()
                                if tid and headsign:
                                    trip_headsigns[tid] = headsign
                                trip_route = (trow.get("route_id") or "").strip()
                                if tid and trip_route:
                                    trip_routes[tid] = trip_route
                        except Exception as exc:
                            logger.warning("trips.txt non leggibile (%s): capolinei non disponibili.", exc)
                    stops = {}
                    if "stops.txt" in z.namelist():
                        try:
                            stops_content = z.read("stops.txt").decode("utf-8-sig")
                            for srow in csv.DictReader(stops_content.splitlines()):
                                sid = (srow.get("stop_id") or "").strip()
                                if sid:
                                    try:
                                        stops[sid] = {
                                            "name": (srow.get("stop_name") or "").strip(),
                                            "lat": float(srow.get("stop_lat") or 0),
                                            "lon": float(srow.get("stop_lon") or 0),
                                        }
                                    except ValueError:
                                        continue
                        except Exception as exc:
                            logger.warning("stops.txt non leggibile (%s): nomi fermate non disponibili.", exc)

                short_names = {}
                route_id_map = {}
                route_types = {}
                route_colors = {}
                route_text_colors = {}
                for row in csv.DictReader(content.splitlines()):
                    route_id = (row.get("route_id") or "").strip()
                    short = (row.get("route_short_name") or route_id or "").strip()
                    route_long = (row.get("route_long_name") or "").strip()
                    if short:
                        short_names[short] = route_long
                    if route_id and short:
                        route_id_map[route_id] = short
                    try:
                        route_types[short] = int((row.get("route_type") or "3").strip() or 3)
                    except ValueError:
                        route_types[short] = 3
                    color = (row.get("route_color") or "").strip()
                    if color and re.match(r"^[0-9A-Fa-f]{6}$", color):
                        route_colors[short] = f"#{color.upper()}"
                    text_color = (row.get("route_text_color") or "").strip()
                    if text_color and re.match(r"^[0-9A-Fa-f]{6}$", text_color):
                        route_text_colors[short] = f"#{text_color.upper()}"
                if short_names:
                    return {
                        "short_names": short_names,
                        "route_id_map": route_id_map,
                        "route_types": route_types,
                        "route_colors": route_colors,
                        "route_text_colors": route_text_colors,
                        "stops": stops,
                        "trip_headsigns": trip_headsigns,
                        "trip_routes": trip_routes,
                        "zip_bytes": response.content,
                    }
            except Exception as exc:
                print(f"⚠️ [CATALOGO] Download fallito da {url}: {exc}")
                continue
        return {}


# Istanza singleton usata dallo scraper avvisi per validare gli ID linea.
gtfs_manager = GtfsStaticManager()


def get_gtfs_routes() -> dict:
    """Wrapper sincrono compatibile: MAI di rete, MAI vuoto, MAI bloccante."""
    return gtfs_manager.get_routes()


async def get_gtfs_routes_async() -> dict:
    """Wrapper asincrono con refresh intelligente del catalogo remoto."""
    return await gtfs_manager.load_routes()
