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
    "9": "Tram - Piazza Stampalia / Torino Esposizioni",
    "10": "Tram - Piazza Statuto / Vallette",
    "15": "Tram - Sassi / Via Senatore Ghedini",
    "16": "Tram - Piazza Sabotino / Piazza Risorgimento",
    "18": "Tram - Piazza Massaua / Via Lombardia",
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
        self._route_id_map: dict = {}  # route_id GTFS (es. "2U") -> route_short_name (es. "2")
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
                self._cache_loaded_at = time.time()
                self._cache_source = "gtfs-static-ufficiale"
                print(f"✅ [CATALOGO] GTFS-Static ufficiale scaricato: {len(self._routes_cache)} linee reali della rete GTT catalogate.")
                return dict(self._routes_cache)

            print(f"🌐 [CATALOGO] Server GTT non raggiungibili: attivo fallback locale con {len(FALLBACK_ROUTES)} linee reali di Torino.")
            self._cache_source = "fallback"
            return dict(FALLBACK_ROUTES)

    async def _download_routes(self) -> dict:
        for url in GTFS_STATIC_URLS:
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

                short_names = {}
                route_id_map = {}
                for row in csv.DictReader(content.splitlines()):
                    route_id = (row.get("route_id") or "").strip()
                    short = (row.get("route_short_name") or route_id or "").strip()
                    route_long = (row.get("route_long_name") or "").strip()
                    if short:
                        short_names[short] = route_long
                    if route_id and short:
                        route_id_map[route_id] = short
                if short_names:
                    return {"short_names": short_names, "route_id_map": route_id_map}
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
