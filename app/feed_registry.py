"""
Auto-scoperta degli URL dei feed GTT dal catalogo open-data Transitland Atlas.

A prova di futuro: se GTT sposta i propri feed (nuovi domini, nuovi path),
la community aggiorna il catalogo macchina su GitHub e questo middleware
si adatta automaticamente, senza alcuna modifica al codice. Gli URL noti
remangono comunque come fallback: la scoperta aggiunge, non sostituisce.

Fonte: transitland/transitland-atlas (DMFR, formato standard dei feed GTFS).
"""

import logging
import time

import httpx

logger = logging.getLogger("GTT-FeedRegistry")

REGISTRY_URL = (
    "https://raw.githubusercontent.com/transitland/transitland-atlas/"
    "master/feeds/gtt.to.it.dmfr.json"
)
REGISTRY_TTL_SECONDS = 6 * 3600  # il catalogo cambia raramente: 6 ore di cache
REGISTRY_ERROR_TTL_SECONDS = 600  # fallito: si riprova tra 10 min, non a ogni ciclo

_registry_cache: dict = {"urls": None, "fetched_at": 0.0}


def _https_variant(url: str) -> list:
    """Restituisce la variante https PRIMA dell'originale.

    I feed GTT rispondono su https (piu' veloce e affidabile): la variante
    http resta come secondo tentativo nel caso https venga ritirato.
    """
    if url.startswith("http://"):
        return ["https://" + url[len("http://"):], url]
    return [url]


def _fetch_registry() -> dict:
    """Scarica il catalogo DMFR. Mai eccezioni: in caso di errore ritorna {}."""
    try:
        response = httpx.get(REGISTRY_URL, timeout=8.0, follow_redirects=True)
        if response.status_code != 200:
            logger.warning("Catalogo feed (DMFR) non raggiungibile: HTTP %s", response.status_code)
            return {}
        data = response.json()
        urls = {"vehicle_positions": [], "trip_updates": [], "alerts": [], "static": []}
        for feed in data.get("feeds", []):
            spec = feed.get("spec", "")
            feed_urls = feed.get("urls", {})
            if spec == "gtfs-rt":
                for key, target in [
                    ("realtime_vehicle_positions", "vehicle_positions"),
                    ("realtime_trip_updates", "trip_updates"),
                    ("realtime_alerts", "alerts"),
                ]:
                    if feed_urls.get(key):
                        urls[target].extend(_https_variant(feed_urls[key]))
            elif spec == "gtfs" and feed_urls.get("static_current"):
                urls["static"].append(feed_urls["static_current"])
        return urls
    except Exception as exc:
        logger.warning("Catalogo feed (DMFR) non disponibile (%s): uso gli URL noti.", exc)
        return {}


def get_feed_urls() -> dict:
    """URL dei feed GTT scoperti dal catalogo (con cache TTL). Mai eccezioni.

    Ritorna un dict con liste (anche vuote): il chiamante concatena sempre i
    propri URL di fallback dopo quelli scoperti.
    NB: si mette in cache ANCHE il fallimento (TTL corto): altrimenti ogni
    ciclo di sync (~3s) rifarebbe una chiamata HTTP sincrona da dentro
    l'event loop, bloccandolo per l'intero timeout.
    """
    now = time.time()
    if _registry_cache["urls"] is None:
        # Mai riuscito (o ultimo tentativo fallito): retry lento, mai a ogni ciclo
        stale = (now - _registry_cache["fetched_at"]) > REGISTRY_ERROR_TTL_SECONDS
    else:
        stale = (now - _registry_cache["fetched_at"]) > REGISTRY_TTL_SECONDS
    if stale:
        discovered = _fetch_registry()
        _registry_cache["urls"] = discovered or None
        _registry_cache["fetched_at"] = now
        if discovered:
            logger.info(
                "🗂️ [REGISTRY] Feed GTT auto-scoperti dal catalogo open-data: veicoli=%s corse=%s avvisi=%s statico=%s",
                len(discovered.get("vehicle_positions", [])),
                len(discovered.get("trip_updates", [])),
                len(discovered.get("alerts", [])),
                len(discovered.get("static", [])),
            )
        else:
            logger.warning("🗂️ [REGISTRY] Catalogo feed non disponibile: uso gli URL noti (riprovo tra %s min).", REGISTRY_ERROR_TTL_SECONDS // 60)
    return _registry_cache["urls"] or {"vehicle_positions": [], "trip_updates": [], "alerts": [], "static": []}
