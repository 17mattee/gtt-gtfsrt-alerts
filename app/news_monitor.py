"""Monitoraggio notizie locali su scioperi ed eventi che impattano la mobilita' a Torino.

Fonte: RSS di TorinoToday (stampa locale), con filtro a parole chiave su
scioperi, manifestazioni, cortei, incidenti stradali, chiusure ed eventi
che storicamente impattano la rete GTT (partite, concerti, mercati).

Il risultato si incrocia nella sync con gli avvisi UFFICIALI GTT
(cause=STRIKE e linee coinvolte) per la sezione 'Scioperi & Eventi'.

Mai eccezioni, mai blocchi: cache TTL 30 minuti, backoff sui fallimenti.
"""

import logging
import re
import time
import xml.etree.ElementTree as ET

import httpx

logger = logging.getLogger("GTT-News")

NEWS_RSS_URLS = [
    "https://www.torinotoday.it/rss",
]
NEWS_CACHE_TTL = 1800        # 30 minuti: le notizie non cambiano ogni ciclo
NEWS_FAIL_TTL = 300         # RSS giu': si riprova dopo 5 minuti, non ogni ciclo
NEWS_MAX_ITEMS = 20

# SOLO notizie su scioperi e mezzi pubblici (richiesta esplicita): la notizia
# deve parlare di trasporto pubblico. Termini generici come 'mercato', 'chiuso',
# 'partita', 'concerto', 'incidente' NON bastano: catturavano cronaca che non
# c'entra nulla con i mezzi GTT.
KEYWORDS = re.compile(
    r"(scioper\w*|agitazion\w*\s+sindacal|sindacat\w*|"
    r"\bgtt\b|autobus|autocorriera\w*|autolinee|\btram\b|metropolitana|mezzi pubblici|"
    r"trasporti pubblici|trasporto pubblico|"
    r"fermata|corse\w*\s+(sospes|soppres|deviat|limitat)|"
    r"servizio\w*\s+(sospes|soppres|deviat|limitat)|"
    r"linee?\s+(sospes\w*|soppres\w*|deviat\w*)|"
    r"corteo|manifestazion\w*\s+(per\s+)?(il\s+)?(blocco|deviazion))",
    re.IGNORECASE,
)
# Solo notizie RECENTI: oltre MAX_AGE_DAYS giorni vengono scartate.
MAX_AGE_DAYS = 3

_news_cache = {"data": None, "fetched_at": 0.0}
_news_fail_at = 0.0


def _parse_rss(xml_bytes) -> list:
    """Estrae le notizie dal feed RSS. Struttura [{titolo, link, data, descrizione}]."""
    items = []
    try:
        root = ET.fromstring(xml_bytes)
        for it in root.iter("item"):
            titolo = (it.findtext("title") or "").strip()
            if not titolo:
                continue
            items.append({
                "titolo": titolo,
                "link": (it.findtext("link") or "").strip(),
                "data": (it.findtext("pubDate") or "").strip(),
                "descrizione": (it.findtext("description") or "").strip()[:300],
            })
    except ET.ParseError as exc:
        logger.warning("RSS notizie non valido: %s", exc)
    return items


def _item_age_days(data_pubblicazione: str, now: float) -> float | None:
    """Eta' in giorni della notizia, o None se la data non e' leggibile."""
    from datetime import timezone
    from email.utils import parsedate_to_datetime
    try:
        dt = parsedate_to_datetime(data_pubblicazione)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (now - dt.timestamp()) / 86400
    except (TypeError, ValueError):
        return None


def _filter_relevant(items: list, max_items: int = NEWS_MAX_ITEMS, now: float = None) -> list:
    """Solo notizie RECENTI (<= MAX_AGE_DAYS) su scioperi e mezzi pubblici."""
    import time as _t
    now = _t.time() if now is None else now
    out = []
    for it in items:
        text = f"{it['titolo']} {it['descrizione']}"
        if not KEYWORDS.search(text):
            continue
        eta = _item_age_days(it.get("data") or "", now)
        if eta is None or eta > MAX_AGE_DAYS:
            continue  # data illeggibile o troppo vecchia: scartata
        out.append({**it, "eta_giorni": round(eta, 1)})
    return out[:max_items]


def get_transport_news() -> list:
    """Notizie locali su scioperi/eventi che impattano i trasporti. Mai eccezioni.

    Sincrona (httpx sync): da chiamare in thread dalla sync principale.
    """
    global _news_fail_at
    now = time.time()
    if _news_fail_at and (now - _news_fail_at) < NEWS_FAIL_TTL:
        return _news_cache["data"] or []
    if _news_cache["data"] is not None and (now - _news_cache["fetched_at"]) < NEWS_CACHE_TTL:
        return _news_cache["data"]

    for url in NEWS_RSS_URLS:
        try:
            response = httpx.get(
                url,
                timeout=8.0,
                follow_redirects=True,
                headers={"User-Agent": "GTT-ControlRoom/2.2 (+https://github.com/17mattee/gtt-gtfsrt-alerts)"},
            )
            if response.status_code == 200 and response.content:
                items = _filter_relevant(_parse_rss(response.content))
                _news_cache["data"] = items
                _news_cache["fetched_at"] = now
                _news_fail_at = 0.0
                if items:
                    print(f"📰 [NEWS] {len(items)} notizie di mobilita' da fonte locale (RSS).")
                return items
        except Exception as exc:
            logger.warning("RSS notizie non raggiungibile (%s): %s", url, exc)
    _news_fail_at = now
    return _news_cache["data"] or []
