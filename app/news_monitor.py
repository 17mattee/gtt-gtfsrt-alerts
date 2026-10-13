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

# Parole chiave che impattano la mobilita' urbana di Torino.
KEYWORDS = re.compile(
    r"\b(scioper\w*|agitazion\w*|sindacat\w*|manifestazion\w*|corteo|presidi\w*|"
    r"blocc\w+|deviazion\w*|chius\w*|incident\w*|guast\w*|"
    r"gtt|autobus|bus|tram|metropolitana|metro|fermata|ferrov\w*|stazion\w*|"
    r"traffico|strad\w*|"
    r"partita|stadio|concerto|maratona|mercato|gran premio|sagra|fiera)\b",
    re.IGNORECASE,
)

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


def _filter_relevant(items: list, max_items: int = NEWS_MAX_ITEMS) -> list:
    """Solo le notizie che impattano la mobilita', con le parole chiave trovate."""
    out = []
    for it in items:
        text = f"{it['titolo']} {it['descrizione']}"
        keywords = sorted({m.group(0).lower() for m in KEYWORDS.finditer(text)})
        if keywords:
            out.append({**it, "parole_chiave": keywords[:6]})
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
