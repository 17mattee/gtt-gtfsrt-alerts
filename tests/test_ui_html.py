"""Test di INTEGRITA' DELLA UI (static/index.html).

Il frontend e' un file unico editato di continuo e non eseguito dai test
Python: questi check catturano le regressioni strutturali che un browser
maschererebbe (il parser HTML autochiude i tag, quindi un </div> mancante
non si vedrebbe mai a occhio) e i marcatori chiave delle funzionalita'.
"""

import re

HTML = open("static/index.html", encoding="utf-8").read()


def test_tag_html_bilanciati():
    """Ogni <div>/<span>/<button> aperto (anche nel JS che genera HTML)
    deve essere chiuso: il popup aveva il contenitore esterno mai chiuso
    (rigenerato correttamente solo per l'auto-close del browser)."""
    for tag in ("div", "span", "button", "header", "main", "aside", "style", "script"):
        aperti = len(re.findall(rf"<{tag}[ >]", HTML))
        chiusi = HTML.count(f"</{tag}>")
        assert aperti == chiusi, f"<{tag}>: {aperti} aperti vs {chiusi} chiusi"


def test_marcatori_funzionalita_presenti():
    """I marcatori delle feature costruite in questo progetto non devono
    sparire con refactor della UI."""
    marcatori = {
        "etichette tab compatte da mobile": "lbl-short",
        "soglia ritardi coerente (2 min)": "min < 2",
        "contatore fix dichiarato": "Ultimo fix GPS",
        "posizioni stimate distinte": "position_source === 'stimata'",
        "fuori percorso col motivo": "fuori_percorso",
        "rientro deposito nel popup": "rientro al deposito",
        "deviazioni non annunciate": "deviazione_rilevata",
        "link RSS sanitizzati": "safeLink",
        "escaping XSS": "function esc(value)",
        "watchdog WS": "lastMsgAt",
    }
    for nome, marker in marcatori.items():
        assert marker in HTML, f"marcatore mancante: {nome} ({marker!r})"


def test_nessun_cdn_esterno_nel_codice():
    """Tutte le risorse sono self-hosted (CSP senza origini di terze parti)."""
    assert "unpkg.com" not in HTML and "cdn.jsdelivr" not in HTML and "googleapis.com" not in HTML
    assert '/static/vendor/leaflet/leaflet.js' in HTML
