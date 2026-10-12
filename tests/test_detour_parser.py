"""Test di regressione del parser delle deviazioni GTT (casi reali del feed)."""
from app.gtfs_rt import _parse_detour_details


def test_itinerario_via_per_via():
    r = _parse_detour_details(
        "Linea 15 deviata in direzione Sassi",
        "Da corso Re Umberto angolo corso Stati Uniti per corso Stati Uniti, via Sacchi, via XX Settembre, percorso regolare.\n"
        "Causa impedimento al transito in corso Re Umberto angolo corso Stati Uniti.",
    )
    assert r["itinerario"]["da"] == "corso Re Umberto angolo corso Stati Uniti"
    assert r["itinerario"]["strade"] == ["corso Stati Uniti", "via Sacchi", "via XX Settembre"]
    assert r["itinerario"]["ritorno"] is True
    assert r["itinerario"]["nome"] == "Sassi"
    assert "impedimento" in r["motivo"]


def test_periodo_e_motivo_senza_itinerario():
    r = _parse_detour_details(
        "Linee 39, 1N deviate su percorsi alternativi",
        "Da Martedi' 13 a sabato 24 ottobre 2026,\ndalle ore 8.00 alle ore 18.00 circa.\n"
        "Causa lavori Italgas nel comune di Nichelino.\n"
        "Per conoscere la deviazione della tua linea consulta la sezione dedicata \"linee e orari\" del sito www.gtt.to.it",
    )
    assert "ottobre 2026" in r["periodo"]
    assert "Italgas" in r["motivo"]
    # il rinvio al sito GTT deve essere rilevato (bug storico: "deviazione"
    # contiene "via" e ingannava il riconoscimento itinerario)
    assert r["fonte_esterna"] is True
    assert r["itinerario"] is None


def test_direzioni_multiple():
    r = _parse_detour_details(
        "Linea 17 deviata in entrambe le direzioni",
        "Lunedi' 12 ottobre 2026 dalle ore 14.30 sino a termine del servizio.\n"
        "Direzione via Ventimiglia: da corso Sebastopoli angolo via Tripoli devia per corso Sebastopoli, corso IV Novembre, corso Monte Lungo, piazzale Costantino il Grande, corso Galileo Ferraris, corso Sebastopoli, percorso regolare;\n"
        "Direzione Rivoli (Ospedale): da corso Sebastopoli angolo corso Galileo Ferraris devia per corso Galileo Ferraris, corso Sebastopoli, percorso regolare;",
    )
    assert len(r["direzioni"]) == 2
    d1 = r["direzioni"][0]
    assert d1["nome"] == "via Ventimiglia"
    assert d1["da"] == "corso Sebastopoli angolo via Tripoli"
    assert "corso IV Novembre" in d1["strade"]
    assert d1["ritorno"] is True
    assert r["direzioni"][1]["nome"].startswith("Rivoli")
    assert "ottobre 2026" in r["periodo"]


def test_testo_vuoto_o_nullo():
    assert _parse_detour_details("Test", "") is None
    assert _parse_detour_details("Test", None) is None


def test_testo_incomprensibile_non_crasha():
    r = _parse_detour_details("Linea X deviata", "qwertz yuiop 12345")
    # nessuna struttura riconoscibile -> None (la UI mostra il testo grezzo)
    assert r is None
