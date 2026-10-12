# Contribuire al Middleware GTT GTFS-RT Torino

[English](CONTRIBUTING.md) | **Italiano**

Grazie dell'interesse per questo progetto! Segui queste linee guida per mantenere il codice veloce, testato e resiliente.

## Per iniziare

```bash
git clone https://github.com/17mattee/gtt-gtfsrt-alerts.git
cd gtt-gtfsrt-alerts
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Organizzazione del progetto

- `app/` — solo backend. Un modulo per responsabilità (feed, catalogo, persistenza, WebSocket).
- `static/` — dashboard frontend (JS vanilla + Leaflet, nessun build step).
- `tests/` — tutti i test pytest. I nuovi test vanno qui.
- `scripts/` — script eseguibili di QA/ops (audit, ispettore feed, locust). Nessun test pytest qui.
- `sql/` — schema del database, tenuto allineato con `app/database_writer.py`.

## Regole fondamentali

1. **Mai bloccare l'event loop.** Tutto l'I/O esterno nei percorsi async deve usare chiamate `httpx` asincrone o `asyncio.to_thread`. La funzione sincrona `get_gtfs_routes()` non deve mai toccare la rete.
2. **Mai eccezioni, mai dati finti.** `fetch_all_gtt_data()` e `fetch_gtt_vehicle_positions()` non devono mai lanciare eccezioni; un risultato vuoto è legittimo quando GTT non pubblica nulla. I veicoli simulati sono vietati — la UI mostra la verità con un banner esplicito.
3. **Ogni nuova dipendenza esterna richiede un timeout.** Nessuna chiamata verso l'esterno senza timeout esplicito e un try/except che degrada in modo controllato.
4. **Escapare tutto.** La dashboard renderizza dati provenienti da feed esterni: usa sempre l'helper `esc()` prima di inserire qualsiasi stringa in `innerHTML`.
5. **Tieni i fallback aggiornati con la realtà.** La lista linee di fallback locale deve contenere solo linee GTT reali.

## Flusso di lavoro

1. Crea un branch: `git checkout -b feat/tua-funzionalita`.
2. Apporta la modifica con un diff minimale, rispettando lo stile esistente (log in italiano con prefissi emoji, snake_case, docstring in italiano benvenute).
3. Esegui il gate di qualità completo — tutti e tre devono passare:

   ```bash
   pytest
   python scripts/run_ultimate_check.py
   python scripts/run_enterprise_audit.py
   ```

4. Se hai toccato `app/database_writer.py`, aggiorna anche `sql/schema.sql` e verifica che l'SQL resti idempotente.
5. Committa con un messaggio convenzionale (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`).
6. Apri una Pull Request verso `main` descrivendo cosa è cambiato e come lo hai verificato.

## Aggiungere una nuova fonte dati

Inseriscila in `app/gtfs_rt.py` o in un modulo dedicato seguendo lo stesso pattern:

- funzione asincrona, che non lancia mai eccezioni, che restituisce una lista/dict;
- flag a livello modulo `last_<nome>_source` (`live` / `unavailable`);
- righe di `print`/log chiare per ogni fase, in italiano;
- cache TTL quando il dato non cambia a ogni ciclo.

## Segnalare bug

Apri una issue con: cosa ti aspettavi, cosa è successo, come riprodurlo, e l'output del terminale (i log `[SYNC]`, `[SCRAPER]`, `[GTFS-RT]`, `[WS]` sono pensati per facilitare la diagnosi).

## Licenza

Contribuendo accetti che i tuoi contributi siano rilasciati con licenza MIT. I dati di trasporto restano CC BY 4.0, attribuzione GTT / Città di Torino.
