# GTT Torino — Middleware Enterprise GTFS-RT & Control Room

[English](README.md) | **Italiano**

> Piattaforma di infomobilità in tempo reale ad alte prestazioni per la rete di trasporto pubblico di Torino (**GTT** — Metropolitana, Tram e linee urbane/suburbane). Telemetria live della flotta, ritardi, deviazioni e avvisi ufficiali di servizio, direttamente dai feed open-data ufficiali GTT.

![CI](https://github.com/17mattee/gtt-gtfsrt-alerts/actions/workflows/ci.yml/badge.svg)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![Licenza MIT](https://img.shields.io/badge/licenza-MIT-green)
![Dati CC BY 4.0](https://img.shields.io/badge/dati-CC%20BY%204.0-orange)

---

## Cosa fa

- **Telemetria live della flotta** — ogni mezzo GTT sulla mappa (~440 bus/tram/metro in servizio), posizione GPS, velocità, direzione, aggiornato ogni ~5 secondi.
- **Ritardi reali** — Trip Updates ufficiali GTFS-RT, ritardo per corsa in minuti, incrociato con il feed veicoli per mostrare sempre il numero di linea.
- **Deviazioni e avvisi, super chiari** — ogni avviso ufficiale GTT viene classificato (deviazione, interruzione, fermata spostata, ritardo, informazione), tradotto in italiano comprensibile, e le linee coinvolte vengono estratte automaticamente anche quando GTT le indica solo nel testo dell'avviso.
- **Catalogo di 215 linee** — il catalogo completo ufficiale delle linee, scaricato dinamicamente dal feed GTFS-Static di GTT e rinnovato ogni ora.
- **Resilienza a prova di bomba** — cascata multi-endpoint su ogni fonte dati con circuit breaker e cache. Se GTT cambia sito, il servizio continua a funzionare.
- **Solo dati veri** — la Control Room mostra esattamente ciò che GTT pubblica. Se il feed veicoli di GTT si blocca (guasto loro), la barra di stato lo dichiara e la mappa si repopola da sola quando riprendono — mai mezzi simulati.
- **Control Room** — dashboard Leaflet in dark mode con marker colorati (verde in orario, tolleranza sotto i 2 min / blu da 2 min in anticipo in poi / arancio da 2 a 5 min di ritardo / rosso oltre 5 min), tab live per flotta, ritardi e avvisi, WebSocket con riconnessione automatica.

## Architettura

```text
 [ Feed open-data ufficiali GTT (aperTO / gtt.to.it) ]
        │  GTFS-Static (linee) · GTFS-RT protobuf (veicoli, ritardi, avvisi)
        ▼
 ┌─────────────────────────────────────────────────┐
 │  Middleware asincrono FastAPI                    │
 │   · refresher unico in background (~ogni 2s)    │
 │   · download dei feed in parallelo (gather)     │
 │   · cache TTL + cascate multi-endpoint          │
 │   · payload JSON protetti da XSS                │
 └──────────────┬───────────────────────┬───────────┘
                ▼                       ▼
   [ Supabase / PostgreSQL ]    [ Stream WebSocket ]
   4 tabelle relazionali        [ Control Room Leaflet ]
   (vedi sql/schema.sql)
```

Endpoint:

| Endpoint | Descrizione |
|---|---|
| `GET /` | Dashboard Control Room (Leaflet dark mode) |
| `GET /health` | Stato del servizio + contatori live (mezzi, avvisi, corse, linee) |
| `GET /gtfs-rt/alerts.pb` | Feed protobuf GTFS-RT 2.0 standard (proxy del feed ufficiale GTT) |
| `WS /ws` | Stream real-time: flotta, ritardi, avvisi, catalogo, metadati sorgenti |

## Struttura del progetto

```text
gtt-gtfsrt-alerts/
├── app/                    # Backend (FastAPI)
│   ├── main.py             # Server, stream WebSocket, refresher in background
│   ├── gtt_full_sync.py    # Orchestrazione di tutti i feed (parallela, mai vuota)
│   ├── gtfs_rt.py          # Feed GTFS-RT ufficiali: ritardi + avvisi servizio
│   ├── vehicles_scraper.py # Posizioni veicoli + flotta di fallback intelligente
│   ├── gtfs_static.py      # Catalogo linee (GTFS-Static) con cache + fallback locale
│   ├── scraper.py           # Scraper portale GTT (fonte di backup avvisi, circuit breaker)
│   ├── database.py          # Persistenza psycopg (storico allerte)
│   ├── database_writer.py  # Writer Supabase (linee, mezzi, posizioni, avvisi)
│   ├── notifier.py          # Notifiche Telegram opzionali
│   └── websocket_manager.py
├── static/                 # Frontend — dashboard Control Room Leaflet
├── tests/                  # Suite pytest (unitari + integrazione)
├── scripts/                # Audit QA, ispettore feed, load testing (locust)
├── sql/schema.sql          # Schema Supabase/PostgreSQL (idempotente)
├── .github/workflows/      # CI (pytest), scansione sicurezza CodeQL
├── Dockerfile · docker-compose.yml
├── README.md · README.it.md
└── CONTRIBUTING.md · CONTRIBUTING.it.md
```

## Avvio rapido

### Locale (Python)

```bash
git clone https://github.com/17mattee/gtt-gtfsrt-alerts.git
cd gtt-gtfsrt-alerts
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Per sviluppo e test: `pip install -r requirements-dev.txt`.

Apri **http://127.0.0.1:8000** — la mappa si riempie di mezzi reali in pochi secondi.

### Docker

```bash
docker compose up --build
```

## Pubblicazione gratuita (Render)

L'intero progetto si pubblica gratuitamente su [Render](https://render.com) (runtime Docker, WebSocket inclusi):

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://dashboard.render.com/select-repo?type=blueprint)

1. Registrati su [render.com](https://render.com) con l'account GitHub.
2. **New → Blueprint** → scegli la repository `17mattee/gtt-gtfsrt-alerts`.
3. Render legge automaticamente il file [`render.yaml`](render.yaml) incluso (piano free, regione Francoforte, health check su `/health`). Lascia vuote `SUPABASE_URL`/`SUPABASE_KEY` oppure compilale per attivare la persistenza.
4. Deploy: dopo ~3 minuti ottieni un URL pubblico tipo `https://gtt-gtfsrt-alerts.onrender.com`.

Note: col piano free il servizio si addormenta dopo ~15 minuti senza traffico e si riattiva alla prima richiesta — la riconnessione automatica WebSocket della dashboard gestisce tutto in modo trasparente. Alternative con piano gratuito: Koyeb, Hugging Face Spaces (Docker).

## Database (Supabase)

1. Apri il tuo progetto Supabase → **SQL Editor**.
2. Incolla tutto il contenuto di [`sql/schema.sql`](sql/schema.sql) ed eseguilo. Lo script è idempotente: si può rilanciare anche sopra le tabelle già create in dashboard (le aggiorna con nuove colonne, vincoli e indici).
3. Imposta le variabili d'ambiente e riavvia:

```bash
export SUPABASE_URL="https://<tuo-progetto>.supabase.co"
export SUPABASE_KEY="<tua-service-role-o-anon-key>"
uvicorn app.main:app --reload
```

Il middleware salva così: il catalogo delle 215 linee e i mezzi della flotta con ultimo avvistamento a ogni ciclo di refresh, una riga GPS per mezzo (time-series) ogni 10 minuti di default, e lo storico completo di avvisi/deviazioni — con `category` (`deviazione`, `interruzione`, `ritardo`, ...), cause/effetti in italiano e linee coinvolte.

### Opzionale

| Variabile | Effetto |
|---|---|
| `DATABASE_URL` | Attiva la persistenza psycopg dello storico allerte |
| `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` | Invia un messaggio Telegram se il portale GTT cambia layout o lo scraper fallisce |
| `POSITIONS_PERSIST_SECONDS` | Cadenza di scrittura delle posizioni GPS per mezzo (default `600` = 10 min). Con ~490 mezzi il default tiene il database free tier ampiamente sotto quota; abbassala solo se serve piu' granularita' di storico |

## Test e QA

```bash
pytest                                   # suite unitaria + integrazione
python scripts/run_ultimate_check.py     # audit end-to-end live a 5 verifiche
python scripts/run_enterprise_audit.py   # audit enterprise a 6 verifiche
python scripts/run_full_audit.py         # pytest + coverage + pip-audit + endpoint live
python scripts/inspect_feed.py           # ispeziona il feed GTFS-RT servito
locust -f scripts/locustfile.py          # load testing
```

## Fonti dati e licenze

- I feed GTFS-Static e GTFS-RT sono gli open data ufficiali GTT / Città di Torino (dataset *“Feed GTFS Real-Time trasporti GTT”*, [aperTO](https://aperto.comune.torino.it/dataset/feed-gtfs-real-time-trasporti-gtt)), licenza **CC BY 4.0**.
- Il codice di questo progetto è rilasciato con licenza **MIT**.

## Design della resilienza

| Guasto | Comportamento |
|---|---|
| Feed veicoli ufficiale giù | Endpoint di fallback, poi stato vuoto onesto con banner esplicito — nessun mezzo simulato (solo dati reali) |
| Feed avvisi giù | Scraper HTML del portale (retry + circuit breaker), poi avviso informativo di default |
| GTFS-Static giù | 2 mirror + catalogo in cache + fallback locale di 71 linee; i percorsi sincroni non toccano mai la rete |
| GTT cambia formato dei route-id | Gli ID passano invariati; le etichette si degradano in modo controllato |
| Fallisce un ciclo WebSocket | try/except per iterazione: il ciclo sopravvive e invia l'ultimo payload valido |
