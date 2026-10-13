# GTT Torino — GTFS-RT Enterprise Middleware & Control Room

**English** | [Italiano](README.it.md)

> High-performance real-time infomobility platform for the public transport network of Turin (**GTT** — Metro, Trams, urban and suburban bus lines). Live fleet telemetry, delays, detours and official service alerts, straight from GTT's official open-data feeds.

![CI](https://github.com/17mattee/gtt-gtfsrt-alerts/actions/workflows/ci.yml/badge.svg)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![License MIT](https://img.shields.io/badge/license-MIT-green)
![Data CC BY 4.0](https://img.shields.io/badge/data-CC%20BY%204.0-orange)

---

## What it does

- **Live fleet telemetry** — every GTT vehicle on the map (~440 buses/trams/metro in service), GPS position, speed, heading, refreshed every ~5 seconds.
- **Real delays** — official GTFS-RT Trip Updates, per-trip delay in minutes, joined with the fleet feed to always show the line number.
- **Detours & service alerts, crystal clear** — every official GTT alert is classified (detour, disruption, stop moved, delay, info), translated to plain Italian, and affected lines are extracted automatically even when GTT only mentions them in the alert text.
- **215-line catalog** — the complete official route catalog, downloaded dynamically from GTT's GTFS-Static feed and refreshed hourly.
- **Bullet-proof resilience** — multi-endpoint cascade on every data source with circuit breaker and caches. If GTT changes its website, the service keeps working.
- **Real data only** — the Control Room shows exactly what GTT publishes. If GTT's vehicle feed goes quiet (their-side outage), the status bar says so and the map repopulates automatically when they resume — no simulated vehicles, ever.
- **Control Room UI** — dark-mode Leaflet dashboard with color-coded markers (green on time ±2 min / blue >2 min early / orange 2–5 min late / red >5 min), live tabs for fleet, delays and alerts, auto-reconnect WebSocket.

## Architecture

```text
 [ GTT official open-data feeds (aperTO / gtt.to.it) ]
        │  GTFS-Static (routes) · GTFS-RT protobuf (vehicles, trip updates, alerts)
        ▼
 ┌─────────────────────────────────────────────────┐
 │  FastAPI async middleware                        │
 │   · single background refresher (~every 2s)     │
 │   · parallel feed downloads (asyncio.gather)    │
 │   · TTL caches + multi-endpoint cascades        │
 │   · XSS-escaped JSON payloads                    │
 └──────────────┬───────────────────────┬───────────┘
                ▼                       ▼
   [ Supabase / PostgreSQL ]    [ WebSocket stream ]
   4 relational tables           [ Leaflet Control Room ]
   (see sql/schema.sql)
```

Endpoints:

| Endpoint | Description |
|---|---|
| `GET /` | Control Room dashboard (Leaflet dark mode) |
| `GET /health` | Service health + live counters (vehicles, alerts, trips, routes) |
| `GET /gtfs-rt/alerts.pb` | Standard GTFS-RT 2.0 protobuf feed (proxy of the official GTT feed) |
| `WS /ws` | Real-time stream: fleet, delays, alerts, catalog, sources metadata |

## Project structure

```text
gtt-gtfsrt-alerts/
├── app/                    # Backend (FastAPI)
│   ├── main.py             # Server, WebSocket stream, background refresher
│   ├── gtt_full_sync.py    # Orchestration of all feeds (parallel, never empty)
│   ├── gtfs_rt.py          # Official GTFS-RT feeds: trip updates + service alerts
│   ├── vehicles_scraper.py # Vehicle positions + smart fallback fleet
│   ├── gtfs_static.py      # Route catalog (GTFS-Static) with cache + local fallback
│   ├── scraper.py          # GTT portal scraper (alerts backup source, circuit breaker)
│   ├── database.py         # psycopg persistence (alerts history)
│   ├── database_writer.py # Supabase writer (routes, vehicles, positions, alerts)
│   ├── notifier.py         # Optional Telegram notifications
│   └── websocket_manager.py
├── static/                 # Frontend — Leaflet Control Room dashboard
├── tests/                  # Pytest suite (unit + integration)
├── scripts/                # QA audits, feed inspector, load testing (locust)
├── sql/schema.sql          # Supabase/PostgreSQL schema (idempotent)
├── .github/workflows/      # CI (pytest), CodeQL security scan
├── Dockerfile · docker-compose.yml
├── README.md · README.it.md
└── CONTRIBUTING.md · CONTRIBUTING.it.md
```

## Quick start

### Local (Python)

```bash
git clone https://github.com/17mattee/gtt-gtfsrt-alerts.git
cd gtt-gtfsrt-alerts
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

For development and tests: `pip install -r requirements-dev.txt`.

Open **http://127.0.0.1:8000** — the map fills with real vehicles in seconds.

### Docker

```bash
docker compose up --build
```

## Free publishing (Render)

The whole project deploys for free on [Render](https://render.com) (Docker runtime, WebSocket support included):

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://dashboard.render.com/select-repo?type=blueprint)

1. Sign up on [render.com](https://render.com) with your GitHub account.
2. **New → Blueprint** → pick the `17mattee/gtt-gtfsrt-alerts` repository.
3. Render reads the included [`render.yaml`](render.yaml) automatically (free plan, Frankfurt region, health check on `/health`). Leave `SUPABASE_URL`/`SUPABASE_KEY` empty or fill them to enable persistence.
4. Deploy: after ~3 minutes you get a public URL like `https://gtt-gtfsrt-alerts.onrender.com`.

Notes: on the free plan the service sleeps after ~15 minutes without traffic and wakes up on the first request — the dashboard's WebSocket auto-reconnect handles this transparently. Alternatives with a free tier: Koyeb, Hugging Face Spaces (Docker).

## Database (Supabase)

1. Open your Supabase project → **SQL Editor**.
2. Paste the whole content of [`sql/schema.sql`](sql/schema.sql) and run it. The script is idempotent: safe to re-run on top of existing tables (it upgrades them with new columns, constraints and indexes).
3. Set the environment variables and restart:

```bash
export SUPABASE_URL="https://<your-project>.supabase.co"
export SUPABASE_KEY="<your-service-role-or-anon-key>"
uvicorn app.main:app --reload
```

The middleware then stores, at every refresh cycle: the 215-line catalog, fleet vehicles with last-seen, one GPS row per vehicle (time-series), and the full history of alerts/detours — with `category` (`deviazione`, `interruzione`, `ritardo`, ...), Italian cause/effect labels and affected lines.

### Optional

| Variable | Effect |
|---|---|
| `DATABASE_URL` | Enables psycopg persistence of alerts history |
| `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` | Sends a Telegram message when the GTT portal changes layout or the scraper fails |

## Tests & QA

```bash
pytest                                   # unit + integration suite
python scripts/run_ultimate_check.py     # 5-check live end-to-end audit
python scripts/run_enterprise_audit.py   # 6-check enterprise audit
python scripts/run_full_audit.py         # pytest + coverage + pip-audit + live endpoints
python scripts/inspect_feed.py           # inspect the served GTFS-RT feed
locust -f scripts/locustfile.py          # load testing
```

## Data sources & license

- GTFS-Static and GTFS-RT feeds are the official GTT / City of Turin open data (dataset *“Feed GTFS Real-Time trasporti GTT”*, [aperTO](https://aperto.comune.torino.it/dataset/feed-gtfs-real-time-trasporti-gtt)), licensed **CC BY 4.0**.
- This project code is released under the **MIT license**.

## Resilience design

| Failure | Behaviour |
|---|---|
| Official vehicle feed down | Fallback endpoints, then an honest empty state with a clear banner — no simulated vehicles (real data only) |
| Alerts feed down | Portal HTML scraper (retry + circuit breaker), then default informational alert |
| GTFS-Static down | 2 mirrors + cached catalog + local 71-line fallback; sync paths never touch the network |
| GTT changes route-id format | Raw IDs pass through unchanged; labels degrade gracefully |
| One WebSocket cycle fails | Per-iteration try/except: the loop survives and sends the last valid payload |
