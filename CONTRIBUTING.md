# Contributing to GTT Torino GTFS-RT Middleware

**English** | [Italiano](CONTRIBUTING.it.md)

Thanks for your interest in improving this project! Follow these guidelines to keep the codebase fast, tested and resilient.

## Getting started

```bash
git clone https://github.com/17mattee/gtt-gtfsrt-alerts.git
cd gtt-gtfsrt-alerts
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Project layout

- `app/` — backend only. One concern per module (feeds, catalog, persistence, WebSocket).
- `static/` — frontend dashboard (vanilla JS + Leaflet, no build step).
- `tests/` — all pytest tests. New tests go here.
- `scripts/` — runnable QA/ops scripts (audits, feed inspector, locust). No pytest tests here.
- `sql/` — database schema, kept in sync with `app/database_writer.py`.

## Ground rules

1. **Never block the event loop.** All external I/O inside async paths must use `httpx` async calls or `asyncio.to_thread`. The sync `get_gtfs_routes()` must never touch the network.
2. **Never return empty.** `fetch_all_gtt_data()` and `fetch_gtt_vehicle_positions()` must never raise and never return empty structures — the map must never be blank.
3. **Every new external dependency needs a timeout.** No call to the outside world without an explicit timeout and a try/except that degrades gracefully.
4. **Escape everything.** The dashboard renders data coming from external feeds: always use the `esc()` helper before putting any string into `innerHTML`.
5. **Keep fallbacks in sync with reality.** The local fallback route list must only contain real GTT lines.

## Workflow

1. Create a branch: `git checkout -b feat/your-feature`.
2. Make your change with a minimal diff, matching the existing style (Italian log messages with emoji prefixes, snake_case, docstrings in Italian are welcome).
3. Run the full quality gate — all three must pass:

   ```bash
   pytest
   python scripts/run_ultimate_check.py
   python scripts/run_enterprise_audit.py
   ```

4. If you touched `app/database_writer.py`, also update `sql/schema.sql` and make sure the SQL stays idempotent.
5. Commit with a conventional message (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`).
6. Open a Pull Request against `main` and describe what changed and how you verified it.

## Adding a new data source

Wrap it in `app/gtfs_rt.py` or a dedicated module following the same pattern:

- async function, never raises, returns a list/dict;
- module-level `last_<name>_source` flag (`live` / `unavailable`);
- clear `print`/`log` lines for every phase, in Italian;
- TTL cache when the data does not change every cycle.

## Reporting bugs

Open an issue with: what you expected, what happened, how to reproduce it, and the terminal output (the `[SYNC]`, `[SCRAPER]`, `[GTFS-RT]`, `[WS]` logs are designed to make diagnosis easy).

## License

By contributing you agree that your contributions are licensed under the MIT license. Transit data remains CC BY 4.0, attribution GTT / City of Turin.
