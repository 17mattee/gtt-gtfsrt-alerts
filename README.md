cat << 'EOF' > README.md
# GTT GTFS-RT Service Alerts Synthesizer 🚌🇮🇹

[![CI/CD Pipeline](https://github.com/17mattee/gtt-gtfsrt-alerts/actions/workflows/ci.yml/badge.svg)](https://github.com/17mattee/gtt-gtfsrt-alerts/actions/workflows/ci.yml)
[![CodeQL Analysis](https://github.com/17mattee/gtt-gtfsrt-alerts/actions/workflows/codeql.yml/badge.svg)](https://github.com/17mattee/gtt-gtfsrt-alerts/actions/workflows/codeql.yml)
[![Python 3.14](https://img.shields.io/badge/python-3.14-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-green.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An enterprise-grade, high-performance middleware designed to scrape, semantically parse, validate, and serve GTT (Gruppo Torinese Trasporti) public transport alerts as standard **GTFS-Realtime (Protobuf)** feeds.

---

## 📌 Overview & Architecture

Public transport agencies often provide service alerts and route disruptions in unstructured formats (such as raw HTML web pages). This middleware automatically scrapes, parses, normalizes, and translates these alerts into a standardized Protocol Buffer (`.pb`) stream, making it fully compatible with third-party navigation apps, mapping software, and transit monitoring systems (Google Maps, Apple Maps, Moovit, OpenTripPlanner).

### 🔑 Key Technical Highlights
- **Resilience & Fault Tolerance (Tenacity & Circuit Breaker):** Intelligent exponential backoff retry mechanisms coupled with a `pybreaker` circuit breaker to protect the scraping pipeline against upstream GTT portal outages.
- **Distributed Caching & In-Memory Fallback:** Redis-backed caching layer with automatic thread-safe in-memory fallback (`threading.Lock`) to eliminate race conditions and support zero-latency horizontal scaling.
- **Cold Start Resiliency:** Automatic fallback and cache rehydration from a persistent PostgreSQL (Supabase) history store upon container startup.
- **Long-Term Historical Persistence:** Every service disruption is permanently archived in a cloud PostgreSQL database via session pooling for deep reliability analytics.
- **Observability & Monitoring:** Built-in Prometheus instrumentation exposing `/metrics`, a comprehensive health-check diagnostic endpoint (`/health`), and automated Telegram alert notifications for critical failures.
- **Security & Rate-Limiting:** SlowAPI IP-based rate limiting protecting core public endpoints against abuse and Denial of Service (DoS) attacks.
- **Network Optimization:** Native Gzip response compression (`GZipMiddleware`) to minimize payload latency and bandwidth consumption.

---

## ⚡ Caratteristiche Principali (Features)
- **Parsing Multilinea & Cause/Effetti:** Riconoscimento semantico avanzato di scioperi (`STRIKE`), cantieri (`CONSTRUCTION`), manifestazioni (`DEMONSTRATION`), incidenti (`ACCIDENT`), deviazioni (`DETOUR`) e soppressioni (`NO_SERVICE`).
- **Validazione Open Data 5T:** Incrocia rigorosamente i codici linea estratti con il dataset statico `routes.txt` di **5T Torino**.
- **Architettura Asincrona Moderna:** Sviluppato in **FastAPI** con gestione `lifespan` asincrona.

---

## 📡 API Endpoints & Interfaces

| Endpoint | Method | Description |
| :--- | :---: | :--- |
| `/` | `GET` | Enterprise Web Dashboard & Status Overview |
| `/gtfs-rt/alerts.pb` | `GET` | Official GTFS-Realtime Binary Feed (Protobuf) |
| `/health` | `GET` | System Health Diagnostics & DB/Redis Status (JSON) |
| `/metrics` | `GET` | Prometheus Observability Metrics |
| `/docs` | `GET` | Interactive Swagger OpenAPI Documentation |

---

## 🏗️ Project Structure

```text
gtt-gtfsrt-alerts/
├── app/
│   ├── __init__.py
│   ├── main.py          # Core FastAPI, Lifespan, Redis, Prometheus & Scheduler
│   ├── scraper.py       # BeautifulSoup Scraping, Circuit Breaker & Semantic Parsing
│   ├── database.py      # Supabase PostgreSQL Conductor with Pooling & Cold Start
│   ├── notifier.py      # Asynchronous Telegram Alert Bot
│   └── gtfs_static.py   # GTFS Static Data Validation (5T Turin)
├── tests/               # Comprehensive Pytest suite (API, Resilienza, Container testing)
├── .github/workflows/   # GitHub Actions CI/CD Pipeline & CodeQL Security Scanning
├── Dockerfile           # Production Container Configuration
├── requirements.txt     # Locked Dependency Manifest
└── README.md





🚀 Quick Start & Installation

Clone the repository:
git clone [https://github.com/17mattee/gtt-gtfsrt-alerts.git](https://github.com/17mattee/gtt-gtfsrt-alerts.git)
cd gtt-gtfsrt-alerts

Create and activate a virtual environment:
python3 -m venv venv
source venv/bin/activate

Install dependencies:
pip install -r requirements.txt

Configure Environment Variables:
DATABASE_URL="postgresql://postgres.project_ref:password@aws-0-eu-central-1.pooler.supabase.com:6543/postgres"
REDIS_URL="redis://default:password@host:port"
TELEGRAM_BOT_TOKEN="your_telegram_bot_token"
TELEGRAM_CHAT_ID="your_telegram_chat_id"

Run tests with coverage:
pytest --cov=app -v

Start the development server:
uvicorn app.main:app --reload --port 8000

🤝 How to Contribute
First of all, thank you for taking the time to contribute! 🎉
Fork the Repository and clone your fork locally.
Create a Feature Branch: git checkout -b feature/amazing-feature
Run the Test Suite: Ensure all tests pass successfully with pytest --cov=app -v.
Commit & Push: Push your changes to your fork and open a Pull Request against the main branch: git push origin feature/amazing-feature