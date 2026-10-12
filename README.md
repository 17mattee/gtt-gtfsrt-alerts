# GTT GTFS-RT Service Alerts Synthesizer 🚌🇮🇹

Engine open source e middleware ad alte prestazioni che converte gli avvisi di deviazione non strutturati del trasporto pubblico di Torino (GTT) in un feed binario standard **GTFS-Realtime ServiceAlerts (v2.0 Protobuf)**.

## 📌 Descrizione
Il progetto risolve uno dei principali problemi dell'infomobilità a Torino: la mancanza di un feed standardizzato in tempo reale per deviazioni, scioperi, cantieri e cortei. Il middleware effettua lo scraping e il parsing degli avvisi ufficiali GTT, valida le linee interessate incrociandole con il dataset GTFS statico rilasciato da **5T Torino**, e genera un feed `alerts.pb` conforme alle specifiche di Google, Moovit, OpenTripPlanner e Apple Maps.

## 📌 Description
GTT Turin GTFS-RT Middleware
Overview

GTT Turin GTFS-RT Middleware is an enterprise-grade, high-performance, and zero-budget software infrastructure designed to bridge the gap between Turin's public transport provider (GTT) and the international standard for real-time passenger information: GTFS-Realtime (Protobuf).

Public transport agencies often provide service alerts and route disruptions in unstructured formats (such as raw HTML web pages). This middleware automatically scrapes, parses, normalizes, and translates these alerts into a standardized Protocol Buffer (.pb) stream, making it fully compatible with third-party navigation apps, mapping software, and transit monitoring systems.

Key Architecture & Technical Highlights
Resilience and Fault Tolerance (tenacity): The scraper and database connectors implement intelligent exponential backoff retry mechanisms. If the GTT source portal or the cloud database experiences temporary latency or micro-outages, the system handles the retries gracefully without dropping states or crashing.
Long-Term Historical Persistence (Supabase PostgreSQL): Beyond real-time conversion, every single service disruption is permanently archived in a cloud PostgreSQL database (hosted on Supabase) via IPv4 session pooling, enabling deep historical analysis and tracking of public transport reliability.
Thread-Safe In-Memory Caching (threading.Lock): To eliminate race conditions between the background scheduler (which refreshes alerts periodically) and incoming HTTP consumer requests, the internal cache is fully thread-locked, guaranteeing 100% data integrity and precision.
Network Optimization (GZipMiddleware): Large Protocol Buffer payloads are automatically compressed on-the-fly using Gzip, significantly minimizing network latency and bandwidth consumption for client applications.
Security & DoS Protection (slowapi): Core endpoints are protected by strict IP-based rate limiting to prevent abuse, scraping floods, or denial-of-service (DoS) attempts against the public server.
Proactive Monitoring (Telegram Bot): In the rare event of an unrecoverable upstream parsing error or database timeout, the system autonomously dispatches structured alert notifications directly via Telegram, ensuring immediate awareness for maintainers.
Core Technology Stack
Language: Python 3.14+
Web Framework: FastAPI, Uvicorn (ASGI)
Data Processing: BeautifulSoup4, Pandas, Google GTFS Realtime Bindings (protobuf)
Database & Persistence: PostgreSQL, Supabase (Session Pooler)
Resilience & Rate Limiting: Tenacity, SlowApi
Testing Suite: Pytest, HTTPX, Responses (Full unit, integration, and mocking coverage)
Deployment & Containerization: Docker, Render Cloud PaaS
API Endpoints & Interfaces
GET / (Interactive Dashboard): A lightweight HTML status page displaying live metrics, system health, active alert counts, and security configurations.
GET /gtfs-rt/alerts.pb (Official GTFS-RT Feed): The primary production endpoint returning a raw binary stream (application/x-protobuf) conforming to the official Google GTFS-Realtime specification.
GET /docs (Swagger OpenAPI): Fully auto-generated interactive documentation for developers looking to explore or integrate with the API.




## ⚡ Caratteristiche Principali
- **Parsing Multilinea & Cause**: Riconosce automaticamente scioperi, cantieri, cortei e manifestazioni sportive.
- **Validazione Open Data 5T**: Incrocia i codici linea con il file statico `routes.txt` di 5T Torino.
- **Cache ad Alte Prestazioni**: Aggiornamento in background tramite `APScheduler` con risposte HTTP sub-millisecondo.
- **Architettura Asincrona Moderna**: Sviluppato in **FastAPI** con gestione `lifespan` asincrona (pronto per Python 3.11+).

## 🏗️ Architettura e Struttura del Progetto

Il codice è organizzato in modo modulare all'interno del package `app/`:

```text
gtt-gtfsrt-alerts/
├── app/
│   ├── __init__.py
│   ├── main.py          # Core FastAPI, Lifespan, Gzip, Thread-Lock & Scheduler
│   ├── scraper.py       # Estrazione HTML (BeautifulSoup) e parsing semantico
│   ├── database.py      # Connettore Supabase PostgreSQL con retry (psycopg v3)
│   ├── notifier.py      # Sistema di allerta asincrono via Telegram Bot
│   └── gtfs_static.py   # Gestione e validazione dataset statico GTFS (5T Torino)
├── tests/               # Suite di test avanzata (API, Resilienza, Rate-Limiting)
├── gtfs_data/           # Dati statici di riferimento
├── Dockerfile           # Configurazione container di produzione
├── requirements.txt     # Dipendenze bloccate per versione
└── README.md

🚀 Caratteristiche Tecniche Principali
Standard GTFS-Realtime Conforme: Genera feed binari strutturati in protocol buffer (.pb) interpretando cause (STRIKE, CONSTRUCTION, ecc.) ed effetti (DETOUR, NO_SERVICE) direttamente dai testi degli avvisi GTT.

Resilienza e Fault Tolerance (tenacity): Politiche di retry con exponential backoff sia sulle chiamate di scraping verso il portale GTT che sulle transazioni di scrittura verso il database.

Persistenza Storica a Lungo Termine (Supabase): Archiviazione automatica di ogni deviazione all'interno della tabella PostgreSQL gtt_alerts_history tramite connessione sicura con Session Pooler su IPv4 (porta 6543).

Thread Safety & Performance: Gestione concorrente della cache in memoria protetta da threading.Lock per prevenire race conditions, affiancata dal middleware GZipMiddleware per comprimere i payload di rete.

Sicurezza e Protezione Anti-Hacker (slowapi): Limitazione rigida delle richieste per indirizzo IP per prevenire attacchi di tipo Denial of Service (DoS).

Monitoraggio in Tempo Reale: Notifiche push automatiche su Telegram in caso di errori critici dello scraper o del database.


🛠️ Stack Tecnologico
Linguaggio: Python 3.14+
Framework Web: FastAPI & Uvicorn
Elaborazione Dati: BeautifulSoup4, Pandas, Protobuf (gtfs-realtime-bindings)
Database: PostgreSQL / Supabase
Resilienza & Sicurezza: Tenacity, SlowApi
Testing: Pytest, HTTPX, Responses

Deployment: Render (Cloud PaaS) / Docker

## 🚀 Installazione Rapida

Clona il repository:
~~~bash
git clone https://github.com/17mattee/gtt-gtfsrt-alerts.git
cd gtt-gtfsrt-alerts
~~~

Crea e attiva l'ambiente virtuale:
~~~bash
python3 -m venv venv
source venv/bin/activate
~~~

Installa le dipendenze:
~~~bash
pip install -r requirements.txt
~~~

Configura le variabili d'ambiente:
Crea un file .env nella root del progetto:

TELEGRAM_BOT_TOKEN="il_tuo_token_bot"
TELEGRAM_CHAT_ID="il_tuo_chat_id"
DATABASE_URL="postgresql://postgres.project_ref:password@aws-0-eu-central-1.pooler.supabase.com:6543/postgres"


Avvia il server:
~~~bash
uvicorn main:app --reload --port 8000
~~~

Il feed sarà subito disponibile all'indirizzo: `http://127.0.0.1:8000/gtfs-rt/alerts.pb`


🧪 Esecuzione dei Test
Il progetto dispone di una suite di test multilivello che coprono validazione delle rotte, test di carico e mocking dei servizi esterni:






## 📄 Licenza
Rilasciato sotto licenza MIT.


Endpoint API Disponibili
GET / — Dashboard Web Informativa: Interfaccia HTML di stato del middleware e delle metriche di caching.
GET /gtfs-rt/alerts.pb — Feed Protobuf Ufficiale: Endpoint consumabile da qualsiasi client di mobilità per leggere gli avvisi in tempo reale.

GET /docs — Documentazione Interattiva: Swagger UI autogenerata per esplorare e testare le API.









🇬🇧 English Version
First of all, thank you for taking the time to contribute! 🎉
🚀 How to Contribute
Fork the Repository and clone your fork locally.

1 Create a Virtual Environment & Install Dependencies:
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt


2 Create a Feature Branch: git checkout -b feature/amazing-feature
3 Run the Test Suite: Ensure all tests pass successfully with:  pytest -v.
4 Commit & Push: Push your changes to your fork and open a Pull Request against the main branch: git push origin feature/amazing-feature
