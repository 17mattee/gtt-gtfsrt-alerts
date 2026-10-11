# GTT GTFS-RT Service Alerts Synthesizer 🚌🇮🇹

Engine open source e middleware ad alte prestazioni che converte gli avvisi di deviazione non strutturati del trasporto pubblico di Torino (GTT) in un feed binario standard **GTFS-Realtime ServiceAlerts (v2.0 Protobuf)**.

## 📌 Descrizione
Il progetto risolve uno dei principali problemi dell'infomobilità a Torino: la mancanza di un feed standardizzato in tempo reale per deviazioni, scioperi, cantieri e cortei. Il middleware effettua lo scraping e il parsing degli avvisi ufficiali GTT, valida le linee interessate incrociandole con il dataset GTFS statico rilasciato da **5T Torino**, e genera un feed `alerts.pb` conforme alle specifiche di Google, Moovit, OpenTripPlanner e Apple Maps.

## ⚡ Caratteristiche Principali
- **Parsing Multilinea & Cause**: Riconosce automaticamente scioperi, cantieri, cortei e manifestazioni sportive.
- **Validazione Open Data 5T**: Incrocia i codici linea con il file statico `routes.txt` di 5T Torino.
- **Cache ad Alte Prestazioni**: Aggiornamento in background tramite `APScheduler` con risposte HTTP sub-millisecondo.
- **Architettura Asincrona Moderna**: Sviluppato in **FastAPI** con gestione `lifespan` asincrona (pronto per Python 3.11+).

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

Avvia il server:
~~~bash
uvicorn main:app --reload --port 8000
~~~
Il feed sarà subito disponibile all'indirizzo: `http://127.0.0.1:8000/gtfs-rt/alerts.pb`

## 📄 Licenza
Rilasciato sotto licenza MIT.
