# Guida al Contributo (CONTRIBUTING.md) 🤝

Grazie per voler contribuire a **GTT GTFS-RT Service Alerts Synthesizer**!  
Questo progetto nasce per migliorare la qualità dei dati del trasporto pubblico a Torino rendendoli aperti, standardizzati e fruibili da tutta la community.

## 🛠️ Come Contribuire
Accettiamo qualsiasi tipo di contributo:
- Segnalazione e Fix di Bug
- Nuove Funzionalità (es. geolocalizzazione fermate soppresse)
- Miglioramenti alla Documentazione e ai test

## 🔄 Workflow per le Pull Request
1. Fai un Fork del repository ufficiale su GitHub.
2. Clona il tuo fork in locale.
3. Crea un nuovo branch per la tua modifica:
~~~bash
git checkout -b feature/nome-funzionalita
~~~
4. Installa le dipendenze:
~~~bash
pip install -r requirements.txt
~~~
5. Esegui i test:
~~~bash
pytest
~~~
(Tutti i test devono passare con successo prima di inviare una Pull Request).
6. Fai il commit e il push, quindi apri una PR verso il ramo `main`.
