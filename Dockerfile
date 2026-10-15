FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .

# gcc serve solo a compilare eventuali sorgenti durante l'installazione:
# viene rimosso nello stesso layer per non gonfiare l'immagine finale.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y gcc \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# Il container NON gira come root: un eventuale exploit ha permessi minimi.
RUN useradd --create-home --uid 10001 gtt
USER gtt

EXPOSE 8000

# La porta e' configurabile via env (Render/Koyeb/HF Spaces usano PORT).
# exec: uvicorn sostituisce la shell e resta il processo che riceve SIGTERM
# (shutdown pulito nei deploy), mantenendo l'espansione di ${PORT}.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
