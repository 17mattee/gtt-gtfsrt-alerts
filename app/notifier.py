import os
import logging
import requests

logger = logging.getLogger("GTT-Notifier")

def send_telegram_alert(message: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        logger.warning("Token o Chat ID non configurati. Notifica Telegram saltata.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        # Testo semplice: con parse_mode Markdown un messaggio contenente
        # caratteri come '_' o '[' (tipico nei messaggi d'errore con path
        # o nomi host) veniva rifiutato da Telegram con 400 e la notifica
        # non partiva MAI, in silenzio.
        "text": f"[GTT Alert Monitor]\n\n{message}",
    }
    
    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Errore nell'invio della notifica Telegram: {e}")
