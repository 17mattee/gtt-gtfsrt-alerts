import os
import requests

def send_telegram_alert(message: str):
    # Legge token e chat_id dalle variabili d'ambiente per massima sicurezza
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        print("[Notifier] Token o Chat ID non configurati. Notifica saltata.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": f"🚨 **GTT Alert Monitor**\n\n{message}",
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
    except Exception as e:
        print(f"[Notifier] Errore nell'invio della notifica Telegram: {e}")
