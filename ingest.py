"""
TG-HL-01: Ringkasan Obrolan & Highlight User Tertentu (Grup Saham)

Alur:
1. Baca lastRunTimestamp & whitelist highlight user dari Firestore.
2. Ambil pesan grup Telegram (topic tertentu saja) sejak run terakhir.
3. Ringkas keseluruhan obrolan via Gemini API.
4. Kumpulkan pesan dari user whitelist apa adanya (verbatim).
5. Kirim hasil ke Telegram (chat pribadi via bot).
6. Update lastRunTimestamp di Firestore.
"""

import os
import json
import time
import asyncio
import datetime

import requests
import firebase_admin
from firebase_admin import credentials, firestore
from telethon import TelegramClient
from telethon.sessions import StringSession

# ── Environment / Secrets ────────────────────────────────────────────────
TG_API_ID = int(os.environ["TG_API_ID"])
TG_API_HASH = os.environ["TG_API_HASH"]
TG_SESSION_STRING = os.environ["TG_SESSION_STRING"]
TG_GROUP_ID = int(os.environ["TG_GROUP_ID"])
TG_TOPIC_ID = int(os.environ["TG_TOPIC_ID"])

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

FIREBASE_CREDENTIALS_JSON = os.environ["FIREBASE_CREDENTIALS_JSON"]

# ── Firebase setup ───────────────────────────────────────────────────────
cred = credentials.Certificate(json.loads(FIREBASE_CREDENTIALS_JSON))
firebase_admin.initialize_app(cred)
db = firestore.client()

STATE_DOC = db.collection("ingestState").document("telegramStockGroup")


# ── Firestore helpers ────────────────────────────────────────────────────
def get_highlight_user_ids() -> set[int]:
    docs = db.collection("highlightUsers").where("active", "==", True).stream()
    return {doc.to_dict()["telegramUserId"] for doc in docs}


def _start_of_today_timestamp() -> int:
    now = datetime.datetime.utcnow()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start_of_day.timestamp())


def get_last_run_timestamp() -> int:
    doc = STATE_DOC.get()
    if doc.exists:
        return doc.to_dict().get("lastRunTimestamp", _start_of_today_timestamp())
    return _start_of_today_timestamp()


def set_last_run_timestamp(ts: int):
    STATE_DOC.set({"lastRunTimestamp": ts})


# ── Gemini summarization ─────────────────────────────────────────────────
def summarize_conversation(all_messages_text: str) -> str | None:
    prompt = (
        "Berikut kumpulan pesan dari grup diskusi saham hari ini. "
        "Buat ringkasan singkat (maks 5-7 poin bullet) dalam bahasa Indonesia, "
        "mencakup: topik/saham yang dibahas, sentimen umum, dan hal penting yang disebut.\n\n"
        f"{all_messages_text}"
    )
    try:
        response = requests.post(
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}",
            headers={"Content-Type": "application/json"},
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        print(f"[summarize_conversation] gagal: {e}")
        return None


# ── Telegram delivery ─────────────────────────────────────────────────────
def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chunk in [text[i : i + 4000] for i in range(0, len(text), 4000)]:
        try:
            resp = requests.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": chunk,
                    "parse_mode": "Markdown",
                },
                timeout=15,
            )
            resp.raise_for_status()
        except Exception as e:
            print(f"[send_telegram_message] gagal kirim chunk: {e}")


# ── Topic filter ──────────────────────────────────────────────────────────
def is_in_target_topic(msg) -> bool:
    if not msg.reply_to:
        return False
    top_id = getattr(msg.reply_to, "reply_to_top_id", None) or msg.reply_to.reply_to_msg_id
    return top_id == TG_TOPIC_ID


# ── Main ingest logic ─────────────────────────────────────────────────────
async def run_ingest():
    client = TelegramClient(StringSession(TG_SESSION_STRING), TG_API_ID, TG_API_HASH)
    await client.start()

    last_run = get_last_run_timestamp()
    highlight_ids = get_highlight_user_ids()

    all_texts = []
    highlight_messages = []

    async for msg in client.iter_messages(TG_GROUP_ID, limit=1000):
        if msg.date.timestamp() < last_run:
            break  # pesan sudah lama, stop (Telethon urut dari terbaru ke lama)
        if not is_in_target_topic(msg):
            continue
        if not msg.text:
            continue

        sender_name = getattr(msg.sender, "first_name", "Unknown") if msg.sender else "Unknown"
        all_texts.append(f"{sender_name}: {msg.text}")

        if msg.sender_id in highlight_ids:
            highlight_messages.append(f"*{sender_name}*: {msg.text}")

    await client.disconnect()

    # 1. Ringkasan keseluruhan obrolan
    summary = summarize_conversation("\n".join(reversed(all_texts))) if all_texts else None

    # 2. Susun pesan final
    final_message = "📊 *Ringkasan Grup Saham Hari Ini*\n\n"
    final_message += summary or "_Tidak ada pesan baru sejak run terakhir._"

    if highlight_messages:
        final_message += "\n\n⭐ *Pesan dari User Tertentu (Highlight)*\n\n"
        final_message += "\n\n".join(reversed(highlight_messages))

    send_telegram_message(final_message)
    set_last_run_timestamp(int(time.time()))

    print(f"Selesai. {len(all_texts)} pesan diproses, {len(highlight_messages)} highlight ditemukan.")


if __name__ == "__main__":
    asyncio.run(run_ingest())
