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
    from google.cloud.firestore_v1.base_query import FieldFilter

    docs = db.collection("highlightUsers").where(filter=FieldFilter("active", "==", True)).stream()
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
MAX_MESSAGES_FOR_SUMMARY = 150  # pengaman: batasi payload biar tidak terlalu besar
GEMINI_MODELS = ["gemini-2.5-flash", "gemini-2.0-flash-001", "gemini-flash-latest"]  # fallback berurutan


def _call_gemini(model: str, prompt: str) -> str | None:
    max_retries = 2
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}",
                headers={"Content-Type": "application/json"},
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 503 and attempt < max_retries:
                wait = 5 * attempt
                print(f"[_call_gemini:{model}] 503, retry {attempt}/{max_retries} setelah {wait}s...")
                time.sleep(wait)
                continue
            print(f"[_call_gemini:{model}] gagal: {e}")
            return None
        except Exception as e:
            print(f"[_call_gemini:{model}] gagal: {e}")
            return None
    return None


def summarize_conversation(all_messages_text: str) -> str | None:
    prompt = (
        "Berikut kumpulan pesan dari grup diskusi saham hari ini. "
        "Buat ringkasan singkat (maks 5-7 poin bullet) dalam bahasa Indonesia, "
        "mencakup: topik/saham yang dibahas, sentimen umum, dan hal penting yang disebut.\n\n"
        f"{all_messages_text}"
    )
    for model in GEMINI_MODELS:
        result = _call_gemini(model, prompt)
        if result:
            return result
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


def send_telegram_document(text: str, filename: str, caption: str = ""):
    """Kirim teks sebagai file .txt — dipakai untuk fallback saat semua model Gemini gagal,
    biar user tinggal download & copy-paste manual ke ChatGPT/tool lain."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        resp = requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
            files={"document": (filename, text.encode("utf-8"), "text/plain")},
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"[send_telegram_document] gagal: {e}")


# ── Topic filter & reply helpers ─────────────────────────────────────────
def is_in_target_topic(msg) -> bool:
    """
    Topic "General" (topic_id=1) itu kasus khusus: pesan yang diposting
    langsung ke General biasanya TIDAK punya msg.reply_to sama sekali
    (beda dari topic lain yang selalu ada reply_to_top_id).
    Jadi kalau target topic-nya General, anggap match kalau reply_to kosong
    ATAU top_id-nya memang 1.
    """
    if not msg.reply_to:
        return TG_TOPIC_ID == 1

    top_id = getattr(msg.reply_to, "reply_to_top_id", None) or msg.reply_to.reply_to_msg_id
    return top_id == TG_TOPIC_ID


def get_genuine_reply_id(msg):
    """
    Bedakan antara reply_to yang cuma penanda topic thread (reply_to_top_id)
    dengan reply beneran ke pesan spesifik (reply_to_msg_id berbeda dari top_id).
    Return message id yang di-reply kalau ini reply beneran, None kalau bukan.
    """
    if not msg.reply_to:
        return None
    reply_msg_id = getattr(msg.reply_to, "reply_to_msg_id", None)
    top_id = getattr(msg.reply_to, "reply_to_top_id", None) or reply_msg_id
    if reply_msg_id and reply_msg_id != top_id:
        return reply_msg_id
    return None


# ── Main ingest logic ─────────────────────────────────────────────────────
async def run_ingest():
    client = TelegramClient(StringSession(TG_SESSION_STRING), TG_API_ID, TG_API_HASH)
    await client.start()

    last_run = get_last_run_timestamp()
    highlight_ids = get_highlight_user_ids()

    print(f"[debug] last_run timestamp = {last_run} ({datetime.datetime.utcfromtimestamp(last_run)} UTC)")
    print(f"[debug] highlight_ids = {highlight_ids}")

    all_texts = []
    highlight_entries = []  # list of (sender_name, text, reply_id)
    msg_cache = {}  # msg.id -> (sender_name, text), untuk resolve reply tanpa API call ekstra
    raw_count = 0
    topic_match_count = 0

    async for msg in client.iter_messages(TG_GROUP_ID, limit=1000):
        raw_count += 1
        if msg.date.timestamp() < last_run:
            break  # pesan sudah lama, stop (Telethon urut dari terbaru ke lama)
        if not is_in_target_topic(msg):
            continue
        topic_match_count += 1
        if not msg.text:
            continue

        sender_name = getattr(msg.sender, "first_name", "Unknown") if msg.sender else "Unknown"
        msg_cache[msg.id] = (sender_name, msg.text)
        all_texts.append(f"{sender_name}: {msg.text}")

        if msg.sender_id in highlight_ids:
            reply_id = get_genuine_reply_id(msg)
            highlight_entries.append((sender_name, msg.text, reply_id))

    print(f"[debug] pesan mentah dicek (sebelum stop by date) = {raw_count}")
    print(f"[debug] pesan yang match topic filter = {topic_match_count}")

    # Resolve konteks reply untuk highlight (pakai cache dulu, fallback fetch kalau tidak ada)
    highlight_messages = []
    for sender_name, text, reply_id in highlight_entries:
        context_line = ""
        if reply_id:
            if reply_id in msg_cache:
                reply_sender, reply_text = msg_cache[reply_id]
            else:
                try:
                    reply_msg = await client.get_messages(TG_GROUP_ID, ids=reply_id)
                    reply_sender = (
                        getattr(reply_msg.sender, "first_name", "Unknown") if reply_msg and reply_msg.sender else "Unknown"
                    )
                    reply_text = reply_msg.text if reply_msg and reply_msg.text else "(pesan tanpa teks/media)"
                except Exception:
                    reply_sender, reply_text = None, None

            if reply_text:
                snippet = reply_text if len(reply_text) <= 150 else reply_text[:150] + "..."
                context_line = f"↪️ _membalas {reply_sender}: \"{snippet}\"_\n"

        highlight_messages.append(f"{context_line}*{sender_name}*: {text}")

    await client.disconnect()

    # 1. Ringkasan keseluruhan obrolan (batasi ke N pesan terbaru biar payload tidak kebesaran)
    # all_texts urut dari terbaru->terlama (sesuai urutan iter_messages), jadi ambil dari depan
    texts_for_summary = all_texts[:MAX_MESSAGES_FOR_SUMMARY] if all_texts else []
    payload_text = "\n".join(reversed(texts_for_summary))

    print(f"[debug] jumlah pesan dikirim ke Gemini = {len(texts_for_summary)}")
    print(f"[debug] panjang teks payload = {len(payload_text)} karakter")
    print(f"[debug] preview payload (300 karakter pertama):\n{payload_text[:300]}")
    print(f"[debug] preview payload (300 karakter terakhir):\n{payload_text[-300:]}")

    summary = summarize_conversation(payload_text) if texts_for_summary else None

    # 2. Susun pesan final
    final_message = "📊 *Ringkasan Grup Saham Hari Ini*\n\n"

    if summary:
        final_message += summary
    elif texts_for_summary:
        final_message += (
            "_Gagal generate ringkasan otomatis (semua model Gemini gagal). "
            "Teks mentah obrolan dikirim sebagai file di bawah — bisa di-copy manual ke ChatGPT._"
        )
    else:
        final_message += "_Tidak ada pesan baru sejak run terakhir._"

    if highlight_messages:
        final_message += "\n\n⭐ *Pesan dari User Tertentu (Highlight)*\n\n"
        final_message += "\n\n".join(reversed(highlight_messages))

    send_telegram_message(final_message)

    if not summary and texts_for_summary:
        today_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        send_telegram_document(
            payload_text,
            filename=f"obrolan-mentah-{today_str}.txt",
            caption="Teks mentah obrolan hari ini (fallback karena ringkasan Gemini gagal).",
        )

    set_last_run_timestamp(int(time.time()))

    print(f"Selesai. {len(all_texts)} pesan diproses, {len(highlight_messages)} highlight ditemukan.")


if __name__ == "__main__":
    asyncio.run(run_ingest())
