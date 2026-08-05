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
import base64
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
# Bisa isi lebih dari 1 topic, dipisah koma, contoh: "1,3"
TG_TOPIC_IDS = {int(x.strip()) for x in os.environ["TG_TOPIC_ID"].split(",")}

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
    """
    Awal hari dihitung berdasarkan WIB (UTC+7) — setara 01:00 WITA.
    """
    wib = datetime.timezone(datetime.timedelta(hours=7))
    now_wib = datetime.datetime.now(wib)
    start_of_day = now_wib.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start_of_day.timestamp())


def get_last_run_timestamp() -> int:
    doc = STATE_DOC.get()
    if doc.exists:
        return doc.to_dict().get("lastRunTimestamp", _start_of_today_timestamp())
    return _start_of_today_timestamp()


def set_last_run_timestamp(ts: int):
    STATE_DOC.set({"lastRunTimestamp": ts})


# ── Gemini summarization ─────────────────────────────────────────────────
MAX_MESSAGES_FOR_SUMMARY = 300  # dinaikkan dari 150 — grup sangat aktif (~1000 pesan/hari), beri buffer lebih besar
GEMINI_MODELS = ["gemini-2.0-flash-001", "gemini-flash-latest", "gemini-2.5-flash-lite"]  # fallback berurutan


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


def analyze_chart_image(image_bytes: bytes, caption: str) -> str | None:
    """
    Analisis gambar chart saham via Gemini Vision. Dipakai khusus untuk pesan
    highlight yang berupa foto (misal watchlist dari mentor di topic mentor).
    """
    prompt = (
        "Ini adalah gambar chart saham yang dikirim mentor ke grup diskusi saham. "
        f"Caption yang menyertai: \"{caption or '(tidak ada caption)'}\"\n\n"
        "Jelaskan singkat dalam bahasa Indonesia (2-4 kalimat): saham/ticker apa yang dimaksud "
        "(kalau terlihat di gambar atau caption), level harga penting yang terlihat (support/resistance/target), "
        "dan pola candlestick/tren yang tampak. Kalau ada elemen yang tidak jelas, sebutkan saja apa yang terlihat."
    )
    image_b64 = base64.b64encode(image_bytes).decode("utf-8")

    for model in GEMINI_MODELS:
        try:
            response = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}",
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [
                        {
                            "parts": [
                                {"text": prompt},
                                {"inline_data": {"mime_type": "image/jpeg", "data": image_b64}},
                            ]
                        }
                    ]
                },
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        except Exception as e:
            print(f"[analyze_chart_image:{model}] gagal: {e}")
            continue
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
                    # parse_mode SENGAJA tidak dipakai (plain text) — teks highlight berisi
                    # konten mentah dari user (bisa ada _ atau * yang bukan markup),
                    # kalau pakai parse_mode=Markdown itu bikin Telegram nolak seluruh
                    # pesan dengan 400 Bad Request kalau markup-nya tidak seimbang.
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
    Jadi kalau salah satu target topic-nya General, anggap match kalau
    reply_to kosong ATAU top_id-nya ada di TG_TOPIC_IDS.
    """
    if not msg.reply_to:
        return 1 in TG_TOPIC_IDS

    top_id = getattr(msg.reply_to, "reply_to_top_id", None) or msg.reply_to.reply_to_msg_id
    return top_id in TG_TOPIC_IDS


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

    wita = datetime.timezone(datetime.timedelta(hours=8))
    print(
        f"[debug] last_run timestamp = {last_run} "
        f"({datetime.datetime.fromtimestamp(last_run, datetime.timezone.utc)} UTC / "
        f"{datetime.datetime.fromtimestamp(last_run, wita)} WITA)"
    )
    print(f"[debug] highlight_ids = {highlight_ids}")
    print(f"[debug] TG_TOPIC_IDS = {TG_TOPIC_IDS}")

    all_texts = []
    highlight_entries = []  # list of (sender_name, text, reply_id)
    msg_cache = {}  # msg.id -> (sender_name, text), untuk resolve reply tanpa API call ekstra
    raw_count = 0
    topic_match_count = 0

    async for msg in client.iter_messages(TG_GROUP_ID, limit=3000):
        raw_count += 1
        if msg.date.timestamp() < last_run:
            break  # pesan sudah lama, stop (Telethon urut dari terbaru ke lama)
        if not is_in_target_topic(msg):
            continue
        topic_match_count += 1

        is_highlight_sender = msg.sender_id in highlight_ids
        has_photo = bool(msg.photo)

        # Pesan biasa tanpa teks & tanpa foto: skip. Tapi foto dari highlight user
        # tetap diproses meski caption kosong (misal watchlist chart tanpa caption).
        if not msg.text and not (is_highlight_sender and has_photo):
            continue

        sender_name = getattr(msg.sender, "first_name", "Unknown") if msg.sender else "Unknown"

        if msg.text:
            msg_cache[msg.id] = (sender_name, msg.text)
            all_texts.append(f"{sender_name}: {msg.text}")

        if is_highlight_sender:
            reply_id = get_genuine_reply_id(msg)
            highlight_entries.append((sender_name, msg.text or "", reply_id, msg if has_photo else None))

    print(f"[debug] pesan mentah dicek (sebelum stop by date) = {raw_count}")
    print(f"[debug] pesan yang match topic filter = {topic_match_count}")

    # Resolve konteks reply + analisis gambar untuk highlight (pakai cache dulu, fallback fetch kalau tidak ada)
    highlight_messages = []
    for sender_name, text, reply_id, photo_msg in highlight_entries:
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
                context_line = f"↪️ membalas {reply_sender}: \"{snippet}\"\n"

        image_analysis_line = ""
        if photo_msg is not None:
            try:
                image_bytes = await client.download_media(photo_msg, file=bytes)
                analysis = analyze_chart_image(image_bytes, text)
                if analysis:
                    image_analysis_line = f"🖼️ [Analisis chart] {analysis}\n"
                else:
                    image_analysis_line = "🖼️ [Ada gambar chart, tapi gagal dianalisis otomatis]\n"
            except Exception as e:
                print(f"[image_analysis] gagal download/analisis: {e}")
                image_analysis_line = "🖼️ [Ada gambar chart, tapi gagal diproses]\n"

        text_line = f"{sender_name}: {text}" if text else f"{sender_name}: (foto tanpa caption)"
        highlight_messages.append(f"{context_line}{text_line}\n{image_analysis_line}".rstrip())

    await client.disconnect()

    # 1. Ringkasan keseluruhan obrolan (batasi ke N pesan terbaru biar payload tidak kebesaran)
    # all_texts urut dari terbaru->terlama (sesuai urutan iter_messages), jadi ambil dari depan
    texts_for_summary = all_texts[:MAX_MESSAGES_FOR_SUMMARY] if all_texts else []
    if len(all_texts) > MAX_MESSAGES_FOR_SUMMARY:
        print(
            f"[debug] PERINGATAN: {len(all_texts)} pesan ditemukan, dipotong ke "
            f"{MAX_MESSAGES_FOR_SUMMARY} pesan terbaru untuk ringkasan (sisanya tidak diringkas)."
        )
    payload_text = "\n".join(reversed(texts_for_summary))

    print(f"[debug] jumlah pesan dikirim ke Gemini = {len(texts_for_summary)}")
    print(f"[debug] panjang teks payload = {len(payload_text)} karakter")
    print(f"[debug] preview payload (300 karakter pertama):\n{payload_text[:300]}")
    print(f"[debug] preview payload (300 karakter terakhir):\n{payload_text[-300:]}")

    summary = summarize_conversation(payload_text) if texts_for_summary else None

    # 2. Susun pesan final
    final_message = "📊 Ringkasan Obrolan Grup Saham (beberapa jam terakhir)\n\n"

    if summary:
        final_message += summary
    elif texts_for_summary:
        final_message += (
            "Gagal generate ringkasan otomatis (semua model Gemini gagal). "
            "Teks mentah obrolan dikirim sebagai file di bawah — bisa di-copy manual ke ChatGPT."
        )
    else:
        final_message += "Tidak ada pesan baru sejak run terakhir."

    if highlight_messages:
        final_message += "\n\n⭐ Pesan dari User Tertentu (Highlight)\n\n"
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
