import os
import sqlite3
from datetime import datetime, timezone

import requests
from telethon import TelegramClient, events
from telethon.sessions import StringSession


# =========================
# ENV VARIABLES
# =========================

api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()

SESSION_NAME = os.getenv("SESSION_NAME", "cruise_parser").strip()
TELEGRAM_SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "").strip()

N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "").strip()

channels_raw = os.getenv("CHANNELS", "cruise_ukraine,Chcruises")
CHANNELS = [channel.strip() for channel in channels_raw.split(",") if channel.strip()]

backfill_raw = os.getenv("BACKFILL_LAST_MESSAGES", "17").strip()
DB_PATH = os.getenv("DB_PATH", "processed_posts.db").strip()

# --- XO TO: мониторинг каналов туроператоров (отдельный поток, не Compare AI) ---
channels_xo_to_raw = os.getenv(
    "CHANNELS_XO_TO", "unittravelua,dynamictravelservices,GoToOnline"
)
CHANNELS_XO_TO = [c.strip() for c in channels_xo_to_raw.split(",") if c.strip()]

N8N_WEBHOOK_URL_XO_TO = os.getenv("N8N_WEBHOOK_URL_XO_TO", "").strip()


# =========================
# VALIDATION
# =========================

if not api_id_raw:
    raise ValueError("TELEGRAM_API_ID is empty. Add it in Railway Variables.")

if not api_id_raw.isdigit():
    raise ValueError(f"TELEGRAM_API_ID must be a number, got: {api_id_raw!r}")

if not API_HASH:
    raise ValueError("TELEGRAM_API_HASH is empty. Add it in Railway Variables.")

try:
    BACKFILL_LAST_MESSAGES = int(backfill_raw)
except ValueError:
    raise ValueError("BACKFILL_LAST_MESSAGES must be a number, for example 17 or 0.")

API_ID = int(api_id_raw)


# =========================
# TELEGRAM CLIENT
# =========================

if TELEGRAM_SESSION_STRING:
    session = StringSession(TELEGRAM_SESSION_STRING)
    print("Using TELEGRAM_SESSION_STRING.")
else:
    session = SESSION_NAME
    print(f"Using SESSION_NAME: {SESSION_NAME}")

client = TelegramClient(session, API_ID, API_HASH)


# =========================
# DATABASE
# =========================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_channel TEXT NOT NULL,
            telegram_message_id INTEGER NOT NULL,
            telegram_chat_id TEXT,
            source_link TEXT,
            message_date TEXT,
            sent_at TEXT NOT NULL,
            n8n_status INTEGER,
            UNIQUE(source_channel, telegram_message_id)
        )
    """)

    # --- XO TO: отдельная таблица дедупликации, не пересекается с Compare AI ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS xo_to_processed_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT NOT NULL,
            post_id INTEGER NOT NULL,
            sent_at TEXT NOT NULL,
            n8n_status INTEGER,
            UNIQUE(channel, post_id)
        )
    """)

    conn.commit()
    conn.close()


def is_processed(source_channel, telegram_message_id):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT 1 FROM processed_posts
        WHERE source_channel = ? AND telegram_message_id = ?
        LIMIT 1
        """,
        (source_channel, telegram_message_id),
    )

    result = cur.fetchone()
    conn.close()

    return result is not None


def mark_processed(payload, status_code):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT OR IGNORE INTO processed_posts (
            source_channel,
            telegram_message_id,
            telegram_chat_id,
            source_link,
            message_date,
            sent_at,
            n8n_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.get("source_channel"),
            payload.get("telegram_message_id"),
            str(payload.get("telegram_chat_id")),
            payload.get("source_link"),
            payload.get("date"),
            datetime.now(timezone.utc).isoformat(),
            status_code,
        ),
    )

    conn.commit()
    conn.close()


# --- XO TO: дедупликация в отдельной таблице ---

def is_processed_xo_to(channel, post_id):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT 1 FROM xo_to_processed_posts
        WHERE channel = ? AND post_id = ?
        LIMIT 1
        """,
        (channel, post_id),
    )

    result = cur.fetchone()
    conn.close()

    return result is not None


def mark_processed_xo_to(channel, post_id, status_code):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT OR IGNORE INTO xo_to_processed_posts (
            channel, post_id, sent_at, n8n_status
        ) VALUES (?, ?, ?, ?)
        """,
        (
            channel,
            post_id,
            datetime.now(timezone.utc).isoformat(),
            status_code,
        ),
    )

    conn.commit()
    conn.close()


# =========================
# N8N
# =========================

def send_to_n8n(payload):
    source_channel = payload.get("source_channel")
    message_id = payload.get("telegram_message_id")

    if is_processed(source_channel, message_id):
        print(f"Skip duplicate: {source_channel} {message_id}")
        return

    if not N8N_WEBHOOK_URL or N8N_WEBHOOK_URL == "https://placeholder.com":
        print(f"N8N_WEBHOOK_URL not set, skipping: {source_channel} {message_id}")
        return

    try:
        response = requests.post(
            N8N_WEBHOOK_URL,
            json=payload,
            timeout=20,
        )

        status_code = response.status_code
        print(f"Sent to n8n: {status_code} | {source_channel} | {message_id}")

        if 200 <= status_code < 300:
            mark_processed(payload, status_code)
        else:
            print(f"n8n returned error status: {status_code}")
            print(response.text[:500])

    except Exception as e:
        print(f"Error sending to n8n: {e}")


# --- XO TO: отправка на отдельный webhook (workflow "XO TO - 1. Ingest & Classify") ---

def send_to_n8n_xo_to(payload):
    channel = payload.get("channel")
    post_id = payload.get("post_id")

    if is_processed_xo_to(channel, post_id):
        print(f"Skip duplicate (XO TO): {channel} {post_id}")
        return

    if not N8N_WEBHOOK_URL_XO_TO:
        print(f"N8N_WEBHOOK_URL_XO_TO not set, skipping: {channel} {post_id}")
        return

    try:
        response = requests.post(
            N8N_WEBHOOK_URL_XO_TO,
            json=payload,
            timeout=20,
        )

        status_code = response.status_code
        print(f"Sent to n8n (XO TO): {status_code} | {channel} | {post_id}")

        if 200 <= status_code < 300:
            mark_processed_xo_to(channel, post_id, status_code)
        else:
            print(f"n8n (XO TO) returned error status: {status_code}")
            print(response.text[:500])

    except Exception as e:
        print(f"Error sending to n8n (XO TO): {e}")


# =========================
# TELEGRAM PAYLOAD
# =========================

async def build_payload(message, chat, chat_id):
    text = message.message or ""

    if not text.strip():
        return None

    source_channel = getattr(chat, "username", None) or str(chat_id)

    source_link = None
    if getattr(chat, "username", None):
        source_link = f"https://t.me/{source_channel}/{message.id}"

    return {
        "source_channel": source_channel,
        "text": text,
        "telegram_message_id": message.id,
        "telegram_chat_id": chat_id,
        "date": message.date.isoformat() if message.date else None,
        "source_link": source_link,
    }


# --- XO TO: payload с метриками вовлечённости (views, forwards, reactions, comments) ---

def extract_reactions(message):
    """Возвращает {emoji: count}. Пустой dict если реакций нет или скрыты."""
    reactions = {}
    if getattr(message, "reactions", None) and message.reactions.results:
        for r in message.reactions.results:
            emoji = getattr(r.reaction, "emoticon", None) or "custom"
            reactions[emoji] = r.count
    return reactions


async def build_payload_xo_to(message, chat, chat_id):
    text = message.message or ""

    if not text.strip():
        return None

    source_channel = getattr(chat, "username", None) or str(chat_id)

    return {
        "channel": source_channel,
        "post_id": message.id,
        "date": message.date.isoformat() if message.date else None,
        "text": text,
        "views": getattr(message, "views", 0) or 0,
        "forwards": getattr(message, "forwards", 0) or 0,
        "comments": (
            message.replies.replies if getattr(message, "replies", None) else 0
        ),
        "reactions": extract_reactions(message),
    }


async def handle_new_message(event):
    try:
        message = event.message
        chat = await event.get_chat()

        payload = await build_payload(message, chat, event.chat_id)

        if payload:
            print(
                f"New post: {payload['source_channel']} "
                f"{payload['telegram_message_id']}"
            )
            send_to_n8n(payload)

    except Exception as e:
        print(f"Error handling new message: {e}")


# --- XO TO: отдельный обработчик для каналов туроператоров ---

async def handle_new_xo_to_message(event):
    try:
        message = event.message
        chat = await event.get_chat()

        payload = await build_payload_xo_to(message, chat, event.chat_id)

        if payload:
            print(f"New XO TO post: {payload['channel']} {payload['post_id']}")
            send_to_n8n_xo_to(payload)

    except Exception as e:
        print(f"Error handling new XO TO message: {e}")


# =========================
# MAIN
# =========================

async def main():
    init_db()

    print("Cruise parser starting...")
    print(f"Channels (Compare AI): {CHANNELS}")
    print(f"Channels (XO TO): {CHANNELS_XO_TO}")
    print(f"Backfill last messages: {BACKFILL_LAST_MESSAGES}")
    print(f"DB path: {DB_PATH}")

    await client.start()

    print("Telegram client connected.")

    valid_entities = []

    for channel in CHANNELS:
        try:
            entity = await client.get_entity(channel)
            valid_entities.append(entity)

            print(f"Watching (Compare AI): {channel}")

            if BACKFILL_LAST_MESSAGES > 0:
                print(f"Checking last {BACKFILL_LAST_MESSAGES} posts from {channel}...")

                async for message in client.iter_messages(
                    entity,
                    limit=BACKFILL_LAST_MESSAGES,
                ):
                    payload = await build_payload(message, entity, entity.id)

                    if payload:
                        send_to_n8n(payload)

        except Exception as e:
            print(f"Cannot watch channel: {channel} | Error: {e}")

    # --- XO TO: тот же паттерн (валидация канала + backfill), отдельный список ---
    valid_entities_xo_to = []

    for channel in CHANNELS_XO_TO:
        try:
            entity = await client.get_entity(channel)
            valid_entities_xo_to.append(entity)

            print(f"Watching (XO TO): {channel}")

            if BACKFILL_LAST_MESSAGES > 0:
                print(f"Checking last {BACKFILL_LAST_MESSAGES} posts from {channel} (XO TO)...")

                async for message in client.iter_messages(
                    entity,
                    limit=BACKFILL_LAST_MESSAGES,
                ):
                    payload = await build_payload_xo_to(message, entity, entity.id)

                    if payload:
                        send_to_n8n_xo_to(payload)

        except Exception as e:
            print(f"Cannot watch XO TO channel: {channel} | Error: {e}")

    if not valid_entities and not valid_entities_xo_to:
        print("No valid channels to watch. Stopping.")
        return

    if valid_entities:
        client.add_event_handler(
            handle_new_message,
            events.NewMessage(chats=valid_entities),
        )

    if valid_entities_xo_to:
        client.add_event_handler(
            handle_new_xo_to_message,
            events.NewMessage(chats=valid_entities_xo_to),
        )

    print("Now waiting for new Telegram posts...")

    await client.run_until_disconnected()


if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
