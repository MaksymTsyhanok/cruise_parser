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


# =========================
# MAIN
# =========================

async def main():
    init_db()

    print("Cruise parser starting...")
    print(f"Channels: {CHANNELS}")
    print(f"Backfill last messages: {BACKFILL_LAST_MESSAGES}")
    print(f"DB path: {DB_PATH}")

    await client.start()

    print("Telegram client connected.")

    valid_entities = []

    for channel in CHANNELS:
        try:
            entity = await client.get_entity(channel)
            valid_entities.append(entity)

            print(f"Watching: {channel}")

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

    if not valid_entities:
        print("No valid channels to watch. Stopping.")
        return

    client.add_event_handler(
        handle_new_message,
        events.NewMessage(chats=valid_entities),
    )

    print("Now waiting for new Telegram posts...")

    await client.run_until_disconnected()


if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
