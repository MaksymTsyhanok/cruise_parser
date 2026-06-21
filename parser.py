import os
import sqlite3
from datetime import datetime, timezone

import requests
from telethon import TelegramClient, events

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_NAME = os.environ.get("SESSION_NAME", "cruise_parser")
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL", "")

CHANNELS = [
    "cruise_ukraine",
    "Chcruises",
]

BACKFILL_LAST_MESSAGES = 17

DB_PATH = "processed_posts.db"

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)


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
        (source_channel, telegram_message_id)
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
            source_channel, telegram_message_id, telegram_chat_id,
            source_link, message_date, sent_at, n8n_status
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
        )
    )
    conn.commit()
    conn.close()


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
        response = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=20)
        status_code = response.status_code
        print("Sent to n8n:", status_code, source_channel, message_id)
        if 200 <= status_code < 300:
            mark_processed(payload, status_code)
        else:
            print("Not marked as processed because n8n returned:", status_code)
    except Exception as e:
        print("Error sending to n8n:", e)


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
    message = event.message
    chat = await event.get_chat()
    payload = await build_payload(message, chat, event.chat_id)
    if payload:
        send_to_n8n(payload)


async def main():
    init_db()
    print("Cruise parser started.")
    valid_entities = []

    for channel in CHANNELS:
        try:
            entity = await client.get_entity(channel)
            valid_entities.append(entity)
            print("Watching:", channel)
            if BACKFILL_LAST_MESSAGES > 0:
                print(f"Sending last {BACKFILL_LAST_MESSAGES} posts from {channel} to n8n...")
                async for message in client.iter_messages(entity, limit=BACKFILL_LAST_MESSAGES):
                    payload = await build_payload(message, entity, entity.id)
                    if payload:
                        send_to_n8n(payload)
        except Exception as e:
            print("Cannot watch channel:", channel, "| Error:", e)

    if not valid_entities:
        print("No valid channels to watch.")
        return

    client.add_event_handler(
        handle_new_message,
        events.NewMessage(chats=valid_entities)
    )
    print("Now waiting for new posts...")
    await client.run_until_disconnected()


with client:
    client.loop.run_until_complete(main())
