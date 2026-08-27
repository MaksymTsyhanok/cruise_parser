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

backfill_raw = os.getenv("BACKFILL_LAST_MESSAGES", "17").strip()
DB_PATH = os.getenv("DB_PATH", "processed_posts.db").strip()

# --- XO TO: мониторинг каналов туроператоров (контент-план для агентов XO ТО) ---
channels_xo_to_raw = os.getenv(
    "CHANNELS_XO_TO", "unittravelua,dynamictravelservices,GoToOnline"
)
CHANNELS_XO_TO = [c.strip() for c in channels_xo_to_raw.split(",") if c.strip()]

N8N_WEBHOOK_URL_XO_TO = os.getenv("N8N_WEBHOOK_URL_XO_TO", "").strip()

# --- BEST CRUISES: отбор лучших круизных постов для @TourBonjur ---
channels_best_cruises_raw = os.getenv(
    "CHANNELS_BEST_CRUISES", "Chcruises,cruise_ukraine,cruise_4gates,apltravel"
)
CHANNELS_BEST_CRUISES = [c.strip() for c in channels_best_cruises_raw.split(",") if c.strip()]

N8N_WEBHOOK_URL_BEST_CRUISES = os.getenv("N8N_WEBHOOK_URL_BEST_CRUISES", "").strip()

# --- KNOCKOUT OFFER: анализ тур-постов конкурентов для сборки "нокаут-оффера" клиенту ---
channels_knockout_raw = os.getenv(
    "CHANNELS_KNOCKOUT", "unittravelua,joinupfm,GoToOnline,dynamictravelservices"
)
CHANNELS_KNOCKOUT = [c.strip() for c in channels_knockout_raw.split(",") if c.strip()]

N8N_WEBHOOK_URL_KNOCKOUT = os.getenv("N8N_WEBHOOK_URL_KNOCKOUT", "").strip()


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

    # --- XO TO: отдельная таблица дедупликации ---
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

    # --- BEST CRUISES: отдельная таблица дедупликации ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS best_cruises_processed_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT NOT NULL,
            post_id INTEGER NOT NULL,
            sent_at TEXT NOT NULL,
            n8n_status INTEGER,
            UNIQUE(channel, post_id)
        )
    """)

    # --- KNOCKOUT OFFER: отдельная таблица дедупликации ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS knockout_offer_processed_posts (
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


def is_processed_generic(table, channel, post_id):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT 1 FROM {table}
        WHERE channel = ? AND post_id = ?
        LIMIT 1
        """,
        (channel, post_id),
    )

    result = cur.fetchone()
    conn.close()

    return result is not None


def mark_processed_generic(table, channel, post_id, status_code):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        f"""
        INSERT OR IGNORE INTO {table} (
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

def send_to_n8n_generic(stream_label, table, webhook_url, payload):
    """Общая отправка для потоков с engagement-схемой (XO TO / Best Cruises / Knockout)."""
    channel = payload.get("channel")
    post_id = payload.get("post_id")

    if is_processed_generic(table, channel, post_id):
        print(f"Skip duplicate ({stream_label}): {channel} {post_id}")
        return

    if not webhook_url:
        print(f"Webhook not set for {stream_label}, skipping: {channel} {post_id}")
        return

    try:
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=20,
        )

        status_code = response.status_code
        print(f"Sent to n8n ({stream_label}): {status_code} | {channel} | {post_id}")

        if 200 <= status_code < 300:
            mark_processed_generic(table, channel, post_id, status_code)
        else:
            print(f"n8n ({stream_label}) returned error status: {status_code}")
            print(response.text[:500])

    except Exception as e:
        print(f"Error sending to n8n ({stream_label}): {e}")


def send_to_n8n_xo_to(payload):
    send_to_n8n_generic("XO TO", "xo_to_processed_posts", N8N_WEBHOOK_URL_XO_TO, payload)


def send_to_n8n_best_cruises(payload):
    send_to_n8n_generic(
        "Best Cruises", "best_cruises_processed_posts", N8N_WEBHOOK_URL_BEST_CRUISES, payload
    )


def send_to_n8n_knockout(payload):
    send_to_n8n_generic(
        "Knockout Offer", "knockout_offer_processed_posts", N8N_WEBHOOK_URL_KNOCKOUT, payload
    )


# =========================
# TELEGRAM PAYLOAD
# =========================

def extract_reactions(message):
    """Возвращает {emoji: count}. Пустой dict если реакций нет или скрыты."""
    reactions = {}
    if getattr(message, "reactions", None) and message.reactions.results:
        for r in message.reactions.results:
            emoji = getattr(r.reaction, "emoticon", None) or "custom"
            reactions[emoji] = r.count
    return reactions


async def build_payload_engagement(message, chat, chat_id):
    """Общий payload с метриками вовлечённости — используется XO TO / Best Cruises / Knockout."""
    text = message.message or ""

    if not text.strip():
        return None

    source_channel = getattr(chat, "username", None) or str(chat_id)

    source_link = None
    if getattr(chat, "username", None):
        source_link = f"https://t.me/{source_channel}/{message.id}"

    return {
        "channel": source_channel,
        "post_id": message.id,
        "date": message.date.isoformat() if message.date else None,
        "text": text,
        "source_link": source_link,
        "views": getattr(message, "views", 0) or 0,
        "forwards": getattr(message, "forwards", 0) or 0,
        "comments": (
            message.replies.replies if getattr(message, "replies", None) else 0
        ),
        "reactions": extract_reactions(message),
        "reactions_total": sum(extract_reactions(message).values()),
    }


async def handle_new_engagement_message(sender, event):
    """Общий обработчик для потоков XO TO / Best Cruises / Knockout."""
    try:
        message = event.message
        chat = await event.get_chat()

        payload = await build_payload_engagement(message, chat, event.chat_id)

        if payload:
            print(f"New post ({sender.__name__}): {payload['channel']} {payload['post_id']}")
            sender(payload)

    except Exception as e:
        print(f"Error handling new message ({sender.__name__}): {e}")


# =========================
# MAIN
# =========================

async def watch_engagement_stream(label, channels, sender):
    """Валидирует каналы, делает backfill и возвращает список валидных entity."""
    valid_entities = []

    for channel in channels:
        try:
            entity = await client.get_entity(channel)
            valid_entities.append(entity)

            print(f"Watching ({label}): {channel}")

            if BACKFILL_LAST_MESSAGES > 0:
                print(f"Checking last {BACKFILL_LAST_MESSAGES} posts from {channel} ({label})...")

                async for message in client.iter_messages(
                    entity,
                    limit=BACKFILL_LAST_MESSAGES,
                ):
                    payload = await build_payload_engagement(message, entity, entity.id)

                    if payload:
                        sender(payload)

        except Exception as e:
            print(f"Cannot watch {label} channel: {channel} | Error: {e}")

    return valid_entities


async def main():
    init_db()

    print("Cruise parser starting...")
    print(f"Channels (XO TO): {CHANNELS_XO_TO}")
    print(f"Channels (Best Cruises): {CHANNELS_BEST_CRUISES}")
    print(f"Channels (Knockout Offer): {CHANNELS_KNOCKOUT}")
    print(f"Backfill last messages: {BACKFILL_LAST_MESSAGES}")
    print(f"DB path: {DB_PATH}")

    await client.start()

    print("Telegram client connected.")

    valid_entities_xo_to = await watch_engagement_stream("XO TO", CHANNELS_XO_TO, send_to_n8n_xo_to)
    valid_entities_best_cruises = await watch_engagement_stream(
        "Best Cruises", CHANNELS_BEST_CRUISES, send_to_n8n_best_cruises
    )
    valid_entities_knockout = await watch_engagement_stream(
        "Knockout Offer", CHANNELS_KNOCKOUT, send_to_n8n_knockout
    )

    if (
        not valid_entities_xo_to
        and not valid_entities_best_cruises
        and not valid_entities_knockout
    ):
        print("No valid channels to watch. Stopping.")
        return

    if valid_entities_xo_to:
        client.add_event_handler(
            lambda event: handle_new_engagement_message(send_to_n8n_xo_to, event),
            events.NewMessage(chats=valid_entities_xo_to),
        )

    if valid_entities_best_cruises:
        client.add_event_handler(
            lambda event: handle_new_engagement_message(send_to_n8n_best_cruises, event),
            events.NewMessage(chats=valid_entities_best_cruises),
        )

    if valid_entities_knockout:
        client.add_event_handler(
            lambda event: handle_new_engagement_message(send_to_n8n_knockout, event),
            events.NewMessage(chats=valid_entities_knockout),
        )

    print("Now waiting for new Telegram posts...")

    await client.run_until_disconnected()


if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
