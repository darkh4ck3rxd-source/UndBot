import asyncio
import logging
import os
import re
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qs, urlparse

import aiosqlite
from dotenv import load_dotenv
from PIL import Image
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("bot1")

BOT1_TOKEN = os.environ["BOT1_TOKEN"]
BOT1_USERNAME = os.getenv("BOT1_USERNAME", "dark_react3bot").lstrip("@").strip()
OPERATOR_CHAT_ID = int(os.environ["OPERATOR_CHAT_ID"])
TELEGRAM_API_ID = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELEGRAM_SESSION = os.environ["TELEGRAM_SESSION"]
BOT2_USERNAME = os.getenv("BOT2_USERNAME", "EasyAI94_Bot").lstrip("@").strip()
DB_PATH = os.getenv("DB_PATH", "bot1.sqlite3")
BOT2_TIMEOUT_SECONDS = int(os.getenv("BOT2_TIMEOUT_SECONDS", "900"))
BOTTOM_CROP_PERCENT = max(0.0, min(float(os.getenv("BOTTOM_CROP_PERCENT", "10")), 50.0))

MENU_CALLBACK = "request_und_image"
JOB_PREFIX = "BOT1JOB:"
RESULT_READY_MARKER = "image result has been sent"
VIEW_RESULT_BUTTON_TEXT = "view result"

PROCESSING_TEXT = "⏳ Your image has been sent for processing. Please wait."
RESULT_READY_TEXT = "🎉 Your image result is ready!"


class JobStore:
    def __init__(self, path: str):
        self.path = path
        self.db: Optional[aiosqlite.Connection] = None

    async def open(self) -> None:
        self.db = await aiosqlite.connect(self.path)
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                user_chat_id INTEGER NOT NULL,
                username TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                operator_message_id INTEGER,
                error TEXT,
                result_message TEXT
            )
            """
        )
        await self.db.commit()

    async def close(self) -> None:
        if self.db:
            await self.db.close()

    async def create(self, job_id: str, user_chat_id: int, username: str | None) -> None:
        assert self.db is not None
        await self.db.execute(
            "INSERT INTO jobs (id, user_chat_id, username, status, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                job_id,
                user_chat_id,
                username,
                "queued",
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        await self.db.commit()

    async def set_operator_message(self, job_id: str, message_id: int) -> None:
        assert self.db is not None
        await self.db.execute(
            "UPDATE jobs SET operator_message_id = ? WHERE id = ?",
            (message_id, job_id),
        )
        await self.db.commit()

    async def update(self, job_id: str, status: str, error: str | None = None, result_message: str | None = None) -> None:
        assert self.db is not None
        await self.db.execute(
            "UPDATE jobs SET status = ?, error = ?, result_message = ? WHERE id = ?",
            (status, error, result_message, job_id),
        )
        await self.db.commit()

    async def get(self, job_id: str) -> Optional[dict]:
        assert self.db is not None
        cursor = await self.db.execute(
            "SELECT id, user_chat_id, username, status, created_at, operator_message_id, error, result_message FROM jobs WHERE id = ?",
            (job_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if not row:
            return None
        keys = [
            "id",
            "user_chat_id",
            "username",
            "status",
            "created_at",
            "operator_message_id",
            "error",
            "result_message",
        ]
        return dict(zip(keys, row))


store = JobStore(DB_PATH)
user_client: TelegramClient | None = None
bot1_app: Application | None = None
bot2_entity = None
bot2_job_lock = asyncio.Lock()


def menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("UND***S IMAGE", callback_data=MENU_CALLBACK)]]
    )


def job_caption(job_id: str, user: object) -> str:
    username = getattr(user, "username", None)
    display = f"@{username}" if username else str(getattr(user, "id", "unknown"))
    return f"{JOB_PREFIX}{job_id}\nUser: {display}"


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    context.user_data["awaiting_image"] = False
    await update.message.reply_text(
        "Please select the feature you want to use:",
        reply_markup=menu_markup(),
    )


async def feature_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.message:
        return
    await query.answer()
    context.user_data["awaiting_image"] = True
    await query.message.reply_text("Please send the image you want to process.")


async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message or not update.message.photo:
        return
    if not context.user_data.get("awaiting_image"):
        await update.message.reply_text(
            "First select `UND***S IMAGE` from /start.",
            parse_mode="Markdown",
            reply_markup=menu_markup(),
        )
        return

    context.user_data["awaiting_image"] = False
    job_id = uuid.uuid4().hex[:12]
    user = update.effective_user
    username = user.username
    await store.create(job_id, user.id, username)

    try:
        sent = await update.get_bot().send_photo(
            chat_id=OPERATOR_CHAT_ID,
            photo=update.message.photo[-1].file_id,
            caption=job_caption(job_id, user),
        )
        await store.set_operator_message(job_id, sent.message_id)
        await update.message.reply_text(
            "✅ Image received. It has been forwarded and processing will start shortly."
        )
    except Exception as exc:
        logger.exception("Could not forward job %s to operator", job_id)
        await store.update(job_id, "forward_failed", error=str(exc))
        await update.message.reply_text(
            "❌ Image forward nahi ho payi. Please try again later."
        )


async def wait_for_bot2_message(predicate, timeout: int = 60, from_entity=None):
    if user_client is None:
        raise RuntimeError("Human Telegram client is not ready")

    source = from_entity or bot2_entity
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    async def handler(event):
        try:
            message = event.message
            if predicate(message) and not future.done():
                future.set_result(message)
        except Exception as exc:
            if not future.done():
                future.set_exception(exc)

    user_client.add_event_handler(handler, events.NewMessage(from_users=source))
    try:
        return await asyncio.wait_for(future, timeout=timeout)
    finally:
        user_client.remove_event_handler(handler)


async def wait_for_image_messages(from_entity, timeout: int, count: int = 1):
    if user_client is None:
        raise RuntimeError("Human Telegram client is not ready")

    loop = asyncio.get_running_loop()
    future = loop.create_future()
    messages = []

    def is_image_message(message) -> bool:
        if getattr(message, "photo", None):
            return True
        document = getattr(message, "document", None)
        mime_type = getattr(document, "mime_type", "") if document else ""
        return bool(mime_type and mime_type.startswith("image/"))

    async def handler(event):
        try:
            message = event.message
            if is_image_message(message):
                messages.append(message)
                logger.info("Received image %d/%d from %s", len(messages), count, str(from_entity))
                if len(messages) >= count and not future.done():
                    future.set_result(messages[:count])
        except Exception as exc:
            if not future.done():
                future.set_exception(exc)

    user_client.add_event_handler(handler, events.NewMessage(from_users=from_entity))
    try:
        return await asyncio.wait_for(future, timeout=timeout)
    finally:
        user_client.remove_event_handler(handler)


def parse_and_add_time(text: str, extra_seconds: int = 5) -> Optional[str]:
    minutes = 0
    seconds = 0
    min_match = re.search(r"(\d+)\s*min", text, re.IGNORECASE)
    sec_match = re.search(r"(\d+)\s*sec", text, re.IGNORECASE)
    
    if not min_match and not sec_match:
        return None
        
    if min_match: minutes = int(min_match.group(1))
    if sec_match: seconds = int(sec_match.group(1))
        
    total_seconds = minutes * 60 + seconds + extra_seconds
    
    new_minutes = total_seconds // 60
    new_seconds = total_seconds % 60
    
    parts = []
    if new_minutes > 0:
        parts.append(f"{new_minutes} minute{'s' if new_minutes > 1 else ''}")
    if new_seconds > 0:
        parts.append(f"{new_seconds} second{'s' if new_seconds > 1 else ''}")
        
    return " ".join(parts) if parts else "a few seconds"


def crop_bottom(input_path: str, output_path: str) -> int:
    with Image.open(input_path) as source:
        source = source.convert("RGB")
        width, height = source.size
        crop_pixels = int(height * BOTTOM_CROP_PERCENT / 100)
        crop_pixels = min(max(crop_pixels, 0), max(height - 1, 0))
        cropped = source.crop((0, 0, width, height - crop_pixels))
        cropped.save(output_path, format="JPEG", quality=95, optimize=True)
        return crop_pixels


async def run_bot2_flow(job_id: str, operator_event: events.NewMessage.Event) -> None:
    global user_client, bot2_entity, bot1_app
    job = await store.get(job_id)
    if not job or job["status"] != "queued":
        return

    temporary_dir = tempfile.mkdtemp(prefix=f"bot1-{job_id}-")
    image_path = os.path.join(temporary_dir, "input-image")

    try:
        await store.update(job_id, "bridge_received")
        logger.info("Downloading media for job %s", job_id)
        
        downloaded_path = await user_client.download_media(operator_event.message, file=image_path)
        if not downloaded_path or not os.path.exists(downloaded_path):
            downloaded_path = await user_client.download_media(operator_event.message.media, file=image_path)

        if not downloaded_path or not os.path.exists(downloaded_path):
            raise RuntimeError(f"Image download failed")
            
        image_path = downloaded_path

        if bot1_app is not None:
            await bot1_app.bot.send_message(job["user_chat_id"], PROCESSING_TEXT)

        async with bot2_job_lock:
            logger.info("Starting BOT2 flow for job %s", job_id)
            
            # 1. Start listener for wait time
            async def wait_time_predicate(message):
                raw = (message.raw_text or "").lower()
                return "wait time" in raw

            wait_time_waiter = asyncio.create_task(wait_for_bot2_message(wait_time_predicate, timeout=45))
            
            # 2. Trigger BOT2 menu
            menu_waiter = asyncio.create_task(
                wait_for_bot2_message(
                    lambda message: "select the feature" in (message.raw_text or "").lower(),
                    timeout=30,
                )
            )
            await user_client.send_message(bot2_entity, "/start")
            try:
                menu_message = await menu_waiter
                if menu_message.buttons:
                    await menu_message.click(0, 0)
                    await asyncio.sleep(1)
            except:
                logger.warning("Menu not received, trying to send file anyway")

            # 3. Send file to BOT2
            await user_client.send_file(bot2_entity, image_path)
            await store.update(job_id, "sent_to_bot2")

            # 4. Get wait time and calculate sleep
            sleep_duration = 60 # Default
            try:
                wait_time_message = await wait_time_waiter
                raw_text = wait_time_message.raw_text or ""
                logger.info("Wait time message: %s", raw_text)
                
                minutes = 0
                seconds = 0
                min_match = re.search(r"(\d+)\s*min", raw_text, re.IGNORECASE)
                sec_match = re.search(r"(\d+)\s*sec", raw_text, re.IGNORECASE)
                if min_match: minutes = int(min_match.group(1))
                if sec_match: seconds = int(sec_match.group(1))
                
                actual_wait = minutes * 60 + seconds
                sleep_duration = actual_wait + 5
                
                adjusted_str = parse_and_add_time(raw_text, extra_seconds=5)
                if bot1_app:
                    await bot1_app.bot.send_message(job["user_chat_id"], f"⏱ Estimated wait time: {adjusted_str}")
            except:
                logger.warning("No wait time message, using default sleep")
                if bot1_app:
                    await bot1_app.bot.send_message(job["user_chat_id"], "⏱ Estimated wait time: 1 minute")

            # 5. Sleep as instructed
            logger.info("Sleeping for %d seconds before checking result bot", sleep_duration)
            await asyncio.sleep(sleep_duration)

            # 6. Switch to Result Bot
            result_bot_username = "EasyAIResult6_Bot"
            result_entity = await user_client.get_entity(result_bot_username)
            
            # Start image listener
            image_waiter = asyncio.create_task(wait_for_image_messages(result_entity, timeout=180, count=1))
            
            # Trigger result bot
            await user_client.send_message(result_entity, "/start")
            
            # 7. Wait for result image
            try:
                result_messages = await image_waiter
                result_message = result_messages[0]
                
                result_path_base = os.path.join(temporary_dir, "result")
                cropped_result_path = os.path.join(temporary_dir, "cropped.jpg")
                
                downloaded_res = await user_client.download_media(result_message, file=result_path_base)
                if not downloaded_res:
                    downloaded_res = await user_client.download_media(result_message.media, file=result_path_base)
                
                if not downloaded_res:
                    raise RuntimeError("Result download failed")
                
                crop_pixels = crop_bottom(downloaded_res, cropped_result_path)
                
                # Send to user
                if bot1_app:
                    with open(cropped_result_path, "rb") as f:
                        await bot1_app.bot.send_photo(job["user_chat_id"], photo=f, caption="🎉 Your image result is ready!")
                
                # Delete from result bot
                try:
                    await user_client.delete_messages(result_entity, [result_message.id], revoke=True)
                    logger.info("Deleted result image")
                except:
                    pass
                    
                await store.update(job_id, "completed")
                logger.info("Job %s finished", job_id)
                
            except asyncio.TimeoutError:
                raise RuntimeError("Result image not received in result bot")

    except Exception as exc:
        logger.exception("Job %s failed", job_id)
        await store.update(job_id, "failed", error=str(exc))
        if bot1_app:
            await bot1_app.bot.send_message(job["user_chat_id"], "❌ Processing failed. Please try again.")

    finally:
        shutil.rmtree(temporary_dir, ignore_errors=True)


async def operator_message_handler(event) -> None:
    text = event.raw_text or ""
    match = re.search(rf"{re.escape(JOB_PREFIX)}([a-f0-9]+)", text, re.IGNORECASE)
    if not match or not event.message.media:
        return
    job_id = match.group(1)
    job = await store.get(job_id)
    if not job or job["status"] != "queued":
        return
    asyncio.create_task(run_bot2_flow(job_id, event))


async def main() -> None:
    global user_client, bot1_app, bot2_entity
    await store.open()

    user_client = TelegramClient(StringSession(TELEGRAM_SESSION), TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await user_client.start()
    logger.info("Human bridge connected")
    
    bot2_entity = await user_client.get_entity(BOT2_USERNAME)
    user_client.add_event_handler(operator_message_handler, events.NewMessage(from_users=OPERATOR_CHAT_ID))

    bot1_app = Application.builder().token(BOT1_TOKEN).build()
    bot1_app.add_handler(CommandHandler("start", start_command))
    bot1_app.add_handler(CallbackQueryHandler(feature_button, pattern=f"^{MENU_CALLBACK}$"))
    bot1_app.add_handler(MessageHandler(filters.PHOTO, photo_handler))

    async with bot1_app:
        await bot1_app.initialize()
        await bot1_app.start()
        await bot1_app.updater.start_polling()
        logger.info("BOT1 is running")
        
        while True:
            await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
