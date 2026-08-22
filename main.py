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
BOT1_USERNAME = os.getenv("BOT1_USERNAME", "").lstrip("@").strip()
OPERATOR_CHAT_ID = int(os.environ["OPERATOR_CHAT_ID"])
TELEGRAM_API_ID = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELEGRAM_SESSION = os.environ["TELEGRAM_SESSION"]
BOT2_USERNAME = os.getenv("BOT2_USERNAME", "EasyAI94_Bot").lstrip("@").strip()
DB_PATH = os.getenv("DB_PATH", "bot1.sqlite3")
BOT2_TIMEOUT_SECONDS = int(os.getenv("BOT2_TIMEOUT_SECONDS", "900"))
BOTTOM_CROP_PERCENT = max(0.0, min(float(os.getenv("BOTTOM_CROP_PERCENT", "5")), 50.0))

MENU_CALLBACK = "request_und_image"
JOB_PREFIX = "BOT1JOB:"
RESULT_READY_MARKER = "Your image result is ready"
VIEW_RESULT_BUTTON_TEXT = "view result"

PROCESSING_TEXT = "⏳ Your image has been sent for processing. Please wait."
RESULT_READY_TEXT = (
    "🎉 Your image result is ready!\n\n"
    "The clear result is available in the result bot."
)


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
    if user_client is None or bot2_entity is None:
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

    builder = events.NewMessage(from_users=source)
    user_client.add_event_handler(handler, builder)
    try:
        return await asyncio.wait_for(future, timeout=timeout)
    finally:
        user_client.remove_event_handler(handler, builder)


async def wait_for_two_image_messages(from_entity, timeout: int):
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
                if len(messages) >= 2 and not future.done():
                    future.set_result(messages[:2])
        except Exception as exc:
            if not future.done():
                future.set_exception(exc)

    builder = events.NewMessage(from_users=from_entity)
    user_client.add_event_handler(handler, builder)
    try:
        return await asyncio.wait_for(future, timeout=timeout)
    finally:
        user_client.remove_event_handler(handler, builder)


def _button_text(button) -> str:
    return str(getattr(button, "text", "") or "").strip().lower()


async def click_view_result_button(message):
    if user_client is None:
        raise RuntimeError("Human Telegram client is not ready")
    if not message.buttons:
        raise RuntimeError("BOT2 result-ready message has no buttons")

    for row_index, row in enumerate(message.buttons):
        for column_index, button in enumerate(row):
            if VIEW_RESULT_BUTTON_TEXT not in _button_text(button):
                continue

            url = getattr(button, "url", None)
            if url:
                parsed = urlparse(url)
                query = parse_qs(parsed.query)
                if parsed.scheme == "tg":
                    target = query.get("domain", [None])[0]
                else:
                    path_parts = [part for part in parsed.path.split("/") if part]
                    target = path_parts[0] if path_parts else None
                if not target:
                    raise RuntimeError(f"Could not resolve View Result URL: {url}")
                target_entity = await user_client.get_entity(target)
                payload = query.get("start", query.get("startapp", [None]))[0]
                command = "/start"
                await user_client.send_message(target_entity, command)
                return target_entity

            await message.click(row_index, column_index)
            return bot2_entity

    raise RuntimeError("BOT2 result-ready message has no View Result button")


def parse_and_add_time(text: str, extra_seconds: int = 15) -> Optional[str]:
    # Match patterns like "40 seconds", "1 minute", "2 minutes 30 seconds"
    # Simplified: just look for the first occurrence of minutes/seconds
    minutes = 0
    seconds = 0
    
    min_match = re.search(r"(\d+)\s*minute", text, re.IGNORECASE)
    sec_match = re.search(r"(\d+)\s*second", text, re.IGNORECASE)
    
    if not min_match and not sec_match:
        return None
        
    if min_match:
        minutes = int(min_match.group(1))
    if sec_match:
        seconds = int(sec_match.group(1))
        
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
        logger.info("Attempting to download media for job %s from message %s. Chat: %s", job_id, operator_event.message.id, operator_event.chat_id)
        
        if not operator_event.message.media:
            logger.error("Message %s has no media", operator_event.message.id)
            raise RuntimeError("Message has no media")
        
        # Log message details to debug accessibility
        logger.info("Message media type: %s", type(operator_event.message.media))
        
        downloaded_path = await user_client.download_media(operator_event.message, file=image_path)
        logger.info("Download result for job %s: %s", job_id, downloaded_path)
        
        if not downloaded_path or not os.path.exists(downloaded_path):
            # Try alternative: download from the photo/document directly if possible
            logger.warning("Standard download failed for job %s, trying alternative...", job_id)
            downloaded_path = await user_client.download_media(operator_event.message.media, file=image_path)
            logger.info("Alternative download result for job %s: %s", job_id, downloaded_path)

        if not downloaded_path or not os.path.exists(downloaded_path):
            logger.error("All download attempts failed for job %s. Path: %s", job_id, downloaded_path)
            raise RuntimeError(f"Image download failed. Path: {downloaded_path}")
            
        image_path = downloaded_path

        if bot1_app is not None:
            await bot1_app.bot.send_message(job["user_chat_id"], PROCESSING_TEXT)

        async with bot2_job_lock:
            logger.info("Starting BOT2 flow for job %s", job_id)
            menu_waiter = asyncio.create_task(
                wait_for_bot2_message(
                    lambda message: "Please select the feature you want to use" in (message.raw_text or ""),
                    timeout=60,
                )
            )
            await user_client.send_message(bot2_entity, "/start")
            menu_message = await menu_waiter

            if not menu_message.buttons:
                raise RuntimeError("BOT2 feature menu did not contain inline buttons")
            await menu_message.click(0, 0)
            await asyncio.sleep(0.8)

            result_waiter = asyncio.create_task(
                wait_for_bot2_message(
                    lambda message: RESULT_READY_MARKER.lower() in (message.raw_text or "").lower(),
                    timeout=BOT2_TIMEOUT_SECONDS,
                )
            )
            # Start a listener for the wait time message
            wait_time_waiter = asyncio.create_task(
                wait_for_bot2_message(
                    lambda message: "Estimated wait time" in (message.raw_text or ""),
                    timeout=30,
                )
            )

            await user_client.send_file(bot2_entity, image_path)
            await store.update(job_id, "sent_to_bot2")

            # Try to catch the wait time message
            try:
                wait_time_message = await wait_time_waiter
                adjusted_time = parse_and_add_time(wait_time_message.raw_text or "")
                if adjusted_time and bot1_app is not None:
                    await bot1_app.bot.send_message(
                        job["user_chat_id"],
                        f"⏱ Estimated wait time: {adjusted_time}"
                    )
            except asyncio.TimeoutError:
                logger.info("No wait time message received within 30s for job %s", job_id)
            except Exception:
                logger.exception("Error processing wait time message for job %s", job_id)

            ready_message = await result_waiter
            ready_text = ready_message.raw_text or RESULT_READY_TEXT
            await store.update(job_id, "result_ready", result_message=ready_text)

            # 1. Resolve the result bot (user specified @EasyAIResult6_Bot)
            result_bot_username = "EasyAIResult6_Bot"
            logger.info("Switching to result bot: %s", result_bot_username)
            result_entity = await user_client.get_entity(result_bot_username)
            
            # 2. Start listening for the image in the result bot
            image_waiter = asyncio.create_task(
                wait_for_two_image_messages(result_entity, BOT2_TIMEOUT_SECONDS)
            )
            
            # 3. Send /start to the result bot (or click button if preferred, but /start is safer)
            await user_client.send_message(result_entity, "/start")
            await store.update(job_id, "view_result_clicked")
            
            # 4. Wait for the images (usually one blurred preview and one clear result)
            result_messages = await image_waiter
            first_result, second_result = result_messages

            # 5. Download and process the clear result (usually the second one)
            second_result_path_base = os.path.join(temporary_dir, "second-result")
            cropped_result_path = os.path.join(temporary_dir, "processed-result.jpg")
            
            logger.info("Downloading clear result from %s", result_bot_username)
            downloaded_second_path = await user_client.download_media(second_result, file=second_result_path_base)
            
            if not downloaded_second_path or not os.path.exists(downloaded_second_path):
                logger.warning("Standard download failed for result, trying alternative...")
                downloaded_second_path = await user_client.download_media(second_result.media, file=second_result_path_base)

            if not downloaded_second_path or not os.path.exists(downloaded_second_path):
                raise RuntimeError("Clear result image download failed")
                
            second_result_path = downloaded_second_path
            crop_pixels = crop_bottom(second_result_path, cropped_result_path)
            await store.update(job_id, "completed", result_message=ready_text)

            if bot1_app is not None:
                await bot1_app.bot.send_photo(
                    job["user_chat_id"],
                    photo=cropped_result_path,
                    caption=(
                        "✅ Your processed image is ready.\n"
                        f"Bottom crop applied: {crop_pixels}px."
                    ),
                )

        logger.info("BOT2 result processed successfully for job %s", job_id)

    except asyncio.TimeoutError:
        error = "Timed out while waiting for BOT2 response"
        logger.error("%s: %s", job_id, error)
        await store.update(job_id, "timeout", error=error)
        if bot1_app is not None:
            await bot1_app.bot.send_message(
                job["user_chat_id"],
                "⚠️ Processing is taking longer than expected. Please try again later.",
            )
    except Exception as exc:
        logger.exception("BOT2 flow failed for job %s", job_id)
        await store.update(job_id, "failed", error=str(exc))
        if bot1_app is not None:
            await bot1_app.bot.send_message(
                job["user_chat_id"],
                "❌ Processing could not be completed. Please try again later.",
            )
    finally:
        shutil.rmtree(temporary_dir, ignore_errors=True)


def extract_job_id(text: str) -> Optional[str]:
    match = re.search(rf"{re.escape(JOB_PREFIX)}([a-f0-9]+)", text or "", re.IGNORECASE)
    return match.group(1) if match else None


async def operator_message_handler(event) -> None:
    text = event.raw_text or ""
    logger.info("Received message from operator chat. Text: %s", text[:50])
    job_id = extract_job_id(text)
    if not job_id:
        logger.debug("No job ID found in message")
        return
    if not event.message.media:
        logger.debug("Message for job %s has no media", job_id)
        return
    job = await store.get(job_id)
    if not job or job["status"] != "queued":
        return
    asyncio.create_task(run_bot2_flow(job_id, event))


async def main() -> None:
    global user_client, bot1_app, bot2_entity

    await store.open()

    user_client = TelegramClient(
        StringSession(TELEGRAM_SESSION),
        TELEGRAM_API_ID,
        TELEGRAM_API_HASH,
    )
    await user_client.start()
    me = await user_client.get_me()
    logger.info("Human Telegram bridge connected as %s", getattr(me, "username", None) or me.id)

    bot2_entity = await user_client.get_entity(BOT2_USERNAME)
    if BOT1_USERNAME:
        bot1_entity = await user_client.get_entity(BOT1_USERNAME)
        user_client.add_event_handler(
            operator_message_handler,
            events.NewMessage(from_users=bot1_entity),
        )
    else:
        logger.warning("BOT1_USERNAME is empty; operator bridge listener is disabled")

    bot1_app = Application.builder().token(BOT1_TOKEN).build()
    bot1_app.add_handler(CommandHandler("start", start_command))
    bot1_app.add_handler(CallbackQueryHandler(feature_button, pattern=f"^{MENU_CALLBACK}$"))
    bot1_app.add_handler(MessageHandler(filters.PHOTO, photo_handler))

    await bot1_app.initialize()
    await bot1_app.start()
    await bot1_app.updater.start_polling(drop_pending_updates=False)
    logger.info("BOT1 is running")

    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    finally:
        if bot1_app.updater:
            await bot1_app.updater.stop()
        await bot1_app.stop()
        await bot1_app.shutdown()
        await user_client.disconnect()
        await store.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
