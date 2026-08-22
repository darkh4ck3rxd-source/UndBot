import asyncio
import logging
import os
import re
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict
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

# --- Configuration ---
BOT1_TOKEN = os.environ["BOT1_TOKEN"]
BOT1_USERNAME = os.getenv("BOT1_USERNAME", "dark_react3bot").lstrip("@").strip()
OPERATOR_CHAT_ID = int(os.environ["OPERATOR_CHAT_ID"])
TELEGRAM_API_ID = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELEGRAM_SESSION = os.environ["TELEGRAM_SESSION"]
BOT2_USERNAME = os.getenv("BOT2_USERNAME", "EasyAI94_Bot").lstrip("@").strip()
RESULT_BOT_USERNAME = "EasyAIResult6_Bot"
DB_PATH = os.getenv("DB_PATH", "bot1.sqlite3")
BOT2_TIMEOUT_SECONDS = int(os.getenv("BOT2_TIMEOUT_SECONDS", "900"))
BOTTOM_CROP_PERCENT = 10.0  # Force 10% as requested

MENU_CALLBACK = "request_und_image"
JOB_PREFIX = "BOT1JOB:"
PROCESSING_TEXT = "⏳ Your image has been sent for processing. Please wait."

# --- Database ---
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

    async def create(self, job_id: str, user_chat_id: int, username: str | None) -> None:
        assert self.db is not None
        await self.db.execute(
            "INSERT INTO jobs (id, user_chat_id, username, status, created_at) VALUES (?, ?, ?, ?, ?)",
            (job_id, user_chat_id, username, "queued", datetime.now(timezone.utc).isoformat()),
        )
        await self.db.commit()

    async def update(self, job_id: str, status: str, error: str | None = None) -> None:
        assert self.db is not None
        await self.db.execute("UPDATE jobs SET status = ?, error = ? WHERE id = ?", (status, error, job_id))
        await self.db.commit()

    async def get(self, job_id: str) -> Optional[dict]:
        assert self.db is not None
        cursor = await self.db.execute("SELECT id, user_chat_id, username, status FROM jobs WHERE id = ?", (job_id,))
        row = await cursor.fetchone()
        await cursor.close()
        return dict(zip(["id", "user_chat_id", "username", "status"], row)) if row else None

store = JobStore(DB_PATH)

# --- Global State ---
user_client: TelegramClient | None = None
bot1_app: Application | None = None
bot2_id = None
result_bot_id = None
bot2_job_lock = asyncio.Lock()

# Queues for incoming messages from the bots
bot2_msg_queue = asyncio.Queue()
result_bot_img_queue = asyncio.Queue()

# --- Utilities ---
def parse_wait_time(text: str) -> int:
    """Parses wait time from text and returns total seconds."""
    minutes = 0
    seconds = 0
    min_match = re.search(r"(\d+)\s*min", text, re.IGNORECASE)
    sec_match = re.search(r"(\d+)\s*sec", text, re.IGNORECASE)
    if min_match: minutes = int(min_match.group(1))
    if sec_match: seconds = int(sec_match.group(1))
    return minutes * 60 + seconds

def crop_image(input_path: str, output_path: str) -> int:
    with Image.open(input_path) as img:
        img = img.convert("RGB")
        w, h = img.size
        crop_h = int(h * BOTTOM_CROP_PERCENT / 100)
        cropped = img.crop((0, 0, w, h - crop_h))
        cropped.save(output_path, "JPEG", quality=95)
        return crop_h

# --- Bot Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            "Please select the feature you want to use:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("UND***S IMAGE", callback_data=MENU_CALLBACK)]])
        )

async def feature_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query and update.callback_query.message:
        await update.callback_query.answer()
        context.user_data["awaiting_image"] = True
        await update.callback_query.message.reply_text("Please send the image you want to process.")

async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message or not update.message.photo:
        return
    if not context.user_data.get("awaiting_image"):
        await update.message.reply_text("First select `UND***S IMAGE` from /start.")
        return

    context.user_data["awaiting_image"] = False
    job_id = uuid.uuid4().hex[:12]
    try:
        sent = await update.get_bot().send_photo(
            chat_id=OPERATOR_CHAT_ID,
            photo=update.message.photo[-1].file_id,
            caption=f"{JOB_PREFIX}{job_id}\nUser: {update.effective_user.id}",
        )
        await store.create(job_id, update.effective_user.id, update.effective_user.username)
        await update.message.reply_text("✅ Image received. Forwarded to operator.")
    except Exception as e:
        logger.exception("Forward failed")
        await update.message.reply_text("❌ Error forwarding image.")

# --- Bridge Logic ---
async def run_job_flow(job_id: str, operator_msg) -> None:
    global user_client, bot1_app, bot2_id, result_bot_id
    job = await store.get(job_id)
    if not job: return

    tmp_dir = tempfile.mkdtemp(prefix=f"job-{job_id}-")
    in_img = os.path.join(tmp_dir, "input.jpg")
    
    try:
        # 1. Download from operator
        path = await user_client.download_media(operator_msg, file=in_img)
        if not path: raise RuntimeError("Download failed")

        if bot1_app: await bot1_app.bot.send_message(job["user_chat_id"], PROCESSING_TEXT)

        async with bot2_job_lock:
            # Clear old messages from queues
            while not bot2_msg_queue.empty(): bot2_msg_queue.get_nowait()
            while not result_bot_img_queue.empty(): result_bot_img_queue.get_nowait()

            # 2. Trigger BOT2
            await user_client.send_message(bot2_id, "/start")
            
            # Wait for menu
            try:
                msg = await asyncio.wait_for(bot2_msg_queue.get(), timeout=20)
                if msg.buttons: await msg.click(0, 0)
            except: logger.warning("No menu from BOT2")

            # 3. Send file to BOT2
            await user_client.send_file(bot2_id, path)
            
            # 4. Wait for wait time message
            sleep_time = 60
            try:
                while True:
                    msg = await asyncio.wait_for(bot2_msg_queue.get(), timeout=30)
                    text = (msg.raw_text or "").lower()
                    if "wait time" in text:
                        seconds = parse_wait_time(text)
                        sleep_time = seconds + 5
                        if bot1_app:
                            parts = []
                            if (sleep_time // 60) > 0: parts.append(f"{sleep_time // 60} minute{'s' if (sleep_time // 60) > 1 else ''}")
                            if (sleep_time % 60) > 0: parts.append(f"{sleep_time % 60} second{'s' if (sleep_time % 60) > 1 else ''}")
                            wait_str = " ".join(parts) or "a few seconds"
                            await bot1_app.bot.send_message(job["user_chat_id"], f"⏱ Estimated wait time: {wait_str}")
                        break
            except: logger.warning("No wait time msg")

            # 5. Sleep
            logger.info("Job %s sleeping for %d seconds", job_id, sleep_time)
            await asyncio.sleep(sleep_time)

            # 6. Trigger Result Bot
            await user_client.send_message(result_bot_id, "/start")
            
            # 7. Wait for image from result bot
            try:
                res_msg = await asyncio.wait_for(result_bot_img_queue.get(), timeout=120)
                res_path = os.path.join(tmp_dir, "result.jpg")
                out_path = os.path.join(tmp_dir, "final.jpg")
                
                await user_client.download_media(res_msg, file=res_path)
                crop_image(res_path, out_path)
                
                # Send to user
                if bot1_app:
                    with open(out_path, "rb") as f:
                        await bot1_app.bot.send_photo(job["user_chat_id"], photo=f, caption="🎉 Your image result is ready!")
                
                # Delete from result bot
                try: await user_client.delete_messages(result_bot_id, [res_msg.id], revoke=True)
                except: pass
                
                await store.update(job_id, "completed")
            except asyncio.TimeoutError:
                raise RuntimeError("Result bot timeout")

    except Exception as e:
        logger.exception("Job %s failed", job_id)
        await store.update(job_id, "failed", error=str(e))
        if bot1_app: await bot1_app.bot.send_message(job["user_chat_id"], "❌ Processing failed. Please try again.")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

# --- Bridge Event Handlers ---
async def on_user_client_message(event):
    global bot2_id, result_bot_id
    if event.chat_id == OPERATOR_CHAT_ID:
        text = event.raw_text or ""
        match = re.search(rf"{re.escape(JOB_PREFIX)}([a-f0-9]+)", text, re.IGNORECASE)
        if match and event.message.media:
            asyncio.create_task(run_job_flow(match.group(1), event.message))
    elif event.chat_id == bot2_id:
        await bot2_msg_queue.put(event.message)
    elif event.chat_id == result_bot_id:
        if event.message.media:
            # Check if it's an image
            is_img = False
            if getattr(event.message, 'photo', None): is_img = True
            elif getattr(event.message, 'document', None):
                mime = getattr(event.message.document, 'mime_type', '')
                if mime.startswith('image/'): is_img = True
            
            if is_img:
                await result_bot_img_queue.put(event.message)
        else:
            # Might be a text message, ignore or log
            pass

async def main():
    global user_client, bot1_app, bot2_id, result_bot_id
    await store.open()

    user_client = TelegramClient(StringSession(TELEGRAM_SESSION), TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await user_client.start()
    
    bot2_entity = await user_client.get_entity(BOT2_USERNAME)
    bot2_id = bot2_entity.id
    result_bot_entity = await user_client.get_entity(RESULT_BOT_USERNAME)
    result_bot_id = result_bot_entity.id
    
    user_client.add_event_handler(on_user_client_message, events.NewMessage())

    bot1_app = Application.builder().token(BOT1_TOKEN).build()
    bot1_app.add_handler(CommandHandler("start", start_command))
    bot1_app.add_handler(CallbackQueryHandler(feature_button, pattern=f"^{MENU_CALLBACK}$"))
    bot1_app.add_handler(MessageHandler(filters.PHOTO, photo_handler))

    async with bot1_app:
        await bot1_app.initialize()
        await bot1_app.start()
        await bot1_app.updater.start_polling()
        logger.info("Bots are running...")
        while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
