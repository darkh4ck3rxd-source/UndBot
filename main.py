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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
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
DB_PATH = os.getenv("DB_PATH", "bot1.sqlite3")
BOTTOM_CROP_PERCENT = 10.0

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
                created_at TEXT NOT NULL
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

    async def update(self, job_id: str, status: str) -> None:
        assert self.db is not None
        await self.db.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id))
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
bot2_job_lock = asyncio.Lock()

# Queues for incoming messages
bot2_queue = asyncio.Queue()
result_bot_queues = {} # username -> Queue

# --- Utilities ---
def parse_wait_time(text: str) -> int:
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

def extract_bot_and_payload(url: str):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    
    if parsed.scheme == "tg":
        bot = query.get("domain", [None])[0]
        payload = query.get("start", [None])[0]
    else:
        # https://t.me/BotUsername?start=payload
        path_parts = [p for p in parsed.path.split("/") if p]
        bot = path_parts[0] if path_parts else None
        payload = query.get("start", [None])[0]
        
    return bot, payload

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
        await update.get_bot().send_photo(
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
    global user_client, bot1_app, bot2_id
    job = await store.get(job_id)
    if not job: return

    tmp_dir = tempfile.mkdtemp(prefix=f"job-{job_id}-")
    in_img = os.path.join(tmp_dir, "input.jpg")
    
    try:
        logger.info("Processing job %s", job_id)
        path = await user_client.download_media(operator_msg, file=in_img)
        if not path: raise RuntimeError("Download failed")

        if bot1_app: await bot1_app.bot.send_message(job["user_chat_id"], PROCESSING_TEXT)

        async with bot2_job_lock:
            # Clear bot2 queue
            while not bot2_queue.empty(): bot2_queue.get_nowait()

            # 1. Trigger BOT2
            await user_client.send_message(bot2_id, "/start")
            
            # Wait for menu and click
            try:
                msg = await asyncio.wait_for(bot2_queue.get(), timeout=15)
                if msg.buttons: await msg.click(0, 0)
            except: pass

            # 2. Send file to BOT2
            await user_client.send_file(bot2_id, path)
            
            # 3. Listen for wait time and result ready
            result_bot_username = None
            start_payload = None
            
            start_time = datetime.now()
            while (datetime.now() - start_time).total_seconds() < 900: # 15 min max
                msg = await asyncio.wait_for(bot2_queue.get(), timeout=300)
                text = (msg.raw_text or "").lower()
                
                if "wait time" in text:
                    seconds = parse_wait_time(text)
                    if bot1_app:
                        adj = seconds + 5
                        m, s = divmod(adj, 60)
                        wait_str = f"{m} minute{'s' if m!=1 else ''} {s} second{'s' if s!=1 else ''}" if m > 0 else f"{s} seconds"
                        await bot1_app.bot.send_message(job["user_chat_id"], f"⏱ Estimated wait time: {wait_str}")
                
                if "image result has been sent" in text and msg.buttons:
                    # Found the result button!
                    for row in msg.buttons:
                        for btn in row:
                            if btn.url:
                                result_bot_username, start_payload = extract_bot_and_payload(btn.url)
                                break
                        if result_bot_username: break
                    if result_bot_username: break

            if not result_bot_username:
                raise RuntimeError("Result bot link not found in message")

            logger.info("Switching to result bot: %s with payload: %s", result_bot_username, start_payload)
            
            # 4. Handle Result Bot
            result_entity = await user_client.get_entity(result_bot_username)
            rb_id = result_entity.id
            
            # Create queue for this bot if not exists
            if rb_id not in result_bot_queues:
                result_bot_queues[rb_id] = asyncio.Queue()
            
            # Clear queue
            while not result_bot_queues[rb_id].empty(): result_bot_queues[rb_id].get_nowait()
            
            # Send /start to result bot
            cmd = f"/start {start_payload}" if start_payload else "/start"
            await user_client.send_message(result_entity, cmd)
            
            # 5. Wait for image
            try:
                res_msg = await asyncio.wait_for(result_bot_queues[rb_id].get(), timeout=120)
                res_path = os.path.join(tmp_dir, "result.jpg")
                out_path = os.path.join(tmp_dir, "final.jpg")
                
                await user_client.download_media(res_msg, file=res_path)
                crop_image(res_path, out_path)
                
                # Send to user
                if bot1_app:
                    with open(out_path, "rb") as f:
                        await bot1_app.bot.send_photo(job["user_chat_id"], photo=f, caption="🎉 Your image result is ready!")
                
                # Delete from result bot
                try: await user_client.delete_messages(result_entity, [res_msg.id], revoke=True)
                except: pass
                
                await store.update(job_id, "completed")
            except asyncio.TimeoutError:
                raise RuntimeError("Result image timeout")

    except Exception as e:
        logger.exception("Job %s failed", job_id)
        await store.update(job_id, f"failed: {str(e)}")
        if bot1_app: await bot1_app.bot.send_message(job["user_chat_id"], "❌ Processing failed. Please try again.")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

# --- Bridge Event Handlers ---
async def on_user_client_message(event):
    global bot2_id
    sid = event.chat_id
    logger.info("Bridge received message from chat_id: %s, text: %s", sid, (event.raw_text or "")[:50])
    
    if sid == OPERATOR_CHAT_ID:
        text = event.raw_text or ""
        match = re.search(rf"{re.escape(JOB_PREFIX)}([a-f0-9]+)", text, re.IGNORECASE)
        if match and event.message.media:
            asyncio.create_task(run_job_flow(match.group(1), event.message))
            
    elif sid == bot2_id:
        await bot2_queue.put(event.message)
        
    elif sid in result_bot_queues:
        if event.message.media:
            is_img = False
            if getattr(event.message, 'photo', None): is_img = True
            elif getattr(event.message, 'document', None):
                mime = getattr(event.message.document, 'mime_type', '')
                if mime.startswith('image/'): is_img = True
            if is_img:
                await result_bot_queues[sid].put(event.message)

async def main():
    global user_client, bot1_app, bot2_id
    await store.open()

    user_client = TelegramClient(StringSession(TELEGRAM_SESSION), TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await user_client.start()
    
    bot2_entity = await user_client.get_entity(BOT2_USERNAME)
    bot2_id = bot2_entity.id
    
    user_client.add_event_handler(on_user_client_message, events.NewMessage())

    bot1_app = Application.builder().token(BOT1_TOKEN).build()
    bot1_app.add_handler(CommandHandler("start", start_command))
    bot1_app.add_handler(CallbackQueryHandler(feature_button, pattern=f"^{MENU_CALLBACK}$"))
    bot1_app.add_handler(MessageHandler(filters.PHOTO, photo_handler))

    async with bot1_app:
        await bot1_app.initialize()
        await bot1_app.start()
        await bot1_app.updater.start_polling()
        logger.info("Bot is running...")
        while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
