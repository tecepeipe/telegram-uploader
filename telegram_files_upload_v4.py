# Uploads all files in subfolders to Telegram with captions, auto‑splitting large files into parts.
# Supports local bot, parallel uploads, skip existing, retry uploads, time outs, progress bars, 
# flood control and automatic cleanup of temporary split segments.

import os
import math
import tempfile
import asyncio
import random
import time
import itertools
from tqdm import tqdm
from telegram import Bot
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest
from telegram.ext import Application
from telegram.error import RetryAfter, NetworkError, TimedOut
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeFilename

# -----------------------------
# CONFIGURATION
# -----------------------------
BOT_TOKEN = "YOUR_BOT_TOKEN_FROM_@BotFather"
CHAT_ID = "YOUR_CHANNEL_ID_FROM_@JsonDumpBot-100xxxxxx"
MAX_SIZE = 1850 * 1024 * 1024  # 1.8GB safety threshold
CHUNK_SIZE = 4 * 1024 * 1024  # 4MB read chunks
MAX_PARALLEL = 2  # async parallel uploads
API_ID = 123456
API_HASH = "0hash0hash"
ROOT = r"C:\_Incoming\Filmez"

client = TelegramClient("session", API_ID, API_HASH)
upload_semaphore = asyncio.Semaphore(MAX_PARALLEL)

request = HTTPXRequest(
    connect_timeout=30,     # time to establish connection
    read_timeout=600,       # time to wait for server response
    write_timeout=600,      # time allowed to upload the file
    pool_timeout=30         # time to wait for a free connection
)

# Create the Bot with a custom base_url
bot = Bot(
    token=BOT_TOKEN,
    base_url="http://192.168.1.130:8081/bot",   # Local Bot API server aiogram/telegram-bot-api
    request=request                             # TELEGRAM_API_HASH TELEGRAM_API_ID TELEGRAM_MAX_CONNECTIONS=250 TELEGRAM_MAX_WEBHOOK_CONNECTIONS=200 TELEGRAM_MAX_THREADS=8
)

# Build the Application using your custom bot
app = Application.builder().bot(bot).build()

# Remote Bot
#bot = Bot(token=BOT_TOKEN)

processed_files = set()

# -----------------------------
# TELEGRAM DISPATCHER
# -----------------------------
class TelegramDispatcher:
    def __init__(self, bot, rate_limit=1.5):
        self.bot = bot
        self.queue = asyncio.PriorityQueue()
        self.rate_limit = rate_limit
        self.last_request_time = 0
        self.chat_cooldowns = {}  # chat_id -> timestamp
        self.counter = itertools.count()  

    async def throttle(self):
        now = time.time()
        delta = now - self.last_request_time
        if delta < self.rate_limit:
            await asyncio.sleep(self.rate_limit - delta)
        self.last_request_time = time.time()

    async def submit(self, priority, chat_id, coro):
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        count = next(self.counter)
        await self.queue.put((priority, chat_id, count, coro, future))
        return future

    async def run(self):
        while True:
            priority, chat_id, count, coro, future = await self.queue.get()

            # Per-chat cooldown
            cooldown_until = self.chat_cooldowns.get(chat_id, 0)
            now = time.time()
            if now < cooldown_until:
                await asyncio.sleep(cooldown_until - now)

            try:
                await self.throttle()
                result = await coro()
                future.set_result(result)
            except RetryAfter as e:
                wait = e.retry_after
                print(f"[Dispatcher] Flood control: waiting {wait}s")
                self.chat_cooldowns[chat_id] = time.time() + wait
                await asyncio.sleep(wait)
                await self.queue.put((priority, chat_id, next(self.counter), coro, future))

            except (TimedOut, NetworkError) as e:
                print(f"[Dispatcher] Network error: {e}. Retrying in 3s")
                await asyncio.sleep(3)
                await self.queue.put((priority, chat_id, next(self.counter), coro, future))
                #print(f"[Dispatcher] Network error (no retry): {repr(e)}")
                #future.set_exception(e)

            except Exception as e:
                print(f"[Dispatcher] Unhandled error: {e}")
                future.set_exception(e)
                self.queue.task_done()
                continue

            finally:
                self.queue.task_done()


# -----------------------------
# RETRY FAILED UPLOADS
# -----------------------------
async def retry_async(
    func,
    *args,
    retries=5,
    base_delay=2,
    max_delay=30,
    exceptions=(NetworkError, TimedOut, OSError),
    **kwargs
):
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)

        except exceptions as e:
            if attempt == retries:
                raise

            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay = delay * (0.7 + random.random() * 0.6)  # jitter

            print(f"[Retry {attempt}/{retries}] Error: {e}. Retrying in {delay:.1f}s")
            await asyncio.sleep(delay)

# -----------------------------
# NORMALIZIE FILE NAME FOR DISPLAYING
# -----------------------------
def normalize_filename(name, start=35, end=20):
    if len(name) > start + end + 3:
        return name[:start] + "..." + name[-end:]
    if len(name) <= start + end + 3:
        return name.ljust(58)
    return name

# -----------------------------
# FETCH OLD UPLOADS
# -----------------------------
async def fetch_existing_captions():
    captions = set()
    seen = {}          # normalized_filename → msg
    duplicates = []    # messages to delete

    entity = await client.get_entity(CHAT_ID)

    async for msg in client.iter_messages(entity):

        # --- Detect duplicates based on Telegram's file_name 
        if msg.document :
            tg_filename = None

            for attr in msg.document.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    tg_filename = attr.file_name
                    break

            if tg_filename:
                if tg_filename in seen:
                    duplicates.append(msg)
                else:
                    seen[tg_filename] = msg

        # --- Collect captions
        if msg.message:
            captions.add(msg.message.strip())
        elif msg.text:
            captions.add(msg.text.strip())

    # --- Delete duplicates ---
    for dup in duplicates:
        try:
            #print(f"🗑️ Removed duplicate: {dup.id}")
            await client.delete_messages(CHAT_ID, dup.id)
        except Exception as e:
            print(f"⚠️ Failed to delete duplicate: {dup.id}: {e}")

    return captions


# -----------------------------
# FILE SPLITTING
# -----------------------------
def split_file(filepath, temp_dir):
    """Split file into N parts based on MAX_SIZE."""
    size = os.path.getsize(filepath)
    if size <= MAX_SIZE:
        return [filepath]

    filename = os.path.basename(filepath)
    num_parts = math.ceil(size / MAX_SIZE)
    part_paths = []

    with open(filepath, "rb") as f:
        for i in range(num_parts):
            #print(f"Splitting {filename}") 
            part_path = os.path.join(temp_dir, f"{filename}.part{i+1}")
            with open(part_path, "wb") as p:
                remaining = MAX_SIZE
                while remaining > 0:
                    chunk = f.read(min(CHUNK_SIZE, remaining))
                    if not chunk:
                        break
                    p.write(chunk)
                    remaining -= len(chunk)
            part_paths.append(part_path)

            # ❗ Delete empty files
            if os.path.getsize(part_path) == 0:
                os.remove(part_path)
                continue

    return part_paths

# -----------------------------
# ASYNC UPLOAD WITH PROGRESS BAR
# -----------------------------
async def upload_file_with_progress(file_path, caption):
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        return
    filename = os.path.basename(file_path)
    short = normalize_filename(filename)
    
    async def _send():
        with open(file_path, "rb") as f, tqdm(
            total=file_size,
            unit="B",
            unit_scale=True,
            desc=f"Uploading {short}",
        ) as progress:

            class StreamWrapper:
                name = filename

                def read(self, n=-1):
                    chunk = f.read(n)
                    if chunk:
                        progress.update(len(chunk))
                    return chunk

            return await bot.send_document(
                chat_id=CHAT_ID,
                document=StreamWrapper(),
                filename=filename,
                caption=caption,
                parse_mode=ParseMode.HTML,
            )

    future = await dispatcher.submit(
        priority=10,
        chat_id=CHAT_ID,
        coro=_send
    )
    await future


# -----------------------------
# SPLIT AND UPLOAD
# -----------------------------
async def split_and_upload(full_path, expected_captions):
    filename = os.path.basename(full_path)

    async with upload_semaphore:
        with tempfile.TemporaryDirectory() as temp_dir:
            parts = split_file(full_path, temp_dir)

            for idx, part in enumerate(parts, start=1):
                caption = expected_captions[idx - 1]
                # Upload each part (dispatcher is used INSIDE this function)
                await upload_file_with_progress(part, caption)

# -----------------------------
# PROCESS A SINGLE FILE
# -----------------------------
async def process_single_file(full_path, folder_name, existing_captions):
    filename = os.path.basename(full_path)
    caption_base = f"{filename}\n#{folder_name.replace(' ', '_')}"

    # First: determine how many parts WOULD be created
    file_size = os.path.getsize(full_path)
    part_count = math.ceil(file_size / MAX_SIZE)

    # Build all expected captions WITHOUT splitting
    expected_captions = []
    for idx in range(1, part_count + 1):
        cap = caption_base
        if part_count > 1:
            cap += f"\nPart {idx}/{part_count}"
        expected_captions.append(cap)

    # If ALL parts already exist → skip entire file
    if all(cap in existing_captions for cap in expected_captions):
        # print(f"⏭️ Skipping fully uploaded file: {filename}")
        return
    
    # Otherwise split and upload now
    await split_and_upload(full_path, expected_captions)

# -----------------------------
# WALK FOLDERS + PARALLEL UPLOAD
# -----------------------------
async def process_folder(root_folder):
    print("Fetching existing messages...")
    existing_captions = await fetch_existing_captions()
    print(f"Loaded {len(existing_captions)} existing files")

    tasks = []

    for folder, _, files in os.walk(root_folder):
        if folder == root_folder:
            continue

        folder_name = os.path.basename(folder)

        for file in files:
            full_path = os.path.join(folder, file)

            # Prevent duplicate processing
            if full_path in processed_files:
                continue
            processed_files.add(full_path)

            tasks.append(asyncio.create_task(
                process_single_file(full_path, folder_name, existing_captions)
            ))

    # wait for all file tasks
    await asyncio.gather(*tasks)


# -----------------------------
# MAIN ENTRY
# -----------------------------
if __name__ == "__main__":
    async def main():
        global dispatcher
        await client.start()  # login once

        # Create dispatcher and start its loop
        dispatcher = TelegramDispatcher(bot, rate_limit=1.0)  # tune if needed
        asyncio.create_task(dispatcher.run())

        # Run folder processing
        await process_folder(ROOT)

        # Wait until all queued uploads are done
        await dispatcher.queue.join()
        print("All uploads completed.")

    asyncio.run(main())
