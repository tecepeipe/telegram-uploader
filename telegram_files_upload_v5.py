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
# Replace these placeholders with the API credentials from https://my.telegram.org.
API_ID = 123456
API_HASH = "YOUR_API_HASH_HERE"
ROOT = r"C:\_Incoming\Filmez"
BOT_API_BASE_URL = "http://192.168.1.130:8081/bot"

# -----------------------------
# TELEGRAM DISPATCHER
# -----------------------------
class TelegramDispatcher:
    def __init__(self, bot, rate_limit=1.5, max_retries=MAX_RETRIES):
        self.bot = bot
        self.queue = asyncio.PriorityQueue()
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.last_request_time = 0
        self.rate_lock = asyncio.Lock()
        self.counter = itertools.count()

    async def throttle(self):
        async with self.rate_lock:
            now = time.monotonic()
            delta = now - self.last_request_time
            if delta < self.rate_limit:
                await asyncio.sleep(self.rate_limit - delta)
            self.last_request_time = time.monotonic()

    async def submit(self, priority, chat_id, coro):
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        await self.queue.put(
            (priority, 0.0, next(self.counter), chat_id, coro, future, 0)
        )
        return future

    async def run(self):
        while True:
            item = await self.queue.get()
            if item[5] is None:
                self.queue.task_done()
                return

            (priority, not_before, _, chat_id, coro, future,
             retry_count) = item

            try:
                delay = not_before - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                await self.throttle()
                result = await coro()
                if not future.done():
                    future.set_result(result)
            except RetryAfter as e:
                retry_after = e.retry_after
                delay = (
                    retry_after.total_seconds()
                    if isinstance(retry_after, timedelta)
                    else float(retry_after)
                )
                delay = max(0.0, delay)
                self._retry_or_fail(
                    priority, chat_id, coro, future, retry_count, delay, e
                )
            except (TimedOut, NetworkError) as e:
                delay = min(30.0, 2.0 ** retry_count)
                self._retry_or_fail(
                    priority, chat_id, coro, future, retry_count, delay, e
                )
            except Exception as e:
                if not future.done():
                    future.set_exception(e)
                print(f"[Dispatcher] Upload failed: {e}")
            finally:
                self.queue.task_done()

    def _retry_or_fail(
        self, priority, chat_id, coro, future, retry_count, delay, error
    ):
        if retry_count >= self.max_retries:
            if not future.done():
                future.set_exception(error)
            print(f"[Dispatcher] Retries exhausted: {error}")
            return

        print(
            f"[Dispatcher] Retry {retry_count + 1}/{self.max_retries} "
            f"in {delay:.1f}s after: {error}"
        )
        self.queue.put_nowait(
            (
                priority,
                time.monotonic() + delay,
                next(self.counter),
                chat_id,
                coro,
                future,
                retry_count + 1,
            )
        )

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
    seen = {}          # filename → first message with that filename
    duplicates = []    # messages to delete

    entity = await client.get_entity(CHAT_ID)

    async for msg in client.iter_messages(entity):

        # Preserve filename-only cleanup: same-named files from different
        # folders can be treated as duplicates.
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
            if os.path.getsize(part_path) == 0:
                os.remove(part_path)
                continue
            part_paths.append(part_path)

    if len(part_paths) != num_parts:
        raise IOError(
            f"Expected {num_parts} non-empty parts for {filepath}, "
            f"created {len(part_paths)}"
        )

    return part_paths

# -----------------------------
# ASYNC UPLOAD WITH PROGRESS BAR
# -----------------------------
async def upload_file_with_progress(file_path, caption):
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        raise ValueError(f"Cannot upload an empty file: {file_path}")
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
                caption=escape(caption, quote=False),
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
async def split_and_upload(full_path, expected_captions, existing_captions):
    async with upload_semaphore:
        with tempfile.TemporaryDirectory() as temp_dir:
            parts = split_file(full_path, temp_dir)

            for idx, part in enumerate(parts, start=1):
                caption = expected_captions[idx - 1]
                if caption in existing_captions:
                    print(f"Skipping already uploaded part: {os.path.basename(part)}")
                    continue
                await upload_file_with_progress(part, caption)

# -----------------------------
# PROCESS A SINGLE FILE
# -----------------------------
async def process_single_file(full_path, folder_name, existing_captions):
    filename = os.path.basename(full_path)
    caption_base = f"{filename}\n#{folder_name.replace(' ', '_')}"

    # First: determine how many parts WOULD be created
    file_size = os.path.getsize(full_path)
    if file_size == 0:
        raise ValueError(f"Cannot upload an empty file: {full_path}")
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
    await split_and_upload(full_path, expected_captions, existing_captions)

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

            tasks.append(asyncio.create_task(
                process_single_file(full_path, folder_name, existing_captions)
            ))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = [result for result in results if isinstance(result, Exception)]
    if errors:
        details = "\n - ".join(
            f"{type(error).__name__}: {error}" for error in errors
        )
        raise RuntimeError(f"{len(errors)} file(s) failed:\n - {details}")


# -----------------------------
# MAIN ENTRY
# -----------------------------
if __name__ == "__main__":
    async def main():
        global bot, client, dispatcher, upload_semaphore

        if (
            API_ID <= 0
            or API_ID == 123456
            or not API_HASH
            or API_HASH == "YOUR_API_HASH_HERE"
            or not BOT_TOKEN
            or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE"
        ):
            raise RuntimeError(
                "Replace BOT_TOKEN, API_ID, and API_HASH in the CONFIG "
                "section with your Telegram credentials."
            )

        request = HTTPXRequest(
            connect_timeout=30,
            read_timeout=600,
            write_timeout=600,
            pool_timeout=30,
        )
        bot = Bot( # Create the Bot with a custom base_url
            # TELEGRAM_API_HASH TELEGRAM_API_ID TELEGRAM_MAX_CONNECTIONS=250 TELEGRAM_MAX_WEBHOOK_CONNECTIONS=200 TELEGRAM_MAX_THREADS=8
            token=BOT_TOKEN,
            base_url=BOT_API_BASE_URL,
            request=request,
        )
        client = TelegramClient("session", API_ID, API_HASH)
        upload_semaphore = asyncio.Semaphore(MAX_PARALLEL)
        dispatcher = TelegramDispatcher(bot, rate_limit=1.0)
        dispatcher_tasks = []
        bot_initialized = False
        try:
            await bot.initialize()
            bot_initialized = True
            await client.start()

            dispatcher_tasks = [
                asyncio.create_task(dispatcher.run())
                for _ in range(MAX_PARALLEL)
            ]
            await process_folder(ROOT)
            await dispatcher.queue.join()
            print("All uploads completed.")
        finally:
            if dispatcher_tasks:
                await dispatcher.queue.join()
                for _ in dispatcher_tasks:
                    await dispatcher.queue.put(
                        (float("inf"), 0.0, next(dispatcher.counter),
                         0, None, None, 0)
                    )
                await dispatcher.queue.join()
                await asyncio.gather(*dispatcher_tasks)
            await client.disconnect()
            if bot_initialized:
                await bot.shutdown()

    asyncio.run(main())
