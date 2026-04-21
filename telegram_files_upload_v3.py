# Uploads all files in subfolders to Telegram with captions, auto‑splitting large files into parts.
# Supports local bot, parallel uploads, skip existng, retry uploads, progress bars, time outs and automatic cleanup of temporary split segments.

import os
import math
import tempfile
import asyncio
import asyncio
import random
from tqdm import tqdm
from telegram import Bot
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest
from telegram.ext import Application
from telegram.error import NetworkError, TimedOut
from telethon import TelegramClient

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

client = TelegramClient("session", API_ID, API_HASH)

request = HTTPXRequest(
    connect_timeout=30,     # time to establish connection
    read_timeout=600,       # time to wait for server response
    write_timeout=600,      # time allowed to upload the file
    pool_timeout=30         # time to wait for a free connection
)

# Create the Bot with a custom base_url
bot = Bot(
    token=BOT_TOKEN,
    base_url="http://192.168.1.130:8081/bot",   # Local Bot API server
    request=request
)

# Build the Application using your custom bot
app = Application.builder().bot(bot).build()

# remote Bot
#bot = Bot(token=BOT_TOKEN)

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
# FETCH OLD UPLOADS
# -----------------------------
async def fetch_existing_captions():
    captions = set()

    entity = await client.get_entity(CHAT_ID)

    async for msg in client.iter_messages(entity):
        # Captions for media messages
        if msg.message:
            captions.add(msg.message.strip())

        # Captions for text messages
        elif msg.text:
            captions.add(msg.text.strip())

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
# REDUCES FILE NAME FOR DISPLAYING
# -----------------------------
def shorten_filename(name, start=35, end=20):
    if len(name) <= start + end + 3:
        return name
    return name[:start] + "..." + name[-end:]

# -----------------------------
# ASYNC UPLOAD WITH PROGRESS BAR
# -----------------------------
async def upload_file_with_progress(file_path, caption):
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        print("⚠️ Skipping empty file:", file_path)
        return
    filename = os.path.basename(file_path)
    short = shorten_filename(filename)

    with open(file_path, "rb") as f, tqdm(
        total=file_size,
        unit="B",
        unit_scale=True,
        desc=f"Uploading {short}",
    ) as progress:

        class StreamWrapper:
            name = os.path.basename(file_path)

            def read(self, n=-1):
                chunk = f.read(n)
                if chunk:
                    progress.update(len(chunk))
                return chunk

        async def _send():
            return await bot.send_document(
                chat_id=CHAT_ID,
                document=StreamWrapper(),
                filename=os.path.basename(file_path),
                caption=caption,
                parse_mode=ParseMode.HTML,
            )

        await retry_async(_send)


# -----------------------------
# PROCESS A SINGLE FILE
# -----------------------------
async def process_single_file(full_path, folder_name, existing_captions):
    caption_base = f"{os.path.basename(full_path)}\n#{folder_name.replace(' ', '_')}"

    with tempfile.TemporaryDirectory() as temp_dir:
        parts = split_file(full_path, temp_dir)

        for idx, part in enumerate(parts, start=1):
            caption = caption_base
            if len(parts) > 1:
                caption += f"\nPart {idx}/{len(parts)}"

            # Skip only if THIS specific part exists
            if caption in existing_captions:
                #print(f"⏭️ Skipping existing part: {caption}")
                continue

            await upload_file_with_progress(part, caption)


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

            tasks.append(
                asyncio.create_task(
                    process_single_file(full_path, folder_name, existing_captions)
                )
            )

            # Limit concurrency
            if len(tasks) >= MAX_PARALLEL:
                await asyncio.gather(*tasks)
                tasks = []

    # Process remaining tasks
    if tasks:
        await asyncio.gather(*tasks)


# -----------------------------
# MAIN ENTRY
# -----------------------------
if __name__ == "__main__":
    ROOT = r"D:\Filmez"
    async def main():
        await client.start()  # login once
        await process_folder(ROOT)

    asyncio.run(main())
