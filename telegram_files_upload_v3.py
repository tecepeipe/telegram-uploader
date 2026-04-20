# Uploads all files in subfolders to Telegram with captions, auto‑splitting large files into parts.
# Supports local bot, parallel uploads, skip already uploaded, progress bars, time outs and automatic cleanup of temporary split segments.

import os
import math
import tempfile
import asyncio
from tqdm import tqdm
from telegram import Bot
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest
from telegram.ext import Application

# -----------------------------
# CONFIGURATION
# -----------------------------
BOT_TOKEN = "YOUR_BOT_TOKEN_FROM_@BotFather"
CHAT_ID = "YOUR_CHANNEL_ID_FROM_@JsonDumpBot-100xxxxxx"
MAX_SIZE = 1850 * 1024 * 1024  # 1.8GB safety threshold
CHUNK_SIZE = 4 * 1024 * 1024  # 4MB read chunks
MAX_PARALLEL = 2  # async parallel uploads

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
# FETCH OLD UPLOADS
# -----------------------------

async def fetch_existing_captions():
    updates = await bot.get_updates(offset=0, limit=10000)
    captions = set()

    for u in updates:
        msg = u.message
        if msg and msg.caption:
            captions.add(msg.caption.strip())

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

    return part_paths


# -----------------------------
# ASYNC UPLOAD WITH PROGRESS BAR
# -----------------------------
async def upload_file_with_progress(file_path, caption):
    file_size = os.path.getsize(file_path)

    with open(file_path, "rb") as f, tqdm(
        total=file_size,
        unit="B",
        unit_scale=True,
        desc=f"Uploading {os.path.basename(file_path)}",
    ) as progress:

        class StreamWrapper:
            name = os.path.basename(file_path)

            def read(self, n=-1):
                chunk = f.read(n)
                if chunk:
                    progress.update(len(chunk))
                return chunk

        await bot.send_document(
            chat_id=CHAT_ID,
            document=StreamWrapper(),
            filename=os.path.basename(file_path),
            caption=caption,
            parse_mode=ParseMode.HTML,
        )


# -----------------------------
# PROCESS A SINGLE FILE
# -----------------------------
async def process_single_file(full_path, folder_name, existing_captions):
    caption_base = f"<b>{os.path.basename(full_path)}</b>\n#{folder_name.replace(' ', '_')}"

    with tempfile.TemporaryDirectory() as temp_dir:
        parts = split_file(full_path, temp_dir)

        for idx, part in enumerate(parts, start=1):
            caption = caption_base
            if len(parts) > 1:
                caption += f"\nPart {idx}/{len(parts)}"

            # Skip only if THIS specific part exists
            if caption in existing_captions:
                print(f"⏭️ Skipping existing part: {caption}")
                continue

            await upload_file_with_progress(part, caption)


# -----------------------------
# WALK FOLDERS + PARALLEL UPLOAD
# -----------------------------
async def process_folder(root_folder):
    print("Fetching existing messages...")
    existing_captions = await fetch_existing_captions()
    print(f"Loaded {len(existing_captions)} existing captions")

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
    asyncio.run(process_folder(ROOT))
