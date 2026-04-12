# Uploads all files in subfolders to Telegram with captions, auto‑splitting large files into parts.
# Supports parallel uploads, progress bars, and automatic cleanup of temporary split segments.

import os
import math
import tempfile
from concurrent.futures import ThreadPoolExecutor
from telegram import Bot
from telegram.constants import ParseMode
from tqdm import tqdm

# -----------------------------
# CONFIGURATION
# -----------------------------
BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
CHAT_ID = "YOUR_CHAT_ID"
MAX_SIZE = 1.85 * 1024 * 1024 * 1024  # 1.8GB safety threshold
CHUNK_SIZE = 1024 * 1024  # 1MB read chunks for progress bar
MAX_WORKERS = 4  # parallel uploads

bot = Bot(token=BOT_TOKEN)


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
# UPLOAD WITH PROGRESS BAR
# -----------------------------
def upload_file_with_progress(file_path, caption):
    """Upload a file with a tqdm progress bar."""
    file_size = os.path.getsize(file_path)

    with open(file_path, "rb") as f, tqdm(
        total=file_size,
        unit="B",
        unit_scale=True,
        desc=f"Uploading {os.path.basename(file_path)}",
    ) as progress:

        class StreamWrapper:
            def read(self, n):
                chunk = f.read(n)
                if chunk:
                    progress.update(len(chunk))
                return chunk

        bot.send_document(
            chat_id=CHAT_ID,
            document=StreamWrapper(),
            filename=os.path.basename(file_path),
            caption=caption,
            parse_mode=ParseMode.HTML,
        )


# -----------------------------
# PROCESS A SINGLE FILE
# -----------------------------
def process_single_file(full_path, folder_name):
    caption_base = f"<b>{os.path.basename(full_path)}</b>\n#{folder_name.replace(' ', '_')}"

    with tempfile.TemporaryDirectory() as temp_dir:
        parts = split_file(full_path, temp_dir)

        for idx, part in enumerate(parts, start=1):
            caption = caption_base
            if len(parts) > 1:
                caption += f"\nPart {idx}/{len(parts)}"

            upload_file_with_progress(part, caption)

        # Auto-delete split parts (temp_dir auto-cleans)


# -----------------------------
# WALK FOLDERS + PARALLEL UPLOAD
# -----------------------------
def process_folder(root_folder):
    tasks = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for folder, _, files in os.walk(root_folder):
            if folder == root_folder:
                continue

            folder_name = os.path.basename(folder)

            for file in files:
                full_path = os.path.join(folder, file)
                tasks.append(
                    executor.submit(process_single_file, full_path, folder_name)
                )

        # Wait for all uploads to finish
        for t in tasks:
            t.result()


if __name__ == "__main__":
    ROOT = "\_Incoming\Filmez"
    process_folder(ROOT)