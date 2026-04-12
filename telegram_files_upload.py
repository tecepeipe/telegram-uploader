# Uploads all files in subfolders to Telegram with captions, auto‑splitting large files into parts

import os
import shutil
import tempfile
from telegram import Bot
from telegram.constants import ParseMode

# -----------------------------
# CONFIGURATION
# -----------------------------
BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
CHAT_ID = "YOUR_CHAT_ID"
MAX_SIZE = 1.85 * 1024 * 1024 * 1024  # 1.85GB safety threshold
bot = Bot(token=BOT_TOKEN)


def split_file(filepath, temp_dir):
    """Split file into two parts if larger than MAX_SIZE."""
    size = os.path.getsize(filepath)
    if size <= MAX_SIZE:
        return [filepath]  # No split needed

    filename = os.path.basename(filepath)
    part1 = os.path.join(temp_dir, filename + ".part1")
    part2 = os.path.join(temp_dir, filename + ".part2")

    with open(filepath, "rb") as f:
        with open(part1, "wb") as p1:
            p1.write(f.read(int(size / 2)))
        with open(part2, "wb") as p2:
            p2.write(f.read())

    return [part1, part2]


def upload_file(file_path, caption):
    """Upload a single file to Telegram."""
    with open(file_path, "rb") as f:
        bot.send_document(
            chat_id=CHAT_ID,
            document=f,
            caption=caption,
            parse_mode=ParseMode.HTML
        )


def process_folder(root_folder):
    """Walk through subfolders and upload files."""
    for folder, _, files in os.walk(root_folder):
        if folder == root_folder:
            continue  # Skip root itself

        folder_name = os.path.basename(folder)
        hashtag = f"#{folder_name.replace(' ', '_')}"

        for file in files:
            full_path = os.path.join(folder, file)
            caption = f"<b>{file}</b>\n{hashtag}"

            with tempfile.TemporaryDirectory() as temp_dir:
                parts = split_file(full_path, temp_dir)

                for idx, part in enumerate(parts, start=1):
                    part_caption = caption
                    if len(parts) > 1:
                        part_caption += f"\nPart {idx}/{len(parts)}"

                    print(f"Uploading: {part}")
                    upload_file(part, part_caption)


if __name__ == "__main__":
    ROOT = "\_Incoming\Filmez"
    process_folder(ROOT)