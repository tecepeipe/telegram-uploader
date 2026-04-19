# Reassembles .7z.partN files into a complete archive and extracts the original folder contents.
# Automatically detects all parts, merges them in order, and restores the folder.

import asyncio
import re
from pathlib import Path
from tqdm import tqdm
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument, DocumentAttributeFilename

# ============================
# CONFIG
# ============================
API_ID = 1234
API_HASH = "hashhash"
CHANNEL = -100123456789  # channel ID or @username
DOWNLOAD_ROOT = Path("downloads")
MAX_WORKERS = 3  # parallel downloads
# ============================


# -----------------------------
# FILENAME EXTRACTION
# -----------------------------

def get_filename(msg):
    media = msg.document or msg.video or msg.audio

    if hasattr(media, "attributes"):
        for attr in media.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                return attr.file_name

    return f"{msg.id}.bin"


# -----------------------------
# CAPTION → FOLDER NAME PARSING
# -----------------------------

def extract_folder_name(msg):
    # Bot uploads: caption is in msg.text
    raw = msg.text or msg.message or ""

    if not raw:
        return "_Root"

    # Split into non-empty lines
    lines = [line.strip() for line in raw.splitlines() if line.strip()]

    if not lines:
        return "_Root"

    # 1) Prefer a line with a hashtag (e.g. #Folder_1)
    for line in lines:
        if "#" in line:
            folder_line = line
            break
    else:
        # 2) Prefer a line that does NOT look like a filename (no dot + extension)
        non_file_lines = [
            line for line in lines
            if "." not in line or " " in line  # crude but effective
        ]
        if non_file_lines:
            folder_line = non_file_lines[-1]
        else:
            # 3) Fallback: last line (filename usually first, caption second)
            folder_line = lines[-1]

    # Remove leading '#' and sanitize
    folder_line = folder_line.lstrip("#").strip()
    safe_caption = "".join(
        c for c in folder_line if c.isalnum() or c in " _-"
    ).strip()

    if not safe_caption:
        safe_caption = "NoCaption"

    return safe_caption


# -----------------------------
# PART MERGING LOGIC
# -----------------------------

def find_part_groups(folder: Path):
    groups = {}
    for file in folder.iterdir():
        if file.is_file() and re.search(r"\.part\d+$", file.name):
            base = re.sub(r"\.part\d+$", "", file.name)
            groups.setdefault(base, []).append(file)
    return groups


def merge_parts(folder: Path):
    groups = find_part_groups(folder)
    for base_name, parts in groups.items():
        parts_sorted = sorted(
            parts,
            key=lambda p: int(re.search(r"part(\d+)$", p.name).group(1))
        )

        output_file = folder / base_name
        print(f"\n🔧 Restoring: {output_file.name}")

        with open(output_file, "wb") as outfile:
            for part in parts_sorted:
                print(f"   ➕ Adding {part.name}")
                with open(part, "rb") as infile:
                    outfile.write(infile.read())

        for part in parts_sorted:
            part.unlink()

        print(f"✅ Restored and cleaned: {output_file.name}")


# -----------------------------
# DOWNLOAD WITH PROGRESS BAR
# -----------------------------

async def download_with_progress(client, msg, output_path):
    media = msg.document or msg.video or msg.audio
    size = media.size or 0

    with tqdm(
        total=size,
        unit="B",
        unit_scale=True,
        desc=f"{output_path.name}",
        ascii=True,
    ) as bar:

        async def progress_callback(current, total):
            bar.update(current - bar.n)

        await client.download_media(
            msg,
            file=output_path,
            progress_callback=progress_callback
        )


# -----------------------------
# WORKER
# -----------------------------

async def download_worker(queue, client):
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        msg, folder = item

        try:
            filename = get_filename(msg)
            safe_filename = "".join(c for c in filename if c not in "\\/:*?\"<>|")
            output_path = folder / safe_filename

            if output_path.exists():
                print(f"⏭️ Skipping existing file: {output_path}")
                queue.task_done()
                continue

            await download_with_progress(client, msg, output_path)

            merge_parts(folder)

        except Exception as e:
            print(f"❌ Worker error: {e}")

        finally:
            queue.task_done()


# -----------------------------
# MAIN
# -----------------------------

async def main():
    client = TelegramClient("session", API_ID, API_HASH)
    await client.start()

    DOWNLOAD_ROOT.mkdir(exist_ok=True)

    print("📥 Fetching full channel history...")

    queue = asyncio.Queue()

    workers = [
        asyncio.create_task(download_worker(queue, client))
        for _ in range(MAX_WORKERS)
    ]

    count = 0

    async for msg in client.iter_messages(CHANNEL, reverse=True):
        if not isinstance(msg.media, MessageMediaDocument):
            continue

        folder_name = extract_folder_name(msg)
        folder = DOWNLOAD_ROOT / folder_name
        folder.mkdir(parents=True, exist_ok=True)

        await queue.put((msg, folder))
        count += 1

        if count % 50 == 0:
            print(f"📦 Queued {count} files...")

    print(f"📦 Total files queued: {count}")

    for _ in workers:
        await queue.put(None)

    await queue.join()

    for w in workers:
        await w

    print("\n🎉 All files downloaded and restored.")


if __name__ == "__main__":
    asyncio.run(main())
