# Reassembles .7z.partN files into a complete archive and extracts the original folder contents.
# Automatically detects all parts, merges them in order, and restores the folder.

import asyncio
import re
from collections import defaultdict
from pathlib import Path
from tqdm import tqdm
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument, DocumentAttributeFilename

# ============================
# CONFIG
# ============================
API_ID = 123456
API_HASH = "hash0hash"
CHANNEL = -100123456789 # channel ID or @username
DOWNLOAD_ROOT = Path("Downloads")
MAX_WORKERS = 3  # parallel downloads
# ============================

# -----------------------------
# GLOBAL STATE (thread‑safe via asyncio)
# -----------------------------

expected_parts = defaultdict(int)        # basename → highest part number
downloaded_parts = defaultdict(set)      # basename → {1,2,3,...}
active_downloads = defaultdict(int)      # basename → workers still downloading
merge_locks = defaultdict(asyncio.Lock)  # basename → merge lock
folder_for_base = {}                     # basename → folder path


# -----------------------------------------
# PART PARSING
# -----------------------------------------

def parse_part(filename: str):
    # Extract base name + part number: file.ext.part1, file.ext.part2, ...
    m = re.search(r"\.part(\d+)$", filename)
    if not m:
        return filename, None

    part = int(m.group(1))
    basename = filename[: -(len(m.group(1)) + 5)]  # strip ".partN"
    return basename, part


# -----------------------------------------
# CHECK IF ALL PARTS EXIST
# -----------------------------------------

def all_parts_present(folder: Path, basename: str):
    total = expected_parts[basename]
    for i in range(1, total + 1):
        part_file = folder / f"{basename}.part{i}"
        if not part_file.exists():
            return False
    return True


# -----------------------------------------
# GET FILENAME FROM TELEGRAM MESSAGE
# -----------------------------------------

def get_filename(msg):
    media = msg.document or msg.video or msg.audio

    if hasattr(media, "attributes"):
        for attr in media.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                return attr.file_name

    return f"{msg.id}.bin"


# -----------------------------------------
# EXTRACT FOLDER NAME FROM CAPTION
# -----------------------------------------

def extract_folder_name(msg):
    # Bot uploads: caption is in msg.text
    raw = msg.text or msg.message or ""
    if not raw:
        return "_Root"

    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if not lines:
        return "_Root"

    # Prefer hashtag line
    for line in lines:
        if "#" in line:
            folder_line = line
            break
    else:
        # Otherwise pick last non-filename line
        non_file_lines = [l for l in lines if "." not in l or " " in l]
        folder_line = non_file_lines[-1] if non_file_lines else lines[-1]

    folder_line = folder_line.lstrip("#").strip()

    safe_caption = "".join(
        c for c in folder_line if c not in "\\/:*?\"<>|"
    ).strip().replace("_", " ")

    return safe_caption or "_Root"


# -----------------------------------------
# MERGE PARTS 
# -----------------------------------------

def merge_parts(folder: Path, base_name: str):
    parts = []

    for p in folder.iterdir():
        if p.is_file():
            m = re.search(rf"^{re.escape(base_name)}\.part(\d+)$", p.name)
            if m:
                parts.append((p, int(m.group(1))))

    if not parts:
        print(f"No parts found for {base_name}")
        return None

    parts_sorted = sorted(parts, key=lambda x: x[1])

    output_file = folder / base_name
    print(f"\n🔧 Restoring: {output_file.name}")

    with open(output_file, "wb") as outfile:
        for part_file, _ in parts_sorted:
            print(f"   ➕ Adding {part_file.name}")
            with open(part_file, "rb") as infile:
                outfile.write(infile.read())

    # Delete parts after merged
    for part_file, _ in parts_sorted:
        part_file.unlink()

    print(f"✅ Restored and cleaned: {output_file.name}")
    return output_file


# -----------------------------------------
# DOWNLOAD WITH PROGRESS BAR
# -----------------------------------------

async def download_with_progress(client, msg, output_path):
    media = msg.document or msg.video or msg.audio
    size = media.size or 0

    with tqdm(total=size, unit="B", unit_scale=True,
              desc=f"{output_path.name}", ascii=True) as bar:

        async def progress_callback(current, total):
            bar.update(current - bar.n)

        await client.download_media(
            msg,
            file=output_path,
            progress_callback=progress_callback
        )


# -----------------------------------------
# DOWNLOAD WORKER
# -----------------------------------------

async def download_worker(queue, client):
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        msg, folder = item
        basename = None

        try:
            filename = get_filename(msg)
            safe_filename = "".join(c for c in filename if c not in "\\/:*?\"<>|")
            output_path = folder / safe_filename

            basename, part = parse_part(safe_filename)
            # Track folder for this base
            folder_for_base[basename] = folder

            if part: # Register expected parts
                expected_parts[basename] = max(expected_parts[basename], part)

            active_downloads[basename] += 1

            if output_path.exists():
                print(f"⏭️ Skipping existing file: {output_path}")
                if part:
                    downloaded_parts[basename].add(part)
            else:
                await download_with_progress(client, msg, output_path)
                if part:
                    downloaded_parts[basename].add(part)

        except Exception as e:
            print(f"❌ Worker error: {e}")

        finally:
            if basename:
                active_downloads[basename] -= 1
            queue.task_done()


# -----------------------------------------
# MERGE MANAGER (runs in background)
# -----------------------------------------

async def merge_manager():
    """
    Periodically checks if all parts for any archive are ready.
    Merges only when:
      - all expected parts exist
      - no worker is still downloading
    """
    while True:
        for basename in list(expected_parts.keys()):

            if active_downloads[basename] > 0:
                continue

            if len(downloaded_parts[basename]) != expected_parts[basename]:
                continue

            folder = folder_for_base[basename]

            if not all_parts_present(folder, basename):
                continue

            async with merge_locks[basename]:
                print(f"🔄 Merging {basename}…")
                merged = merge_parts(folder, basename)

                if merged:
                    del expected_parts[basename]
                    del downloaded_parts[basename]
                    del active_downloads[basename]
                    del merge_locks[basename]
                    del folder_for_base[basename]

        await asyncio.sleep(1)

        if not expected_parts:
            print("🛑 Merge manager shutting down — no pending merges.")
            return


# -----------------------------------------
# START RESTORE PIPELINE
# -----------------------------------------

async def start_restore(client, items, workers=5):
    queue = asyncio.Queue()

    merge_task = asyncio.create_task(merge_manager())

    worker_tasks = [
        asyncio.create_task(download_worker(queue, client))
        for _ in range(workers)
    ]

    for msg, folder in items:
        await queue.put((msg, folder))

    for _ in range(workers):
        await queue.put(None)

    await queue.join()

    for t in worker_tasks:
        await t

    await merge_task


# -----------------------------------------
# MAIN
# -----------------------------------------

async def main():
    client = TelegramClient("session", API_ID, API_HASH)
    await client.start()

    DOWNLOAD_ROOT.mkdir(exist_ok=True)

    print("📥 Fetching full channel history...")

    items = []

    async for msg in client.iter_messages(CHANNEL, reverse=True):
        if not isinstance(msg.media, MessageMediaDocument):
            continue

        folder_name = extract_folder_name(msg)
        folder = DOWNLOAD_ROOT / folder_name
        folder.mkdir(parents=True, exist_ok=True)

        items.append((msg, folder))

    print(f"📦 Total files queued: {len(items)}")

    await start_restore(client, items)

    print("\n🎉 All files downloaded and restored.")


if __name__ == "__main__":
    asyncio.run(main())
