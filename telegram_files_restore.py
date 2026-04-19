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

expected_parts = defaultdict(int)        # base → total parts expected
downloaded_parts = defaultdict(set)      # base → {1,2,3,...}
active_downloads = defaultdict(int)      # base → number of workers still downloading
merge_locks = defaultdict(asyncio.Lock)  # base → lock to prevent double merges

# -----------------------------
# HELPERS
# -----------------------------

def parse_part(filename: str):
    """
    Extract base name + part number.
    Example: 'backup.7z.003' → ('backup.7z', 3)
    """
    p = Path(filename)
    if p.suffix[1:].isdigit():
        part = int(p.suffix[1:])
        base = p.with_suffix("").name
        return base, part
    return filename, None


def all_parts_present(folder: Path, base: str):
    """
    Check if all expected parts exist on disk.
    """
    total = expected_parts[base]
    for i in range(1, total + 1):
        part_file = folder / f"{base}.{i:03d}"
        if not part_file.exists():
            return False
    return True


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
        c for c in folder_line
    ).strip().replace("_", " ")

    if not safe_caption:
        safe_caption = "_Root"

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


def merge_parts(folder: Path, base_name: str):
    # Find all parts for this specific base
    parts = [
        p for p in folder.iterdir()
        if p.is_file() and p.name.startswith(base_name) and re.search(r"\.part\d+$", p.name)
    ]

    if not parts:
        print(f"No parts found for {base_name}")
        return None

    # Sort numerically: part1, part2, part3...
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

    # Delete parts after successful merge
    for part in parts_sorted:
        part.unlink()

    print(f"✅ Restored and cleaned: {output_file.name}")
    return output_file


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
# DOWNLOAD WORKER
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

            base, part = parse_part(safe_filename)

            if part is not None:
                # Register expected parts
                expected_parts[base] = max(expected_parts.get(base, 0), part)

            # Track active download
            active_downloads[base] += 1

            if output_path.exists():
                print(f"⏭️ Skipping existing file: {output_path}")
            else:
                await download_with_progress(client, msg, output_path)

            # Mark part as downloaded
            if part:
                downloaded_parts[base].add(part)

        except Exception as e:
            print(f"❌ Worker error: {e}")

        finally:
            active_downloads[base] -= 1
            queue.task_done()


# -----------------------------
# MERGE MANAGER (runs in background)
# -----------------------------

async def merge_manager(folder: Path):
    """
    Periodically checks if all parts for any archive are ready.
    Merges only when:
      - all expected parts exist
      - no worker is still downloading
    """
    while True:
        for base in list(expected_parts.keys()):
            # Skip if still downloading
            if active_downloads[base] > 0:
                continue

            # Skip if not all parts downloaded
            if len(downloaded_parts[base]) != expected_parts[base]:
                continue

            # Skip if files not all present on disk
            if not all_parts_present(folder, base):
                continue

            # Merge safely
            async with merge_locks[base]:
                print(f"🔄 Merging {base}…")
                try:
                    merged_file = merge_parts(folder, base)
                    print(f"✅ Merge complete: {merged_file}")

                    # -----------------------------
                    # DELETE PART FILES AFTER SUCCESSFUL MERGE
                    # -----------------------------
                    total = expected_parts[base]
                    for i in range(1, total + 1):
                        part_file = folder / f"{base}.{i:03d}"
                        if part_file.exists():
                            part_file.unlink()
                            print(f"🗑️ Deleted part: {part_file}")


                    # Cleanup registry
                    del expected_parts[base]
                    del downloaded_parts[base]
                    del active_downloads[base]
                    del merge_locks[base]

                except Exception as e:
                    print(f"❌ Merge error for {base}: {e}")

        await asyncio.sleep(1)  # low CPU overhead


# -----------------------------
# START RESTORE PIPELINE
# -----------------------------

async def start_restore(client, messages, folder: Path, workers=5):
    queue = asyncio.Queue()

    # Start merge manager
    asyncio.create_task(merge_manager(folder))

    # Start workers
    worker_tasks = [
        asyncio.create_task(download_worker(queue, client))
        for _ in range(workers)
    ]

    # Enqueue messages
    for msg in messages:
        await queue.put((msg, folder))

    # Signal workers to exit
    for _ in range(workers):
        await queue.put(None)

    # Wait for all workers
    await queue.join()

    # Wait for workers to finish
    for t in worker_tasks:
        await t


# -----------------------------
# MAIN
# -----------------------------

async def main():
    client = TelegramClient("session", API_ID, API_HASH)
    await client.start()

    DOWNLOAD_ROOT.mkdir(exist_ok=True)

    print("📥 Fetching full channel history...")

    queue = asyncio.Queue()

    # 🔥 Start merge manager 
    asyncio.create_task(merge_manager(DOWNLOAD_ROOT))

    # Start workers

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

    # Signal workers to exit

    for _ in workers:
        await queue.put(None)

    # Wait for all workers to finish

    await queue.join()

    for w in workers:
        await w

    print("\n🎉 All files downloaded and restored.")


if __name__ == "__main__":
    asyncio.run(main())
