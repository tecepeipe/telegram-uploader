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

expected_parts = defaultdict(int)        # basename → total parts expected
downloaded_parts = defaultdict(set)      # basename → {1,2,3,...}
active_downloads = defaultdict(int)      # basename → number of workers still downloading
merge_locks = defaultdict(asyncio.Lock)  # basename → lock to prevent double merges
folder_for_base = {}                     # folder to check for parts

# -----------------------------
# HELPERS
# -----------------------------

def parse_part(filename: str):
    """
    Extract base name + part number.
    Supports:
      - file.ext.001
      - file.ext.part1
    """
    m = re.search(r"\.(\d{3})$", filename)
    if m:
        part = int(m.group(1))
        basename = filename[: -4]  # strip .001
        return basename, part

    # Pattern: .part1 or .part2 etc
    m = re.search(r"\.part(\d+)$", filename)
    if m:
        part = int(m.group(1))
        basename = filename[: -(len(m.group(1)) + 5)]  # strip .partN
        return basename, part

    return filename, None


def all_parts_present(folder: Path, basename: str):
    """
    Check if all expected parts exist on disk.
    """
    total = expected_parts[basename]
    for i in range(1, total + 1):
        # Support both .001 and .part1 formats
        part1 = folder / f"{basename}.{i:03d}"
        part2 = folder / f"{basename}.part{i}"
        if not part1.exists() and not part2.exists():
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

    # Remove leading '#' and sanitize (Windows-safe)
    folder_line = folder_line.lstrip("#").strip()
    safe_caption = "".join(
        c for c in folder_line
        if c not in "\\/:*?\"<>|"
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
        basename = None 

        try:
            filename = get_filename(msg)
            safe_filename = "".join(c for c in filename if c not in "\\/:*?\"<>|")
            output_path = folder / safe_filename

            basename, part = parse_part(safe_filename)
            # 🔥 Track folder for this base
            folder_for_base[basename] = folder

            if part is not None:
                # Register expected parts
                expected_parts[basename] = max(expected_parts.get(basename, 0), part)

            # Track active download
            active_downloads[basename] += 1

            if output_path.exists():
                print(f"⏭️ Skipping existing file: {output_path}")
                # 🔥 Still register the part as downloaded
                if part is not None:
                    expected_parts[basename] = max(expected_parts.get(basename, 0), part)
                    downloaded_parts[basename].add(part)
            else:
                await download_with_progress(client, msg, output_path)

                # Mark part as downloaded
                if part is not None:
                    downloaded_parts[basename].add(part)

        except Exception as e:
            print(f"❌ Worker error: {e}")

        finally:
            if basename is not None:
                active_downloads[basename] -= 1
            queue.task_done()


# -----------------------------
# MERGE MANAGER (runs in background)
# -----------------------------

async def merge_manager():
    """
    Periodically checks if all parts for any archive are ready.
    Merges only when:
      - all expected parts exist
      - no worker is still downloading
    """
    while True:
        for basename in list(expected_parts.keys()):
            # Skip if still downloading
            if active_downloads[basename] > 0:
                continue

            # Skip if not all parts downloaded
            if len(downloaded_parts[basename]) != expected_parts[basename]:
                continue
            
            # Determine the correct folder for this base
            folder = folder_for_base[basename]

            # Skip if files not all present on disk
            if not all_parts_present(folder, basename):
                continue

            # Merge safely
            async with merge_locks[basename]:
                print(f"🔄 Merging {basename}…")
                try:
                    merged_file = merge_parts(folder, basename)
                    print(f"✅ Merge complete: {merged_file}")

                    # -----------------------------
                    # DELETE PART FILES AFTER SUCCESSFUL MERGE
                    # -----------------------------
                    total = expected_parts[basename]
                    for i in range(1, total + 1):
                        part_file = folder / f"{basename}.{i:03d}"
                        if part_file.exists():
                            part_file.unlink()
                            print(f"🗑️ Deleted part: {part_file}")


                    # Cleanup registry
                    del expected_parts[basename]
                    del downloaded_parts[basename]
                    del active_downloads[basename]
                    del merge_locks[basename]
                    del folder_for_base[basename]

                except Exception as e:
                    print(f"❌ Merge error for {basename}: {e}")

        await asyncio.sleep(1)  # low CPU overhead

        if not expected_parts:
            print("🛑 Merge manager shutting down — no pending merges.")
            return


# -----------------------------
# START RESTORE PIPELINE
# -----------------------------

async def start_restore(client, items, workers=5):
    queue = asyncio.Queue()

    # Start merge manager (it will use folder_for_base, not this arg)
    merge_task = asyncio.create_task(merge_manager())

    # Start workers
    worker_tasks = [
        asyncio.create_task(download_worker(queue, client))
        for _ in range(workers)
    ]

    # Enqueue (msg, folder) pairs
    for msg, folder in items:
        await queue.put((msg, folder))

    # Signal workers to exit
    for _ in range(workers):
        await queue.put(None)

    # Wait for all workers
    await queue.join()

    # Wait for workers to finish
    for t in worker_tasks:
        await t

    # Wait for merge manager to finish
    await merge_task


# -----------------------------
# MAIN
# -----------------------------

async def main():
    client = TelegramClient("session", API_ID, API_HASH)
    await client.start()

    DOWNLOAD_ROOT.mkdir(exist_ok=True)

    print("📥 Fetching full channel history...")

    messages = []
    items = []  # (msg, folder) pairs

    async for msg in client.iter_messages(CHANNEL, reverse=True):
        if not isinstance(msg.media, MessageMediaDocument):
            continue

        folder_name = extract_folder_name(msg)
        folder = DOWNLOAD_ROOT / folder_name
        folder.mkdir(parents=True, exist_ok=True)

        items.append((msg, folder))

    print(f"📦 Total files queued: {len(items)}")

    # 🔥 Start merge manager 
    await start_restore(client, items)

    print("\n🎉 All files downloaded and restored.")


if __name__ == "__main__":
    asyncio.run(main())
