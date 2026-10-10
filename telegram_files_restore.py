# Reassembles .7z.partN files into a complete archive and extracts the original folder contents.
# Automatically detects all parts, merges them in order, and restores the folder.

import asyncio
import re
import shutil
from pathlib import Path
from tqdm import tqdm
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument, DocumentAttributeFilename

# ============================
# CONFIG
# ============================
# Replace these placeholders with the API credentials from https://my.telegram.org.
API_ID = 123456
API_HASH = "YOUR_API_HASH_HERE"
CHAT_ID = -100123456789 # channel ID or @username
DOWNLOAD_ROOT = Path(r"Downloads")
MAX_WORKERS = 3  # parallel downloads
# ============================

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
# GET FILENAME FROM TELEGRAM MESSAGE
# -----------------------------------------

def get_filename(msg):
    media = msg.document or msg.video or msg.audio

    if hasattr(media, "attributes"):
        for attr in media.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                return attr.file_name

    return f"{msg.id}.bin"


def safe_filename(filename: str, fallback: str) -> str:
    result = "".join(c for c in filename if c not in "\\/:*?\"<>|").strip()
    return fallback if result in ("", ".", "..") else result


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

    return safe_caption if safe_caption not in ("", ".", "..") else "_Root"


# -----------------------------------------
# MERGE PARTS 
# -----------------------------------------

def merge_parts(folder: Path, base_name: str, part_numbers: set[int]):
    parts_sorted = [
        folder / f"{base_name}.part{number}"
        for number in sorted(part_numbers)
    ]
    missing = [path.name for path in parts_sorted if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            f"Missing archive part(s) for {base_name}: {', '.join(missing)}"
        )

    output_file = folder / base_name
    temporary_file = folder / f"{base_name}.merge.tmp"
    print(f"\n🔧 Restoring: {output_file.name}")

    try:
        with open(temporary_file, "wb") as outfile:
            for part_file in parts_sorted:
                print(f"   ➕ Adding {part_file.name}")
                with open(part_file, "rb") as infile:
                    shutil.copyfileobj(infile, outfile, length=1024 * 1024)
        temporary_file.replace(output_file)
    except Exception:
        if temporary_file.exists():
            temporary_file.unlink()
        raise

    for part_file in parts_sorted:
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

        def progress_callback(current, _total):
            bar.total = _total
            bar.update(current - bar.n)

        await client.download_media(
            msg,
            file=output_path,
            progress_callback=progress_callback
        )


# -----------------------------------------
# DOWNLOAD WORKER
# -----------------------------------------

async def download_worker(queue, client, downloaded_parts, failures):
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        msg, folder = item

        try:
            filename = safe_filename(get_filename(msg), f"{msg.id}.bin")
            output_path = folder / filename
            basename, part = parse_part(filename)

            media = msg.document or msg.video or msg.audio
            expected_size = media.size

            if (output_path.is_file() and expected_size is not None
                    and output_path.stat().st_size == expected_size):
                print(f"⏭️ Skipping existing file: {output_path}")
            else:
                await download_with_progress(client, msg, output_path)
                if not output_path.is_file():
                    raise IOError(f"Download did not create {output_path}")
                if (expected_size is not None
                        and output_path.stat().st_size != expected_size):
                    raise IOError(
                        f"Incomplete download for {output_path}: "
                        f"expected {expected_size} bytes, got "
                        f"{output_path.stat().st_size}"
                    )

            if part is not None:
                downloaded_parts[(folder, basename)].add(part)

        except Exception as e:
            failure = f"message {msg.id} in {folder}: {type(e).__name__}: {e}"
            failures.append(failure)
            print(f"❌ Worker error: {failure}")

        finally:
            queue.task_done()


# -----------------------------------------
# START RESTORE PIPELINE
# -----------------------------------------

async def start_restore(client, items, workers=MAX_WORKERS):
    queue = asyncio.Queue()
    archive_parts = {}
    downloaded_parts = {}
    failures = []

    for msg, folder in items:
        filename = safe_filename(get_filename(msg), f"{msg.id}.bin")
        basename, part = parse_part(filename)
        if part is not None:
            archive_parts.setdefault((folder, basename), set()).add(part)

    for key in archive_parts:
        downloaded_parts[key] = set()

    worker_tasks = [
        asyncio.create_task(
            download_worker(queue, client, downloaded_parts, failures)
        )
        for _ in range(workers)
    ]

    for msg, folder in items:
        await queue.put((msg, folder))

    for _ in range(workers):
        await queue.put(None)

    await queue.join()

    for t in worker_tasks:
        await t

    for (folder, basename), part_numbers in archive_parts.items():
        expected_numbers = set(range(1, max(part_numbers) + 1))
        if part_numbers != expected_numbers:
            missing = sorted(expected_numbers - part_numbers)
            failures.append(
                f"{folder / basename}: archive numbering has gaps; "
                f"missing part(s) {missing}"
            )
            continue

        downloaded = downloaded_parts[(folder, basename)]
        missing = sorted(part_numbers - downloaded)
        if missing:
            failures.append(
                f"{folder / basename}: not merging because part(s) "
                f"{missing} did not download successfully"
            )
            continue

        try:
            merge_parts(folder, basename, part_numbers)
        except Exception as e:
            failure = (
                f"{folder / basename}: {type(e).__name__}: {e}"
            )
            failures.append(failure)
            print(f"❌ Merge error: {failure}")

    if failures:
        details = "\n - ".join(failures)
        raise RuntimeError(f"Restore finished with errors:\n - {details}")


# -----------------------------------------
# MAIN
# -----------------------------------------

async def main():
    if API_ID <= 0 or API_HASH == "YOUR_API_HASH_HERE":
        raise RuntimeError(
            "Replace API_ID and API_HASH in the CONFIG section with your "
            "Telegram API credentials."
        )

    client = TelegramClient("session", API_ID, API_HASH)
    await client.start()

    try:
        DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)

        print("📥 Fetching full channel history...")

        items = []

        async for msg in client.iter_messages(CHAT_ID, reverse=True):
            if not isinstance(msg.media, MessageMediaDocument):
                continue

            folder_name = extract_folder_name(msg)
            folder = DOWNLOAD_ROOT / folder_name
            folder.mkdir(parents=True, exist_ok=True)

            items.append((msg, folder))

        print(f"📦 Total files queued: {len(items)}")

        await start_restore(client, items)

        print("\n🎉 All files downloaded and restored.")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
