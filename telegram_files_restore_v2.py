# Reassembles .7z.partN files into a complete archive and extracts the original folder contents.
# Automatically detects all parts, merges them in order, and restores the folder.

import asyncio
import re
from pathlib import Path
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument

# ============================
# CONFIG
# ============================
API_ID = 123445
API_HASH = "1234"
CHANNEL = -1003805416168  # channel ID or @username
DOWNLOAD_ROOT = Path("downloads")
MAX_WORKERS = 3  # parallel downloads
# ============================


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
# DOWNLOAD WORKER
# -----------------------------

async def download_worker(queue, client):
    while True:
        item = await queue.get()
        if item is None:
            break

        msg, folder = item

        media = msg.document or msg.video or msg.audio
        filename = media.file.name or f"{msg.id}.bin"
        output_path = folder / filename

        if output_path.exists():
            print(f"⏭️ Skipping existing file: {output_path}")
            queue.task_done()
            continue

        print(f"⬇️ Downloading: {filename} → {folder}")

        try:
            await client.download_media(msg, file=output_path)
        except Exception as e:
            print(f"❌ Error downloading {filename}: {e}")

        merge_parts(folder)
        queue.task_done()


# -----------------------------
# MAIN DOWNLOAD LOGIC
# -----------------------------

async def main():
    client = TelegramClient("session", API_ID, API_HASH)
    await client.start()

    DOWNLOAD_ROOT.mkdir(exist_ok=True)

    print("📥 Fetching full channel history...")

    queue = asyncio.Queue()

    # Start workers
    workers = [
        asyncio.create_task(download_worker(queue, client))
        for _ in range(MAX_WORKERS)
    ]

    async for msg in client.iter_messages(CHANNEL, reverse=True):
        if not isinstance(msg.media, MessageMediaDocument):
            continue

        caption = msg.message or "NoCaption"
        safe_caption = "".join(c for c in caption if c.isalnum() or c in " _-").strip()
        folder = DOWNLOAD_ROOT / safe_caption
        folder.mkdir(parents=True, exist_ok=True)

        await queue.put((msg, folder))

    # Stop workers
    for _ in workers:
        await queue.put(None)

    await queue.join()

    for w in workers:
        await w

    print("\n🎉 All files downloaded and restored.")


if __name__ == "__main__":
    asyncio.run(main())
