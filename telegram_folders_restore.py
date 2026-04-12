# Reassembles .7z.partN files into a complete archive and extracts the original folder contents.
# Automatically detects all parts, merges them in order, and restores the folder.

import os
import re
import subprocess

def restore_archives(root_folder):
    """Reassemble .7z.partN files and extract them."""
    for folder, _, files in os.walk(root_folder):
        part_groups = {}

        # Group by base archive name
        for f in files:
            match = re.match(r"(.+\.7z)\.part(\d+)$", f)
            if match:
                base, num = match.groups()
                part_groups.setdefault(base, []).append((int(num), f))

        for base, parts in part_groups.items():
            parts.sort()
            merged_path = os.path.join(folder, base)

            print(f"Reassembling {merged_path}")

            # Merge parts
            with open(merged_path, "wb") as out:
                for _, part_file in parts:
                    part_path = os.path.join(folder, part_file)
                    with open(part_path, "rb") as p:
                        out.write(p.read())

            print(f"Extracting {merged_path}")

            # Extract archive
            subprocess.run(["7z", "x", merged_path, f"-o{folder}"], check=True)

            print(f"Restored folder from {merged_path}")


if __name__ == "__main__":
    ROOT = "/_Incoming/Gamez"
    restore_archives(ROOT)