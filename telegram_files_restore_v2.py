import os

def restore_files(root_folder):
    """Reassemble .part1 and .part2 files into original file."""
    for folder, _, files in os.walk(root_folder):
        part1_files = [f for f in files if f.endswith(".part1")]

        for p1 in part1_files:
            base = p1[:-6]  # remove .part1
            p2 = base + ".part2"

            part1_path = os.path.join(folder, p1)
            part2_path = os.path.join(folder, p2)

            if not os.path.exists(part2_path):
                print(f"Missing second part for {p1}")
                continue

            restored_path = os.path.join(folder, base)
            print(f"Restoring: {restored_path}")

            with open(restored_path, "wb") as out:
                with open(part1_path, "rb") as f1:
                    out.write(f1.read())
                with open(part2_path, "rb") as f2:
                    out.write(f2.read())

            print(f"Restored: {restored_path}")


if __name__ == "__main__":
    ROOT = "path/to/folder/with/parts"
    restore_files(ROOT)