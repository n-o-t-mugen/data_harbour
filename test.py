import os

# ==== CONFIG ====
OUTPUT_FILE = "FULL_PROJECT_CODE_DUMP.txt"

INCLUDE_EXTENSIONS = {
    ".py", ".html", ".css", ".js", ".env"
}

EXCLUDE_DIRS = {
    "venv",
    "__pycache__",
    ".git",
    "node_modules",
    "staticfiles",
    ".idea",
    ".vscode"
}

# =================

def should_include_file(filename):
    if filename == ".env":
        return True
    return os.path.splitext(filename)[1] in INCLUDE_EXTENSIONS


def dump_code(root_dir):
    with open(OUTPUT_FILE, "w", encoding="utf-8", errors="ignore") as out:
        out.write("=" * 80 + "\n")
        out.write("FULL PROJECT CODE DUMP\n")
        out.write(f"Root Directory: {root_dir}\n")
        out.write("=" * 80 + "\n\n")

        for root, dirs, files in os.walk(root_dir):
            # Remove excluded directories in-place
            dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

            for file in sorted(files):
                if should_include_file(file):
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, root_dir)

                    out.write("\n" + "#" * 80 + "\n")
                    out.write(f"FILE: {rel_path}\n")
                    out.write("#" * 80 + "\n\n")

                    try:
                        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                            out.write(f.read())
                    except Exception as e:
                        out.write(f"<< ERROR READING FILE: {e} >>")

                    out.write("\n\n")

    print(f"\n✅ Code dump created: {OUTPUT_FILE}")


if __name__ == "__main__":
    dump_code(os.getcwd())
