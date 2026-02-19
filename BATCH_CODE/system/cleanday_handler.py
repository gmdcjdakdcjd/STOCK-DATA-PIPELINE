import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# ENV 로딩
BASE_DIR = Path(__file__).resolve().parents[3]
if os.getenv("APP_ENV") != "docker":
    load_dotenv(BASE_DIR / ".env.local")

CLEAN_DAYS = int(os.getenv("CLEAN_DAYS", 15))

TARGET_DIR_KEYS = [
    "CLEAN_ARCHIVE_DIR",
    "CLEAN_BATCH_IN_DIR",
    "CLEAN_BATCH_OUT_DIR",
    "CLEAN_ERROR_DIR",
    "CLEAN_BATCH_LOG_DIR",
    "CLEAN_PYTHON_LOG_DIR",
    "CLEAN_STOCK_LOG_DIR",
]


def clean_directory(dir_path: Path, cutoff):
    if not dir_path.exists():
        return

    for item in dir_path.iterdir():
        try:
            mtime = datetime.fromtimestamp(item.stat().st_mtime)
            if mtime < cutoff:
                if item.is_file() or item.is_symlink():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
        except Exception:
            pass


def main():
    cutoff = datetime.now() - timedelta(days=CLEAN_DAYS)

    for key in TARGET_DIR_KEYS:
        path = os.getenv(key)
        if path:
            clean_directory(Path(path), cutoff)

    return 0


if __name__ == "__main__":
    exit(main())
