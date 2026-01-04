from BATCH_CODE.common.config import *
import os
from datetime import datetime
from pathlib import Path

# ❌ load_dotenv() 제거
# env는 common/config.py에서 이미 로딩됨

# =========================
# ENV
# =========================
BATCH_OUT_DIR = os.getenv("BATCH_OUT_DIR")
if not BATCH_OUT_DIR:
    raise RuntimeError("BATCH_OUT_DIR is not set. Check environment variables")

DELIM = os.getenv("TXT_DELIM", "|")

BASE_OUT_DIR = Path(BATCH_OUT_DIR)


def today_yyyymmdd():
    return datetime.now().strftime("%Y%m%d")


def get_out_dir():
    """
    {BATCH_OUT_DIR}/YYYYMMDD
    없으면 자동 생성
    """
    out_dir = BASE_OUT_DIR / today_yyyymmdd()
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def get_out_file(prefix: str):
    """
    prefix_YYYYMMDD.txt 전체 경로 반환
    """
    return get_out_dir() / f"{prefix}_{today_yyyymmdd()}.txt"
