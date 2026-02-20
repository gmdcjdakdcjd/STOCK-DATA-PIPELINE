import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# =====================================================
# ENV 로딩 (APP_ENV 기반)
# =====================================================
BASE_DIR = Path(__file__).resolve().parents[2]

APP_ENV = os.getenv("APP_ENV", "local")
env_file = BASE_DIR / f".env.{APP_ENV}"

if not env_file.exists():
    raise RuntimeError(f"ENV FILE NOT FOUND: {env_file}")

load_dotenv(env_file)

# =====================================================
# ENV 설정
# =====================================================
BATCH_OUT_DIR = os.getenv("BATCH_OUT_DIR")
DELIM = os.getenv("TXT_DELIM", "|")
ENCODING = os.getenv("TXT_ENCODING", "utf-8")

if not BATCH_OUT_DIR:
    raise RuntimeError("BATCH_OUT_DIR not set")

BASE_OUT_DIR = Path(BATCH_OUT_DIR)

# =====================================================
# TIME (KST 통일)
# =====================================================
KST = timezone(timedelta(hours=9))


def _today():
    return datetime.now(KST).strftime("%Y%m%d")


def _now_kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def _get_out_file(file_prefix: str):
    out_dir = BASE_OUT_DIR / _today()
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{file_prefix}_{_today()}.txt"


def write_rows(
    file_prefix: str,
    headers: list,
    rows: list
):
    """
    file_prefix : 파일명 prefix (예: INVESTOR_TREND_DAILY)
    headers     : 컬럼 리스트 (last_update 제외)
    rows        : 2차원 리스트 (데이터)
    """

    path = _get_out_file(file_prefix)

    # 헤더 작성 (last_update 자동 포함)
    if not path.exists():
        with open(path, "w", encoding=ENCODING) as f:
            header_line = DELIM.join(headers + ["last_update"])
            f.write(header_line + "\n")

    # 데이터 append
    now_str = _now_kst()

    with open(path, "a", encoding=ENCODING) as f:
        for row in rows:
            row_with_time = row + [now_str]
            f.write(DELIM.join(map(str, row_with_time)) + "\n")

    return path