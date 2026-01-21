import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# =====================================================
# ENV 로딩 (프로젝트 루트 기준)
# =====================================================
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env.local")

# =====================================================
# ENV 설정
# =====================================================
BATCH_OUT_DIR = os.getenv("BATCH_OUT_DIR")
if not BATCH_OUT_DIR:
    raise RuntimeError("BATCH_OUT_DIR not set")

DELIM = os.getenv("TXT_DELIM", "|")
ENCODING = os.getenv("TXT_ENCODING", "utf-8")

BASE_OUT_DIR = Path(BATCH_OUT_DIR)

# =====================================================
# TIME / UTIL
# =====================================================
KST = timezone(timedelta(hours=9))


def _today():
    return datetime.now().strftime("%Y%m%d")


def _now_kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def _get_out_file():
    out_dir = BASE_OUT_DIR / _today()
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"DAILY_PRICE_STOCKINDEX_{_today()}.txt"


def _write_header_if_needed(path: Path):
    if not path.exists():
        with open(path, "w", encoding=ENCODING) as f:
            f.write(
                DELIM.join([
                    "code",
                    "date",
                    "change_amount",
                    "change_rate",
                    "close",
                    "last_update"
                ]) + "\n"
            )


def append_indicator_row(
    code: str,
    date: str,
    change_amount,
    change_rate,
    close
):
    path = _get_out_file()
    _write_header_if_needed(path)

    row = DELIM.join([
        str(code),
        str(date),
        str(change_amount),
        str(change_rate),
        str(close),
        _now_kst()
    ])

    with open(path, "a", encoding=ENCODING) as f:
        f.write(row + "\n")
