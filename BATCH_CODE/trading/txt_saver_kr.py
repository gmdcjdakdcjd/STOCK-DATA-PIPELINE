from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import os

# 반드시 실행
load_dotenv()

KST = timezone(timedelta(hours=9))

DELIM = os.getenv("TXT_DELIM", "|")
ENCODING = os.getenv("TXT_ENCODING", "utf-8")

BATCH_OUT_DIR = os.getenv("BATCH_OUT_DIR")
if not BATCH_OUT_DIR:
    raise RuntimeError("BATCH_OUT_DIR is not set. Check .env")

BASE_OUT_DIR = Path(BATCH_OUT_DIR)

today = datetime.now().strftime("%Y%m%d")
OUT_DIR = BASE_OUT_DIR / today
OUT_DIR.mkdir(parents=True, exist_ok=True)

RESULT_FILE = OUT_DIR / f"STRATEGY_RESULT_KR_{today}.txt"
DETAIL_FILE = OUT_DIR / f"STRATEGY_DETAIL_KR_{today}.txt"


def _now_kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


# =====================================================
# STRATEGY_RESULT (전략 요약)
# =====================================================
def save_strategy_result(strategy_name, signal_date, total_data):

    if not RESULT_FILE.exists():
        with open(RESULT_FILE, "w", encoding=ENCODING) as f:
            f.write(
                DELIM.join([
                    "strategy_name",
                    "signal_date",
                    "signal_type",
                    "total_data",
                    "created_at"
                ]) + "\n"
            )

    with open(RESULT_FILE, "a", encoding=ENCODING) as f:
        f.write(
            DELIM.join([
                strategy_name,
                signal_date,
                strategy_name,
                str(int(total_data)),
                _now_kst()
            ]) + "\n"
        )


# =====================================================
# STRATEGY_DETAIL (종목 상세)
# =====================================================
def save_strategy_detail(
    signal_date,
    action,
    code,
    name,
    prev_close,
    price,
    diff,
    volume,
    special_value,
    result_id
):

    if not DETAIL_FILE.exists():
        with open(DETAIL_FILE, "w", encoding=ENCODING) as f:
            f.write(
                DELIM.join([
                    "signal_date",
                    "action",
                    "code",
                    "created_at",
                    "diff",
                    "name",
                    "prev_close",
                    "price",
                    "result_id",
                    "special_value",
                    "volume"
                ]) + "\n"
            )

    with open(DETAIL_FILE, "a", encoding=ENCODING) as f:
        f.write(
            DELIM.join([
                signal_date,
                action,
                str(code),
                _now_kst(),
                str(diff),
                name,
                str(prev_close),
                str(price),
                str(result_id),
                str(special_value),
                str(volume)
            ]) + "\n"
        )
