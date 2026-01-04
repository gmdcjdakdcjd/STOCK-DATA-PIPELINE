import sys
from pathlib import Path

# 🔥 여기 핵심
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # /workspace
sys.path.insert(0, str(PROJECT_ROOT))

from BATCH_CODE.common import config

import os
import pandas as pd
from datetime import datetime, timedelta
import pymysql
from FinanceDataReader.investing.data import InvestingDailyReader


# ❌ load_dotenv / load_env 전부 제거
# env는 common/config.py에서 이미 로딩됨

# =========================
# ENV
# =========================
BATCH_OUT_DIR = os.getenv("BATCH_OUT_DIR")
if not BATCH_OUT_DIR:
    raise RuntimeError("BATCH_OUT_DIR not set")

DELIM = os.getenv("TXT_DELIM", "|")
ENCODING = os.getenv("TXT_ENCODING", "utf-8")

DB_PORT = os.getenv("DB_PORT")
if not DB_PORT or not DB_PORT.isdigit():
    raise RuntimeError(f"DB_PORT invalid: {DB_PORT}")

BASE_OUT_DIR = Path(BATCH_OUT_DIR)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(DB_PORT),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}


# =========================
# UTIL
# =========================
def _today():
    return datetime.now().strftime("%Y%m%d")


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _get_out_file():
    out_dir = BASE_OUT_DIR / _today()
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"BOND_DAILY_PRICE_{_today()}.txt"


def _write_header_if_needed(path):
    if not path.exists():
        with open(path, "w", encoding=ENCODING) as f:
            f.write(DELIM.join([
                "date",
                "code",
                "close",
                "diff",
                "high",
                "last_update",
                "low",
                "open"
            ]) + "\n")


# -------------------------------------------------------------
# 1) MariaDB에서 bond ticker 조회
# -------------------------------------------------------------
def load_bond_tickers():
    conn = pymysql.connect(**DB_CONFIG)

    sql = """
        SELECT ticker
        FROM bond_info
        ORDER BY country, maturity
    """

    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    conn.close()

    return [r["ticker"] for r in rows]


# -------------------------------------------------------------
# 2) Investing.com 최신 채권 금리
# -------------------------------------------------------------
def fetch_latest_yield(ticker: str):
    try:
        today = datetime.now()
        start = today - timedelta(days=7)

        reader = InvestingDailyReader(
            symbol=ticker,
            start=start.strftime("%Y-%m-%d"),
            end=today.strftime("%Y-%m-%d")
        )
        df = reader.read()

        if df is None or df.empty:
            print(f"[WARN] No data in range for {ticker}")
            return None

        df = df.reset_index()
        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Price": "close"
        }, inplace=True)

        df["date"] = pd.to_datetime(df["date"])
        latest = df.sort_values("date", ascending=False).head(1).copy()
        latest["date"] = latest["date"].dt.strftime("%Y-%m-%d")

        return latest[["date", "open", "high", "low", "close"]]

    except Exception as e:
        print(f"[ERROR] {ticker} → {e}")
        return None


# -------------------------------------------------------------
# 3) TXT append
# -------------------------------------------------------------
def write_bond_txt(code, df):
    path = _get_out_file()
    _write_header_if_needed(path)

    df = df.sort_values("date").reset_index(drop=True)
    df["diff"] = df["close"].diff().fillna(0)

    with open(path, "a", encoding=ENCODING) as f:
        for _, r in df.iterrows():
            row = [
                r["date"] + " 00:00:00",
                code,
                r["close"],
                r["diff"],
                r["high"],
                _now(),
                r["low"],
                r["open"]
            ]
            f.write(DELIM.join(map(str, row)) + "\n")


# -------------------------------------------------------------
# 4) 실행
# -------------------------------------------------------------
def run():
    print("=== BOND DAILY Batch-Out 시작 ===")

    tickers = load_bond_tickers()
    print(f"[INFO] ticker count = {len(tickers)}")

    for ticker in tickers:
        print(f"[FETCH] {ticker}")
        df = fetch_latest_yield(ticker)
        if df is None or df.empty:
            continue
        write_bond_txt(ticker, df)

    print("=== BOND DAILY Batch-Out 완료 ===")


if __name__ == "__main__":
    run()
