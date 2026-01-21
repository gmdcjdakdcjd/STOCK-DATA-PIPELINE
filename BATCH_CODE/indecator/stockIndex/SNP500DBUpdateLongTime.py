# ============================================================
# 0. 프로젝트 루트 설정 + import 경로 등록
# ============================================================
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # /workspace
sys.path.insert(0, str(PROJECT_ROOT))

# ============================================================
# 1. 기본 import
# ============================================================
import pandas as pd
import yfinance as yf
from datetime import datetime

from BATCH_CODE.common import config   # 🔥 env 로딩
from BATCH_CODE.indecator.indicator_common_flie_saver import append_indicator_row


class SP500YFinanceInitBatchOut:
    def execute(self):
        print("[INFO] SNP500 yfinance 초기 적재 Batch-Out 시작")

        ticker = "^GSPC"

        df = yf.download(
            ticker,
            period="3y",
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=False
        )

        if df.empty:
            print("[WARN] 데이터 없음")
            return

        df.reset_index(inplace=True)

        # MultiIndex 방지
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        # 날짜 컬럼 통일
        for cand in ["Date", "Datetime", "date"]:
            if cand in df.columns:
                df.rename(columns={cand: "date"}, inplace=True)
                break

        df.rename(columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
        }, inplace=True)

        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df["code"] = "SNP500"

        df["change_amount"] = df["close"].diff().fillna(0)
        df["change_rate"] = (
            df["change_amount"] /
            (df["close"] - df["change_amount"]).replace(0, pd.NA)
        ) * 100
        df["change_rate"] = df["change_rate"].fillna(0)

        df = df[["code", "date", "change_amount", "change_rate", "close"]]
        df = df.sort_values("date")   # 과거 → 최신

        for idx, r in df.iterrows():
            append_indicator_row(
                code=r["code"],
                date=r["date"] + " 00:00:00",
                change_amount=r["change_amount"],
                change_rate=r["change_rate"],
                close=r["close"]
            )

            if idx % 50 == 0:
                print(f"[{datetime.now():%Y-%m-%d %H:%M}] INIT WRITE {idx+1}/{len(df)}")

        print(f"[INFO] SNP500 초기 적재 완료 ({len(df)} rows)")


if __name__ == "__main__":
    SP500YFinanceInitBatchOut().execute()
