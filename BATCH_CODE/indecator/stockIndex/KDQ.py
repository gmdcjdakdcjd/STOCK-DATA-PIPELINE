import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # /workspace
sys.path.insert(0, str(PROJECT_ROOT))

import json
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os

from BATCH_CODE.common import config
from BATCH_CODE.indecator.stockIndex_common_flie_saver import append_indicator_row

class KDQDDailyBatchOut:
    def __init__(self):
        # =============================
        # ENV 기반 설정
        # =============================
        config_path = os.getenv("COMMON_CONFIG_PATH")
        if not config_path:
            raise RuntimeError("COMMON_CONFIG_PATH not set")

        config_path = Path(config_path)

        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
            self.pages_to_fetch = config.get("pages_to_fetch", 1)

        print(f"[INFO] KDQ pages_to_fetch={self.pages_to_fetch}")


    # ---------------------------------------------------------------------
    # 1) KDQ 단일 페이지 수집
    # ---------------------------------------------------------------------
    def read_KDQ_page(self, page):
        try:
            url = f"https://finance.naver.com/sise/sise_index_day.naver?code=KOSDAQ&page={page}"
            html = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).text
            return pd.read_html(html)[0]
        except Exception:
            return pd.DataFrame()

    # ---------------------------------------------------------------------
    # 2) 페이지 수집
    # ---------------------------------------------------------------------
    def collect_latest(self):
        frames = []

        for page in range(1, self.pages_to_fetch + 1):
            print(f"[INFO] KDQ {page}/{self.pages_to_fetch} 페이지 수집", end="\r")

            df = self.read_KDQ_page(page)
            if df.empty:
                break

            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)

        df = df.rename(columns={
            "날짜": "date",
            "체결가": "close",
            "전일비": "change_amount",
            "등락률": "change_rate"
        })

        df["date"] = df["date"].astype(str)
        df = df[df["date"].str.contains(r"\d{4}\.\d{2}\.\d{2}")]
        df["date"] = df["date"].str.replace(".", "-", regex=False)
        df["date"] = df["date"] + " 00:00:00"

        df["close"] = df["close"].astype(str).str.replace(",", "").astype(float)

        df["change_amount"] = (
            df["change_amount"]
            .astype(str)
            .str.replace(",", "")
            .str.replace("▲", "")
            .str.replace("▼", "")
            .str.extract(r"(-?\d+\.?\d*)")[0]
            .astype(float)
        )

        df["change_rate"] = (
            df["change_rate"]
            .astype(str)
            .str.replace("%", "")
            .astype(float)
        )

        df = df.sort_values("date", ascending=False)

        # 🔥 최신 1일만
        #return df.head(1)

        return df.copy()

    # ---------------------------------------------------------------------
    # 3) TXT로 append
    # ---------------------------------------------------------------------
    def write_indicator(self, df):
        for idx, r in df.iterrows():
            append_indicator_row(
                code="KDQ",
                date=r["date"],
                change_amount=r["change_amount"],
                change_rate=r["change_rate"],
                close=r["close"]
            )

            tmnow = datetime.now().strftime("%Y-%m-%d %H:%M")
            print(f"[{tmnow}] #{idx+1:04d} KDQ > WRITE TXT OK")

    # ---------------------------------------------------------------------
    # 4) 실행
    # ---------------------------------------------------------------------
    def execute(self):
        print("[INFO] KDQ Batch-Out 시작")
        df = self.collect_latest()

        if df.empty:
            print("[WARN] KDQ 데이터 없음")
            return

        self.write_indicator(df)
        print("[INFO] KDQ Batch-Out 완료")


if __name__ == "__main__":
    KDQDDailyBatchOut().execute()
