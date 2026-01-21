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
from BATCH_CODE.indecator.physical_common_flie_saver import append_indicator_row


class GoldGlobalDailyBatchOut:
    def __init__(self):
        # ------------------------------------------
        # 공용 config.json 로드 (env 기반)
        # ------------------------------------------
        config_path = os.getenv("COMMON_CONFIG_PATH")

        if not config_path:
            raise RuntimeError("COMMON_CONFIG_PATH not set")

        config_path = Path(config_path)

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
                self.pages_to_fetch = config.get("pages_to_fetch", 1)
        except FileNotFoundError:
            raise RuntimeError(f"[FATAL] config.json not found: {config_path}")

        print(f"[INFO] GOLD_GLOBAL pages_to_fetch = {self.pages_to_fetch}")


    # ------------------------------------------------------------------
    # 1) 국제 금(GOLD_GLOBAL) 일별 시세 한 페이지 수집
    # ------------------------------------------------------------------
    def read_gold_daily(self, page=1):
        try:
            url = (
                "https://finance.naver.com/marketindex/worldDailyQuote.naver"
                f"?marketindexCd=CMDT_GC&fdtc=2&page={page}"
            )
            headers = {"User-Agent": "Mozilla/5.0"}

            html = requests.get(url, headers=headers).text
            soup = BeautifulSoup(html, "lxml")

            rows = soup.select("table.tbl_exchange.today tbody tr")
            data = []

            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue

                date_raw = cols[0].get_text(strip=True)
                if date_raw.count(".") != 2:
                    continue
                date = date_raw.replace(".", "-") + " 00:00:00"

                close = float(cols[1].get_text(strip=True).replace(",", ""))

                diff_td = cols[2]
                sign = 1
                img = diff_td.find("img")
                if img and "하락" in img.get("alt", ""):
                    sign = -1

                m = re.search(r"-?\d+\.?\d*", diff_td.get_text(strip=True))
                if not m:
                    continue
                change_amount = sign * float(m.group())

                rate_raw = cols[3].get_text(strip=True)
                rate_raw = rate_raw.replace("%", "").replace("+", "").replace(",", "")
                change_rate = float(rate_raw)

                data.append([
                    "GOLD_GLOBAL",
                    date,
                    change_amount,
                    round(change_rate, 4),
                    close
                ])

            return pd.DataFrame(
                data,
                columns=["code", "date", "change_amount", "change_rate", "close"]
            )

        except Exception as e:
            print("[ERROR] GOLD_GLOBAL page error:", e)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # 2) 여러 페이지 수집
    # ------------------------------------------------------------------
    def collect_latest(self):
        frames = []

        for page in range(1, self.pages_to_fetch + 1):
            print(f"[INFO] GOLD_GLOBAL page {page}/{self.pages_to_fetch}", end="\r")

            df = self.read_gold_daily(page)
            if df.empty:
                break

            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        df_all = df_all.sort_values("date", ascending=False)

        # 🔥 최신 1일만
        #return df_all.head(1)
        return df_all.copy()

    # ------------------------------------------------------------------
    # 3) TXT append (공통 writer 사용)
    # ------------------------------------------------------------------
    def write_indicator(self, df):
        for idx, r in df.iterrows():
            append_indicator_row(
                code=r["code"],              # GOLD_GLOBAL
                date=r["date"],              # YYYY-MM-DD 00:00:00
                change_amount=r["change_amount"],
                change_rate=r["change_rate"],
                close=r["close"]
            )

            tmnow = datetime.now().strftime("%Y-%m-%d %H:%M")
            print(f"[{tmnow}] #{idx + 1:04d} GOLD_GLOBAL > WRITE TXT OK")

    # ------------------------------------------------------------------
    # 4) 실행
    # ------------------------------------------------------------------
    def execute(self):
        print("[INFO] GOLD_GLOBAL Batch-Out 시작")
        df = self.collect_latest()

        if df.empty:
            print("[WARN] GOLD_GLOBAL 데이터 없음")
            return

        self.write_indicator(df)
        print("[INFO] GOLD_GLOBAL Batch-Out 완료")


if __name__ == "__main__":
    GoldGlobalDailyBatchOut().execute()
