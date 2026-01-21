# ============================================================
# 0. 프로젝트 루트 설정 + import 경로 등록 (가장 먼저!)
# ============================================================
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # /workspace
sys.path.insert(0, str(PROJECT_ROOT))

# ============================================================
# 1. 기본 import
# ============================================================
import json
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os

from BATCH_CODE.common import config   # 🔥 여기서 env 로딩 완료
from BATCH_CODE.indecator.physical_common_flie_saver import append_indicator_row


class COFFEEDailyBatchOut:
    def __init__(self):
        # ------------------------------------------
        # 공용 config.json 로드
        # ------------------------------------------
        config_path = os.getenv("COMMON_CONFIG_PATH")
        if not config_path:
            raise RuntimeError("COMMON_CONFIG_PATH not set")

        config_path = Path(config_path)

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
                self.pages_to_fetch = cfg.get("pages_to_fetch", 1)
        except FileNotFoundError:
            raise RuntimeError(f"[FATAL] config.json not found: {config_path}")

        print(f"[INFO] COFFEE pages_to_fetch = {self.pages_to_fetch}")

    # -------------------------------------------------
    def read_COFFEE_page(self, page=1):
        try:
            url = (
                "https://finance.naver.com/marketindex/worldDailyQuote.naver"
                f"?marketindexCd=CMDT_KC&fdtc=2&page={page}"
            )
            headers = {"User-Agent": "Mozilla/5.0"}
            soup = BeautifulSoup(requests.get(url, headers=headers).text, "lxml")

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
                sign = -1 if diff_td.find("img") and "하락" in diff_td.img.get("alt", "") else 1
                m = re.search(r"-?\d+\.?\d*", diff_td.get_text(strip=True))
                if not m:
                    continue

                change_amount = sign * float(m.group())
                rate = float(
                    cols[3].get_text(strip=True)
                    .replace("%", "")
                    .replace(",", "")
                    .replace("+", "")
                )

                data.append(["COFFEE", date, change_amount, round(rate, 4), close])

            return pd.DataFrame(
                data,
                columns=["code", "date", "change_amount", "change_rate", "close"]
            )

        except Exception as e:
            print("[ERROR] COFFEE page error:", e)
            return pd.DataFrame()

    # -------------------------------------------------
    def collect_latest(self):
        frames = []
        for page in range(1, self.pages_to_fetch + 1):
            df = self.read_COFFEE_page(page)
            if df.empty:
                break
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        return (
            pd.concat(frames, ignore_index=True)
              .sort_values("date", ascending=False)
              #.head(1)
        )

    # -------------------------------------------------
    def execute(self):
        print("[INFO] COFFEE Batch-Out 시작")
        df = self.collect_latest()

        if df.empty:
            print("[WARN] 데이터 없음")
            return

        for _, r in df.iterrows():
            append_indicator_row(
                code=r["code"],
                date=r["date"],
                change_amount=r["change_amount"],
                change_rate=r["change_rate"],
                close=r["close"]
            )

        print("[INFO] COFFEE Batch-Out 완료")


if __name__ == "__main__":
    COFFEEDailyBatchOut().execute()
