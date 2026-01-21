# 달러 대비 엔화 지수
## 원화 대비 엔화 지수로 변경했으므로 더이상 수집 안함.
### 결론적으로 2026.01.08 이후로 사용 안하는 코드.

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
from BATCH_CODE.indecator.indicator_common_flie_saver import append_indicator_row


class USDJPYDailyBatchOut:
    def __init__(self):
        # =============================
        # ENV 기반 설정
        # =============================
        batch_out = os.getenv("BATCH_OUT_DIR")
        if not batch_out:
            raise RuntimeError("BATCH_OUT_DIR not set")

        self.BASE_OUT = Path(batch_out)
        self.DELIMITER = os.getenv("TXT_DELIM", "|")

        config_path = os.getenv("COMMON_CONFIG_PATH")
        if not config_path:
            raise RuntimeError("COMMON_CONFIG_PATH not set")

        config_path = Path(config_path)

        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
            self.pages_to_fetch = config.get("pages_to_fetch", 1)

        print(f"[INFO] USD_JPY pages_to_fetch={self.pages_to_fetch}")


    # -------------------------------------------------
    # 1) USD/JPY 한 페이지 수집
    # -------------------------------------------------
    def read_usd_jpy(self, page):
        url = (
            "https://finance.naver.com/marketindex/worldDailyQuote.naver"
            f"?marketindexCd=FX_USDJPY&fdtc=4&page={page}"
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
            sign = -1 if diff_td.find("img") and "하락" in diff_td.img.get("alt", "") else 1
            m = re.search(r"-?\d+\.?\d*", diff_td.get_text(strip=True))
            if not m:
                continue
            change_amount = sign * float(m.group())

            rate_raw = cols[3].get_text(strip=True)
            rate_raw = rate_raw.replace("%", "").replace("+", "").replace(",", "")
            change_rate = float(rate_raw)

            data.append([
                "USD_JPY",
                date,
                change_amount,
                change_rate,
                close
            ])

        return pd.DataFrame(
            data,
            columns=["code", "date", "change_amount", "change_rate", "close"]
        )

    # -------------------------------------------------
    # 2) 최신 데이터 수집
    # -------------------------------------------------
    def collect_latest(self):
        frames = []

        for page in range(1, self.pages_to_fetch + 1):
            print(f"[INFO] USD_JPY page {page}/{self.pages_to_fetch}")
            df = self.read_usd_jpy(page)
            if df.empty:
                break
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        df_all = df_all.sort_values("date", ascending=False)

        # 최신 1일만
        return df_all.head(1)
        #return df_all.copy()


    # -------------------------------------------------
    # 4) 실행
    # -------------------------------------------------
    def execute(self):
        print("[INFO] USD_JPY Batch-Out 시작")
        df = self.collect_latest()

        if df.empty:
            print("[WARN] USD_JPY 데이터 없음")
            return

        for idx, r in df.iterrows():
            append_indicator_row(
                code=r["code"],  # "USD_JPY"
                date=r["date"],  # "YYYY-MM-DD 00:00:00"
                change_amount=r["change_amount"],
                change_rate=r["change_rate"],
                close=r["close"]
            )

            tmnow = datetime.now().strftime("%Y-%m-%d %H:%M")
            print(f"[{tmnow}] #{idx + 1:04d} USD_JPY > WRITE INDICATOR OK")

        print("[OK] USD_JPY DAILY_PRICE_INDICATOR append 완료")
        print(f"ROWCOUNT={len(df)}")


if __name__ == "__main__":
    batch = USDJPYDailyBatchOut()
    batch.execute()
