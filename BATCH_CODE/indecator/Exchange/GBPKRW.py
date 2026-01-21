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
from BATCH_CODE.indecator.exchange_common_flie_saver import append_indicator_row


class FXDailyBatchOut:
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

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
                self.pages_to_fetch = config.get("pages_to_fetch", 1)
        except FileNotFoundError:
            raise RuntimeError(f"[FATAL] config.json not found: {config_path}")

        print(f"[INFO] pages_to_fetch={self.pages_to_fetch}")



    # -------------------------------------------------
    # 1) GBPKRW 한 페이지 수집
    # -------------------------------------------------
    def read_fx_GBPKRW(self, page):
        url = (
            "https://finance.naver.com/marketindex/exchangeDailyQuote.naver"
            f"?marketindexCd=FX_GBPKRW&page={page}"
        )

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://finance.naver.com/marketindex/exchangeDetail.naver?marketindexCd=FX_GBPKRW"
        }

        html = requests.get(url, headers=headers).text
        soup = BeautifulSoup(html, "lxml")

        rows = soup.select("table.tbl_exchange.today tbody tr")
        data = []

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 3:
                continue

            date_raw = cols[0].text.strip()
            if date_raw.count(".") != 2:
                continue

            date = date_raw.replace(".", "-")

            close = float(cols[1].text.strip().replace(",", ""))

            diff_td = cols[2]
            sign = -1 if diff_td.find("img") and "하락" in diff_td.img.get("alt", "") else 1
            text = diff_td.get_text(strip=True)
            text = re.sub(r"[^\d.]", "", text)

            if not text:
                continue

            change_amount = sign * float(text)

            # 네이버 특유 보정
            if abs(change_amount) >= 100:
                change_amount /= 100

            change_rate = (change_amount / (close - change_amount)) * 100 if close != change_amount else 0

            data.append([
                "GBPKRW",
                f"{date} 00:00:00",
                close,
                change_amount,
                round(change_rate, 4)
            ])

        return pd.DataFrame(
            data,
            columns=["code", "date", "close", "change_amount", "change_rate"]
        )

    # -------------------------------------------------
    # 2) 전체 페이지 수집 (최신 기준)
    # -------------------------------------------------
    def collect_latest(self):
        frames = []

        for page in range(1, self.pages_to_fetch + 1):
            print(f"[INFO] GBPKRW page {page}/{self.pages_to_fetch}")
            df = self.read_fx_GBPKRW(page)
            if df.empty:
                break
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        df_all = df_all.sort_values("date", ascending=False)

        # TODO: 현재는 최신 1건만 반환. 제한 풀려면 head(1) 제거하고 전체 반환
        # 🔥 최신 1일만
        # return df_all.head(1)
        return df_all.copy()

    # -------------------------------------------------
    # 3) TXT 저장
    # -------------------------------------------------
    def save_to_txt(self, df):
        today = datetime.now().strftime("%Y%m%d")
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        out_dir = self.BASE_OUT / today
        out_dir.mkdir(parents=True, exist_ok=True)

        txt_path = out_dir / f"DAILY_PRICE_INDICATOR_{today}.txt"

        fields = ["code", "date", "change_amount", "change_rate", "close", "last_update"]

        write_header = not os.path.exists(txt_path)

        with open(txt_path, "a", encoding="utf-8") as f:
            if write_header:
                f.write(self.DELIMITER.join(fields) + "\n")

            for idx, r in df.iterrows():
                row = [
                    r["code"],  # GBPKRW
                    r["date"],  # YYYY-MM-DD 00:00:00
                    r["change_amount"],
                    r["change_rate"],
                    r["close"],
                    now_str
                ]
                f.write(self.DELIMITER.join(map(str, row)) + "\n")

                tmnow = datetime.now().strftime("%Y-%m-%d %H:%M")
                print(f"[{tmnow}] #{idx + 1:04d} GBPKRW > WRITE INDICATOR OK")

        print(f"[OK] DAILY_PRICE_INDICATOR TXT append 완료")
        print(f"ROWCOUNT={len(df)}")
        print(f"OUTPUT={txt_path}")

    # -------------------------------------------------
    # 4) 실행
    # -------------------------------------------------
    def execute(self):
        print("[INFO] GBPKRW Batch-Out 시작")
        df = self.collect_latest()

        if df.empty:
            print("[WARN] 수집 데이터 없음")
            return

        for idx, r in df.iterrows():
            append_indicator_row(
                code=r["code"],  # "GBPKRW"
                date=r["date"],  # "YYYY-MM-DD 00:00:00"
                change_amount=r["change_amount"],
                change_rate=r["change_rate"],
                close=r["close"]
            )

            tmnow = datetime.now().strftime("%Y-%m-%d %H:%M")
            print(f"[{tmnow}] #{idx + 1:04d} GBPKRW > WRITE INDICATOR OK")

        print("[OK] DAILY_PRICE_INDICATOR TXT append 완료")
        print(f"ROWCOUNT={len(df)}")


if __name__ == "__main__":
    batch = FXDailyBatchOut()
    batch.execute()
