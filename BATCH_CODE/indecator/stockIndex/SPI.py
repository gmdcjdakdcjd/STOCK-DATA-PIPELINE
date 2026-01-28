# SNP500
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]  # /workspace
sys.path.insert(0, str(PROJECT_ROOT))

import json
import requests
import pandas as pd
from datetime import datetime
import os

from BATCH_CODE.common import config
from BATCH_CODE.indecator.stockIndex_common_flie_saver import append_indicator_row


class SPIDailyBatchOut:
    def __init__(self):
        # ----------------------------------------------------
        # 공용 config.json 로드 (env 기반)
        # ----------------------------------------------------
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

        print(f"[INFO] SPI pages_to_fetch = {self.pages_to_fetch}")

    # ===============================================================
    # 1) S&P500 일별 시세 수집 (네이버 JSON API)
    # ===============================================================
    def read_SPI(self):
        try:
            all_rows = []

            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://finance.naver.com/",
            }

            for page in range(1, self.pages_to_fetch + 1):
                url = (
                    "https://finance.naver.com/world/worldDayListJson.naver"
                    f"?symbol=SPI@SPX&fdtc=0&page={page}"
                )

                res = requests.get(url, headers=headers)
                data = res.json()

                # JSON 구조 정규화
                if isinstance(data, dict) and "worldDayList" in data:
                    rows = data["worldDayList"]
                elif isinstance(data, list):
                    rows = data
                else:
                    print("[ERROR] SPI JSON 구조 이상:", data)
                    break

                if not rows:
                    break  # 마지막 페이지

                all_rows.extend(rows)

            if not all_rows:
                return pd.DataFrame()

            df = pd.DataFrame(all_rows)

            # 날짜
            if "day" in df.columns:
                df["date"] = df["day"].str.replace(".", "-")
            elif "xymd" in df.columns:
                df["date"] = pd.to_datetime(
                    df["xymd"], format="%Y%m%d"
                ).dt.strftime("%Y-%m-%d")
            else:
                raise Exception("date field not found")

            df["date"] = df["date"] + " 00:00:00"

            # 종가
            if "close" in df.columns:
                df["close"] = df["close"].astype(str).str.replace(",", "").astype(float)
            elif "clos" in df.columns:
                df["close"] = df["clos"].astype(str).str.replace(",", "").astype(float)
            else:
                raise Exception("close field not found")

            # 전일비
            if "diff" in df.columns:
                df["change_amount"] = (
                    df["diff"].astype(str).str.replace(",", "").astype(float)
                )
            elif "dff" in df.columns:
                df["change_amount"] = (
                    df["dff"].astype(str).str.replace(",", "").astype(float)
                )
            else:
                raise Exception("diff field not found")

            # 등락률
            if "rate" in df.columns:
                df["change_rate"] = df["rate"].astype(float)
            else:
                df["change_rate"] = 0.0

            df = df[["date", "close", "change_amount", "change_rate"]]
            df = df.sort_values("date", ascending=False)

            # 최근 1건
            df = (df.sort_values("date", ascending=False).head(1))

            return df.copy()

        except Exception as e:
            print("[ERROR] SPI read error:", e)
            return pd.DataFrame()

    # ===============================================================
    # 2) TXT append (공통 writer)
    # ===============================================================
    def write_indicator(self, df):
        for idx, r in df.iterrows():
            append_indicator_row(
                code="SPI",
                date=r["date"],
                change_amount=r["change_amount"],
                change_rate=r["change_rate"],
                close=r["close"],
            )

            tmnow = datetime.now().strftime("%Y-%m-%d %H:%M")
            print(f"[{tmnow}] #{idx + 1:04d} SPI > WRITE TXT OK")

    # ===============================================================
    # 3) 실행
    # ===============================================================
    def execute(self):
        print("[INFO] SPI Batch-Out 시작")
        df = self.read_SPI()

        if df.empty:
            print("[WARN] SPI 데이터 없음")
            return

        self.write_indicator(df)
        print("[INFO] SPI Batch-Out 완료")


# 실행부
if __name__ == "__main__":
    SPIDailyBatchOut().execute()
