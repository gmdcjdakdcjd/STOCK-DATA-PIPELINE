import requests
import pandas as pd
import re
import time
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# ------------------------------------------
# ENV 로딩 (APP_ENV 기준)
# ------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]

APP_ENV = os.getenv("APP_ENV", "local")   # local / docker
env_file = BASE_DIR / f".env.{APP_ENV}"

if not env_file.exists():
    raise RuntimeError(f"ENV FILE NOT FOUND: {env_file}")

load_dotenv(env_file)



class UsEtfInfoBatchOut:
    def __init__(self):
        # =============================
        # ENV
        # =============================
        batch_out = os.getenv("BATCH_OUT_DIR")
        if not batch_out:
            raise RuntimeError("BATCH_OUT_DIR not set")

        self.BASE_OUT = batch_out
        self.DELIMITER = os.getenv("TXT_DELIM", "|")

        self.FIELDS = [
            "code",
            "name",
            "issuer",
            "market",
            "last_update",
        ]


    # ------------------------------------------------------------
    # 1. 미국 ETF 리스트 수집 (Nasdaq API)
    # ------------------------------------------------------------
    def fetch_us_etf_list(self):
        base_url = (
            "https://api.nasdaq.com/api/screener/etf"
            "?tableonly=true&limit=50&offset={}"
        )

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nasdaq.com/market-activity/etfs",
        }

        all_rows = []
        offset = 0

        while True:
            url = base_url.format(offset)
            r = requests.get(url, headers=headers, timeout=20)

            if r.status_code != 200:
                print(f"[WARN] 요청 실패 (offset={offset}) → {r.status_code}")
                break

            data = r.json()
            try:
                rows = data["data"]["records"]["data"]["rows"]
            except KeyError:
                print(f"[WARN] 데이터 구조 오류 (offset={offset})")
                break

            if not rows:
                break

            all_rows.extend(rows)
            print(f"[INFO] {offset} ~ {offset + 50} 수집 (누적 {len(all_rows)}개)")

            offset += 50
            time.sleep(0.3)

            if offset > 5000:
                break

        df = pd.DataFrame(all_rows)[["symbol", "companyName"]]
        df.columns = ["code", "name"]

        df["market"] = "US_ETF"
        df = df.dropna(subset=["code", "name"])
        df["code"] = df["code"].astype(str).str.strip()
        df["name"] = (
            df["name"]
            .astype(str)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

        # ------------------------------------------------------------
        # 운용사(issuer) 추출
        # ------------------------------------------------------------
        issuer_patterns = {
            "Vanguard": "Vanguard",
            "iShares": "BlackRock (iShares)",
            "SPDR": "State Street (SPDR)",
            "Invesco": "Invesco",
            "Schwab": "Charles Schwab",
            "Global X": "Mirae Asset (Global X)",
            "ARK": "ARK Invest",
            "VanEck": "VanEck",
            "WisdomTree": "WisdomTree",
            "ProShares": "ProShares",
            "Direxion": "Direxion",
            "Amplify": "Amplify",
            "First Trust": "First Trust",
            "PIMCO": "PIMCO",
            "JPMorgan": "J.P. Morgan",
        }

        def extract_issuer(name):
            for keyword, issuer_name in issuer_patterns.items():
                if re.search(rf"\b{keyword}\b", name, re.IGNORECASE):
                    return issuer_name
            return "Unknown"

        df["issuer"] = df["name"].apply(extract_issuer)

        print(f"[DONE] 총 {len(df)}개 미국 ETF 수집 완료")
        return df

    # ------------------------------------------------------------
    # 2. TXT 저장 (Batch-Out)
    # ------------------------------------------------------------
    def save_to_txt(self, df):
        today = datetime.now().strftime("%Y%m%d")
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        out_dir = os.path.join(self.BASE_OUT, today)
        os.makedirs(out_dir, exist_ok=True)

        txt_path = os.path.join(out_dir, f"ETF_INFO_US_{today}.txt")

        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(self.DELIMITER.join(self.FIELDS) + "\n")

            for _, r in df.iterrows():
                row = [
                    r["code"],
                    r["name"],
                    r["issuer"],
                    r["market"],
                    now_str,
                ]
                f.write(self.DELIMITER.join(map(str, row)) + "\n")

        print("[OK] ETF_INFO_US TXT 생성 완료")
        print(f"ROWCOUNT={len(df)}")
        print(f"OUTPUT={txt_path}")

    # ------------------------------------------------------------
    # 3. 실행
    # ------------------------------------------------------------
    def execute(self):
        df = self.fetch_us_etf_list()
        self.save_to_txt(df)


# ------------------------------------------------------------
# main
# ------------------------------------------------------------
if __name__ == "__main__":
    batch = UsEtfInfoBatchOut()
    batch.execute()
