import os
import pandas as pd
import urllib.request
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


class UsSp500CodeBatchOut:
    def __init__(self):
        # =============================
        # ENV 기반 Batch-Out 설정
        # =============================
        batch_out = os.getenv("BATCH_OUT_DIR")
        if not batch_out:
            raise RuntimeError("BATCH_OUT_DIR not set")

        self.BASE_OUT = Path(batch_out)
        self.DELIMITER = os.getenv("TXT_DELIM", "|")

        self.URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        self.HEADERS = {"User-Agent": "Mozilla/5.0"}


    # ------------------------------------------------------------
    # 1. S&P500 리스트 수집
    # ------------------------------------------------------------
    def fetch_sp500(self):
        req = urllib.request.Request(self.URL, headers=self.HEADERS)
        html = urllib.request.urlopen(req).read()

        tables = pd.read_html(html)

        sp500 = None
        for t in tables:
            if "Symbol" in t.columns:
                sp500 = t
                break

        if sp500 is None:
            raise RuntimeError("S&P500 테이블을 찾을 수 없습니다.")

        sp500 = sp500[[
            "Symbol",
            "Security",
            "GICS Sector",
            "GICS Sub-Industry",
            "CIK"
        ]].copy()

        sp500.columns = [
            "code",
            "name",
            "sector",
            "industry",
            "cik"
        ]

        sp500["market"] = "S&P500"
        sp500["code"] = sp500["code"].str.replace(".", "-", regex=False)

        print(f"[INFO] 총 {len(sp500)}개 S&P500 종목 수집 완료")
        return sp500

    # ------------------------------------------------------------
    # 2. TXT 저장 (Batch-Out)
    # ------------------------------------------------------------
    def save_to_txt(self, df):
        today = datetime.now().strftime("%Y%m%d")
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        out_dir = self.BASE_OUT / today
        out_dir.mkdir(parents=True, exist_ok=True)

        txt_path = out_dir / f"COMPANY_INFO_US_{today}.txt"

        fields = [
            "code",
            "name",
            "market",
            "sector",
            "industry",
            "cik",
            "last_update"
        ]

        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(self.DELIMITER.join(fields) + "\n")

            for idx, r in df.iterrows():
                row = [
                    r["code"],
                    r["name"],
                    r["market"],
                    r["sector"],
                    r["industry"],
                    r["cik"],
                    now_str
                ]
                f.write(self.DELIMITER.join(map(str, row)) + "\n")

                tmnow = datetime.now().strftime("%Y-%m-%d %H:%M")
                print(
                    f"[{tmnow}] #{idx+1:04d} "
                    f"{r['name']} ({r['code']}) > WRITE TXT OK"
                )

        print()
        print("[OK] S&P500 COMPANY_INFO_US TXT 생성 완료")
        print(f"ROWCOUNT={len(df)}")
        print(f"OUTPUT={txt_path}")

    # ------------------------------------------------------------
    # 3. 실행
    # ------------------------------------------------------------
    def execute(self):
        df = self.fetch_sp500()
        self.save_to_txt(df)


if __name__ == "__main__":
    batch = UsSp500CodeBatchOut()
    batch.execute()
