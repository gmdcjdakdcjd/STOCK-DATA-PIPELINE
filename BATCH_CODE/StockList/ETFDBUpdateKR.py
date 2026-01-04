import os
import json
import pymysql
import pandas as pd
import requests
from bs4 import BeautifulSoup
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

class EtfDailyPriceBatchOut:
    def __init__(self):
        # =============================
        # ENV
        # =============================
        batch_out = os.getenv("BATCH_OUT_DIR")
        if not batch_out:
            raise RuntimeError("BATCH_OUT_DIR not set")

        self.BASE_OUT = batch_out
        self.DELIMITER = os.getenv("TXT_DELIM", "|")

        self.CONFIG_PATH = os.getenv("COMMON_CONFIG_PATH")
        if not self.CONFIG_PATH:
            raise RuntimeError("COMMON_CONFIG_PATH not set")

        # =============================
        # MariaDB (ETF 마스터 조회용)
        # =============================
        DB_PORT = os.getenv("DB_PORT")
        if not DB_PORT or not DB_PORT.isdigit():
            raise RuntimeError(f"DB_PORT invalid: {DB_PORT}")

        self.conn = pymysql.connect(
            host=os.getenv("DB_HOST"),
            port=int(DB_PORT),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            db=os.getenv("DB_NAME"),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )

        self.FIELDS = [
            "code",
            "date",
            "close",
            "diff",
            "high",
            "last_update",
            "low",
            "open",
            "volume",
        ]

        self.codes = {}


    # -------------------------------------------------
    # ETF 코드 로드 (MariaDB)
    # -------------------------------------------------
    def load_etf_codes(self):
        """
        삼성자산운용(KODEX) ETF만 로드
        """
        sql = """
            SELECT code, name
            FROM etf_info_kr
            WHERE manager = '삼성자산운용'
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        self.codes = {r["code"]: r["name"] for r in rows}

        print(f"[INFO] {len(self.codes)}개 ETF 로드 완료 (삼성자산운용)")

    # -------------------------------------------------
    # 네이버 ETF 일별 시세 수집
    # -------------------------------------------------
    def read_naver(self, code, name, pages_to_fetch):
        try:
            url = f"https://finance.naver.com/item/sise_day.nhn?code={code}"
            html = BeautifulSoup(
                requests.get(url, headers={"User-agent": "Mozilla/5.0"}).text,
                "lxml"
            )

            pgrr = html.find("td", class_="pgRR")
            lastpage = 1 if pgrr is None else int(pgrr.a["href"].split("=")[-1])
            pages = min(lastpage, pages_to_fetch)

            df = pd.DataFrame()

            for page in range(1, pages + 1):
                pg_url = f"{url}&page={page}"
                page_df = pd.read_html(
                    requests.get(pg_url, headers={"User-agent": "Mozilla/5.0"}).text
                )[0]
                df = pd.concat([df, page_df], ignore_index=True)

                tmnow = datetime.now().strftime("%Y-%m-%d %H:%M")
                print(f"[{tmnow}] {name} ({code}) {page}/{pages}", end="\r", flush=True)

            df = df.rename(columns={
                "날짜": "date",
                "종가": "close",
                "전일비": "diff",
                "시가": "open",
                "고가": "high",
                "저가": "low",
                "거래량": "volume",
            })

            df["date"] = df["date"].astype(str).str.replace(".", "-", regex=False)
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            df = df.dropna(subset=["date"])

            df["diff"] = df["diff"].astype(str).str.extract(r"(\d+)")
            df = df.dropna()

            df[["close", "diff", "open", "high", "low", "volume"]] = df[
                ["close", "diff", "open", "high", "low", "volume"]
            ].astype(int)

            # 🔥 최신 1일만 사용
            df = df.sort_values("date", ascending=False).head(1)

            return df[["date", "open", "high", "low", "close", "diff", "volume"]]

        except Exception as e:
            print(f"[ERROR] {name}({code}) → {e}")
            return None

    # -------------------------------------------------
    # TXT 저장
    # -------------------------------------------------
    def save_to_txt(self, df, code, txt_path, write_header):
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = "w" if write_header else "a"

        with open(txt_path, mode, encoding="utf-8") as f:
            if write_header:
                f.write(self.DELIMITER.join(self.FIELDS) + "\n")

            for _, r in df.iterrows():
                row = [
                    code,
                    f"{r['date']} 00:00:00",
                    r["close"],
                    r["diff"],
                    r["high"],
                    now_str,
                    r["low"],
                    r["open"],
                    r["volume"],
                ]
                f.write(self.DELIMITER.join(map(str, row)) + "\n")

        return len(df)

    # -------------------------------------------------
    # 전체 실행
    # -------------------------------------------------
    def update_daily_price(self, pages_to_fetch):
        today = datetime.now().strftime("%Y%m%d")
        out_dir = os.path.join(self.BASE_OUT, today)
        os.makedirs(out_dir, exist_ok=True)

        txt_path = os.path.join(out_dir, f"ETF_DAILY_PRICE_KR_{today}.txt")

        total_rows = 0
        total_codes = len(self.codes)

        for idx, (code, name) in enumerate(self.codes.items(), start=1):
            df = self.read_naver(code, name, pages_to_fetch)
            if df is None or df.empty:
                continue

            row_cnt = self.save_to_txt(
                df,
                code,
                txt_path,
                write_header=(idx == 1)
            )

            total_rows += row_cnt

            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] "
                f"#{idx:04d} {name} ({code}) : {row_cnt} rows saved"
            )

        print()
        print(f"ROWCOUNT={total_rows}")
        print(f"CODECOUNT={total_codes}")
        print(f"OUTPUT={txt_path}")

    # -------------------------------------------------
    # daily 실행
    # -------------------------------------------------
    def execute_daily(self):
        self.load_etf_codes()

        with open(self.CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)

        pages_to_fetch = config.get("pages_to_fetch", 1)
        print(f"[INFO] pages_to_fetch={pages_to_fetch}")

        self.update_daily_price(pages_to_fetch)


if __name__ == "__main__":
    batch = EtfDailyPriceBatchOut()
    batch.execute_daily()
    batch.conn.close()
