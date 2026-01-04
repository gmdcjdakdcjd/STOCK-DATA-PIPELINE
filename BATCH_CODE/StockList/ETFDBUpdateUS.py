import os
import json
import pandas as pd
import yfinance as yf
import pymysql
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


class UsEtfDailyPriceBatchOut:
    def __init__(self):
        # ---------------------------------------------
        # ENV
        # ---------------------------------------------
        batch_out = os.getenv("BATCH_OUT_DIR")
        if not batch_out:
            raise RuntimeError("BATCH_OUT_DIR not set")

        self.BASE_OUT = batch_out
        self.DELIMITER = os.getenv("TXT_DELIM", "|")

        self.CONFIG_PATH = os.getenv("COMMON_CONFIG_PATH")
        if not self.CONFIG_PATH:
            raise RuntimeError("COMMON_CONFIG_PATH not set")

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
            "high",
            "last_update",
            "low",
            "open",
            "volume",
        ]

        self.codes = {}


    # -------------------------------------------------
    # 1️⃣ ETF 코드 로드 (BlackRock / iShares)
    # -------------------------------------------------
    def load_etf_codes(self):
        sql = """
            SELECT code, name
            FROM etf_info_us
            WHERE issuer = 'BlackRock (iShares)'
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        self.codes = {r["code"]: r["name"] for r in rows}

        print(f"불러온 ETF 수: {len(self.codes)}개")

    # -------------------------------------------------
    # 2️⃣ yfinance 수집
    # -------------------------------------------------
    def read_yfinance(self, code, name, period):
        try:
            df = yf.download(
                code,
                period=period,
                interval="1d",
                auto_adjust=True,
                threads=False,
                progress=False
            )

            if df.empty:
                print(f"데이터 없음: {code}")
                return None

            df.reset_index(inplace=True)

            # MultiIndex flatten
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]

            df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume"
            }, inplace=True)

            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df["code"] = code

            df = df[["code", "date", "open", "high", "low", "close", "volume"]]

            # 🔥 최신 1일만 사용
            df = df.sort_values("date", ascending=False).head(1)

            return df

        except Exception as e:
            print(f"{name} ({code}) 처리 중 오류: {e}")
            return None

    # -------------------------------------------------
    # 3️⃣ TXT 저장
    # -------------------------------------------------
    def save_to_txt(self, df, txt_path, write_header):
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = "w" if write_header else "a"

        with open(txt_path, mode, encoding="utf-8") as f:
            if write_header:
                f.write(self.DELIMITER.join(self.FIELDS) + "\n")

            for _, r in df.iterrows():
                row = [
                    r["code"],
                    f"{r['date']} 00:00:00",
                    float(r["close"]),
                    float(r["high"]),
                    now_str,
                    float(r["low"]),
                    float(r["open"]),
                    int(r["volume"]),
                ]
                f.write(self.DELIMITER.join(map(str, row)) + "\n")

        return len(df)

    # -------------------------------------------------
    # 4️⃣ 전체 실행
    # -------------------------------------------------
    def update_daily_price(self, period):
        today = datetime.now().strftime("%Y%m%d")
        out_dir = os.path.join(self.BASE_OUT, today)
        os.makedirs(out_dir, exist_ok=True)

        txt_path = os.path.join(out_dir, f"ETF_DAILY_PRICE_US_{today}.txt")

        total_rows = 0
        processed = 0
        total_codes = len(self.codes)

        for idx, (code, name) in enumerate(self.codes.items()):
            print(f"\n[{idx + 1}/{total_codes}] {name} ({code}) 시세 수집 중...")
            processed += 1

            df = self.read_yfinance(code, name, period)
            if df is None:
                continue

            print(df.tail(3))  # 🔥 Mongo 버전과 동일

            row_cnt = self.save_to_txt(
                df,
                txt_path,
                write_header=(idx == 0)
            )

            total_rows += row_cnt

            print(f"{name} ({code}) 저장 완료")

        print("\n모든 업데이트 완료.")
        print(f"총 저장된 행 수: {total_rows}")
        print(f"총 처리된 ETF 수: {processed}")
        print(f"ROWCOUNT={total_rows}")
        print(f"CODECOUNT={processed}")
        print(f"OUTPUT={txt_path}")

    # -------------------------------------------------
    # entry
    # -------------------------------------------------
    def execute_daily(self):
        self.load_etf_codes()

        with open(self.CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)

        period = config.get("period", "10d")  # 3d → 10d 권장
        self.update_daily_price(period)


if __name__ == "__main__":
    batch = UsEtfDailyPriceBatchOut()
    batch.execute_daily()
    batch.conn.close()
