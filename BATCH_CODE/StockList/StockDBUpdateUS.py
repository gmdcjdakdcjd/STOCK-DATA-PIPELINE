import os
import pymysql
import pandas as pd
import yfinance as yf
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

class UsDailyPriceBatchOut:
    def __init__(self):
        batch_out = os.getenv("BATCH_OUT_DIR")
        if not batch_out:
            raise RuntimeError("BATCH_OUT_DIR not set")

        self.BASE_OUT = batch_out
        self.DELIMITER = os.getenv("TXT_DELIM", "|")

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

        self.codes = {}


    # -------------------------------------------------
    # US 종목 코드 로드
    # -------------------------------------------------
    def load_us_codes(self):
        sql = """
            SELECT code, name
            FROM company_info_us
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        self.codes = {r["code"]: r["name"] for r in rows}

        print(f"불러온 종목 수: {len(self.codes)}개")

    # -------------------------------------------------
    # yfinance 수집 (최신 1일)
    # -------------------------------------------------
    def read_yfinance(self, code):
        df = yf.download(
            code,
            period="2y",
            interval="1d",
            auto_adjust=True,
            progress=False
        )

        if df.empty:
            return None

        df.reset_index(inplace=True)

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

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
        df = df.sort_values("date", ascending=False).head(1)

        return df

    # -------------------------------------------------
    # TXT 저장
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
                    r["close"],
                    r["high"],
                    now_str,
                    r["low"],
                    r["open"],
                    r["volume"],
                ]
                f.write(self.DELIMITER.join(map(str, row)) + "\n")

        return len(df)

    # -------------------------------------------------
    # 실행
    # -------------------------------------------------
    def execute_daily(self):
        self.load_us_codes()

        today = datetime.now().strftime("%Y%m%d")
        out_dir = os.path.join(self.BASE_OUT, today)
        os.makedirs(out_dir, exist_ok=True)

        txt_path = os.path.join(out_dir, f"DAILY_PRICE_US_{today}.txt")

        total_count = 0
        processed_codes = 0
        total_codes = len(self.codes)

        for idx, (code, name) in enumerate(self.codes.items(), start=1):
            print(f"\n[{idx}/{total_codes}] {name} ({code}) 데이터 수집 중...")
            processed_codes += 1

            try:
                df = self.read_yfinance(code)
                if df is None or df.empty:
                    print(f"{code}: 데이터 비어 있음")
                    continue

                row_cnt = self.save_to_txt(
                    df,
                    txt_path,
                    write_header=(idx == 1)
                )

                total_count += row_cnt
                print(f"{name} ({code}) 저장 완료")

            except Exception as e:
                print(f"{name} ({code}) 오류: {e}")

        print("\n모든 업데이트 완료.")
        print(f"총 저장된 행 수: {total_count}")
        print(f"총 처리된 종목 수: {processed_codes}")
        print(f"ROWCOUNT={total_count}")
        print(f"CODECOUNT={processed_codes}")
        print(f"OUTPUT={txt_path}")


if __name__ == "__main__":
    batch = UsDailyPriceBatchOut()
    batch.execute_daily()
    batch.conn.close()
