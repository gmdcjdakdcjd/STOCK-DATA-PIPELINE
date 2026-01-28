import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv


class MonthlyCodeBatchOut:
    def __init__(self):
        # ---------------------------------
        # ENV 로딩 (프로젝트 루트 기준)
        # ---------------------------------
        BASE_DIR = Path(__file__).resolve().parents[2]

        APP_ENV = os.getenv("APP_ENV", "local")
        env_file = BASE_DIR / f".env.{APP_ENV}"

        if not env_file.exists():
            raise RuntimeError(f"ENV FILE NOT FOUND: {env_file}")

        load_dotenv(env_file)

        # ---------------------------------
        # ENV 기반 설정
        # ---------------------------------
        self.BASE_OUT = os.getenv("BATCH_OUT_DIR")
        self.DELIMITER = os.getenv("TXT_DELIM", "|")
        self.PATH_KRX = os.getenv("MONTHLY_KRX_CSV")

        if not self.BASE_OUT:
            raise RuntimeError("BATCH_OUT_DIR not set")
        if not self.PATH_KRX:
            raise RuntimeError("MONTHLY_KRX_CSV not set")


    # ------------------------------------------------------
    # 회사 CSV 읽기
    # ------------------------------------------------------
    def read_krx_code(self):
        krx = pd.read_csv(
            self.PATH_KRX,
            encoding="cp949",
            dtype={"한글 종목약명": str}
        )

        krx = krx[[
            "표준코드", "단축코드", "한글 종목약명",
            "시장구분", "증권구분", "주식종류"
        ]]

        krx = krx.rename(columns={
            "표준코드": "std_code",
            "단축코드": "code",
            "한글 종목약명": "name",
            "시장구분": "market_type",
            "증권구분": "security_type",
            "주식종류": "stock_type"
        })

        krx["code"] = krx["code"].astype(str).str.zfill(6)
        krx["name"] = krx["name"].astype(str)

        return krx

    # ------------------------------------------------------
    # TXT 저장 공통
    # ------------------------------------------------------
    def save_to_txt(self, df, filename, fields):
        today = datetime.now().strftime("%Y%m%d")
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        out_dir = os.path.join(self.BASE_OUT, today)
        os.makedirs(out_dir, exist_ok=True)

        txt_path = os.path.join(out_dir, filename)

        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(self.DELIMITER.join(fields) + "\n")

            for idx, r in df.iterrows():
                row = [r.get(col, "") for col in fields[:-1]]
                row.append(now_str)
                f.write(self.DELIMITER.join(map(str, row)) + "\n")

                tmnow = datetime.now().strftime("%Y-%m-%d %H:%M")
                print(f"[{tmnow}] #{idx+1:04d} {r.get('name','')} ({r.get('code','')}) > WRITE TXT OK")

        print(f"[OK] {filename} 생성 완료")
        print(f"ROWCOUNT={len(df)}")
        print(f"OUTPUT={txt_path}")

    # ------------------------------------------------------
    # 회사 정보 Batch-Out
    # ------------------------------------------------------
    def batch_out_company(self):
        df = self.read_krx_code()

        fields = [
            "code",
            "name",
            "market_type",
            "security_type",
            "stock_type",
            "std_code",
            "last_update"
        ]

        self.save_to_txt(
            df,
            f"COMPANY_INFO_KR_{datetime.now().strftime('%Y%m%d')}.txt",
            fields
        )

    # ------------------------------------------------------
    # 전체 실행
    # ------------------------------------------------------
    def execute(self):
        self.batch_out_company()


if __name__ == "__main__":
    batch = MonthlyCodeBatchOut()
    batch.execute()
