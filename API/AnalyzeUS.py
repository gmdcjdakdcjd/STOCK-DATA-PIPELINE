import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import re

from BATCH_CODE.common.config import get_sqlalchemy_db_url


class MarketDB:
    def __init__(self):
        """
        생성자
        SQLAlchemy로 MariaDB 연결
        """
        # db_url = "mysql+pymysql://root:0806@localhost/managing?charset=utf8"
        # self.engine = create_engine(db_url)

        self.engine = create_engine(
            get_sqlalchemy_db_url(),
            pool_pre_ping=True,
            pool_recycle=3600
        )

        # 🔥 코드 매핑
        self.code_to_name = {}
        self.name_to_code = {}

        self.get_comp_info()

    # =====================================================================
    # 미국 종목 기본 정보 로딩
    # =====================================================================
    def get_comp_info(self):
        sql = text("""
            SELECT code, name
            FROM company_info_us
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        if df.empty:
            print("⚠ company_info_us 데이터 없음")
            return

        self.code_to_name = dict(zip(df["code"], df["name"]))
        self.name_to_code = dict(zip(df["name"], df["code"]))

    # =====================================================================
    # 미국 종목 일별 시세 로딩
    # =====================================================================
    def get_daily_price(self, code, start_date=None, end_date=None):
        # 날짜 처리
        if start_date is None:
            start_date = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        else:
            start_date = self._normalize_date(start_date)

        if end_date is None:
            end_date = datetime.today().strftime("%Y-%m-%d")
        else:
            end_date = self._normalize_date(end_date)

        # 🔥 코드/이름 매핑
        if code in self.code_to_name:
            pass
        elif code in self.name_to_code:
            code = self.name_to_code[code]
        else:
            print(f"⚠ Code({code}) doesn't exist.")
            return None

        try:
            sql = text("""
                SELECT date, open, high, low, close, volume
                FROM daily_price_us
                WHERE code = :code
                  AND date BETWEEN :start AND :end
                ORDER BY date ASC
            """)

            with self.engine.connect() as conn:
                df = pd.read_sql(
                    sql,
                    conn,
                    params={
                        "code": code,
                        "start": start_date,
                        "end": end_date
                    }
                )

            if df.empty:
                print(f"⚠ MariaDB: {code} 데이터 없음.")
                return None

            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)

            return df

        except Exception as e:
            print(f"[MariaDB ERROR] get_daily_price({code}): {e}")
            return None

    # =====================================================================
    def _normalize_date(self, date_str):
        lst = re.split(r"\D+", date_str)
        lst = [x for x in lst if x]
        year, month, day = map(int, lst[:3])
        return f"{year:04d}-{month:02d}-{day:02d}"

    # ----------------------------------------------------------------------
    # get_comp_info_optimization — DataFrame 반환 버전
    # ----------------------------------------------------------------------

    def get_comp_info_optimization(self):
        """
        종목코드/이름을 DataFrame 형태로 반환 (MariaDB)
        """
        try:
            sql = text("""
                SELECT code, name
                FROM company_info_us
               """)

            with self.engine.connect() as conn:
                df = pd.read_sql(sql, conn)

            if df.empty:
                print("⚠ MariaDB company_info 데이터 없음")
                return pd.DataFrame(columns=["code", "name"])

            # self.codes 업데이트
            self.codes = dict(zip(df["code"], df["name"]))

            return df[["code", "name"]]

        except Exception as e:
            print(f"[MariaDB ERROR] get_comp_info_optimization: {e}")
            return pd.DataFrame(columns=["code", "name"])

    # =====================================================================
    def get_latest_date(self, date_str):
        """
        date <= date_str 인 가장 최근 거래일 반환
        """
        try:
            sql = text("""
                SELECT DATE_FORMAT(date, '%Y-%m-%d') AS date
                FROM daily_price_us
                WHERE date <= :target
                ORDER BY date DESC
                LIMIT 1
            """)

            with self.engine.connect() as conn:
                row = conn.execute(sql, {"target": date_str}).fetchone()

            return row.date if row else None

        except Exception as e:
            print(f"[MariaDB ERROR] get_latest_date: {e}")
            return None

    # =====================================================================
    def get_all_daily_prices(self, start_date, end_date):
        sql = text("""
            SELECT code, date, open, high, low, close, volume, last_update
            FROM daily_price_us
            WHERE date BETWEEN :start AND :end
            ORDER BY code, date
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(
                sql,
                conn,
                params={"start": start_date, "end": end_date}
            )

        return df
