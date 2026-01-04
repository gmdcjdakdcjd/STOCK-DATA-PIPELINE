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

        self.codes = {}
        self.get_comp_info()

    def get_comp_info(self):
        """
        기존 MariaDB 기반 코드 (주석 처리)
        """

        sql = text("""
            SELECT code, name
            FROM company_info_kr
            WHERE stock_type = '보통주'
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        if df.empty:
            print("company_info_kr 데이터 없음")
            self.codes = {}
            return

        self.codes = dict(zip(df["code"], df["name"]))

    # ----------------------------------------------------------------------
    # get_daily_price
    # ----------------------------------------------------------------------
    def get_daily_price(self, code, start_date=None, end_date=None):
        # 날짜 처리
        if start_date is None:
            start_date = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')
        else:
            start_date = self._normalize_date(start_date)

        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
        else:
            end_date = self._normalize_date(end_date)

        # 코드 매핑 (기존 로직 그대로 유지)
        keys = list(self.codes.keys())
        vals = list(self.codes.values())

        if code in keys:
            pass
        elif code in vals:
            code = keys[vals.index(code)]
        else:
            print(f"⚠ Code({code}) doesn't exist.")
            return None

        try:
            sql = text("""
                SELECT date, open, high, low, close, volume, diff
                FROM daily_price_kr
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

    # ----------------------------------------------------------------------
    # 날짜 포맷 정규화 (그대로)
    # ----------------------------------------------------------------------
    def _normalize_date(self, date_str):
        lst = re.split(r'\D+', date_str)
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
                FROM company_info_kr
                WHERE stock_type = '보통주'
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

    # ----------------------------------------------------------------------
    # 날짜 보정: date <= 기준일 중 가장 최근 날짜
    # ----------------------------------------------------------------------
    def get_latest_date(self, date_str):
        """
        date <= date_str 인 가장 최근 거래일 반환 (MariaDB)
        """
        try:
            sql = text("""
                SELECT DATE_FORMAT(date, '%Y-%m-%d') AS date
                FROM daily_price_kr
                WHERE date <= :target
                ORDER BY date DESC
                LIMIT 1
            """)

            with self.engine.connect() as conn:
                row = conn.execute(sql, {"target": date_str}).fetchone()

            if row:
                return row.date

            return None

        except Exception as e:
            print(f"[MariaDB ERROR] get_latest_date: {e}")
            return None

    # ----------------------------------------------------------------------
    # 전체 가격 데이터 조회 (기간 내 전체 종목 한 번에 가져오기)
    # ----------------------------------------------------------------------
    def get_all_daily_prices(self, start_date, end_date):
        # ------------------------------
        # MariaDB 버전 (실제 사용)
        # ------------------------------
        sql = text("""
            SELECT code, date, open, high, low, close, volume, diff, last_update
            FROM daily_price_kr
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
