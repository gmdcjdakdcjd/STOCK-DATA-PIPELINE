import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

from BATCH_CODE.common.config import get_sqlalchemy_db_url


class MarketDB:
    def __init__(self):

        # -------------------------------------------------------
        # MariaDB
        # -------------------------------------------------------
        self.engine = create_engine(
            get_sqlalchemy_db_url(),
            pool_pre_ping=True,
            pool_recycle=3600
        )

        self.codes = {}
        self.name_to_code = {}

        # ETF 기본 정보 로딩 (KODEX)
        self.get_etf_info()

    # =====================================================================
    # ETF 기본 정보 (KODEX)
    # =====================================================================
    def get_etf_info(self):
        sql = text("""
            SELECT ci.code, ci.name
            FROM etf_info_kr ci
            WHERE ci.name LIKE '%KODEX%'
              AND EXISTS (
                  SELECT 1
                  FROM etf_daily_price_kr dp
                  WHERE dp.code = ci.code
                    AND dp.volume > 0
                    AND dp.date = (
                        SELECT MAX(date)
                        FROM etf_daily_price_kr
                        WHERE code = ci.code
                    )
              )
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        if df.empty:
            print("⚠ KODEX ETF 기본 정보 없음")
            return

        self.codes = dict(zip(df["code"], df["name"]))
        self.name_to_code = {v: k for k, v in self.codes.items()}

    # =====================================================================
    # ETF 일별 시세 (단일 종목)
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

        # 코드 정규화
        if code in self.codes:
            pass
        elif code in self.name_to_code:
            code = self.name_to_code[code]
        else:
            print(f"⚠ Code({code}) doesn't exist.")
            return None

        try:
            sql = text("""
                SELECT date, open, high, low, close, volume, diff
                FROM etf_daily_price_kr
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
                print(f"⚠ MariaDB: {code} 데이터 없음")
                return None

            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)

            return df

        except Exception as e:
            print(f"[MariaDB ERROR] get_daily_price({code}): {e}")
            return None

    # =====================================================================
    # 날짜 문자열 정규화
    # =====================================================================
    def _normalize_date(self, date_str):
        try:
            dt = pd.to_datetime(date_str)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            raise ValueError(f"Invalid date format: {date_str}")

    # =====================================================================
    # 전략 스캐너용 ETF 정보
    # =====================================================================
    def get_etf_info_optimization(self):
        if not self.codes:
            self.get_etf_info()

        return pd.DataFrame(
            [{"code": k, "name": v} for k, v in self.codes.items()]
        )

    # =====================================================================
    # 날짜 보정: 기준일 이하 가장 최근 거래일
    # =====================================================================
    def get_latest_date(self, date_str):
        try:
            sql = text("""
                SELECT DATE_FORMAT(date, '%Y-%m-%d') AS date
                FROM etf_daily_price_kr
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
    # 전체 ETF 일봉 데이터 1회 조회 (Batch 핵심)
    # =====================================================================
    def get_all_daily_prices(self, start_date, end_date):

        sql = text("""
            SELECT code, date, open, high, low, close, volume, diff, last_update
            FROM etf_daily_price_kr
            WHERE date BETWEEN :start AND :end
            ORDER BY code, date
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(
                sql,
                conn,
                params={"start": start_date, "end": end_date}
            )

        if df.empty:
            return df

        df["date"] = pd.to_datetime(df["date"])
        return df
