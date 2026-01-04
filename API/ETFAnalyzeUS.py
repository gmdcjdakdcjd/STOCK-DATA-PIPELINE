import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

from BATCH_CODE.common.config import get_sqlalchemy_db_url


class MarketDB:
    def __init__(self):
        """
        US ETF MarketDB (iShares 전용)
        - MariaDB only
        - Batch / 분석 공용
        """

        self.engine = create_engine(
            get_sqlalchemy_db_url(),
            pool_pre_ping=True,
            pool_recycle=3600
        )

        # 🔑 코드 매핑
        self.code_to_name = {}
        self.name_to_code = {}

        # ETF 기본 정보 로딩
        self.get_etf_info()

    # =====================================================================
    # 미국 ETF 기본 정보 (BlackRock iShares)
    # =====================================================================
    def get_etf_info(self):
        sql = text("""
            SELECT code, name
            FROM etf_info_us
            WHERE issuer = 'BlackRock (iShares)'
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        if df.empty:
            print("⚠ US ETF 기본 정보 없음 (iShares)")
            self.code_to_name = {}
            self.name_to_code = {}
            return

        self.code_to_name = dict(zip(df["code"], df["name"]))
        self.name_to_code = dict(zip(df["name"], df["code"]))

    # =====================================================================
    # 미국 ETF 일별 시세 (단일 ETF)
    # =====================================================================
    def get_daily_price(self, code, start_date=None, end_date=None):
        """
        특정 ETF의 일봉 데이터 반환
        """

        # 날짜 처리
        if start_date is None:
            start_date = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        else:
            start_date = self._normalize_date(start_date)

        if end_date is None:
            end_date = datetime.today().strftime("%Y-%m-%d")
        else:
            end_date = self._normalize_date(end_date)

        # 코드 정규화 (code / name 허용)
        if code in self.code_to_name:
            pass
        elif code in self.name_to_code:
            code = self.name_to_code[code]
        else:
            print(f"⚠ ETF Code({code}) doesn't exist.")
            return None

        try:
            sql = text("""
                SELECT date, open, high, low, close, volume
                FROM etf_daily_price_us
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
                print(f"⚠ MariaDB: ETF {code} 데이터 없음")
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
    # ETF 코드 / 이름 DataFrame 반환 (전략 스캔용)
    # =====================================================================
    def get_etf_info_optimization(self):
        """
        ETF 코드/이름을 DataFrame 형태로 반환
        """
        if not self.code_to_name:
            self.get_etf_info()

        return pd.DataFrame(
            [{"code": c, "name": n} for c, n in self.code_to_name.items()]
        )

    # =====================================================================
    # 기준일 이전 가장 최근 거래일
    # =====================================================================
    def get_latest_date(self, date_str):
        try:
            sql = text("""
                SELECT DATE_FORMAT(date, '%Y-%m-%d') AS date
                FROM etf_daily_price_us
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
        """
        기간 내 전체 ETF 일봉 데이터 반환
        (Batch / 전략 스캔 전용)
        """

        sql = text("""
            SELECT code, date, open, high, low, close, volume, last_update
            FROM etf_daily_price_us
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
