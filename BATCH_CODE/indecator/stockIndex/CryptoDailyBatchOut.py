import sys
from pathlib import Path

# Finstack 프로젝트 루트 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # /workspace
sys.path.insert(0, str(PROJECT_ROOT))

import yfinance as yf
import pandas as pd
from datetime import datetime
import os

from BATCH_CODE.common import config
from BATCH_CODE.indecator.crypto_common_flie_saver import append_indicator_row


class CryptoDailyBatchOut:
    def __init__(self):
        # ----------------------------------------------------
        # 수집 대상 정의: 증시 영향도가 높은 기축 코인 5종 선정
        # ----------------------------------------------------
        self.crypto_targets = {
            # 비트코인 가상자산 시장 기축 및 전반적인 위험자산 선호도(Risk-on/off) 지표
            "BTC": "BTC-USD",

            # 이더리움 알트코인 대장주 및 나스닥 기술주와의 높은 동조화(Correlation) 지표
            "ETH": "ETH-USD",

            # 솔라나 차세대 고성능 메인넷 및 최근 기관 자금 유입의 핵심 지표
            "SOL": "SOL-USD",

            # 바이넨스 거래소 글로벌 최대 거래소 유동성 및 가상자산 플랫폼 생태계 건전성 지표
            "BNB": "BNB-USD",

            # 스테이블코인 제도권 금융 시스템 연동 및 국제 송금/결제 인프라 지표
            "XRP": "XRP-USD"
        }

        # 1년치 백필(Backfill)을 위해 기간을 "1y"로 설정
        # self.fetch_period = "1y"
        self.fetch_period = "5d"

    # ===============================================================
    # 1) 가상자산 시세 수집 (yfinance API)
    # ===============================================================
    def read_crypto(self, code, ticker):
        try:
            # yfinance 최신 버전의 MultiIndex 대응을 위해 auto_adjust=True 설정
            df = yf.download(ticker, period=self.fetch_period, interval="1d", auto_adjust=True)

            if df.empty:
                print(f"[WARN] {code} ({ticker}) 데이터 없음")
                return pd.DataFrame()

            # ---------------------------------------------------------
            # MultiIndex 컬럼을 단일 레벨 문자열로 변환 (yfinance 업데이트 대응)
            # ---------------------------------------------------------
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # 2. 인덱스 리셋 및 컬럼명 소문자 통일
            df = df.reset_index()
            df.columns = [str(c).lower() for c in df.columns]

            # 3. 날짜 포맷팅 (독일 지수 XTR 포맷과 일치)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d 00:00:00")

            # 4. 전일비 및 등락률 계산 (전체 시계열에 대해 수행)
            df["change_amount"] = df["close"].diff()
            df["change_rate"] = (df["change_amount"] / df["close"].shift(1)) * 100

            # 5. 데이터 정제 및 정렬
            df = df.fillna(0)
            df = df[["date", "close", "change_amount", "change_rate"]]

            # 백필을 위해 과거 데이터부터 순차적으로 반환 (오름차순)
            # return df.sort_values("date", ascending=True).copy()

            # 최근 1건만 필요할 때 (최신 데이터 기준)
            return df.sort_values("date", ascending=False).head(1).copy()

        except Exception as e:
            print(f"[ERROR] {code} read error: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    # ===============================================================
    # 2) TXT append (공통 writer 호출)
    # ===============================================================
    def write_indicator(self, code, df):
        # 1년치 데이터 행을 순회하며 공통 저장소에 기록
        for idx, r in df.iterrows():
            append_indicator_row(
                code=code,
                date=r["date"],
                change_amount=r["change_amount"],
                change_rate=r["change_rate"],
                close=r["close"],
            )

        tmnow = datetime.now().strftime("%Y-%m-%d %H:%M")
        print(f"[{tmnow}] {code} > 1 Year Backfill OK (Total: {len(df)} rows)")

    # ===============================================================
    # 3) 실행
    # ===============================================================
    def execute(self):
        print("[INFO] Crypto 1-Year Backfill 시작")

        for code, ticker in self.crypto_targets.items():
            print(f"[FETCH] {code} ({ticker}) 수집 중...")
            df = self.read_crypto(code, ticker)

            if df.empty:
                continue

            self.write_indicator(code, df)

        print("[INFO] Crypto Batch-Out 완료")


if __name__ == "__main__":
    CryptoDailyBatchOut().execute()