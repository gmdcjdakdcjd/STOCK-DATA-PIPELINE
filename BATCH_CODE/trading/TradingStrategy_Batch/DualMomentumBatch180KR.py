# ===== sys.path 세팅 (최상단) =====
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

# ===== 기존 import =====
import pandas as pd
import warnings
from datetime import datetime, timedelta

from API.AnalyzeKR import MarketDB
from BATCH_CODE.trading.txt_saver_kr import (
    save_strategy_result,
    save_strategy_detail
)

warnings.filterwarnings("ignore", category=RuntimeWarning)

class DualMomentumBatch:

    strategy_name = "DUAL_MOMENTUM_6M_KR"

    def __init__(self):

        self.mk = MarketDB()

        self.MIN_ABS_RETURN = 15.0   # 절대모멘텀 (%)
        self.TOP_RELATIVE = 40       # 상대모멘텀
        self.FINAL_TOP = 20          # 최종 선택

    # ---------------------------------------------------------
    # 날짜 보정
    # ---------------------------------------------------------
    def adjust_date(self, date_str):
        latest = self.mk.get_latest_date(date_str)
        if latest is None:
            print(f"거래일 없음: {date_str}")
            return None
        return latest

    # ---------------------------------------------------------
    # 전체 종목 수익률 계산
    # ---------------------------------------------------------
    def calculate_returns(self, start_date, end_date):

        df_all = self.mk.get_all_daily_prices(start_date, end_date)

        if df_all.empty:
            print("⚠ 전체 가격 데이터 없음")
            return pd.DataFrame()

        # 🔥 date 보정 (중요)
        df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
        df_all = df_all.dropna(subset=["date"])

        pivot = df_all.pivot(index="date", columns="code", values="close")

        start_prices = pivot.iloc[0].dropna()
        end_prices = pivot.iloc[-1].dropna()

        common_codes = start_prices.index.intersection(end_prices.index)

        result = []
        for code in common_codes:
            old = float(start_prices[code])
            new = float(end_prices[code])
            ret = (new / old - 1) * 100

            name = self.mk.codes.get(code, "")

            result.append([code, name, old, new, round(ret, 2)])

        return pd.DataFrame(
            result,
            columns=["code", "name", "prev_close", "price", "diff"]
        )

    # ---------------------------------------------------------
    # 듀얼모멘텀 실행
    # ---------------------------------------------------------
    def run_dual_momentum_batch(self, start_date, end_date):

        start_date = self.adjust_date(start_date)
        end_date = self.adjust_date(end_date)

        if not start_date or not end_date:
            return pd.DataFrame()

        print(f"\n[DUAL MOMENTUM - 6M] ({start_date} ~ {end_date})\n")

        df = self.calculate_returns(start_date, end_date)

        if df.empty:
            print("데이터 없음 → 종료")
            return pd.DataFrame()

        # 상대모멘텀
        df_top40 = df.sort_values("diff", ascending=False).head(self.TOP_RELATIVE)

        # 절대모멘텀
        df_abs = df_top40[df_top40["diff"] > self.MIN_ABS_RETURN]

        # 최종 TOP
        df_final = df_abs.sort_values("diff", ascending=False).head(self.FINAL_TOP)

        if df_final.empty:
            print("절대모멘텀 없음")
            return pd.DataFrame()

        print(df_final.to_string(index=False))

        # -------------------------------------------------
        # TXT 저장
        # -------------------------------------------------
        today_id = datetime.now().strftime("%Y%m%d")
        result_id = f"{today_id}_{self.strategy_name}"

        # SUMMARY
        save_strategy_result(
            strategy_name=self.strategy_name,
            signal_date=end_date,
            total_data=len(df_final)
        )

        # DETAIL
        for rank, row in enumerate(df_final.to_dict("records"), start=1):
            save_strategy_detail(
                signal_date=end_date,
                action=self.strategy_name,
                code=row["code"],
                name=row["name"],
                prev_close=row["prev_close"],
                price=row["price"],
                diff=row["diff"],
                volume=None,
                special_value=rank,   # 듀얼모멘텀은 순위
                result_id=result_id
            )

        print("\nTXT 저장 완료")
        print(f"RESULT_ID = {result_id}")
        print(f"ROWCOUNT  = {len(df_final)}\n")

        return df_final


# ================================ 실행부 ================================
if __name__ == "__main__":
    dm = DualMomentumBatch()
    today = datetime.today()
    start = (today - timedelta(days=180)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    dm.run_dual_momentum_batch(start, end)
