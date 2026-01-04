# ===== sys.path 세팅 (최상단) =====
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

# ===== 기존 import =====
import pandas as pd
from datetime import datetime, timedelta

from API.AnalyzeUS import MarketDB
from BATCH_CODE.trading.txt_saver_us import (
    save_strategy_result,
    save_strategy_detail
)

class DualMomentumBatchUS1Y:

    strategy_name = "DUAL_MOMENTUM_1Y_US"

    def __init__(self):
        # MariaDB 기반 MarketDB
        self.mk = MarketDB()

        self.MIN_ABS_RETURN = 25.0   # 절대모멘텀 (%)
        self.TOP_RELATIVE = 40       # 상대모멘텀 상위
        self.FINAL_TOP = 20          # 최종 선정

    # -------------------------------------------------
    # 거래일 보정
    # -------------------------------------------------
    def adjust_date(self, date_str):
        latest = self.mk.get_latest_date(date_str)
        if latest is None:
            print(f"⚠ 거래일 없음: {date_str}")
        return latest

    # -------------------------------------------------
    # 전체 종목 수익률 계산
    # -------------------------------------------------
    def calculate_returns(self, start_date, end_date):

        df_all = self.mk.get_all_daily_prices(start_date, end_date)

        if df_all.empty:
            print("⚠ 전체 가격 데이터 없음")
            return pd.DataFrame()

        pivot = df_all.pivot(index="date", columns="code", values="close")

        start_prices = pivot.iloc[0].dropna()
        end_prices = pivot.iloc[-1].dropna()

        common_codes = start_prices.index.intersection(end_prices.index)

        rows = []
        for code in common_codes:
            old = float(start_prices[code])
            new = float(end_prices[code])
            r = (new / old - 1) * 100

            rows.append({
                "code": code,
                "name": self.mk.code_to_name.get(code, "UNKNOWN"),
                "old_price": old,
                "new_price": new,
                "returns": round(r, 2)
            })

        return pd.DataFrame(rows)

    # -------------------------------------------------
    # Dual Momentum 실행
    # -------------------------------------------------
    def run(self, start_date, end_date):

        start_date = self.adjust_date(start_date)
        end_date = self.adjust_date(end_date)

        if not start_date or not end_date:
            print("❌ 날짜 보정 실패 → 종료")
            return

        print(f"\n⚡ [DUAL MOMENTUM 1Y US] ({start_date} ~ {end_date})\n")

        df = self.calculate_returns(start_date, end_date)

        if df.empty:
            print("데이터 없음 → 종료")
            return

        # 상대모멘텀 상위 40
        df_top = df.sort_values("returns", ascending=False).head(self.TOP_RELATIVE)

        # 절대모멘텀 필터
        df_abs = df_top[df_top["returns"] >= self.MIN_ABS_RETURN]

        # 최종 TOP 20
        df_final = df_abs.sort_values("returns", ascending=False).head(self.FINAL_TOP)

        if df_final.empty:
            print("절대모멘텀 통과 종목 없음")
            return

        print(df_final.to_string(index=False), "\n")

        # ------------------------------
        # TXT 저장
        # ------------------------------
        today = datetime.now().strftime("%Y%m%d")
        result_id = f"{today}_{self.strategy_name}"

        save_strategy_result(
            strategy_name=self.strategy_name,
            signal_date=end_date,
            total_data=len(df_final)
        )

        for rank, row in enumerate(df_final.to_dict("records"), start=1):
            save_strategy_detail(
                signal_date=end_date,
                action=self.strategy_name,
                code=row["code"],
                name=row["name"],
                prev_close=row["old_price"],
                price=row["new_price"],
                diff=row["returns"],
                volume=None,
                special_value=rank,
                result_id=result_id
            )

        print(f"⚡ TXT 생성 완료 → RESULT_ID={result_id}, ROWCOUNT={len(df_final)}\n")


# ================= 실행 =================
if __name__ == "__main__":
    dm = DualMomentumBatchUS1Y()
    today = datetime.today()
    start = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    dm.run(start, end)
