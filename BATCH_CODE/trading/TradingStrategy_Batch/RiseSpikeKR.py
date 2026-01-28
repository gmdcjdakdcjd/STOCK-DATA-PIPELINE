# ===== sys.path 세팅 (최상단) =====
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

# ===== 기존 import =====
import pandas as pd
import warnings
from datetime import datetime, timedelta
import numpy as np

from API.AnalyzeKR import MarketDB
from BATCH_CODE.trading.txt_saver_kr import (
    save_strategy_result,
    save_strategy_detail
)

warnings.filterwarnings("ignore", category=RuntimeWarning)

# =======================================================
# 1. 기본 세팅
# =======================================================
today = datetime.now().strftime("%Y%m%d")
mk = MarketDB()
company_df = mk.get_comp_info_optimization()
stocks = set(company_df["code"])

print(f"\n총 {len(stocks)}개 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=5)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_RISE_SPIKE_KR"

# =======================================================
# 2. MariaDB 전체 일봉 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n전체 가격 데이터 없음 — 종료")
    exit()

# 종목 필터링
df_all = df_all[df_all["code"].isin(stocks)]
df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
df_all = df_all.dropna(subset=["date"])
df_all = df_all.sort_values(["code", "date"])

rise_candidates = []

# =======================================================
# 3. 상승 스파이크 계산
# =======================================================
for code, group in df_all.groupby("code"):

    group = group.sort_values("date").set_index("date")

    if len(group) < 2:
        continue

    prev = group.iloc[-2]      # 어제
    last = group.iloc[-1]      # 오늘

    if pd.isna(last["volume"]) or last["volume"] <= 0:
        continue

    rate = ((last["close"] - prev["close"]) / prev["close"]) * 100

    # 조건: 전일 대비 +7% AND 종가 10,000 이상
    if rate >= 5 and last["close"] >= 10000:

        rise_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "prev_close": float(prev["close"]),
            "close": float(last["close"]),
            "rate": round(rate, 2),
            "volume": float(last.get("volume", 0))
        })


# =======================================================
# 4. 정렬 + 저장
# =======================================================
if rise_candidates:

    df_rise = pd.DataFrame(rise_candidates).sort_values(by="rate", ascending=False)

    print("\n[일봉] 전일 대비 5% 이상 상승 종목 목록\n")
    print(df_rise.to_string(index=False))
    print(f"\n총 {len(df_rise)}건 감지됨.\n")

    last_date = df_rise.iloc[0]["date"]
    result_id = f"{today}_{strategy_name}"

    # STRATEGY_RESULT (전략 요약)
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_rise)
    )

    # STRATEGY_DETAIL (종목별 상세)
    for rank, row in enumerate(df_rise.to_dict("records"), start=1):
        save_strategy_detail(
            signal_date=row["date"],
            action=strategy_name,
            code=row["code"],
            name=row["name"],
            prev_close=row["prev_close"],
            price=row["close"],
            diff=row["rate"],
            volume=row["volume"],
            special_value=rank,
            result_id=result_id
        )

    print("\nTXT 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_rise)}\n")

else:
    print("\n전일 대비 5% 이상 상승 종목 없음 — 저장 생략\n")
