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
mk = MarketDB()
company_df = mk.get_comp_info_optimization()
stocks = set(company_df["code"])

print(f"\n총 {len(stocks)}개 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.now().strftime("%Y%m%d")

strategy_name = "DAILY_BB_LOWER_TOUCH_KR"

touch_candidates = []

# =======================================================
# 2. 전체 일봉 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("전체 가격 데이터 없음")
    exit()

# date 정리 + index 세팅 (모든 전략 공통)
df_all = df_all[df_all["code"].isin(stocks)]
df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
df_all = (
    df_all
    .dropna(subset=["date"])
    .sort_values(["code", "date"])
    .set_index("date")
)

# =======================================================
# 3. 그룹별 볼린저밴드 계산
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 20:
        continue

    # 볼린저밴드
    ma20 = group["close"].rolling(20).mean()
    std = group["close"].rolling(20).std()
    lower = ma20 - (std * 2)

    if pd.isna(lower.iloc[-1]):
        continue

    prev = group.iloc[-2]
    last = group.iloc[-1]

    close_price = last["close"]
    lower_band = lower.iloc[-1]

    diff = round(((close_price - prev["close"]) / prev["close"]) * 100, 2)
    gap_rate = ((close_price - lower_band) / lower_band) * 100

    # 조건
    if (
            -0.5 <= gap_rate <= 0.5
            and close_price >= 10000
            and close_price >= lower_band * 0.995
            and not pd.isna(last["volume"])
            and last["volume"] > 0
    ):
        touch_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(close_price),
            "prev_close": float(prev["close"]),
            "diff": diff,
            "volume": float(last.get("volume", 0)),
            "special_value": round(float(lower_band), 2)  # 하단선
        })


# =======================================================
# 4. TXT 저장
# =======================================================
if touch_candidates:

    df_touch = pd.DataFrame(touch_candidates).sort_values(by="diff")

    print("\n[일봉] 볼린저 하단 터치 종목 (±0.5%)\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    last_date = df_touch.iloc[0]["date"]
    result_id = f"{today}_{strategy_name}"

    # STRATEGY_RESULT
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_touch)
    )

    # STRATEGY_DETAIL
    for rank, row in enumerate(df_touch.to_dict("records"), start=1):
        save_strategy_detail(
            signal_date=row["date"],
            action=strategy_name,
            code=row["code"],
            name=row["name"],
            prev_close=row["prev_close"],
            price=row["close"],
            diff=row["diff"],
            volume=row["volume"],
            special_value=row["special_value"],  # BB 하단
            result_id=result_id
        )

    print("\nTXT 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_touch)}\n")

else:
    print("\n볼린저 하단 터치 종목 없음 — 저장 생략\n")
