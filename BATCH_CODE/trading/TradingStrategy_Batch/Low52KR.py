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

# =======================================================
# 1. 기본 세팅
# =======================================================
mk = MarketDB()
company_df = mk.get_comp_info_optimization()
stocks = set(company_df["code"])

print(f"\n총 {len(stocks)}개 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=400)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime('%Y-%m-%d')
today = datetime.now().strftime('%Y%m%d')

strategy_name = "WEEKLY_52W_NEW_LOW_KR"

# =======================================================
# 2. MariaDB 전체 일봉 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)
df_all = df_all[df_all["code"].isin(stocks)]

if df_all.empty:
    print("\n전체 가격 데이터 없음 — 종료")
    exit()

# date 처리 + index 세팅은 여기서 1번만
df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
df_all = (
    df_all
    .dropna(subset=["date"])
    .sort_values(["code", "date"])
    .set_index("date")
)

low_candidates = []

# =======================================================
# 3. 종목별 주봉 변환 + 52주 신저가 스캔
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 260:
        continue

    weekly = pd.DataFrame({
        "open": group["close"].resample("W-SAT").first(),
        "high": group["close"].resample("W-SAT").max(),
        "low":  group["close"].resample("W-SAT").min(),
        "close": group["close"].resample("W-SAT").last(),
        "volume": group["volume"].resample("W-SAT").sum(),
    }).dropna()

    if len(weekly) < 52:
        continue

    weekly["LOW_52_CLOSE"] = weekly["close"].rolling(52).min()

    prev = weekly.iloc[-2]
    last = weekly.iloc[-1]

    # 조건: 종가가 52주 최저치 첫 도달 + 종가 >= 10000
    # 여기 추가
    prev_low = weekly["LOW_52_CLOSE"].iloc[-2]

    # 조건 교체
    if last["LOW_52_CLOSE"] >= last["close"] >= 10000 and prev["close"] > prev_low:

        diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

        low_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "volume": float(last["volume"]),
            "diff": diff,
            "special_value": float(last["LOW_52_CLOSE"])
        })

# =======================================================
# 4. TXT 저장
# =======================================================
if low_candidates:

    df_low = pd.DataFrame(low_candidates).sort_values(by="close", ascending=True)

    print("\n[주봉] 52주 종가 신저가 종목 리스트\n")
    print(df_low.to_string(index=False))
    print(f"\n총 {len(df_low)}건 감지됨.\n")

    last_date = df_low.iloc[0]["date"]
    result_id = f"{today}_{strategy_name}"

    # STRATEGY_RESULT
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_low)
    )

    # STRATEGY_DETAIL
    for rank, row in enumerate(df_low.to_dict("records"), start=1):
        save_strategy_detail(
            signal_date=row["date"],
            action=strategy_name,
            code=row["code"],
            name=row["name"],
            prev_close=row["prev_close"],
            price=row["close"],
            diff=row["diff"],
            volume=row["volume"],
            special_value=row["special_value"],
            result_id=result_id
        )


    print("\nTXT 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_low)}\n")

else:
    print("\n52주 신저가 종목 없음 — 저장 생략\n")