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

strategy_name = "RSI_70_OVERHEATED_KR"

# =======================================================
# 2. RSI 계산 함수
# =======================================================
def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


# =======================================================
# 3. 전체 일봉 데이터 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("⚠ 전체 가격 데이터 없음")
    exit()

# 여기서 date 처리 + index 세팅 (중요)
df_all = df_all[df_all["code"].isin(stocks)]
df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
df_all = (
    df_all
    .dropna(subset=["date"])
    .sort_values(["code", "date"])
    .set_index("date")
)

rsi_candidates = []

# =======================================================
# 4. 그룹별 RSI 계산
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 20:
        continue

    group["rsi"] = compute_rsi(group["close"])

    last = group.iloc[-1]
    prev = group.iloc[-2]

    if pd.isna(last["rsi"]):
        continue

    diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    # 조건: RSI 70 이상 + 종가 ≥ 10,000
    if last["rsi"] >= 70 and last["close"] >= 10000:
        rsi_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "diff": diff,
            "volume": float(last.get("volume", 0)),
            "special_value": round(float(last["rsi"]), 2)  # RSI 값
        })


# =======================================================
# 5. TXT 저장
# =======================================================
if rsi_candidates:

    df_rsi = pd.DataFrame(rsi_candidates).sort_values(
        by="special_value", ascending=False
    )

    print("\n[RSI] 70 이상 과열 종목 리스트\n")
    print(df_rsi.to_string(index=False))
    print(f"\n총 {len(df_rsi)}건 감지됨.\n")

    last_date = df_rsi.iloc[0]["date"]
    result_id = f"{today}_{strategy_name}"

    # STRATEGY_RESULT
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_rsi)
    )

    # STRATEGY_DETAIL
    for rank, row in enumerate(df_rsi.to_dict("records"), start=1):
        save_strategy_detail(
            signal_date=row["date"],
            action=strategy_name,
            code=row["code"],
            name=row["name"],
            prev_close=row["prev_close"],
            price=row["close"],
            diff=row["diff"],
            volume=row["volume"],
            special_value=row["special_value"],  # RSI
            result_id=result_id
        )

    print("\nTXT 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_rsi)}\n")

else:
    print("\nRSI 70 이상 과열 종목 없음 — 저장 생략\n")
