# ===== sys.path 세팅 (최상단) =====
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

# ===== 기존 import =====
import pandas as pd
import warnings
from datetime import datetime
import numpy as np

from API.AnalyzeUS import MarketDB
from BATCH_CODE.trading.txt_saver_us import (
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

print(f"\n총 {len(stocks)}개 미국 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
latest_trade_date = mk.get_latest_date(today_str)

strategy_name = "RSI_70_OVERHEATED_US"

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
df_all = mk.get_all_daily_prices(start_date, latest_trade_date)

if df_all.empty:
    print("⚠ 전체 가격 데이터 없음")
    exit()

df_all["date"] = pd.to_datetime(df_all["date"])
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

rsi_list = []

# =======================================================
# 4. 그룹별 RSI 계산
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 20:
        continue

    group = group.sort_values("date").set_index("date")
    group["rsi"] = compute_rsi(group["close"])

    prev = group.iloc[-2]
    last = group.iloc[-1]

    if pd.isna(last["rsi"]):
        continue

    diff = ((last["close"] - prev["close"]) / prev["close"]) * 100

    # 조건: RSI ≥ 70 + 종가 ≥ $10
    if last["rsi"] >= 70 and last["close"] >= 10:
        rsi_list.append({
            "code": code,
            "name": mk.code_to_name.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "diff": round(diff, 2),
            "volume": float(last.get("volume", 0)),
            "special_value": round(float(last["rsi"]), 2)
        })

# =======================================================
# 5. 정렬 + TXT 저장
# =======================================================
if rsi_list:

    df_rsi = pd.DataFrame(rsi_list).sort_values(by="special_value", ascending=False)
    print("\n📈 [US] RSI 70 이상 과열 종목 (종가 ≥ $10)\n")
    print(df_rsi.to_string(index=False))
    print(f"\n총 {len(df_rsi)}건 감지됨.\n")

    today = datetime.now().strftime("%Y%m%d")
    result_id = f"{today}_{strategy_name}"

    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=latest_trade_date,
        total_data=len(df_rsi)
    )

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
            special_value=row["special_value"],  # RSI 값
            result_id=result_id
        )

    print(f"\n⚡ TXT 생성 완료 → RESULT_ID = {result_id}, ROWCOUNT = {len(df_rsi)}\n")

else:
    print("\n💤 RSI 70 이상 과열 종목 없음 — 저장 생략\n")
