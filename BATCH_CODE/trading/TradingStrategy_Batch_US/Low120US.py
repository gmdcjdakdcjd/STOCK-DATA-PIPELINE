# ===== sys.path 세팅 (최상단) =====
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

# ===== 기존 import =====
import pandas as pd
import warnings
from datetime import datetime

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

start_date = (pd.Timestamp.today() - pd.DateOffset(days=200)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
latest_trade_date = mk.get_latest_date(today_str)

strategy_name = "DAILY_120D_NEW_LOW_US"

# =======================================================
# 2. 전체 가격 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, latest_trade_date)

if df_all.empty:
    print("\n전체 가격 데이터 없음 — 종료")
    exit()

df_all["date"] = pd.to_datetime(df_all["date"])
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

low_list = []

# =======================================================
# 3. 종목별 120일 신저가 첫 발생 탐지
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 120:
        continue

    df = group.sort_values("date").copy()
    df.set_index("date", inplace=True)

    df["LOW_120_CLOSE"] = df["close"].rolling(120).min()

    prev = df.iloc[-2]
    last = df.iloc[-1]

    # 오늘 처음 120일 신저가 + 가격 조건
    if (
            last["close"] == last["LOW_120_CLOSE"]
            and prev["close"] > prev["LOW_120_CLOSE"]
            and last["close"] >= 15
    ):
        diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

        low_list.append({
            "code": code,
            "name": mk.code_to_name.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "volume": float(last.get("volume", 0)),
            "diff": diff,
            "special_value": round(float(last["LOW_120_CLOSE"]), 2)
        })

# =======================================================
# 4. 정렬 + TXT 저장
# =======================================================
if low_list:

    df_low = pd.DataFrame(low_list).sort_values(by="close")
    print("\n[US] 120일 종가 신저가 첫 발생 종목\n")
    print(df_low.to_string(index=False))
    print(f"\n총 {len(df_low)}건 감지됨.\n")

    today = datetime.now().strftime("%Y%m%d")
    result_id = f"{today}_{strategy_name}"

    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=latest_trade_date,
        total_data=len(df_low)
    )

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

    print(f"\nTXT 생성 완료 → RESULT_ID = {result_id}, ROWCOUNT = {len(df_low)}\n")

else:
    print("\n120일 종가 신저가 첫 발생 종목 없음 — 저장 생략\n")
