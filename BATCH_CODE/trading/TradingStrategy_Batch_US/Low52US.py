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

start_date = (pd.Timestamp.today() - pd.DateOffset(days=400)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
latest_trade_date = mk.get_latest_date(today_str)
# RESULT_ID 등에 사용할 수 있도록 하이픈(-)이 제거된 형식의 최근 거래일 날짜를 만듭니다.
latest_trade_date_clean = latest_trade_date.replace("-", "")

strategy_name = "WEEKLY_52W_NEW_LOW_US"

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
# 3. 종목별 주봉 변환 + 52주 신저가 첫 발생 탐지
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 260:   # 최소 5년치 일봉
        continue

    df = group.sort_values("date").copy()
    df.set_index("date", inplace=True)

    weekly = pd.DataFrame({
        "open": df["open"].resample("W-SAT").first(),
        "high": df["high"].resample("W-SAT").max(),
        "low": df["low"].resample("W-SAT").min(),
        "close": df["close"].resample("W-SAT").last(),
        "volume": df["volume"].resample("W-SAT").sum()
    }).dropna()

    if len(weekly) < 52:
        continue

    weekly["LOW_52_CLOSE"] = weekly["close"].rolling(52).min()

    prev = weekly.iloc[-2]
    last = weekly.iloc[-1]

    if (
            last["close"] == last["LOW_52_CLOSE"]
            and prev["close"] > prev["LOW_52_CLOSE"]
            and last["close"] >= 15
    ):
        diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

        # 기존에는 주말 날짜(last.name)로 저장했으나, 매일 배치 실행에 대응하기 위해
        # 해당 주봉을 평가한 시점의 가장 최근 거래일 날짜(latest_trade_date)로 기록합니다.
        low_list.append({
            "code": code,
            "name": mk.code_to_name.get(code, "UNKNOWN"),
            "date": latest_trade_date,
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "volume": float(last["volume"]),
            "diff": diff,
            "special_value": round(float(last["LOW_52_CLOSE"]), 2)
        })

# =======================================================
# 4. 정렬 + TXT 저장
# =======================================================
if low_list:

    df_low = pd.DataFrame(low_list).sort_values(by="close")
    print("\n[US] 52주 신저가 ‘첫 발생’ 주봉 종목\n")
    print(df_low.to_string(index=False))
    print(f"\n총 {len(df_low)}건 감지됨.\n")

    # 결과 아이디(result_id) 역시 실제 배치 실행 기준 날짜가 아닌 최신 거래일 날짜 기준으로 통일하여 생성합니다.
    result_id = f"{latest_trade_date_clean}_{strategy_name}"
    weekly_signal_date = df_low.iloc[0]["date"]  # 주봉 날짜 (ex. 2026-01-24)

    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=weekly_signal_date,
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
    print("\n52주 신저가 종목 없음 — 저장 생략\n")
