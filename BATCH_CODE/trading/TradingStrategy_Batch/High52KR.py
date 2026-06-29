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
today = datetime.now().strftime("%Y%m%d")

strategy_name = "WEEKLY_52W_NEW_HIGH_KR"

# =======================================================
# 2. MariaDB 전체 일봉 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)
df_all = df_all[df_all["code"].isin(stocks)]

if df_all.empty:
    print("\n전체 가격 데이터 없음 — 종료")
    exit()

df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
df_all = (
    df_all
    .dropna(subset=["date"])
    .sort_values(["code", "date"])
    .set_index("date")
)

# 전체 데이터 중에서 가장 최근에 거래가 이루어진 날짜를 추출합니다.
# 주봉 계산 기준일은 매일 다르므로, 매일 배치 실행 시 이 날짜를 기준으로 결과가 저장됩니다.
latest_trade_date = df_all.index.max().strftime("%Y-%m-%d")
latest_trade_date_clean = df_all.index.max().strftime("%Y%m%d")

weekly_candidates = []
# =======================================================
# 3. 종목별 주봉 변환 + 52주 신고가 계산
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

    weekly["HIGH_52_CLOSE"] = weekly["close"].rolling(52).max()

    prev = weekly.iloc[-2]
    last = weekly.iloc[-1]

    diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    if (
        last["close"] >= last["HIGH_52_CLOSE"] and
        prev["close"] < prev["HIGH_52_CLOSE"] and
        last["close"] >= 10000
    ):
        # 기존에는 주말 날짜(last.name)로 저장했으나, 매일 배치 실행에 대응하기 위해
        # 해당 주봉을 평가한 시점의 가장 최근 거래일 날짜(latest_trade_date)로 기록합니다.
        weekly_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": latest_trade_date,
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "volume": float(last["volume"]),
            "diff": diff,
            "special_value": float(last["HIGH_52_CLOSE"])
        })

# =======================================================
# 4. TXT 저장
# =======================================================
if weekly_candidates:

    df_weekly = pd.DataFrame(weekly_candidates).sort_values(
        by="close", ascending=False
    )

    print("\n[주봉] 52주 신고가 '첫 발생' 종목\n")
    print(df_weekly.to_string(index=False))
    print(f"\n총 {len(df_weekly)}건 감지됨.\n")

    last_date = df_weekly.iloc[0]["date"]
    # 결과 아이디(result_id) 역시 실제 배치 실행 기준 날짜가 아닌 최신 거래일 날짜 기준으로 통일하여 생성합니다.
    result_id = f"{latest_trade_date_clean}_{strategy_name}"

    # STRATEGY_RESULT
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_weekly)
    )

    # STRATEGY_DETAIL
    for rank, row in enumerate(df_weekly.to_dict("records"), start=1):
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
    print(f"ROWCOUNT  = {len(df_weekly)}\n")

else:
    print("\n주봉 52주 신고가 '첫 발생' 종목 없음 — 저장 생략\n")
