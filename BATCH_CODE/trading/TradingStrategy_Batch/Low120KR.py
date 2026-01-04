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

start_date = (pd.Timestamp.today() - pd.DateOffset(days=200)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.now().strftime("%Y%m%d")

strategy_name = "DAILY_120D_NEW_LOW_KR"

# =======================================================
# 2. 전체 일봉 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

# date 처리 + index 세팅은 여기서 한 번만
df_all = df_all[df_all["code"].isin(stocks)]
df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
df_all = (
    df_all
    .dropna(subset=["date"])
    .sort_values(["code", "date"])
    .set_index("date")
)

low_candidates = []

# =======================================================
# 3. 종목별 120일 신저가 판단
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 120:
        continue

    group["LOW_120_CLOSE"] = group["close"].rolling(120).min()

    prev = group.iloc[-2]
    last = group.iloc[-1]

    # 오늘 처음 120일 신저가 + 종가 10,000 이상
    if (
            last["LOW_120_CLOSE"] >= last["close"] >= 10000 and
            prev["close"] > prev["LOW_120_CLOSE"]
    ):
        diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

        low_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "volume": float(last.get("volume", 0)),
            "diff": diff,
            "special_value": float(last["LOW_120_CLOSE"])
        })

# =======================================================
# 4. TXT 저장
# =======================================================
if low_candidates:

    df_low = pd.DataFrame(low_candidates).sort_values(by="close", ascending=True)

    print("\n[일봉] 120일 종가 신저가 ‘첫 발생’ 종목\n")
    print(df_low.to_string(index=False))
    print(f"\n총 {len(df_low)}건 감지됨.\n")

    last_date = df_low.iloc[0]["date"]
    result_id = f"{today}_{strategy_name}"

    # STRATEGY_RESULT (append)
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_low)
    )

    # STRATEGY_DETAIL (append)
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
    print("\n120일 종가 신저가 발생 종목 없음 — 저장 생략\n")
