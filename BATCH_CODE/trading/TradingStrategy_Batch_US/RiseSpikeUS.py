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

start_date = (pd.Timestamp.today() - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
latest_trade_date = mk.get_latest_date(today_str)

strategy_name = "DAILY_RISE_SPIKE_US"

# =======================================================
# 2. MariaDB 전체 일봉 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, latest_trade_date)

if df_all.empty:
    print("\n⚠ 전체 가격 데이터 없음 — 종료")
    exit()

df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

rise_candidates = []

# =======================================================
# 3. 상승 스파이크 계산
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 2:
        continue

    prev = group.iloc[-2]
    last = group.iloc[-1]

    rate = ((last["close"] - prev["close"]) / prev["close"]) * 100

    # 조건: 전일 대비 +7% AND 종가 ≥ $10
    if rate >= 7 and last["close"] >= 10:
        rise_candidates.append({
            "code": code,
            "name": mk.code_to_name.get(code, "UNKNOWN"),
            "date": last["date"].strftime("%Y-%m-%d"),
            "prev_close": float(prev["close"]),
            "close": float(last["close"]),
            "rate": round(rate, 2),
            "volume": float(last.get("volume", 0))
        })

# =======================================================
# 4. 정렬 + TXT 저장
# =======================================================
if rise_candidates:

    df_rise = pd.DataFrame(rise_candidates).sort_values(by="rate", ascending=False)

    print("\n📈 [미국] 전일 대비 7% 이상 상승 종목 목록\n")
    print(df_rise.to_string(index=False))
    print(f"\n총 {len(df_rise)}건 감지됨.\n")

    today = datetime.now().strftime("%Y%m%d")
    result_id = f"{today}_{strategy_name}"

    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=latest_trade_date,
        total_data=len(df_rise)
    )

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

    print(f"\n⚡ TXT 생성 완료 → RESULT_ID = {result_id}, ROWCOUNT = {len(df_rise)}\n")

else:
    print("\n😴 전일 대비 7% 이상 상승 종목 없음 — 저장 생략\n")
