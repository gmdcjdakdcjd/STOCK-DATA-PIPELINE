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

# -----------------------------
# 1️⃣ 기본 세팅
# -----------------------------
mk = MarketDB()
company = mk.get_comp_info_optimization()
stocks = set(company["code"])

print(f"\n총 {len(stocks)}개 미국 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=5)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
latest_trade_date = mk.get_latest_date(today_str)

strategy_name = "DAILY_TOP20_VOLUME_US"
volume_candidates = []

# =======================================================
# 2. 전체 데이터 1번 조회 (MariaDB)
# =======================================================
df_all = mk.get_all_daily_prices(start_date, latest_trade_date)

if df_all.empty:
    print("⚠ 전체 가격 데이터 없음")
    exit()

df_all["date"] = pd.to_datetime(df_all["date"])
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

# =======================================================
# 3. 종목별 최근 2거래일 비교
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 2:
        continue

    group = group.sort_values("date").set_index("date")

    prev = group.iloc[-2]
    last = group.iloc[-1]

    rate = ((last["close"] - prev["close"]) / prev["close"]) * 100

    volume_candidates.append({
        "code": code,
        "name": mk.code_to_name.get(code, "UNKNOWN"),
        "date": last.name.strftime("%Y-%m-%d"),
        "prev_close": float(prev["close"]),
        "close": float(last["close"]),
        "rate": round(rate, 2),
        "volume": float(last["volume"])
    })

# =======================================================
# 4. 거래량 TOP20 추출 + TXT 저장
# =======================================================
if volume_candidates:

    df_top20 = (
        pd.DataFrame(volume_candidates)
        .sort_values(by="volume", ascending=False)
        .head(20)
    )

    print("\n📊 [US] 일봉 거래량 TOP20 종목 리스트\n")
    print(df_top20[["code", "name", "date", "close", "volume"]].to_string(index=False))
    print(f"\n총 {len(df_top20)}건 감지됨.\n")

    today = datetime.now().strftime("%Y%m%d")
    result_id = f"{today}_{strategy_name}"

    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=latest_trade_date,
        total_data=len(df_top20)
    )

    for rank, row in enumerate(df_top20.to_dict("records"), start=1):
        save_strategy_detail(
            signal_date=row["date"],
            action=strategy_name,
            code=row["code"],
            name=row["name"],
            prev_close=row["prev_close"],
            price=row["close"],
            diff=row["rate"],
            volume=row["volume"],
            special_value=rank,   # ⭐ 거래량 순위
            result_id=result_id
        )

    print(f"\n⚡ TXT 생성 완료 → RESULT_ID = {result_id}, ROWCOUNT = {len(df_top20)}\n")

else:
    print("\n😴 거래량 TOP20 없음 — 저장 생략\n")
