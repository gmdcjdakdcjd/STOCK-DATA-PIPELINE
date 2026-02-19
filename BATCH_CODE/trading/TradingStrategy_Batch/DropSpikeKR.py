# ===== sys.path 세팅 (최상단) =====
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

# ===== 기존 import =====
import pandas as pd
import warnings
from datetime import datetime

from API.AnalyzeKR import MarketDB
from BATCH_CODE.trading.txt_saver_kr import (
    save_strategy_result,
    save_strategy_detail
)

warnings.filterwarnings("ignore", category=RuntimeWarning)


# =======================================================
# 1. 기본 세팅
# =======================================================
today = datetime.now().strftime("%Y%m%d")
mk = MarketDB()
company_df = mk.get_comp_info_optimization()
stocks = set(company_df["code"])  # 빠른 조회 위해 set 사용

print(f"\n총 {len(stocks)}개 종목 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=14)).strftime('%Y-%m-%d')
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_DROP_SPIKE_KR"

# =======================================================
# 2. MariaDB에서 전체 가격 한 번에 조회 (핵심)
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n전체 가격 데이터 없음 종료")
    exit()

# 필요한 종목만 필터링 (우량 필터)
df_all = df_all[df_all["code"].isin(stocks)]
df_all = df_all.sort_values(["code", "date"])

drop_candidates = []

# =======================================================
# 3. 종목별 하락률 계산 (메모리 처리 → 초고속)
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 2:
        continue

    prev, last = group.tail(2).iloc

    rate = ((last["close"] - prev["close"]) / prev["close"]) * 100

    if rate <= -5 and last["close"] >= 10000:
        drop_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last["date"].strftime("%Y-%m-%d"),
            "prev_close": float(prev["close"]),
            "close": float(last["close"]),
            "rate": round(rate, 2),
            "volume": float(last.get("volume", 0))
        })

# =======================================================
# 4. TXT 저장
# =======================================================
if drop_candidates:

    df_drop = pd.DataFrame(drop_candidates).sort_values(by="rate", ascending=True)

    print("\n[일봉] 전일 대비 5% 이상 하락 종목\n")
    print(df_drop.to_string(index=False))

    last_date = df_drop.iloc[0]["date"]
    today = datetime.now().strftime("%Y%m%d")

    result_id = f"{today}_{strategy_name}"

    # STRATEGY_RESULT 저장
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_drop)
    )

    # STRATEGY_DETAIL 저장
    for rank, row in enumerate(df_drop.to_dict("records"), start=1):
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

    print(f"\nTXT 생성 완료 {result_id}, ROWCOUNT = {len(df_drop)}\n")

else:
    print("\n전일 대비 5% 이상 하락 종목 없음 저장 생략\n")
