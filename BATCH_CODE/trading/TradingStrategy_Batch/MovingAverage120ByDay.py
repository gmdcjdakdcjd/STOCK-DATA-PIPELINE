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

# 120일 이평선 계산을 위해 충분한 영업일 데이터(최소 120일 이상)가 확보되도록 최근 12개월 치를 조회합니다.
start_date = (pd.Timestamp.today() - pd.DateOffset(months=12)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "DAILY_TOUCH_MA120_KR"

touch_candidates = []

# =======================================================
# 2. 전체 일봉 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n전체 가격 데이터 없음 — 종료")
    exit()

df_all = (
    df_all[df_all["code"].isin(stocks)]
    .assign(date=lambda x: pd.to_datetime(x["date"], errors="coerce"))
    .dropna(subset=["date"])
    .sort_values(["code", "date"])
    .set_index("date")
)

# =======================================================
# 3. 종목별 120일선 터치 계산
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 120:
        continue

    group["MA120"] = group["close"].rolling(120, min_periods=120).mean()

    prev = group.iloc[-2]
    last = group.iloc[-1]

    if np.isnan(prev["MA120"]) or prev["MA120"] == 0:
        continue

    # 일간 등락률
    diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    # MA120 터치율
    touch_rate = ((last["close"] - prev["MA120"]) / prev["MA120"]) * 100

    # 120일선 터치 조건: 터치율 -1% ~ 1% 이내 이면서 종가 10,000원 이상
    if -1.0 <= touch_rate <= 1.0 and last["close"] >= 10000:
        touch_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": last.name.strftime("%Y-%m-%d"),
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "diff": diff,
            "volume": float(last.get("volume", 0)),
            "special_value": round(float(prev["MA120"]), 2)  # MA120 값
        })

# =======================================================
# 4. TXT 저장
# =======================================================
if touch_candidates:

    df_touch = pd.DataFrame(touch_candidates).sort_values(by="diff")

    print("\n[일봉] 120일선 터치 종목 리스트\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    last_date = df_touch.iloc[0]["date"]
    today_id = datetime.now().strftime("%Y%m%d")
    result_id = f"{today_id}_{strategy_name}"

    # 요약 정보 저장
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_touch)
    )

    # 상세 내역 저장
    for rank, row in enumerate(df_touch.to_dict("records"), start=1):
        save_strategy_detail(
            signal_date=row["date"],
            action=strategy_name,
            code=row["code"],
            name=row["name"],
            prev_close=row["prev_close"],
            price=row["close"],
            diff=row["diff"],
            volume=row["volume"],
            special_value=row["special_value"],  # MA120
            result_id=result_id
        )

    print("\nTXT 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_touch)}\n")

else:
    print("\n[일봉] 120일선 터치 종목 없음 — 저장 생략\n")
