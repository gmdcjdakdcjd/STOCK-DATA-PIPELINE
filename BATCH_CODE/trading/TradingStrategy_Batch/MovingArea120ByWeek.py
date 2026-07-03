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
mk = MarketDB()
company_df = mk.get_comp_info_optimization()
stocks = set(company_df["code"])

print(f"\n총 {len(stocks)}개 종목 스캔 시작...\n")

# 120주 이평선 계산을 위해 충분한 주봉 데이터(최소 120주 이상)가 확보되도록 최근 4년 치 데이터를 가져옵니다.
start_date = (pd.Timestamp.today() - pd.DateOffset(years=4)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "WEEKLY_TOUCH_MA120_KR"

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

# 데이터에 존재하는 가장 최근 거래일
latest_trade_date = df_all.index.max().strftime("%Y-%m-%d")
latest_trade_date_clean = df_all.index.max().strftime("%Y%m%d")

# =======================================================
# 3. 종목별 주봉 + MA120 터치 계산
# =======================================================
for code, group in df_all.groupby("code"):

    # 일봉 데이터를 주봉(W-SAT: 토요일 기준 주간 마감) 데이터로 리샘플링
    weekly = pd.DataFrame({
        "open": group["open"].resample("W-SAT").first(),
        "high": group["high"].resample("W-SAT").max(),
        "low":  group["low"].resample("W-SAT").min(),
        "close": group["close"].resample("W-SAT").last(),
        "volume": group["volume"].resample("W-SAT").sum(),
    }).dropna()

    # 120주 이평선 계산을 위해 최소 120개 이상의 주봉 데이터가 필요합니다.
    if len(weekly) < 120:
        continue

    # 120주 이동평균
    weekly["MA120"] = weekly["close"].rolling(120, min_periods=120).mean()

    prev = weekly.iloc[-2]   # 지난 주
    last = weekly.iloc[-1]   # 이번 주

    if pd.isna(prev["MA120"]) or prev["MA120"] == 0:
        continue

    # 주간 등락률
    diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    # MA120 터치율
    touch_rate = ((last["close"] - prev["MA120"]) / prev["MA120"]) * 100

    # 120주선 터치 조건: 터치율 -1% ~ 1% 이내 이면서 종가 10,000원 이상
    if -1.0 <= touch_rate <= 1.0 and last["close"] >= 10000:
        touch_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": latest_trade_date,
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "diff": diff,
            "volume": float(last["volume"]),
            "special_value": round(float(prev["MA120"]), 2)  # MA120 값
        })

# =======================================================
# 4. TXT 저장
# =======================================================
if touch_candidates:

    df_touch = pd.DataFrame(touch_candidates).sort_values(by="diff")

    print("\n[주봉] 120주선 터치 종목 리스트\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    last_date = df_touch.iloc[0]["date"]
    result_id = f"{latest_trade_date_clean}_{strategy_name}"

    # 요약 정보 저장
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=last_date,
        total_data=len(df_touch)
    )

    # 상세 내역 저장
    for row in df_touch.to_dict("records"):
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
    print("\n120주선 터치 종목 없음 — 저장 생략\n")
