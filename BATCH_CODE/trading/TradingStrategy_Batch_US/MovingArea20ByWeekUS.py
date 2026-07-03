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

# 20주 이평선 계산을 위해 최근 2년 치 데이터를 가져옵니다.
start_date = (pd.Timestamp.today() - pd.DateOffset(years=2)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
latest_trade_date = mk.get_latest_date(today_str)
latest_trade_date_clean = latest_trade_date.replace("-", "")

strategy_name = "WEEKLY_TOUCH_MA20_US"

touch_list = []

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

# =======================================================
# 3. 종목별 주봉 + MA20 터치 계산
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 100:  # 주봉 20개 만들기 위한 안전선
        continue

    df = group.sort_values("date").copy()
    df.set_index("date", inplace=True)

    # 일봉 데이터를 주봉(W-SAT: 토요일 기준 주간 마감) 데이터로 리샘플링
    weekly = pd.DataFrame({
        "open": df["open"].resample("W-SAT").first(),
        "high": df["high"].resample("W-SAT").max(),
        "low":  df["low"].resample("W-SAT").min(),
        "close": df["close"].resample("W-SAT").last(),
        "volume": df["volume"].resample("W-SAT").sum()
    }).dropna()

    # 20주 이평선 계산을 위해 최소 20개 이상의 주봉 데이터가 필요합니다.
    if len(weekly) < 20:
        continue

    # 20주 이동평균
    weekly["MA20"] = weekly["close"].rolling(20, min_periods=20).mean()

    prev = weekly.iloc[-2]   # 지난 주
    last = weekly.iloc[-1]   # 이번 주

    if pd.isna(prev["MA20"]) or prev["MA20"] == 0:
        continue

    # 주간 등락률
    diff = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    # MA20 터치율
    touch_rate = ((last["close"] - prev["MA20"]) / prev["MA20"]) * 100

    # 20주선 터치 조건: 터치율 -1% ~ 1% 이내 이면서 종가 $15 이상
    if -1.0 <= touch_rate <= 1.0 and last["close"] >= 15:
        touch_list.append({
            "code": code,
            "name": mk.code_to_name.get(code, "UNKNOWN"),
            "date": latest_trade_date,
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "diff": diff,
            "volume": float(last["volume"]),
            "special_value": round(float(prev["MA20"]), 2)  # MA20 값
        })

# =======================================================
# 4. 정렬 + TXT 저장
# =======================================================
if touch_list:

    df_touch = pd.DataFrame(touch_list).sort_values(by="diff")

    print(f"\n[US] {strategy_name} 종목 리스트\n")
    print(df_touch.to_string(index=False))
    print(f"\n총 {len(df_touch)}건 감지됨.\n")

    result_id = f"{latest_trade_date_clean}_{strategy_name}"

    # 요약 정보 저장
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=latest_trade_date,
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
            special_value=row["special_value"],  # MA20
            result_id=result_id
        )

    print(f"\nTXT 생성 완료 → RESULT_ID = {result_id}, ROWCOUNT = {len(df_touch)}\n")

else:
    print(f"\n{strategy_name} 조건 만족 종목 없음 — 저장 생략\n")
