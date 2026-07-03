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

# 20주 볼린저 밴드 계산을 위해 최근 2년 치 데이터를 조회합니다.
start_date = (pd.Timestamp.today() - pd.DateOffset(years=2)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")

strategy_name = "WEEKLY_BB_UPPER_TOUCH_KR"

touch_candidates = []

# =======================================================
# 2. 전체 일봉 1회 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("전체 가격 데이터 없음 — 종료")
    exit()

# 종목 필터링 및 인덱스 정렬 설정
df_all = df_all[df_all["code"].isin(stocks)]
df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
df_all = (
    df_all
    .dropna(subset=["date"])
    .sort_values(["code", "date"])
    .set_index("date")
)

# 데이터에 존재하는 가장 최근 거래일
latest_trade_date = df_all.index.max().strftime("%Y-%m-%d")
latest_trade_date_clean = df_all.index.max().strftime("%Y%m%d")

# =======================================================
# 3. 종목별 주봉 변환 및 볼린저 밴드 상단 터치 계산
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

    # 볼린저 밴드 계산을 위해 최소 20개 이상의 주봉 데이터가 필요합니다.
    if len(weekly) < 20:
        continue

    # 볼린저 밴드 (20주, 2표준편차)
    ma20 = weekly["close"].rolling(20).mean()
    std = weekly["close"].rolling(20).std()
    upper = ma20 + (std * 2)

    if pd.isna(upper.iloc[-1]):
        continue

    prev = weekly.iloc[-2]
    last = weekly.iloc[-1]

    # 주간 거래량 0 종목 제외
    if pd.isna(last["volume"]) or last["volume"] <= 0:
        continue

    close_price = last["close"]
    upper_band = upper.iloc[-1]

    # 주간 종가 기준 전주 대비 등락률 및 볼린저 밴드 상단선 대비 괴리율 계산
    diff = round(((close_price - prev["close"]) / prev["close"]) * 100, 2)
    gap_rate = ((close_price - upper_band) / upper_band) * 100

    # 주봉 상단 터치 조건: 괴리율 -1% ~ 1% 범위 내 이면서 종가 10,000원 이상
    if (
        -1.0 <= gap_rate <= 1.0
        and close_price >= 10000
    ):
        touch_candidates.append({
            "code": code,
            "name": mk.codes.get(code, "UNKNOWN"),
            "date": latest_trade_date,
            "close": float(close_price),
            "prev_close": float(prev["close"]),
            "diff": diff,
            "volume": float(last["volume"]),
            "special_value": round(float(upper_band), 2)  # BB 상단
        })


# =======================================================
# 4. TXT 저장
# =======================================================
if touch_candidates:

    df_touch = pd.DataFrame(touch_candidates).sort_values(
        by="diff", ascending=False
    )

    print("\n[주봉] 볼린저 상단 터치 종목 (±1%)\n")
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
            special_value=row["special_value"],  # BB 상단
            result_id=result_id
        )

    print("\nTXT 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_touch)}\n")

else:
    print("\n볼린저 상단 터치 종목 없음 — 저장 생략\n")
