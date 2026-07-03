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

# 주봉 및 RSI(14주) 계산을 안정적으로 수행하기 위해 2년 치 데이터를 가져옵니다.
start_date = (pd.Timestamp.today() - pd.DateOffset(years=2)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
latest_trade_date = mk.get_latest_date(today_str)
latest_trade_date_clean = latest_trade_date.replace("-", "")

strategy_name = "RSI_30_UNHEATED_WEEKLY_US"


# =======================================================
# 2. RSI 계산 함수
# =======================================================
def compute_rsi(series, period=14):
    """
    주어진 종가 시리즈에 대한 RSI(상대강도지수)를 계산합니다.
    """
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


# =======================================================
# 3. 전체 일봉 1회 조회 및 필터링
# =======================================================
df_all = mk.get_all_daily_prices(start_date, latest_trade_date)

if df_all.empty:
    print("전체 가격 데이터 없음 — 종료")
    exit()

# 종목 필터링 및 인덱스 정렬 설정
df_all["date"] = pd.to_datetime(df_all["date"])
df_all = df_all[df_all["code"].isin(stocks)]
df_all = (
    df_all
    .dropna(subset=["date"])
    .sort_values(["code", "date"])
    .set_index("date")
)

rsi_candidates = []

# =======================================================
# 4. 종목별 주봉 변환 및 RSI 계산
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

    # RSI 14를 계산하기 위해 최소 20개 이상의 주봉 데이터가 필요합니다.
    if len(weekly) < 20:
        continue

    weekly["rsi"] = compute_rsi(weekly["close"])

    if len(weekly) < 2:
        continue

    last = weekly.iloc[-1]
    prev = weekly.iloc[-2]

    if pd.isna(last["rsi"]):
        continue

    # 주간 종가 기준 전주 대비 등락률 계산
    rate = round(((last["close"] - prev["close"]) / prev["close"]) * 100, 2)

    # 조건 필터링: 주봉 RSI 30 이하, 종가 $15 이상, 주간 거래량이 유효한 경우
    if (
        last["rsi"] <= 30 and
        last["close"] >= 15 and
        not pd.isna(last["volume"]) and
        last["volume"] > 0
    ):
        rsi_candidates.append({
            "code": code,
            "name": mk.code_to_name.get(code, "UNKNOWN"),
            "date": latest_trade_date,
            "close": float(last["close"]),
            "prev_close": float(prev["close"]),
            "rate": rate,
            "volume": float(last["volume"]),
            "special_value": round(float(last["rsi"]), 2)
        })


# =======================================================
# 5. TXT 저장 처리
# =======================================================
if rsi_candidates:
    df_rsi = pd.DataFrame(rsi_candidates).sort_values(by="special_value")

    print(f"\n[{strategy_name}] RSI 30 이하 & 종가 $15 이상 주봉 종목\n")
    print(df_rsi.to_string(index=False))
    print(f"\n총 {len(df_rsi)}건 감지됨.\n")

    result_id = f"{latest_trade_date_clean}_{strategy_name}"

    # 요약 정보 저장
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=latest_trade_date,
        total_data=len(df_rsi)
    )

    # 상세 내역 저장
    for rank, row in enumerate(df_rsi.to_dict("records"), start=1):
        save_strategy_detail(
            signal_date=row["date"],
            action=strategy_name,
            code=row["code"],
            name=row["name"],
            prev_close=row["prev_close"],
            price=row["close"],
            diff=row["rate"],
            volume=row["volume"],
            special_value=row["special_value"],
            result_id=result_id
        )
    print(f"{strategy_name} TXT 저장 완료 (RESULT_ID: {result_id})")
else:
    print(f"\n{strategy_name} 조건 만족 종목 없음 — 저장 생략\n")
