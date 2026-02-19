# ===== sys.path 세팅 (최상단) =====
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

# ===== 기존 import =====
import pandas as pd
import warnings
from datetime import datetime

from API.ETFAnalyzeKR import MarketDB
from BATCH_CODE.trading.txt_saver_kr import (
    save_strategy_result,
    save_strategy_detail
)

warnings.filterwarnings("ignore", category=RuntimeWarning)

# =======================================================
# 1. 기본 세팅
# =======================================================
mk = MarketDB()
etf_df = mk.get_etf_info_optimization()

etf_codes = set(etf_df["code"])
name_map = dict(zip(etf_df["code"], etf_df["name"]))

print(f"\n총 {len(etf_codes)}개 ETF 스캔 시작...\n")

start_date = (pd.Timestamp.today() - pd.DateOffset(days=14)).strftime("%Y-%m-%d")
today_str = datetime.now().strftime("%Y-%m-%d")
strategy_name = "ETF_TOP20_VOLUME_KR"

# =======================================================
# 2. 전체 가격 한 번에 조회
# =======================================================
df_all = mk.get_all_daily_prices(start_date, today_str)

if df_all.empty:
    print("\n전체 가격 데이터 없음 — 종료")
    exit()

df_all["date"] = pd.to_datetime(df_all["date"])
df_all = df_all[df_all["code"].isin(etf_codes)]
df_all = df_all.sort_values(["code", "date"])

volume_list = []

# =======================================================
# 3. ETF별 거래량 계산
# =======================================================
for code, group in df_all.groupby("code"):

    if len(group) < 2:
        continue

    group = group.sort_values("date")

    prev = group.iloc[-2]
    last = group.iloc[-1]

    if pd.isna(last["volume"]) or last["volume"] <= 0:
        continue

    diff = ((last["close"] - prev["close"]) / prev["close"]) * 100

    volume_list.append({
        "code": code,
        "name": name_map.get(code, "UNKNOWN"),
        "date": last["date"].strftime("%Y-%m-%d"),
        "prev_close": float(prev["close"]),
        "close": float(last["close"]),
        "diff": round(diff, 2),
        "volume": float(last["volume"])
    })

# =======================================================
# 4. 정렬 + TOP20 + TXT 저장
# =======================================================
if volume_list:

    df_final = (
        pd.DataFrame(volume_list)
        .sort_values(by="volume", ascending=False)
        .head(20)
    )

    print("\n[KR ETF] 거래량 TOP20 리스트\n")
    print(df_final.to_string(index=False))
    print(f"\n총 {len(df_final)}건 감지됨.\n")

    # RESULT_ID 규칙 통일
    today_id = datetime.now().strftime("%Y%m%d")
    result_id = f"{today_id}_{strategy_name}"

    # --------------------------
    # RESULT (요약)
    # --------------------------
    save_strategy_result(
        strategy_name=strategy_name,
        signal_date=df_final.iloc[0]["date"],
        total_data=len(df_final)
    )

    # --------------------------
    # DETAIL (상세, special_value = 거래량 순위)
    # --------------------------
    for rank, row in enumerate(df_final.to_dict("records"), start=1):
        save_strategy_detail(
            signal_date=row["date"],
            action=strategy_name,
            code=row["code"],
            name=row["name"],
            prev_close=row["prev_close"],
            price=row["close"],
            diff=row["diff"],
            volume=row["volume"],
            special_value=rank,     # 거래량 순위
            result_id=result_id
        )

    print("\nTXT 저장 완료")
    print(f"RESULT_ID = {result_id}")
    print(f"ROWCOUNT  = {len(df_final)}\n")

else:
    print("\nKR ETF 거래량 TOP20 없음 — 저장 생략\n")
