# ============================================================
# 0. 프로젝트 루트 + import 경로
# ============================================================
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# ============================================================
# 1. 기본 import
# ============================================================
import os
import pandas as pd
from decimal import Decimal, InvalidOperation
from datetime import datetime

from BATCH_CODE.common import config   # ENV 로딩 트리거


# =====================================================
# ENV
# =====================================================
BASE_OUT_DIR = os.getenv("BATCH_OUT_DIR")
if not BASE_OUT_DIR:
    raise RuntimeError("BATCH_OUT_DIR not set")

TIGER_DOWNLOAD_DIR = os.getenv("MONTHLY_TIGER_ETF_INFO_DIR")
if not TIGER_DOWNLOAD_DIR:
    raise RuntimeError("MONTHLY_TIGER_ETF_INFO_DIR not set")

DELIM = os.getenv("TXT_DELIM", "|")
TXT_ENCODING = os.getenv("TXT_ENCODING", "utf-8")


# =====================================================
# 공통 함수
# =====================================================
def today_folder():
    return datetime.now().strftime("%Y%m%d")


def get_file_date():
    return datetime.now().strftime("%Y%m%d")


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_today_base_date():
    return datetime.today().strftime("%Y.%m.%d")


def ensure_out_dir():
    path = os.path.join(BASE_OUT_DIR, today_folder())
    os.makedirs(path, exist_ok=True)
    return path


def get_tiger_excel_path():
    candidates = [
        os.path.join(TIGER_DOWNLOAD_DIR, "TIGER_ETF_INFO.xls"),
        os.path.join(TIGER_DOWNLOAD_DIR, "TIGER_ETF_INFO.xlsx"),
    ]

    for path in candidates:
        if os.path.exists(path):
            return path

    raise FileNotFoundError(
        "TIGER_ETF_INFO.xls 또는 TIGER_ETF_INFO.xlsx 파일을 찾을 수 없습니다. "
        f"dir={TIGER_DOWNLOAD_DIR}"
    )


def normalize_text(v):
    if v is None:
        return ""

    if pd.isna(v):
        return ""

    if isinstance(v, float) and v.is_integer():
        return str(int(v)).strip()

    return str(v).strip()


def normalize_stock_code(v):
    text = normalize_text(v)

    # 종목코드 없는 경우 -> 0 처리
    if not text:
        return "0"

    text = text.replace(".0", "")

    if text == "0":
        return "0"

    # 숫자형 종목코드 자리수 보정
    if text.isdigit() and len(text) < 6:
        return text.zfill(6)

    return text


def to_decimal(v):
    if v is None:
        return ""

    if pd.isna(v):
        return ""

    text = str(v).strip()

    if text == "" or text.lower() in ("null", "none", "-"):
        return ""

    text = text.replace(",", "")
    text = text.replace("%", "")

    try:
        return str(Decimal(text))
    except InvalidOperation:
        return ""


# =====================================================
# TXT Writer – SUMMARY
# =====================================================
def write_summary_txt(rows):
    out_path = os.path.join(
        ensure_out_dir(),
        f"TIGER_ETF_SUMMARY_{get_file_date()}.txt"
    )

    headers = [
        "etf_id",
        "base_date",
        "total_cnt",
        "last_update"
    ]

    write_header = not os.path.exists(out_path)

    with open(out_path, "a", encoding=TXT_ENCODING) as f:
        if write_header:
            f.write(DELIM.join(headers) + "\n")

        for r in rows:
            f.write(DELIM.join([
                r["etf_id"],
                r["base_date"],
                str(r["total_cnt"]),
                now_str()
            ]) + "\n")


# =====================================================
# TXT Writer – HOLDINGS
# =====================================================
def write_holdings_txt(rows):
    out_path = os.path.join(
        ensure_out_dir(),
        f"TIGER_ETF_HOLDINGS_{get_file_date()}.txt"
    )

    headers = [
        "etf_id",
        "base_date",
        "stock_code",
        "stock_name",
        "holding_qty",
        "weight_ratio",
        "last_update"
    ]

    write_header = not os.path.exists(out_path)

    with open(out_path, "a", encoding=TXT_ENCODING) as f:
        if write_header:
            f.write(DELIM.join(headers) + "\n")

        for r in rows:
            f.write(DELIM.join([
                r["etf_id"],
                r["base_date"],
                r["stock_code"],
                r["stock_name"],
                str(r["holding_qty"]),
                str(r["weight_ratio"]),
                now_str()
            ]) + "\n")


# =====================================================
# Excel 파싱
# =====================================================
def get_excel_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".xls":
        return pd.ExcelFile(file_path, engine="xlrd")

    if ext == ".xlsx":
        return pd.ExcelFile(file_path, engine="openpyxl")

    raise RuntimeError(f"지원하지 않는 파일 확장자입니다. file_path={file_path}")


def find_header_row(df):
    max_row = min(len(df), 60)

    for row_idx in range(max_row):
        values = [normalize_text(v) for v in df.iloc[row_idx].values]

        has_stock_code = any("종목코드" in v for v in values)
        has_stock_name = any("종목명" in v for v in values)
        has_qty = any("수량" in v for v in values)
        has_weight = any("비중" in v for v in values)

        if has_stock_code and has_stock_name and has_qty and has_weight:
            return row_idx, values

    return None, []


def build_column_map(header_values):
    col_map = {}

    for idx, value in enumerate(header_values):
        name = normalize_text(value)

        if "종목코드" in name:
            col_map["stock_code"] = idx
        elif "종목명" in name:
            col_map["stock_name"] = idx
        elif "수량" in name:
            col_map["holding_qty"] = idx
        elif "비중" in name:
            col_map["weight_ratio"] = idx

    return col_map


def get_by_idx(values, idx):
    if idx is None:
        return None

    if idx >= len(values):
        return None

    return values[idx]


def is_skip_row(stock_code, stock_name):
    if not stock_name:
        return True

    skip_words = [
        "합계",
        "총계",
        "TOTAL",
        "Total",
        "기준일",
        "종목코드",
        "종목명"
    ]

    return any(word in stock_name for word in skip_words)


def parse_sheet(df, sheet_name):
    etf_id = sheet_name

    # 파일명과 TXT 분리는 실행일 기준.
    # 데이터 base_date도 엑셀 내부값 대신 실행일 기준으로 통일.
    base_date = get_today_base_date()

    header_row_idx, header_values = find_header_row(df)

    if header_row_idx is None:
        print(f"[SKIP] 헤더를 찾지 못했습니다. sheet={sheet_name}")
        return None, []

    col_map = build_column_map(header_values)

    required_cols = [
        "stock_code",
        "stock_name",
        "holding_qty",
        "weight_ratio"
    ]

    for col in required_cols:
        if col not in col_map:
            print(f"[SKIP] 필수 컬럼 누락: {col}, sheet={sheet_name}")
            print(f"[DEBUG] header_values={header_values}")
            print(f"[DEBUG] col_map={col_map}")
            return None, []

    holdings = []

    for row_idx in range(header_row_idx + 1, len(df)):
        values = df.iloc[row_idx].values

        stock_code = normalize_stock_code(
            get_by_idx(values, col_map.get("stock_code"))
        )
        stock_name = normalize_text(
            get_by_idx(values, col_map.get("stock_name"))
        )

        if is_skip_row(stock_code, stock_name):
            continue

        holdings.append({
            "etf_id": etf_id,
            "base_date": base_date,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "holding_qty": to_decimal(
                get_by_idx(values, col_map.get("holding_qty"))
            ),
            "weight_ratio": to_decimal(
                get_by_idx(values, col_map.get("weight_ratio"))
            )
        })

    summary = {
        "etf_id": etf_id,
        "base_date": base_date,
        "total_cnt": len(holdings)
    }

    return summary, holdings


# =====================================================
# MAIN
# =====================================================
def run():
    excel_path = get_tiger_excel_path()

    print(f"[INFO] TIGER Excel Path = {excel_path}")

    excel = get_excel_file(excel_path)

    print(f"[INFO] sheet count = {len(excel.sheet_names)}")

    all_summary_rows = []
    all_holdings_rows = []

    for sheet_name in excel.sheet_names:
        print(f"[SHEET] {sheet_name}")

        df = pd.read_excel(
            excel,
            sheet_name=sheet_name,
            header=None,
            dtype=object
        )

        summary, holdings = parse_sheet(df, sheet_name)

        if summary is None:
            continue

        all_summary_rows.append(summary)
        all_holdings_rows.extend(holdings)

        print(
            f"[OK] sheet={sheet_name}, "
            f"base_date={summary['base_date']}, "
            f"holdings={len(holdings)}"
        )

    if not all_summary_rows:
        raise RuntimeError("파싱된 summary 데이터가 없습니다.")

    if not all_holdings_rows:
        raise RuntimeError("파싱된 holdings 데이터가 없습니다.")

    write_summary_txt(all_summary_rows)
    write_holdings_txt(all_holdings_rows)

    print(
        f"[TXT OK] summary={len(all_summary_rows)}, "
        f"holdings={len(all_holdings_rows)}"
    )

    print("TIGER ETF Batch-Out 완료")


if __name__ == "__main__":
    run()