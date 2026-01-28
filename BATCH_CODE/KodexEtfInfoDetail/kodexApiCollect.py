# ============================================================
# 0. 프로젝트 루트 + import 경로
# ============================================================
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # /workspace
sys.path.insert(0, str(PROJECT_ROOT))

# ============================================================
# 1. 기본 import
# ============================================================
import os
import requests
from datetime import datetime

from BATCH_CODE.common import config   # ENV 로딩 트리거

# =====================================================
# ENV
# =====================================================
BASE_OUT_DIR = os.getenv("BATCH_OUT_DIR")
if not BASE_OUT_DIR:
    raise RuntimeError("BATCH_OUT_DIR not set")

DELIM = os.getenv("TXT_DELIM", "|")


def today_folder():
    return datetime.now().strftime("%Y%m%d")


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_today_gijunYMD():
    return datetime.today().strftime("%Y.%m.%d")


def ensure_out_dir():
    path = os.path.join(BASE_OUT_DIR, today_folder())
    os.makedirs(path, exist_ok=True)
    return path


def to_int(v):
    return int(v) if v not in (None, "", "null") else ""


def to_float(v):
    return float(v) if v not in (None, "", "null") else ""


# =====================================================
# TXT Writer – SUMMARY
# =====================================================
def write_summary_txt(rows, base_date):
    out_path = os.path.join(
        ensure_out_dir(),
        f"KODEX_ETF_SUMMARY_{base_date.replace('.', '')}.txt"
    )

    headers = [
        "etf_id",
        "base_date",
        "etf_name",
        "irp_yn",
        "total_cnt",
        "last_update"
    ]

    write_header = not os.path.exists(out_path)

    with open(out_path, "a", encoding="utf-8") as f:
        if write_header:
            f.write(DELIM.join(headers) + "\n")

        for r in rows:
            f.write(DELIM.join([
                r["etf_id"],
                base_date,
                r["etf_name"],
                str(r["irp_yn"]),
                str(r["total_cnt"]),
                now_str()
            ]) + "\n")


# =====================================================
# TXT Writer – HOLDINGS
# =====================================================
def write_holdings_txt(rows, base_date):
    out_path = os.path.join(
        ensure_out_dir(),
        f"KODEX_ETF_HOLDINGS_{base_date.replace('.', '')}.txt"
    )

    headers = [
        "etf_id",
        "base_date",
        "stock_code",
        "stock_name",
        "holding_qty",
        "current_price",
        "eval_amount",
        "weight_ratio",
        "last_update"
    ]

    write_header = not os.path.exists(out_path)

    with open(out_path, "a", encoding="utf-8") as f:
        if write_header:
            f.write(DELIM.join(headers) + "\n")

        for r in rows:
            f.write(DELIM.join([
                r["etf_id"],
                base_date,
                r["stock_code"],
                r["stock_name"],
                str(r["holding_qty"]),
                str(r["current_price"]),
                str(r["eval_amount"]),
                str(r["weight_ratio"]),
                now_str()
            ]) + "\n")


# =====================================================
# MAIN
# =====================================================
def run():
    url = "https://www.samsungfund.com/api/v1/kodex/product-document.do"

    gijunYMD = get_today_gijunYMD()
    print(f"[INFO] 기준일자(gijunYMD) = {gijunYMD}")

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    page = 1

    while True:
        print(f"[FETCH] pageNo={page}")

        r = requests.get(url, params={
            "pageNo": page,
            "gijunYMD": gijunYMD
        }, headers=headers)
        r.raise_for_status()

        api_json = r.json()
        doc_list = api_json.get("documentList", [])

        if not doc_list:
            print("[INFO] 더 이상 데이터 없음")
            break

        summary_rows = []
        holdings_rows = []

        for doc in doc_list:
            etf_id = doc["fId"]
            pdf_list = doc.get("pdfList", [])

            if not pdf_list:
                continue

            summary_rows.append({
                "etf_id": etf_id,
                "etf_name": doc.get("fNm", ""),
                "irp_yn": doc.get("irpYn", ""),
                "total_cnt": to_int(pdf_list[0].get("totalCnt"))
            })

            for h in pdf_list:
                holdings_rows.append({
                    "etf_id": etf_id,
                    "stock_code": h["itmNo"],
                    "stock_name": h["secNm"],
                    "holding_qty": to_float(h.get("applyQ")),
                    "current_price": to_int(h.get("curp")),
                    "eval_amount": to_int(h.get("evalA")),
                    "weight_ratio": to_float(h.get("ratio"))
                })

        write_summary_txt(summary_rows, gijunYMD)
        write_holdings_txt(holdings_rows, gijunYMD)

        print(f"[OK] page {page} TXT 생성 완료")
        page += 1

    print("KODEX ETF Batch-Out 완료")


if __name__ == "__main__":
    run()
