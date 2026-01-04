import os
from BATCH_CODE.common.output_path import DELIM


# ==================================================
# ITEM (nps_portfolio_item) TXT 정의
# ==================================================
ITEM_HEADER = [
    "institution_code",
    "base_date",
    "asset_type",
    "market",
    "rank_no",
    "name",
    "asset_sub_type",
    "weight_pct",
    "ownership_pct",
    "eval_amount_100m",
]


def write_text(rows, file_path):
    """
    ITEM Batch-Out TXT writer
    - 파일 없으면 생성
    - 있으면 append
    - 최초 생성 시 header 기록
    """

    _ensure_dir(file_path)
    is_new_file = not os.path.exists(file_path)

    with open(file_path, "a", encoding="utf-8") as f:
        if is_new_file:
            f.write(DELIM.join(ITEM_HEADER) + "\n")

        for r in rows:
            f.write(_format_item_row(r) + "\n")


def _format_item_row(r: dict) -> str:
    return DELIM.join([
        r.get("institution", ""),
        r.get("base_date", ""),
        r.get("asset_type", ""),
        r.get("market", ""),
        str(r.get("rank_no") or ""),
        r.get("name") or "",
        r.get("asset_sub_type") or "",
        str(r.get("weight_pct") or ""),
        str(r.get("ownership_pct") or ""),
        str(r.get("eval_amount") or ""),
    ])


# ==================================================
# HEADER (nps_portfolio) TXT 정의
# ==================================================
HEADER_HEADER = [
    "institution_code",
    "base_date",
    "asset_type",
    "market",
    "total_count",
]


def write_header_text(rows, file_path):
    """
    HEADER Batch-Out TXT writer
    - 파일 없으면 생성
    - 있으면 append
    - 최초 생성 시 header 기록
    """

    _ensure_dir(file_path)
    is_new_file = not os.path.exists(file_path)

    with open(file_path, "a", encoding="utf-8") as f:
        if is_new_file:
            f.write(DELIM.join(HEADER_HEADER) + "\n")

        for r in rows:
            f.write(_format_header_row(r) + "\n")


def _format_header_row(r: dict) -> str:
    return DELIM.join([
        r["institution"],
        r["base_date"],
        r["asset_type"],
        r["market"],
        str(r["total_count"]),
    ])


# ==================================================
# Common utils
# ==================================================
def _ensure_dir(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
