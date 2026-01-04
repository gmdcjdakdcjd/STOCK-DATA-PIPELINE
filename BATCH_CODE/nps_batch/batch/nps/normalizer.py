from datetime import date

def to_float(v):
    try:
        return float(v)
    except Exception:
        return None

def to_int(v):
    try:
        return int(v)
    except Exception:
        return None


def normalize(item, asset_type, market):
    return {
        "institution": "NPS",
        "base_date": date.today().strftime("%Y%m%d"),
        "asset_type": asset_type,
        "market": market,
        "rank_no": to_int(item.get("번호")),
        "name": item.get("종목명") or item.get("발행기관명"),
        "asset_sub_type": item.get("종류"),
        "weight_pct": to_float(
            item.get("자산군 내 비중(퍼센트)") or item.get("비중(퍼센트)")
        ),
        "ownership_pct": to_float(item.get("지분율(퍼센트)")),
        "eval_amount": to_float(
            item.get("평가액(억 원)") or item.get("금액(억 원)")
        ),
    }
