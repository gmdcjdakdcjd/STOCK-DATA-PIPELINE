import os
from collections import defaultdict

from .api_paths import API_PATHS
from .api_fetcher import fetch_all
from .normalizer import normalize
from .text_writer import write_text, write_header_text
from BATCH_CODE.common.output_path import get_out_file


def run():
    print("=== NPS BATCH START ===")

    all_rows = []
    total_raw = 0

    # ==================================================
    # 1. API Fetch + Normalize (ITEM 생성)
    # ==================================================
    for key, info in API_PATHS.items():
        print(f"[STEP] Fetching API: {key}")

        raw_items = fetch_all(info["path"])
        raw_count = len(raw_items)
        total_raw += raw_count

        if raw_count == 0:
            print(f"[WARN] {key} returned no data")
            continue

        for idx, item in enumerate(raw_items, start=1):
            try:
                row = normalize(
                    item,
                    asset_type=info["asset_type"],
                    market=info["market"]
                )
                all_rows.append(row)

                if idx % 100 == 0:
                    print(f"[DEBUG] {key} normalized {idx}/{raw_count}")

            except Exception as e:
                print(f"[ERROR] normalize failed ({key}) → {e}")
                print(f"[ERROR] item = {item}")

    print(f"[INFO] Total raw items = {total_raw}")
    print(f"[INFO] Total normalized rows = {len(all_rows)}")

    if not all_rows:
        print("[ERROR] No data generated. Abort.")
        return

    # ==================================================
    # 2. base_date 일관성 체크 (안전장치)
    # ==================================================
    base_dates = {r["base_date"] for r in all_rows}
    if len(base_dates) != 1:
        print(f"[WARN] multiple base_date detected: {base_dates}")

    # ==================================================
    # 3. ITEM 파일 출력
    # ==================================================
    print("[STEP] Writing ITEM file")

    item_file = get_out_file("NPS_PORTFOLIO_ITEM")
    write_text(all_rows, item_file)

    print(f"[OK] ITEM written → {item_file}")

    # ==================================================
    # 4. HEADER 생성 (PK 기준 group-by)
    # ==================================================
    print("[STEP] Building HEADER rows")

    header_map = defaultdict(int)

    for r in all_rows:
        key = (
            r["institution"],
            r["base_date"],
            r["asset_type"],
            r["market"]
        )
        header_map[key] += 1

    header_rows = []
    for (inst, date, asset_type, market), count in header_map.items():
        header_rows.append({
            "institution": inst,
            "base_date": date,
            "asset_type": asset_type,
            "market": market,
            "total_count": count
        })

    # ==================================================
    # 5. HEADER 파일 출력 (하루 1번 가드)
    # ==================================================
    print("[STEP] Writing HEADER file")

    header_file = get_out_file("NPS_PORTFOLIO")

    if not os.path.exists(header_file):
        write_header_text(header_rows, header_file)
        print(f"[OK] HEADER written → {header_file}")
    else:
        print(f"[SKIP] HEADER already exists → {header_file}")

    print("=== NPS BATCH END ===")


if __name__ == "__main__":
    run()
