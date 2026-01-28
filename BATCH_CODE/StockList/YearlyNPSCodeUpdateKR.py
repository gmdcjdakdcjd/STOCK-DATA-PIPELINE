import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict


class NpsPortfolioBatchOut:
    """
    NPS 포트폴리오 CSV → ITEM / HEADER TXT Batch-Out
    (CSV 기반, 실행 전용)
    """

    def __init__(self):
        # ==================================================
        # ENV 로딩
        # ==================================================
        base_dir = Path(__file__).resolve().parents[2]

        app_env = os.getenv("APP_ENV", "local")
        env_file = base_dir / f".env.{app_env}"

        if not env_file.exists():
            raise RuntimeError(f"ENV FILE NOT FOUND: {env_file}")

        load_dotenv(env_file)

        # ==================================================
        # ENV 값
        # ==================================================
        self.BATCH_OUT_DIR = os.getenv("BATCH_OUT_DIR")
        self.DELIM = os.getenv("TXT_DELIM", "|")

        self.NPS_KR_CSV = os.getenv("MONTHLY_NPS_INFO_KR_CSV")
        self.NPS_US_CSV = os.getenv("MONTHLY_NPS_INFO_US_CSV")

        if not self.BATCH_OUT_DIR:
            raise RuntimeError("BATCH_OUT_DIR not set")
        if not self.NPS_KR_CSV:
            raise RuntimeError("MONTHLY_NPS_INFO_KR_CSV not set")
        if not self.NPS_US_CSV:
            raise RuntimeError("MONTHLY_NPS_INFO_US_CSV not set")

        self.BASE_DATE = datetime.now().strftime("%Y%m%d")
        self.NOW_STR = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ==================================================
    # CSV 읽기
    # ==================================================
    def read_nps_csv(self, csv_path: str, market: str) -> pd.DataFrame:
        print(f"[STEP] Read NPS CSV ({market}) → {csv_path}")

        df = pd.read_csv(csv_path, encoding="cp949")
        df.columns = df.columns.str.strip()

        df = df.rename(columns={
            "번호": "rank_no",
            "종목명": "name",
            "평가액(억 원)": "eval_amount_100m",
            "자산군 내 비중(퍼센트)": "weight_pct",
            "지분율(퍼센트)": "ownership_pct"
        })

        df["rank_no"] = df["rank_no"].astype(int)
        df["name"] = df["name"].astype(str)
        df["eval_amount_100m"] = df["eval_amount_100m"].astype(float)
        df["weight_pct"] = df["weight_pct"].astype(float)
        df["ownership_pct"] = df["ownership_pct"].astype(float)

        df["institution_code"] = "NPS"
        df["asset_type"] = "STOCK"
        df["market"] = market
        df["base_date"] = self.BASE_DATE

        print(f"[OK] {market} CSV rows = {len(df)}")
        return df

    # ==================================================
    # ITEM TXT
    # ==================================================
    def write_item_txt(self, df: pd.DataFrame):
        print("[STEP] Write ITEM TXT")

        out_dir = os.path.join(self.BATCH_OUT_DIR, self.BASE_DATE)
        os.makedirs(out_dir, exist_ok=True)

        file_path = os.path.join(
            out_dir,
            f"NPS_PORTFOLIO_ITEM_{self.BASE_DATE}.txt"
        )

        headers = [
            "institution_code",
            "base_date",
            "asset_type",
            "market",
            "rank_no",
            "name",
            "asset_sub_type",
            "weight_pct",
            "ownership_pct",
            "eval_amount_100m"
        ]

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(self.DELIM.join(headers) + "\n")

            for idx, r in df.iterrows():
                row = [
                    r["institution_code"],
                    r["base_date"],
                    r["asset_type"],
                    r["market"],
                    str(r["rank_no"]),
                    r["name"],
                    r.get("asset_sub_type", ""),
                    str(r["weight_pct"]),
                    str(r["ownership_pct"]),
                    str(r["eval_amount_100m"]),
                ]
                f.write(self.DELIM.join(row) + "\n")

                print(
                    f"[WRITE] #{idx+1:04d} "
                    f"{r['name']} ({r['market']}) ITEM OK"
                )

        print(f"[OK] ITEM TXT → {file_path}")

    # ==================================================
    # HEADER TXT
    # ==================================================
    def write_header_txt(self, df: pd.DataFrame):
        print("[STEP] Write HEADER TXT")

        out_dir = os.path.join(self.BATCH_OUT_DIR, self.BASE_DATE)
        os.makedirs(out_dir, exist_ok=True)

        file_path = os.path.join(
            out_dir,
            f"NPS_PORTFOLIO_HEADER_{self.BASE_DATE}.txt"
        )

        header_map = defaultdict(int)
        for _, r in df.iterrows():
            key = (
                r["institution_code"],
                r["base_date"],
                r["asset_type"],
                r["market"]
            )
            header_map[key] += 1

        headers = [
            "institution_code",
            "base_date",
            "asset_type",
            "market",
            "total_count"
        ]

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(self.DELIM.join(headers) + "\n")

            for (inst, date, asset_type, market), cnt in header_map.items():
                row = [
                    inst,
                    date,
                    asset_type,
                    market,
                    str(cnt)
                ]
                f.write(self.DELIM.join(row) + "\n")

        print(f"[OK] HEADER TXT → {file_path}")

    # ==================================================
    # 실행
    # ==================================================
    def execute(self):
        print("=== NPS PORTFOLIO BATCH START ===")

        df_kr = self.read_nps_csv(self.NPS_KR_CSV, "KR")
        df_us = self.read_nps_csv(self.NPS_US_CSV, "US")

        df_all = pd.concat([df_kr, df_us], ignore_index=True)

        if df_all.empty:
            print("[ERROR] No data. Abort.")
            return

        self.write_item_txt(df_all)
        self.write_header_txt(df_all)

        print("=== NPS PORTFOLIO BATCH END ===")


# ==================================================
# main
# ==================================================
if __name__ == "__main__":
    batch = NpsPortfolioBatchOut()
    batch.execute()
