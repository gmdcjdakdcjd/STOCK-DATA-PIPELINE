import os
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

# ------------------------------------------
# ENV 로딩
# ------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]

APP_ENV = os.getenv("APP_ENV", "local")
env_file = PROJECT_ROOT / f".env.{APP_ENV}"

if not env_file.exists():
    raise RuntimeError(f"ENV FILE NOT FOUND: {env_file}")

load_dotenv(env_file)


class InvestorFlowBatch:

    def __init__(self):

        self.CONFIG_PATH = os.getenv("COMMON_CONFIG_PATH")
        if not self.CONFIG_PATH:
            raise RuntimeError("COMMON_CONFIG_PATH not set")

        # DB 컬럼과 1:1 매칭 (last_update 제외)
        self.FIELDS = [
            "base_date",
            "individual",
            "foreigner",
            "institutional",
            "fin_invest",
            "insurance",
            "inv_trust",
            "bank",
            "etc_fin",
            "pension",
            "etc_corp"
        ]


    # ------------------------------------------------------------
    # 네이버 투자자 매매동향 수집
    # ------------------------------------------------------------
    def read_investor_flow(self, pages_to_fetch):

        all_data = []
        target_date = datetime.now().strftime("%Y%m%d")

        base_url = "https://finance.naver.com/sise/investorDealTrendDay.naver"

        for page in range(1, pages_to_fetch + 1):

            params = {
                "bizdate": target_date,
                "sosok": "",
                "page": page
            }

            print(f"[INFO] FETCH PAGE {page}")

            res = requests.get(
                base_url,
                params=params,
                headers={"User-Agent": "Mozilla/5.0"}
            )

            soup = BeautifulSoup(res.text, "lxml")

            table = soup.find("table", class_="type_1")
            if not table:
                continue

            for row in table.find_all("tr"):

                cols = row.find_all("td")

                if len(cols) != 11:
                    continue

                date_td = row.find("td", class_="date2")
                if not date_td:
                    continue

                date_raw = date_td.get_text(strip=True)

                try:
                    dt = datetime.strptime(date_raw, "%y.%m.%d")
                except:
                    continue

                def parse_num(txt):
                    txt = txt.replace(",", "").strip()
                    if not txt or txt == "-":
                        return 0
                    return int(txt)

                all_data.append([
                    dt.strftime("%Y-%m-%d 00:00:00"),
                    parse_num(cols[1].text),
                    parse_num(cols[2].text),
                    parse_num(cols[3].text),
                    parse_num(cols[4].text),
                    parse_num(cols[5].text),
                    parse_num(cols[6].text),
                    parse_num(cols[7].text),
                    parse_num(cols[8].text),
                    parse_num(cols[9].text),
                    parse_num(cols[10].text)
                ])

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame(all_data, columns=self.FIELDS)
        df = df.sort_values("base_date", ascending=False)

        return df.head(1).copy()
        # return df


    # ------------------------------------------------------------
    # 실행
    # ------------------------------------------------------------
    def execute(self):

        with open(self.CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)

        pages_to_fetch = config.get("pages_to_fetch", 1)
        print(f"[INFO] pages_to_fetch={pages_to_fetch}")

        df = self.read_investor_flow(pages_to_fetch)

        if df.empty:
            print("[WARN] 수집 데이터 없음")
            return

        from BATCH_CODE.InvestorTrend.txt_writer import write_rows

        rows = df.values.tolist()

        path = write_rows(
            file_prefix="INVESTOR_TREND_DAILY",
            headers=self.FIELDS,
            rows=rows
        )

        print(f"[OK] ROWCOUNT={len(df)}")
        print(f"[OK] OUTPUT={path}")


if __name__ == "__main__":
    batch = InvestorFlowBatch()
    batch.execute()