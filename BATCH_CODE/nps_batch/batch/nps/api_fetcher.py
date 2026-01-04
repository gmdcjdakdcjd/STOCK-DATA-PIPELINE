import os
import requests

# =====================================================
# ENV
# - common.config / settings.py 에서 이미 load + 검증 완료됨
# =====================================================
NPS_API_BASE_URL = os.getenv("NPS_API_BASE_URL")
NPS_API_KEY = os.getenv("NPS_API_KEY")
# ❗ 여기서 RuntimeError 절대 발생시키지 말 것


def fetch_all(api_path, per_page=100):
    """
    odcloud 공공데이터 API 전체 페이징 조회
    - totalCount 기준 종료
    """

    page = 1
    results = []
    total_count = None

    print(f"[API] START fetch → {api_path}")

    while True:
        url = f"{NPS_API_BASE_URL}/{api_path}"
        params = {
            "page": page,
            "perPage": per_page,
            "serviceKey": NPS_API_KEY
        }

        try:
            res = requests.get(url, params=params, timeout=10)
            res.raise_for_status()
        except Exception as e:
            print(f"[ERROR] API request failed (page={page}) → {e}")
            break

        json_data = res.json()
        data = json_data.get("data", [])

        # 최초 1회 totalCount 확인
        if total_count is None:
            total_count = json_data.get("totalCount", 0)
            print(f"[API] totalCount = {total_count}")

            if total_count == 0:
                print("[WARN] totalCount is 0, abort fetch")
                break

        if not data:
            print(f"[WARN] empty data at page={page}, stop")
            break

        results.extend(data)

        print(
            f"[API] page={page} "
            f"fetched={len(data)} "
            f"cumulative={len(results)}/{total_count}"
        )

        if len(results) >= total_count:
            print("[API] reached totalCount, stop")
            break

        page += 1

    print(f"[API] END fetch → total fetched = {len(results)}")
    return results
