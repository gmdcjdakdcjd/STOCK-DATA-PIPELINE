# ============================================================
# TIGER ETF Excel Downloader
# - 미래에셋 TIGER ETF 전종목 PDF Excel 다운로드
# - DB 적재는 하지 않음
# - 저장 경로:
#   MONTHLY_TIGER_ETF_INFO_DIR/TIGER_ETF_INFO.xlsx
# ============================================================

import sys
from pathlib import Path

# ============================================================
# 0. 프로젝트 루트 + import 경로
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# ============================================================
# 1. 기본 import
# ============================================================
import os
import requests
from datetime import datetime

from BATCH_CODE.common import config  # ENV 로딩 트리거


# ============================================================
# 2. ENV
# ============================================================
TIGER_DOWNLOAD_DIR = os.getenv("MONTHLY_TIGER_ETF_INFO_DIR")

if not TIGER_DOWNLOAD_DIR:
    raise RuntimeError("MONTHLY_TIGER_ETF_INFO_DIR not set")


# ============================================================
# 3. TIGER Excel Download URL
# ============================================================
TIGER_EXCEL_URL = (
    "https://investments.miraeasset.com/tigeretf/ko/product/search/downloadPdfExcelTotal.do"
)


# ============================================================
# 4. 공통 함수
# ============================================================
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_download_dir():
    """
    예:
    D:/STOCK_PROJECT_V2/STOCK-DATA-PIPELINE/BATCH_CODE/csvDir/TIGER_ETF_INFO
    """
    os.makedirs(TIGER_DOWNLOAD_DIR, exist_ok=True)
    return TIGER_DOWNLOAD_DIR


def get_download_file_path():
    """
    예:
    D:/STOCK_PROJECT_V2/STOCK-DATA-PIPELINE/BATCH_CODE/csvDir/TIGER_ETF_INFO/TIGER_ETF_INFO.xlsx
    """
    download_dir = ensure_download_dir()
    file_name = "TIGER_ETF_INFO.xls"

    return os.path.join(download_dir, file_name)


def is_html_response(content: bytes) -> bool:
    """
    다운로드 실패 시 error.do HTML 페이지가 내려올 수 있으므로 방어.
    """
    if not content:
        return False

    head = content[:500].lower()

    return b"<html" in head or b"<!doctype html" in head


def is_excel_like_response(content: bytes) -> bool:
    """
    xlsx는 zip 기반이라 보통 PK로 시작함.
    """
    if not content:
        return False

    # xlsx
    if content[:2] == b"PK":
        return True

    # xls 구형 포맷
    if content[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return True

    return False


# ============================================================
# 5. 다운로드
# ============================================================
def download_tiger_excel():
    file_path = get_download_file_path()

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
            "application/vnd.ms-excel,"
            "application/octet-stream,*/*"
        ),
        "Referer": "https://investments.miraeasset.com/tigeretf/ko/reference/list.kr"
    }

    print("============================================================")
    print("[START] TIGER ETF Excel Download")
    print(f"[TIME] {now_str()}")
    print(f"[URL] {TIGER_EXCEL_URL}")
    print(f"[SAVE_PATH] {file_path}")
    print("============================================================")

    response = requests.get(
        TIGER_EXCEL_URL,
        headers=headers,
        timeout=60
    )

    print(f"[HTTP_STATUS] {response.status_code}")
    print(f"[CONTENT_TYPE] {response.headers.get('Content-Type', '')}")
    print(f"[CONTENT_DISPOSITION] {response.headers.get('Content-Disposition', '')}")

    response.raise_for_status()

    if is_html_response(response.content):
        raise RuntimeError(
            "Excel 파일이 아니라 HTML 응답이 내려왔습니다. "
            "URL, Referer, 접근 제한 여부를 확인하세요."
        )

    if not is_excel_like_response(response.content):
        raise RuntimeError(
            "Excel 파일 형식으로 보이지 않는 응답입니다. "
            "응답 헤더와 내용을 확인하세요."
        )

    with open(file_path, "wb") as f:
        f.write(response.content)

    file_size = os.path.getsize(file_path)

    if file_size <= 0:
        raise RuntimeError(f"다운로드 파일 크기가 0입니다. file_path={file_path}")

    print("[OK] 다운로드 완료")
    print(f"[FILE] {file_path}")
    print(f"[SIZE] {file_size:,} bytes")
    print("============================================================")

    return file_path


# ============================================================
# 6. MAIN
# ============================================================
def run():
    return download_tiger_excel()


if __name__ == "__main__":
    run()