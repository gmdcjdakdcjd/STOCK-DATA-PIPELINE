import os
from pathlib import Path
from dotenv import load_dotenv

# settings.py 기준으로 프로젝트 루트 (STOCK-DATA-PIPELINE)
BASE_DIR = Path(__file__).resolve().parents[3]

# 환경 구분 (local / docker / prod ...)
APP_ENV = os.getenv("APP_ENV", "local")

# .env 파일 선택
ENV_FILE = BASE_DIR / f".env.{APP_ENV}"

# dotenv 로딩
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
else:
    raise RuntimeError(f"환경 파일 없음: {ENV_FILE}")

# 환경변수 로딩
NPS_API_BASE_URL = os.getenv("NPS_API_BASE_URL")
NPS_API_KEY = os.getenv("NPS_API_KEY")

# 검증
if not NPS_API_BASE_URL or not NPS_API_KEY:
    raise RuntimeError(
        f"NPS API 환경변수 누락 "
        f"(APP_ENV={APP_ENV}, ENV_FILE={ENV_FILE})"
    )
