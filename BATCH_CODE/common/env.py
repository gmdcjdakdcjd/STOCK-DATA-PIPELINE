from pathlib import Path
from dotenv import load_dotenv
import os


def load_env():
    """
    프로젝트 루트 기준으로 환경변수 로딩
    - local  → .env.local
    - docker → .env.docker
    """
    base_dir = Path(__file__).resolve().parents[2]

    env = os.getenv("APP_ENV", "local")
    env_file = ".env.docker" if env == "docker" else ".env.local"
    env_path = base_dir / env_file

    if not env_path.exists():
        raise RuntimeError(f"{env_file} not found: {env_path}")

    load_dotenv(env_path)
    return base_dir
