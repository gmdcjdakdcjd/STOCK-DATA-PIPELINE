# common/config.py
import os
from dotenv import load_dotenv


BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)

print("[CONFIG] BASE_DIR =", BASE_DIR)

ENV = os.getenv("APP_ENV", "local")

if ENV == "docker":
    load_dotenv(os.path.join(BASE_DIR, ".env.docker"))
else:
    load_dotenv(os.path.join(BASE_DIR, ".env.local"))


def get_sqlalchemy_db_url():
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    if not DB_PORT or DB_PORT.lower() == "none":
        raise RuntimeError(f"DB_PORT invalid: {DB_PORT}")

    return (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )
