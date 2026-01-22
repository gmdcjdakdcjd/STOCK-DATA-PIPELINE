import os
import pymysql
from dotenv import load_dotenv

# 환경 구분
APP_ENV = os.getenv("APP_ENV", "local")

if APP_ENV == "docker":
    load_dotenv(".env.docker")
else:
    load_dotenv(".env.local")


def get_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        charset="utf8mb4",
        autocommit=False
    )
