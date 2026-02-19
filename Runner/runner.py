import time
import os
import logging
import logging.config
from datetime import datetime
from pathlib import Path

import yaml
from dotenv import load_dotenv

from db import get_conn
from executor import run_handler
from logger import BatchOutLogger

# ======================================================
# ENV 로딩 (Local / Docker 분기)
# ======================================================
BASE_DIR = Path(__file__).resolve().parents[1]

# 로컬 실행 시만 .env.local 로딩
if os.getenv("APP_ENV") != "docker":
    load_dotenv(BASE_DIR / ".env.local")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_DIR = os.getenv("LOG_DIR", "./python_logs")

Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

# ======================================================
# Logging 설정
# ======================================================
LOG_CONFIG_PATH = Path(__file__).parent / "logging.yaml"

with open(LOG_CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Python에서 직접 env 반영
config["handlers"]["console"]["level"] = LOG_LEVEL
config["handlers"]["file"]["level"] = LOG_LEVEL
config["handlers"]["file"]["filename"] = f"{LOG_DIR}/python-runner.log"

config["root"]["level"] = LOG_LEVEL

for name in ["runner", "executor", "handler"]:
    if name in config.get("loggers", {}):
        config["loggers"][name]["level"] = LOG_LEVEL

logging.config.dictConfig(config)
log = logging.getLogger("runner")

# ======================================================
# Runner 설정
# ======================================================
POLL_INTERVAL = 3  # seconds

# ======================================================
# DB Access Functions
# ======================================================
def fetch_one_waiting(conn):
    sql = """
    SELECT wait_id, job_code, batch_out_id
    FROM stock_job_queue
    WHERE status = 'W'
    ORDER BY requested_at
    LIMIT 1
    FOR UPDATE
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()


def fetch_handler_path(conn, job_code):
    sql = """
    SELECT handler_name
    FROM stock_job_info
    WHERE job_code = %s
      AND use_yn = 'Y'
    """
    with conn.cursor() as cur:
        cur.execute(sql, (job_code,))
        row = cur.fetchone()
        return row[0] if row else None


def mark_running(conn, wait_id):
    sql = """
    UPDATE stock_job_queue
    SET status = 'R',
        started_at = NOW()
    WHERE wait_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (wait_id,))


def delete_queue(conn, wait_id):
    sql = "DELETE FROM stock_job_queue WHERE wait_id = %s"
    with conn.cursor() as cur:
        cur.execute(sql, (wait_id,))


# ======================================================
# Core Logic
# ======================================================
def process_once():
    conn = get_conn()
    try:
        job = fetch_one_waiting(conn)
        if not job:
            conn.commit()
            return

        wait_id, job_code, batch_out_id = job

        handler_path = fetch_handler_path(conn, job_code)
        if not handler_path:
            raise RuntimeError(f"handler not found for job_code={job_code}")

        # 상태 R로 변경 후 즉시 커밋 (락 최소화)
        mark_running(conn, wait_id)
        conn.commit()

        batch_logger = BatchOutLogger(conn)

        log.info("Job start: job_code=%s handler=%s", job_code, handler_path)

        start = datetime.now()
        exit_code = run_handler(handler_path)
        end = datetime.now()

        status = "SUCCESS" if exit_code == 0 else "FAIL"
        message = "NO_ERROR" if exit_code == 0 else f"EXIT_CODE={exit_code}"

        # 실행 이력 기록 (DB 전용 로거)
        batch_logger.log(
            job_id=batch_out_id,
            job_name=job_code,        # 🔑 job_code == job_name
            job_info=handler_path,
            start_time=start,
            end_time=end,
            status=status,
            message=message
        )

        delete_queue(conn, wait_id)
        conn.commit()

        log.info(
            "Job end: job_code=%s status=%s duration_ms=%s",
            job_code,
            status,
            int((end - start).total_seconds() * 1000)
        )

    except Exception:
        conn.rollback()
        log.exception("Runner ERROR")

    finally:
        conn.close()


# ======================================================
# Runner Loop
# ======================================================
def main():
    log.info("Python Runner START")
    while True:
        try:
            process_once()
        except Exception:
            # process_once 외 영역 방어
            log.exception("Unexpected runner failure")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
