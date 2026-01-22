from datetime import datetime
from typing import Optional


class BatchOutLogger:
    """
    batch_out_history 전용 로거
    - 실행 결과 기록만 담당
    - commit/rollback은 호출자(runner)가 책임진다
    """

    def __init__(self, conn):
        self.conn = conn

    def log(
        self,
        job_id: int,
        job_name: Optional[str],
        job_info: Optional[str],
        start_time: datetime,
        end_time: datetime,
        status: str,
        message: str
    ):
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        sql = """
        INSERT INTO batch_out_h
        (
            job_id,
            job_name,
            job_info,
            exec_start_time,
            exec_end_time,
            exec_status,
            exec_message,
            exec_date,
            duration_ms
        )
        VALUES
        (
            %s, %s, %s,
            %s, %s,
            %s, %s,
            CURDATE(),
            %s
        )
        """

        with self.conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    job_id,
                    job_name,
                    job_info,
                    start_time,
                    end_time,
                    status,
                    message,
                    duration_ms
                )
            )
