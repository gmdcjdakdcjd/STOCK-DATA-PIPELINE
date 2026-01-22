import subprocess
import os
import sys
import logging

# logger 선언 (logging.yaml 설정을 그대로 사용)
log = logging.getLogger("handler")

# runner/ 디렉터리 기준으로 프로젝트 루트 계산
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run_handler(handler_path: str) -> int:
    """
    handler_path:
      - python 파일 절대 경로
    """

    if not handler_path:
        raise ValueError("handler_path is empty")

    if not os.path.exists(handler_path):
        raise FileNotFoundError(f"handler file not found: {handler_path}")

    cmd = [sys.executable, handler_path]

    log.info("Handler start: %s", handler_path)

    process = subprocess.Popen(
        cmd,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    for line in process.stdout:
        log.info(line.rstrip())

    exit_code = process.wait()
    log.info("Handler end: %s (exit_code=%s)", handler_path, exit_code)

    return exit_code
