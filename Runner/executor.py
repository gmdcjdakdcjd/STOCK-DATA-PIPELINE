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
    Python handler 파일을 subprocess로 실행한다.

    handler_path:
        실행할 Python 배치 스크립트의 절대 경로

    처리 흐름:
        1. handler 파일 존재 여부 검증
        2. python interpreter로 handler 실행
        3. 실행 로그(stdout)를 실시간으로 수집
        4. 실행 종료 후 exit code 반환
    """

    # handler 경로 검증
    if not handler_path:
        raise ValueError("handler_path is empty")

    if not os.path.exists(handler_path):
        raise FileNotFoundError(f"handler file not found: {handler_path}")

    # 실행 명령 구성 (python handler.py)
    cmd = [sys.executable, handler_path]

    log.info("Handler start: %s", handler_path)

    # subprocess로 Python handler 실행
    process = subprocess.Popen(
        cmd,
        cwd=PROJECT_ROOT,              # 프로젝트 루트 기준 실행
        stdout=subprocess.PIPE,        # 실행 로그 캡처
        stderr=subprocess.STDOUT,      # stderr를 stdout으로 통합
        text=True                      # 문자열 모드
    )

    # 실행 로그를 실시간으로 출력
    for line in process.stdout:
        log.info(line.rstrip())

    # 프로세스 종료 대기 및 exit code 수집
    exit_code = process.wait()
    log.info("Handler end: %s (exit_code=%s)", handler_path, exit_code)

    # 실행 결과 코드 반환
    return exit_code
