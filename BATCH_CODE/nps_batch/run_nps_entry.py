import os
import sys
import subprocess
from pathlib import Path
from BATCH_CODE.common.config import ENV

if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[2]

    # ❌ dotenv 로드 절대 금지 (config.py에서만 한다)

    cmd = [
        sys.executable,
        "-m",
        "BATCH_CODE.nps_batch.batch.nps.run_nps_batch"
    ]

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=os.environ   # ← Java에서 받은 APP_ENV 그대로 전달
    )

    sys.exit(result.returncode)
