from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
TARGET = PROJECT_ROOT / "src" / "ui" / "ai_input_streamlit.py"

if __name__ == "__main__":
    cmd = [sys.executable, "-m", "streamlit", "run", str(TARGET)]
    raise SystemExit(subprocess.call(cmd, cwd=str(PROJECT_ROOT)))
