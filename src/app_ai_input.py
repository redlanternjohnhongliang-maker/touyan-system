from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ui.ai_input_streamlit import run


if __name__ == "__main__":
    # 直接 python 启动时自动拉起 streamlit；若已在 streamlit 运行上下文中则直接执行页面函数
    if os.environ.get("STREAMLIT_SERVER_PORT"):
        run()
    else:
        target = PROJECT_ROOT / "src" / "ui" / "ai_input_streamlit.py"
        cmd = [sys.executable, "-m", "streamlit", "run", str(target)]
        raise SystemExit(subprocess.call(cmd, cwd=str(PROJECT_ROOT)))
