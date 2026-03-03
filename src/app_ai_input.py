from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ui.ai_input_streamlit import run


def _is_running_under_streamlit() -> bool:
    """判断当前是否在 Streamlit server 上下文中运行。"""
    if os.environ.get("STREAMLIT_SERVER_PORT"):
        return True
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        return get_script_run_ctx() is not None
    except Exception:
        return False


if _is_running_under_streamlit():
    # Streamlit Cloud 或本地 streamlit run 时直接执行页面
    run()
elif __name__ == "__main__":
    # 直接 python 启动时自动拉起 streamlit
    target = PROJECT_ROOT / "src" / "ui" / "ai_input_streamlit.py"
    cmd = [sys.executable, "-m", "streamlit", "run", str(target)]
    raise SystemExit(subprocess.call(cmd, cwd=str(PROJECT_ROOT)))
