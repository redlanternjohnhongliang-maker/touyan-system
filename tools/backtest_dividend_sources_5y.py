from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.dividend_yield_service import backtest_ready_yield_coverage


def main() -> None:
    parser = argparse.ArgumentParser(description="鍥炴祴杩?骞磋偂鎭巼澶氭簮瑕嗙洊涓庡樊寮?)
    parser.add_argument("--symbol", required=True, help="鑲＄エ浠ｇ爜锛屽 600278")
    parser.add_argument("--years", type=int, default=5, help="鍥炴祴骞存暟锛岄粯璁?")
    parser.add_argument("--out", default="tools/dividend_sources_backtest_latest.json", help="杈撳嚭JSON璺緞")
    args = parser.parse_args()

    result = backtest_ready_yield_coverage(symbol=args.symbol, years=max(1, int(args.years)))

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(out_path)


if __name__ == "__main__":
    main()

