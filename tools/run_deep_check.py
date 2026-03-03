"""Non-interactive deep check for 600519."""
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.collectors.eastmoney_adapter import fetch_stock_bundle

symbol = "600519"
mode = "deep"
print(f"\n寮€濮嬭嚜妫€: symbol={symbol}, mode={mode}")
bundle = fetch_stock_bundle(symbol, mode=mode)

print("\n=== 鎺ュ彛璇婃柇 ===")
for item in bundle.get("_diagnostics", []):
    endpoint = item.get("endpoint", "unknown")
    ok = item.get("ok", False)
    rows = item.get("rows", 0)
    duration_ms = item.get("duration_ms", 0)
    msg = str(item.get("error", "") or "")
    if ok:
        if rows == 0 and msg:
            print(f"[WARN]{endpoint:36} rows=0    time={duration_ms}ms note={msg[:120]}")
        else:
            print(f"[OK]  {endpoint:36} rows={rows:<4} time={duration_ms}ms")
    else:
        err = msg[:180]
        print(f"[ERR] {endpoint:36} rows=0    time={duration_ms}ms err={err}")

print("\n=== 鏁版嵁瑕嗙洊 ===")
keys = ["zygc", "news", "yjbb", "research_report", "notice", "financial_indicator", "gdhs", "hist", "ggcg"]
for key in keys:
    print(f"{key:20}: {len(bundle.get(key, []))}")

if bundle.get("_errors"):
    print("\n=== 閿欒鎽樿 ===")
    for message in bundle["_errors"]:
        print("-", message)

