"""Non-interactive deep check 鈥?no tqdm, immediate flush."""
from __future__ import annotations
import os, sys, time
from pathlib import Path

# Suppress tqdm inside akshare
os.environ["TQDM_DISABLE"] = "1"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def log(msg: str):
    print(msg, flush=True)

log("=== 寮€濮?deep 鑷  symbol=600519 ===")
t0 = time.time()

from src.collectors.eastmoney_adapter import fetch_stock_bundle

bundle = fetch_stock_bundle("600519", mode="deep")

elapsed = time.time() - t0
log(f"fetch_stock_bundle 瀹屾垚, 鑰楁椂 {elapsed:.1f}s\n")

log("=== 鎺ュ彛璇婃柇 ===")
for item in bundle.get("_diagnostics", []):
    endpoint = item.get("endpoint", "unknown")
    ok = item.get("ok", False)
    rows = item.get("rows", 0)
    ms = item.get("duration_ms", 0)
    msg = str(item.get("error", "") or "")
    if ok:
        if rows == 0 and msg:
            log(f"[WARN] {endpoint:36} rows=0    time={ms}ms  note={msg[:120]}")
        else:
            log(f"[OK]   {endpoint:36} rows={rows:<5} time={ms}ms")
    else:
        log(f"[ERR]  {endpoint:36} rows=0    time={ms}ms  err={msg[:180]}")

log("\n=== 鏁版嵁瑕嗙洊 ===")
keys = ["zygc","news","yjbb","research_report","notice",
        "financial_indicator","gdhs","hist","ggcg"]
for k in keys:
    log(f"  {k:22}: {len(bundle.get(k, []))}")

if bundle.get("_errors"):
    log("\n=== 閿欒鎽樿 ===")
    for m in bundle["_errors"]:
        log(f"  - {m}")

log("\n=== 瀹屾垚 ===")

