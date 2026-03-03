"""璋冭瘯鎯呯华寮曟搸锛氭鏌ユ暟鎹祦鍚勬楠?""
import sys, os
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.collectors.eastmoney_adapter import fetch_stock_bundle
from src.services.sentiment_engine import (
    _extract_date_from_record, _build_text_for_record, _classify_text
)
from datetime import date, timedelta

bundle = fetch_stock_bundle("600519", mode="quick")
reports = bundle.get("research_report", [])
print(f"鐮旀姤鎬绘暟: {len(reports)}")

if reports:
    # 鐪嬪墠3鏉″師濮嬫暟鎹?
    print("\n=== 鍓?鏉″師濮嬭褰曠殑 keys ===")
    for i, r in enumerate(reports[:3]):
        print(f"  [{i}] keys: {list(r.keys())}")
        print(f"       閮ㄥ垎鍊? 鎶ュ憡鍚嶇О={r.get('鎶ュ憡鍚嶇О','N/A')[:30]}, 涓滆储璇勭骇={r.get('涓滆储璇勭骇','N/A')}, 鏃ユ湡={r.get('鏃ユ湡','N/A')}")

    # 鏃ユ湡鎻愬彇娴嬭瘯
    print("\n=== 鏃ユ湡鎻愬彇 ===")
    dated, undated = 0, 0
    date_samples = []
    for r in reports:
        d = _extract_date_from_record(r)
        if d:
            dated += 1
            if len(date_samples) < 5:
                date_samples.append(d)
        else:
            undated += 1
    print(f"  鏈夋棩鏈? {dated}, 鏃犳棩鏈? {undated}")
    print(f"  鏃ユ湡鏍锋湰: {date_samples}")

    # 鐪嬬獥鍙ｅ唴鏈夊灏?
    today = date.today()
    window_start = today - timedelta(days=5)
    in_window = sum(1 for r in reports
                    if (_extract_date_from_record(r) or "") >= window_start.strftime("%Y-%m-%d"))
    print(f"  5鏃ョ獥鍙ｅ唴: {in_window}")

    # 鏂囨湰鍒嗙被娴嬭瘯
    print("\n=== 鏂囨湰鍒嗙被 ===")
    sentiments = {"bullish": 0, "bearish": 0, "neutral": 0}
    for r in reports[:20]:
        text = _build_text_for_record(r)
        sent, conf = _classify_text(text)
        sentiments[sent] += 1
    print(f"  鍓?0鏉″垎绫? {sentiments}")
    # 鐪嬪嚑涓緥瀛?
    for r in reports[:3]:
        text = _build_text_for_record(r)
        sent, conf = _classify_text(text)
        print(f"  text={text[:60]}... => {sent} ({conf:.2f})")

