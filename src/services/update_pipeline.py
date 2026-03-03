from __future__ import annotations

from src.collectors.eastmoney_adapter import fetch_stock_bundle
from src.storage.repository import save_stock_snapshot


def manual_update_stock_data(db_path: str, symbol: str, mode: str = "quick") -> dict:
    bundle = fetch_stock_bundle(symbol=symbol, mode=mode)
    save_stock_snapshot(db_path=db_path, symbol=symbol, payload=bundle)
    return bundle
