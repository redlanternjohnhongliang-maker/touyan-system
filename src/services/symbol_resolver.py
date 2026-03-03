from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
import os
import re
from typing import Any

import requests


SEARCH_TOKEN = "D43BF722C8E33BDC906FB84D85E326E8"
SEARCH_URL = "https://searchapi.eastmoney.com/api/suggest/get"

# 全局 Session — 复用 TCP/TLS 连接，大幅减少后续请求延迟
_session = requests.Session()
_session.verify = False


@contextmanager
def _without_proxy_env() -> Any:
    keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ]
    old = {k: os.environ.get(k) for k in keys}
    try:
        for k in keys:
            os.environ.pop(k, None)
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _normalize_code(value: str) -> str:
    raw = str(value or "").strip().upper()
    raw = raw.replace("SH", "").replace("SZ", "").replace("BJ", "")
    raw = raw.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return ""
    return digits[-6:].zfill(6)


def _looks_like_code(value: str) -> bool:
    code = _normalize_code(value)
    return bool(code and re.fullmatch(r"\d{6}", code))


@lru_cache(maxsize=256)
def _query_eastmoney_candidates(keyword: str) -> list[dict[str, Any]]:
    with _without_proxy_env():
        resp = _session.get(
            SEARCH_URL,
            params={
                "input": keyword,
                "type": "14",
                "token": SEARCH_TOKEN,
                "count": "20",
            },
            timeout=5,
        )
        resp.raise_for_status()
        payload = resp.json()
    data = ((payload.get("QuotationCodeTable") or {}).get("Data")) or []
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def warmup_search_api() -> None:
    """在后台预热搜索 API 连接（DNS + TLS 握手），消除首次输入卡顿。"""
    try:
        _query_eastmoney_candidates("000001")
    except Exception:
        pass


def search_stock_suggestions(keyword: str, limit: int = 12) -> list[dict[str, str]]:
    raw = str(keyword or "").strip()
    if not raw:
        return []

    rows = _query_eastmoney_candidates(raw)
    raw_upper = raw.upper()
    a_share: list[dict[str, Any]] = []
    for row in rows:
        cls = str(row.get("Classify", ""))
        sec_name = str(row.get("SecurityTypeName", ""))
        if cls == "AStock" or sec_name in {"沪A", "深A", "京A"}:
            code = _normalize_code(str(row.get("Code", "")))
            if not code:
                continue
            name = str(row.get("Name", "")).strip()
            pinyin = str(row.get("PinYin", "")).strip().upper()
            market = str(row.get("SecurityTypeName", "")).strip()
            a_share.append(
                {
                    "code": code,
                    "name": name,
                    "pinyin": pinyin,
                    "market": market,
                }
            )

    def _score(item: dict[str, str]) -> tuple[int, int, int, str]:
        code = str(item.get("code", ""))
        name = str(item.get("name", ""))
        pinyin = str(item.get("pinyin", ""))
        score = 0
        if code.startswith(raw):
            score += 60
        if name == raw:
            score += 100
        elif raw in name:
            score += 45
        if pinyin == raw_upper:
            score += 90
        elif pinyin.startswith(raw_upper):
            score += 55
        elif raw_upper in pinyin:
            score += 30
        return (-score, len(name), len(code), code)

    uniq: dict[str, dict[str, str]] = {}
    for row in a_share:
        code = row.get("code", "")
        if code and code not in uniq:
            uniq[code] = row

    ordered = sorted(uniq.values(), key=_score)
    return ordered[: max(1, int(limit))]


def resolve_stock_input(value: str) -> dict[str, Any]:
    raw = str(value or "").strip()
    if not raw:
        return {"ok": False, "input": raw, "message": "empty input"}

    if _looks_like_code(raw):
        code = _normalize_code(raw)
        # 尝试通过东方财富搜索接口反查股票名称
        name = ""
        try:
            candidates = _query_eastmoney_candidates(code)
            for c in candidates:
                c_code = _normalize_code(str(c.get("Code", "")))
                if c_code == code:
                    name = str(c.get("Name", "")).strip()
                    break
        except Exception:
            pass
        return {
            "ok": True,
            "input": raw,
            "code": code,
            "name": name,
            "matched_by": "code",
        }

    rows = _query_eastmoney_candidates(raw)
    a_share = []
    for row in rows:
        cls = str(row.get("Classify", ""))
        sec_name = str(row.get("SecurityTypeName", ""))
        if cls == "AStock" or sec_name in {"沪A", "深A", "京A"}:
            a_share.append(row)

    if not a_share:
        return {
            "ok": False,
            "input": raw,
            "message": f"无法解析股票名称/代码: {raw}",
            "candidates": rows[:5],
        }

    raw_upper = raw.upper()
    exact_name = [r for r in a_share if str(r.get("Name", "")).strip() == raw]
    exact_pinyin = [r for r in a_share if str(r.get("PinYin", "")).strip().upper() == raw_upper]
    exact_code = [r for r in a_share if _normalize_code(str(r.get("Code", ""))) == _normalize_code(raw)]
    pick = (exact_name or exact_pinyin or exact_code or a_share)[0]
    code = _normalize_code(str(pick.get("Code", "")))
    if not code:
        return {"ok": False, "input": raw, "message": f"解析失败: {raw}"}

    return {
        "ok": True,
        "input": raw,
        "code": code,
        "name": str(pick.get("Name", "")).strip(),
        "quote_id": str(pick.get("QuoteID", "")).strip(),
        "matched_by": "name_or_pinyin",
    }


def resolve_stock_code(value: str) -> str:
    result = resolve_stock_input(value)
    if not result.get("ok"):
        raise ValueError(str(result.get("message", "resolve failed")))
    return str(result.get("code", "")).strip()

