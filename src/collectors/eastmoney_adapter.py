from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from contextlib import contextmanager
import copy
from datetime import date, timedelta
import logging
import os
import re
import threading
import time
from typing import Any
import requests
from bs4 import BeautifulSoup
from src.services.archive_retention import archive_market_hot_news

logger = logging.getLogger(__name__)


_BUNDLE_CACHE_LOCK = threading.Lock()
_BUNDLE_CACHE: dict[tuple[str, str, str], tuple[float, dict[str, Any]]] = {}
_HOTNEWS_CACHE_LOCK = threading.Lock()
_HOTNEWS_CACHE: dict[tuple[int, str], tuple[float, list[dict[str, Any]]]] = {}
_NOTICE_CONTENT_CACHE_LOCK = threading.Lock()
_NOTICE_CONTENT_CACHE: dict[str, tuple[float, str]] = {}


def _pick_first_value(item: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        val = str(item.get(key, "")).strip()
        if val:
            return val
    return ""


def _normalize_news_title_for_dedupe(title: str) -> str:
    raw = str(title or "").strip().lower()
    if not raw:
        return ""
    compact = "".join(ch for ch in raw if ch.isalnum())
    return compact


def _merge_and_dedupe_hot_news(
    eastmoney_rows: list[dict[str, Any]],
    ths_rows: list[dict[str, Any]],
    max_total: int = 10,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for row in [*eastmoney_rows, *ths_rows]:
        title = str(row.get("标题", "")).strip()
        link = str(row.get("链接", "")).strip()
        key = _normalize_news_title_for_dedupe(title) or link
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        merged.append(row)
        if len(merged) >= max_total:
            break
    return merged


def _today_key() -> str:
    return date.today().isoformat()


def _bundle_cache_key(symbol: str, mode: str, as_of_date: date | None = None) -> tuple[str, str, str]:
    date_key = as_of_date.isoformat() if as_of_date else _today_key()
    return (_normalize_symbol_text(symbol), (mode or "quick").strip().lower(), date_key)


def _bundle_cache_ttl_sec() -> int:
    try:
        return max(0, int(os.getenv("EASTMONEY_BUNDLE_CACHE_TTL", "240")))
    except Exception:
        return 240


def _hotnews_cache_ttl_sec() -> int:
    try:
        return max(0, int(os.getenv("EASTMONEY_HOTNEWS_CACHE_TTL", "180")))
    except Exception:
        return 180


def _cache_get_bundle(key: tuple[str, str, str]) -> dict[str, Any] | None:
    ttl = _bundle_cache_ttl_sec()
    if ttl <= 0:
        return None
    now = time.time()
    with _BUNDLE_CACHE_LOCK:
        item = _BUNDLE_CACHE.get(key)
        if not item:
            return None
        ts, payload = item
        if now - ts > ttl:
            _BUNDLE_CACHE.pop(key, None)
            return None
        return copy.deepcopy(payload)


def _cache_set_bundle(key: tuple[str, str, str], payload: dict[str, Any]) -> None:
    ttl = _bundle_cache_ttl_sec()
    if ttl <= 0:
        return
    with _BUNDLE_CACHE_LOCK:
        _BUNDLE_CACHE[key] = (time.time(), copy.deepcopy(payload))


def _cache_get_hotnews(key: tuple[int, str]) -> list[dict[str, Any]] | None:
    ttl = _hotnews_cache_ttl_sec()
    if ttl <= 0:
        return None
    now = time.time()
    with _HOTNEWS_CACHE_LOCK:
        item = _HOTNEWS_CACHE.get(key)
        if not item:
            return None
        ts, records = item
        if now - ts > ttl:
            _HOTNEWS_CACHE.pop(key, None)
            return None
        return copy.deepcopy(records)


def _cache_set_hotnews(key: tuple[int, str], records: list[dict[str, Any]]) -> None:
    ttl = _hotnews_cache_ttl_sec()
    if ttl <= 0:
        return
    with _HOTNEWS_CACHE_LOCK:
        _HOTNEWS_CACHE[key] = (time.time(), copy.deepcopy(records))


def _cache_get_notice_content(key: str, ttl_sec: int = 24 * 3600) -> str | None:
    if not key:
        return None
    now = time.time()
    with _NOTICE_CONTENT_CACHE_LOCK:
        item = _NOTICE_CONTENT_CACHE.get(key)
        if not item:
            return None
        ts, text = item
        if now - ts > max(60, ttl_sec):
            _NOTICE_CONTENT_CACHE.pop(key, None)
            return None
        return text


def _cache_set_notice_content(key: str, text: str) -> None:
    if not key or not text:
        return
    with _NOTICE_CONTENT_CACHE_LOCK:
        _NOTICE_CONTENT_CACHE[key] = (time.time(), text)


def _symbol_with_market_prefix(symbol: str) -> str:
    code = symbol.strip()
    if code.startswith(("SH", "SZ", "BJ")):
        return code
    if code.startswith(("6", "9")):
        return f"SH{code}"
    if code.startswith(("0", "3")):
        return f"SZ{code}"
    if code.startswith(("8", "4")):
        return f"BJ{code}"
    return code


def _symbol_with_dot_market(symbol: str) -> str:
    code = symbol.strip()
    if "." in code:
        return code
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "3")):
        return f"{code}.SZ"
    if code.startswith(("8", "4")):
        return f"{code}.BJ"
    return code


def _symbol_for_sina(symbol: str) -> str:
    code = symbol.strip()
    if code.lower().startswith(("sh", "sz", "bj")):
        return code.lower()
    if code.startswith(("6", "9")):
        return f"sh{code}"
    if code.startswith(("0", "3")):
        return f"sz{code}"
    if code.startswith(("8", "4")):
        return f"bj{code}"
    return code.lower()


def _to_records(frame: Any, limit: int = 80) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if not hasattr(frame, "to_dict"):
        return []
    safe_frame = frame.head(limit).copy()
    return safe_frame.fillna("").to_dict(orient="records")


def _normalize_symbol_text(value: Any) -> str:
    raw = str(value).strip().upper()
    if not raw:
        return ""
    raw = raw.replace("SH", "").replace("SZ", "").replace("BJ", "")
    raw = raw.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) <= 6 and digits:
        return digits.zfill(6)
    return digits or raw


def _filter_by_symbol(records: list[dict[str, Any]], symbol: str) -> list[dict[str, Any]]:
    normalized_target = _normalize_symbol_text(symbol)
    code_keys = ["股票代码", "代码", "symbol", "证券代码"]
    filtered: list[dict[str, Any]] = []
    for item in records:
        dynamic_keys = [k for k in item.keys() if "代码" in str(k)]
        keys_to_check = list(dict.fromkeys(code_keys + dynamic_keys))
        matched = False
        for key in keys_to_check:
            value = _normalize_symbol_text(item.get(key, ""))
            if value and value == normalized_target:
                matched = True
                break
        if matched:
            filtered.append(item)
    return filtered


def _recent_quarter_ends(max_items: int = 4, ref_date: date | None = None) -> list[str]:
    anchor = ref_date or date.today()
    quarter_dates: list[date] = []
    y = anchor.year
    candidates = [date(y, 3, 31), date(y, 6, 30), date(y, 9, 30), date(y, 12, 31)]
    for d in sorted(candidates, reverse=True):
        if d <= anchor:
            quarter_dates.append(d)
    year_cursor = y - 1
    while len(quarter_dates) < max_items:
        quarter_dates.extend(
            [date(year_cursor, 12, 31), date(year_cursor, 9, 30), date(year_cursor, 6, 30), date(year_cursor, 3, 31)]
        )
        year_cursor -= 1
    return [d.strftime("%Y%m%d") for d in quarter_dates[:max_items]]


def _recent_dates(days: int = 5, ref_date: date | None = None) -> list[str]:
    base = ref_date or date.today()
    return [(base - timedelta(days=offset)).strftime("%Y%m%d") for offset in range(days)]


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    match = re.search(r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})", raw)
    if not match:
        match = re.search(r"(\d{4})(\d{2})(\d{2})", raw)
    if not match:
        return None
    try:
        y = int(match.group(1))
        m = int(match.group(2))
        d = int(match.group(3))
        return date(y, m, d)
    except Exception:
        return None


def _sort_records_by_date(
    records: list[dict[str, Any]],
    date_keys: list[str],
) -> tuple[list[tuple[date, dict[str, Any]]], list[dict[str, Any]]]:
    with_date: list[tuple[date, dict[str, Any]]] = []
    no_date: list[dict[str, Any]] = []
    for rec in records:
        rec_date: date | None = None
        for key in date_keys:
            rec_date = _parse_date(rec.get(key, ""))
            if rec_date:
                break
        if rec_date:
            with_date.append((rec_date, rec))
        else:
            no_date.append(rec)
    with_date.sort(key=lambda x: x[0], reverse=True)
    return with_date, no_date


def _keep_latest_period_records(
    records: list[dict[str, Any]],
    date_keys: list[str],
    max_items: int = 2,
) -> list[dict[str, Any]]:
    if not records:
        return []
    with_date, no_date = _sort_records_by_date(records, date_keys)
    chosen = [rec for _, rec in with_date[:max_items]]
    if len(chosen) < max_items and no_date:
        chosen.extend(no_date[: max_items - len(chosen)])
    return chosen[:max_items]


def _keep_recent_records(
    records: list[dict[str, Any]],
    date_keys: list[str],
    max_days: int,
    max_items: int,
    ref_date: date | None = None,
    max_future_days: int = 0,
) -> list[dict[str, Any]]:
    if not records:
        return []
    anchor = ref_date or date.today()
    with_date, no_date = _sort_records_by_date(records, date_keys)
    recent = [
        rec
        for rec_date, rec in with_date
        if (-max_future_days) <= (anchor - rec_date).days <= max_days
    ]
    if recent:
        return recent[:max_items]
    fallback = [rec for _, rec in with_date[:max_items]]
    if len(fallback) < max_items and no_date:
        fallback.extend(no_date[: max_items - len(fallback)])
    return fallback[:max_items]


def _infer_date_keys(records: list[dict[str, Any]], top_k: int = 5) -> list[str]:
    if not records:
        return []
    key_score: dict[str, int] = {}
    sample = records[:50]
    for rec in sample:
        for key, value in rec.items():
            if _parse_date(value):
                key_score[key] = key_score.get(key, 0) + 1
            else:
                key_score.setdefault(key, 0)
    ranked = [k for k, v in sorted(key_score.items(), key=lambda x: x[1], reverse=True) if v > 0]
    preferred = ["REPORT_DATE", "NOTICE_DATE", "UPDATE_DATE", "report_date", "publish_date", "date"]
    ordered: list[str] = []
    for key in preferred:
        if key in ranked:
            ordered.append(key)
    for key in ranked:
        if key not in ordered:
            ordered.append(key)
    return ordered[:top_k]


def _keep_recent_zygc_records(
    records: list[dict[str, Any]],
    max_days: int = 365,
    max_items: int = 80,
    ref_date: date | None = None,
) -> list[dict[str, Any]]:
    if not records:
        return []
    date_keys = _infer_date_keys(records, top_k=5)
    if not date_keys:
        return records[:max_items]
    anchor = ref_date or date.today()
    with_date, no_date = _sort_records_by_date(records, date_keys)
    recent = [rec for rec_date, rec in with_date if 0 <= (anchor - rec_date).days <= max_days]
    if recent:
        return recent[:max_items]
    if with_date:
        latest_period = with_date[0][0]
        latest_records = [rec for rec_date, rec in with_date if rec_date == latest_period]
        if latest_records:
            return latest_records[:max_items]
    fallback = [rec for _, rec in with_date[:max_items]]
    if len(fallback) < max_items and no_date:
        fallback.extend(no_date[: max_items - len(fallback)])
    return fallback[:max_items]


def _keep_recent_hist_records(
    records: list[dict[str, Any]],
    max_days: int = 60,
    max_items: int = 60,
    ref_date: date | None = None,
) -> list[dict[str, Any]]:
    if not records:
        return []
    anchor = ref_date or date.today()
    with_date: list[tuple[date, dict[str, Any]]] = []
    for rec in records:
        rec_date = (
            _parse_date(rec.get("date", ""))
            or _parse_date(rec.get("日期", ""))
            or _parse_date(rec.get("交易日期", ""))
        )
        if rec_date:
            with_date.append((rec_date, rec))
    if not with_date:
        return records[-max_items:]
    recent = [
        (rec_date, rec)
        for rec_date, rec in with_date
        if 0 <= (anchor - rec_date).days <= max_days
    ]
    source = recent if recent else with_date[-max_items:]
    source.sort(key=lambda x: x[0])
    return [rec for _, rec in source][-max_items:]


def _extract_stock_name_candidates(bundle: dict[str, Any]) -> set[str]:
    name_keys = [
        "股票简称",
        "证券简称",
        "名称",
        "SECURITY_NAME_ABBR",
        "name",
    ]
    names: set[str] = set()
    for section in ("yjbb", "financial_indicator", "gdhs", "notice", "news"):
        for rec in bundle.get(section, [])[:30]:
            if not isinstance(rec, dict):
                continue
            for key in name_keys:
                val = str(rec.get(key, "")).strip()
                if not val or val.lower() == "none":
                    continue
                if any(ch.isdigit() for ch in val):
                    continue
                if len(val) > 20:
                    continue
                names.add(val)
    return names


def _filter_news_by_relevance(
    records: list[dict[str, Any]],
    symbol: str,
    stock_names: set[str],
) -> list[dict[str, Any]]:
    if not records:
        return []
    normalized_symbol = _normalize_symbol_text(symbol)
    raw_symbol = symbol.strip().upper()
    filtered: list[dict[str, Any]] = []
    for rec in records:
        title = str(rec.get("新闻标题", rec.get("title", "")))
        content = str(rec.get("新闻内容", rec.get("content", "")))
        keyword = str(rec.get("关键词", ""))

        title_upper = title.upper()
        text = f"{title}\n{content}\n{keyword}"

        code_hit = bool(
            (normalized_symbol and normalized_symbol in title_upper)
            or (raw_symbol and raw_symbol in title_upper)
        )
        name_hit = any(name in text for name in stock_names)
        if code_hit or name_hit:
            filtered.append(rec)
    return filtered


def _update_diagnostic_rows(bundle: dict[str, Any], endpoint: str, rows: int, note: str = "") -> None:
    for item in bundle.get("_diagnostics", []):
        if item.get("endpoint") == endpoint:
            item["rows"] = rows
            if note:
                prev = str(item.get("error", "") or "")
                item["error"] = f"{prev} | {note}".strip(" |")
            break


def _is_restructuring_notice(title: str, category: str) -> bool:
    text = f"{title} {category}".lower()
    keywords = [
        "重组",
        "并购",
        "资产注入",
        "资产置换",
        "收购",
        "借壳",
        "重大资产",
        "定增",
    ]
    return any(k in text for k in keywords)


def _fetch_notice_by_symbol_eastmoney(
    symbol: str,
    max_items: int = 120,
    max_days: int = 180,
    max_pages: int = 6,
    ref_date: date | None = None,
) -> list[dict[str, Any]]:
    url = "https://np-anotice-stock.eastmoney.com/api/security/ann"
    normalized = _normalize_symbol_text(symbol)
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    today = ref_date or date.today()

    with _without_proxy_env():
        for page in range(1, max_pages + 1):
            try:
                resp = requests.get(
                    url,
                    params={
                        "page_size": 30,
                        "page_index": page,
                        "ann_type": "A",
                        "client_source": "web",
                        "stock_list": normalized,
                        "f_node": "0",
                        "s_node": "0",
                    },
                    timeout=10,
                )
                resp.raise_for_status()
                payload = resp.json()
            except Exception:
                break

            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            rows = data.get("list", []) if isinstance(data, dict) else []
            if not rows:
                break

            for item in rows:
                art_code = str(item.get("art_code", "")).strip()
                if not art_code or art_code in seen:
                    continue
                seen.add(art_code)

                code_info = (item.get("codes") or [{}])[0]
                stock_code = _normalize_symbol_text(code_info.get("stock_code", ""))
                if stock_code and stock_code != normalized:
                    continue

                notice_date_raw = item.get("notice_date", "")
                notice_date = _parse_date(notice_date_raw)
                display_time_raw = str(item.get("display_time", "") or "").strip()
                display_date = _parse_date(display_time_raw)
                effective_date = display_date or notice_date
                if effective_date and (today - effective_date).days > max_days:
                    continue

                col_list = item.get("columns") or []
                col_names = "、".join(str(c.get("column_name", "")) for c in col_list if isinstance(c, dict))
                title = str(item.get("title", "") or item.get("title_ch", "")).strip()
                is_restruct = _is_restructuring_notice(title, col_names)

                out.append(
                    {
                        "代码": stock_code or normalized,
                        "简称": str(code_info.get("short_name", "")),
                        "公告日期": effective_date.isoformat() if effective_date else str(notice_date_raw),
                        "公告自然日": notice_date.isoformat() if notice_date else str(notice_date_raw),
                        "公告发布时间": display_time_raw,
                        "公告标题": title,
                        "公告分类": col_names,
                        "公告链接": f"https://data.eastmoney.com/notices/detail/{stock_code or normalized}/{art_code}.html",
                        "公告来源": "eastmoney_notice_center",
                        "重组相关": is_restruct,
                        "art_code": art_code,
                    }
                )
                if len(out) >= max_items:
                    break
            if len(out) >= max_items:
                break

    out.sort(key=lambda x: str(x.get("公告日期", "")), reverse=True)
    return out[:max_items]


def _merge_notice_records(
    base_records: list[dict[str, Any]],
    extra_records: list[dict[str, Any]],
    max_items: int = 120,
) -> list[dict[str, Any]]:
    """合并公告记录并去重：优先保留 base，并用 extra 补齐缺失字段。"""
    if not base_records and not extra_records:
        return []

    def _record_date_text(item: dict[str, Any]) -> str:
        for key in ("公告日期", "最新公告日期", "NOTICE_DATE", "REPORT_DATE", "日期"):
            val = str(item.get(key, "")).strip()
            if val:
                return val
        return ""

    def _record_key(item: dict[str, Any]) -> str:
        art_code = str(item.get("art_code", "") or item.get("公告编码", "")).strip()
        if art_code:
            return f"art:{art_code}"
        title = str(item.get("公告标题", "") or item.get("title", "")).strip().lower()
        date_text = _record_date_text(item)
        return f"title:{title}|date:{date_text}"

    merged_map: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    for src in (base_records or []):
        if not isinstance(src, dict):
            continue
        key = _record_key(src)
        if key not in merged_map:
            merged_map[key] = copy.deepcopy(src)
            order.append(key)

    for src in (extra_records or []):
        if not isinstance(src, dict):
            continue
        key = _record_key(src)
        if key not in merged_map:
            merged_map[key] = copy.deepcopy(src)
            order.append(key)
            continue
        current = merged_map[key]
        for k, v in src.items():
            if current.get(k) in (None, "", [], {}) and v not in (None, "", [], {}):
                current[k] = v

    merged = [merged_map[k] for k in order]

    def _sort_key(item: dict[str, Any]) -> tuple[date, str]:
        d = (
            _parse_date(item.get("公告日期", ""))
            or _parse_date(item.get("最新公告日期", ""))
            or _parse_date(item.get("NOTICE_DATE", ""))
            or _parse_date(item.get("REPORT_DATE", ""))
            or _parse_date(item.get("日期", ""))
        )
        return (d or date.min, str(item.get("公告标题", "")))

    merged.sort(key=_sort_key, reverse=True)
    return merged[:max_items]


def _extract_text_from_notice_html(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    selectors = [
        ".detail-content",
        ".article-content",
        "#ContentBody",
        ".newsContent",
        ".content",
    ]
    for sel in selectors:
        node = soup.select_one(sel)
        if not node:
            continue
        for bad in node.select("script, style, .statement, .share, .advertisement"):
            bad.decompose()
        text = node.get_text("\n", strip=True)
        if len(text) >= 120:
            return text
    lines: list[str] = []
    for p in soup.select("p"):
        t = p.get_text(" ", strip=True)
        if len(t) >= 16:
            lines.append(t)
    return "\n".join(lines)


def _fetch_notice_main_content(art_code: str, link: str) -> str:
    cache_key = (art_code or "").strip() or (link or "").strip()
    cached = _cache_get_notice_content(cache_key)
    if cached is not None:
        return cached

    text = ""
    if art_code:
        with _without_proxy_env():
            try:
                resp = requests.get(
                    "https://np-cnotice-stock.eastmoney.com/api/content/ann",
                    params={
                        "art_code": art_code,
                        "client_source": "web",
                        "page_index": "1",
                    },
                    timeout=8,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                resp.raise_for_status()
                payload = resp.json()
                data = payload.get("data", {}) if isinstance(payload, dict) else {}
                notice_content = str(data.get("notice_content", "") or "").strip()
                if notice_content:
                    text = _extract_text_from_notice_html(notice_content)
            except Exception:
                text = ""

    if not text and link:
        with _without_proxy_env():
            try:
                resp = requests.get(link, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
                resp.raise_for_status()
                text = _extract_text_from_notice_html(resp.text)
            except Exception:
                text = ""

    text = (text or "").strip()
    if len(text) > 12000:
        text = text[:12000]
    if text:
        _cache_set_notice_content(cache_key, text)
    return text


def _enrich_recent_notice_with_content(
    records: list[dict[str, Any]],
    max_days: int = 30,
    max_items: int = 30,
    ref_date: date | None = None,
) -> list[dict[str, Any]]:
    if not records:
        return []

    recent_records = _keep_recent_records(
        records,
        date_keys=["公告日期", "最新公告日期", "NOTICE_DATE", "REPORT_DATE", "日期"],
        max_days=max_days,
        max_items=max_items,
        ref_date=ref_date,
        max_future_days=1,
    )
    if not recent_records:
        return []

    out = [copy.deepcopy(item) for item in recent_records]
    max_workers = min(4, max(1, len(out)))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {}
        for idx, item in enumerate(out):
            art_code = str(item.get("art_code", "") or item.get("公告编码", "")).strip()
            link = str(item.get("公告链接", "") or item.get("链接", "")).strip()
            future_map[pool.submit(_fetch_notice_main_content, art_code, link)] = idx
        for fut in as_completed(future_map):
            idx = future_map[fut]
            content = ""
            try:
                content = (fut.result() or "").strip()
            except Exception:
                content = ""
            if content:
                out[idx]["公告主要内容"] = content
                out[idx]["公告内容长度"] = len(content)
    return out


def _fetch_eastmoney_hot_news(max_items: int = 5) -> list[dict[str, Any]]:
    url = "https://np-listapi.eastmoney.com/comm/web/getNewsByColumns"
    req_trace = f"codex-{int(time.time() * 1000)}"
    with _without_proxy_env():
        try:
            resp = requests.get(
                url,
                params={
                    "client": "web",
                    "biz": "web_news_col",
                    "column": "345",
                    "order": "1",
                    "needInteractData": "0",
                    "page_index": "1",
                    "page_size": str(max(5, min(max_items, 20))),
                    "req_trace": req_trace,
                },
                timeout=10,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception:
            return []
    rows = ((payload.get("data") or {}).get("list")) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []

    def _extract_news_body_text_from_html(html: str) -> str:
        soup = BeautifulSoup(html or "", "lxml")

        # 常见财经正文容器优先
        selectors = [
            "#ContentBody",
            ".Body",
            ".newsContent",
            ".article-body",
            ".article-content",
            ".content",
        ]
        for sel in selectors:
            node = soup.select_one(sel)
            if not node:
                continue
            for bad in node.select("script, style, .statement, .advertisement, .share, .pagination"):
                bad.decompose()
            text = node.get_text("\n", strip=True)
            if len(text) >= 80:
                return text

        # 回退：抓取较长段落
        lines: list[str] = []
        for p in soup.select("p"):
            t = p.get_text(" ", strip=True)
            if len(t) >= 16:
                lines.append(t)
        if lines:
            return "\n".join(lines)
        return ""

    def _fetch_news_body_text(url: str) -> str:
        link = str(url or "").strip()
        if not link:
            return ""
        with _without_proxy_env():
            try:
                resp = requests.get(link, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
                resp.raise_for_status()
            except Exception:
                return ""
        text = _extract_news_body_text_from_html(resp.text)
        if len(text) > 12000:
            return text[:12000]
        return text

    out: list[dict[str, Any]] = []
    for row in rows[:max_items]:
        if not isinstance(row, dict):
            continue
        link = str(row.get("url", "") or row.get("uniqueUrl", "")).strip()
        summary = str(row.get("summary", "")).strip()
        out.append(
            {
                "时间": str(row.get("showTime", "")).strip(),
                "标题": str(row.get("title", "")).strip(),
                "来源": str(row.get("mediaName", "")).strip(),
                "链接": link,
                "摘要": summary,
                "正文": "",
                "code": str(row.get("code", "")).strip(),
            }
        )

    # 并发抓正文，避免逐条串行导致要闻模块耗时过长
    max_workers = min(6, max(1, len(out)))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(_fetch_news_body_text, item.get("链接", "")): idx
            for idx, item in enumerate(out)
        }
        for fut in as_completed(future_map):
            idx = future_map[fut]
            body_text = ""
            try:
                body_text = fut.result()
            except Exception:
                body_text = ""
            summary = str(out[idx].get("摘要", "")).strip()
            out[idx]["正文"] = body_text or summary

    return out[:max_items]


def _fetch_tonghuashun_hot_news(max_items: int = 5) -> list[dict[str, Any]]:
    try:
        import akshare as ak
    except Exception:
        return []

    try:
        frame = _safe_ak_call(lambda: ak.stock_info_global_ths(), timeout_sec=10)
        records = _to_records(frame, limit=200)
    except Exception:
        return []

    out: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        title = _pick_first_value(item, ["标题", "news", "新闻标题", "title"])
        summary = _pick_first_value(item, ["内容", "摘要", "新闻内容", "content"])
        pub_time = _pick_first_value(item, ["发布时间", "时间", "日期", "datetime", "time"])
        link = _pick_first_value(item, ["链接", "url", "新闻链接", "详情链接"])
        if not title:
            continue
        out.append(
            {
                "时间": pub_time,
                "标题": title,
                "来源": "同花顺",
                "链接": link,
                "摘要": summary,
                "正文": summary or title,
                "code": "",
            }
        )
        if len(out) >= max_items:
            break
    return out


def _fetch_market_hot_news_merged(
    max_items_per_source: int = 5,
    max_total: int = 10,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cache_key = (max_items_per_source, f"merged:{max_total}:{_today_key()}")
    cached = _cache_get_hotnews(cache_key)
    if cached is not None:
        return cached, {
            "eastmoney_rows": 0,
            "ths_rows": 0,
            "merged_rows": len(cached),
            "cached": True,
        }

    eastmoney_rows: list[dict[str, Any]] = []
    ths_rows: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=2) as pool:
        future_eastmoney = pool.submit(_fetch_eastmoney_hot_news, max_items_per_source)
        future_ths = pool.submit(_fetch_tonghuashun_hot_news, max_items_per_source)
        try:
            eastmoney_rows = future_eastmoney.result(timeout=16)
        except Exception:
            eastmoney_rows = []
        try:
            ths_rows = future_ths.result(timeout=16)
        except Exception:
            ths_rows = []

    merged_rows = _merge_and_dedupe_hot_news(eastmoney_rows, ths_rows, max_total=max_total)
    _cache_set_hotnews(cache_key, merged_rows)
    return merged_rows, {
        "eastmoney_rows": len(eastmoney_rows),
        "ths_rows": len(ths_rows),
        "merged_rows": len(merged_rows),
        "cached": False,
    }


def fetch_market_headlines(
    max_items_per_source: int = 5,
    max_total: int = 10,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """公开接口：获取东财+同花顺合并要闻（带内存缓存）。"""
    return _fetch_market_hot_news_merged(
        max_items_per_source=max_items_per_source,
        max_total=max_total,
    )


def _run_with_timeout(func: Any, timeout_sec: int = 8) -> Any:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(func)
    try:
        return future.result(timeout=timeout_sec)
    except FuturesTimeoutError as exc:
        future.cancel()
        raise TimeoutError(f"请求超时({timeout_sec}s)") from exc
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


@contextmanager
def _without_proxy_env() -> Any:
    proxy_keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ]
    old_values = {k: os.environ.get(k) for k in proxy_keys}
    try:
        for key in proxy_keys:
            os.environ.pop(key, None)
        yield
    finally:
        for key, value in old_values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _safe_ak_call(func: Any, timeout_sec: int = 8) -> Any:
    with _without_proxy_env():
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"
        return _run_with_timeout(func, timeout_sec=timeout_sec)


def _run_endpoint(
    bundle: dict[str, Any],
    endpoint_name: str,
    action: Any,
    target_key: str,
    limit: int,
    symbol_filter: str | None = None,
) -> None:
    start = time.perf_counter()
    try:
        frame = action()
        records = _to_records(frame, limit=limit)
        if symbol_filter:
            records = _filter_by_symbol(records, symbol_filter)
        bundle[target_key] = records
        bundle["_diagnostics"].append(
            {
                "endpoint": endpoint_name,
                "ok": True,
                "rows": len(records),
                "duration_ms": int((time.perf_counter() - start) * 1000),
            }
        )
    except Exception as exc:
        message = f"{endpoint_name}失败: {exc}"
        bundle["_errors"].append(message)
        bundle["_diagnostics"].append(
            {
                "endpoint": endpoint_name,
                "ok": False,
                "rows": 0,
                "duration_ms": int((time.perf_counter() - start) * 1000),
                "error": str(exc),
            }
        )


# ---------------------------------------------------------------------------
# 高管增减持 — 多源采集 (3 源自动 fallback)
# ---------------------------------------------------------------------------

def _fetch_ggcg_via_f10(symbol: str) -> list[dict[str, Any]]:
    """源A: 东方财富 f10 高管持股变动 (cgbd)，按个股直接查询，速度快。"""
    prefix = "SH" if symbol.startswith("6") else "SZ" if symbol.startswith(("0", "3")) else "BJ"
    url = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanyManagement/PageAjax"
    params = {"code": f"{prefix}{symbol}"}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    with _without_proxy_env():
        os.environ["NO_PROXY"] = "*"
        resp = requests.get(url, params=params, headers=headers, timeout=15, verify=False)
    data = resp.json()
    cgbd = data.get("cgbd", [])
    if not isinstance(cgbd, list):
        return []
    # 统一字段名为中文，与原 stock_ggcg_em 输出兼容
    records: list[dict[str, Any]] = []
    for row in cgbd[:120]:
        records.append({
            "股票代码": row.get("SECURITY_CODE", symbol),
            "股票简称": row.get("SECURITY_NAME_ABBR", ""),
            "变动日期": str(row.get("END_DATE", ""))[:10],
            "变动人": row.get("HOLDER_NAME", ""),
            "变动数量": row.get("CHANGE_NUM", ""),
            "成交均价": row.get("AVERAGE_PRICE", ""),
            "变动后持股数": row.get("CHANGE_AFTER_HOLDNUM", ""),
            "与高管关系": row.get("EXECUTIVE_RELATION", ""),
            "董监高人员姓名": row.get("EXECUTIVE_NAME", ""),
            "职务": row.get("POSITION", ""),
            "变动途径": row.get("TRADE_WAY", ""),
            "_source": "eastmoney_f10_cgbd",
        })
    return records


def _fetch_ggcg_via_ths(symbol: str, ak_module: Any) -> list[dict[str, Any]]:
    """源B: 同花顺 akshare stock_management_change_ths，按个股查询。"""
    frame = _safe_ak_call(
        lambda: ak_module.stock_management_change_ths(symbol=symbol),
        timeout_sec=20,
    )
    if frame is None or not hasattr(frame, "to_dict"):
        return []
    raw = frame.head(120).fillna("").to_dict(orient="records")
    records: list[dict[str, Any]] = []
    for row in raw:
        records.append({
            "股票代码": symbol,
            "股票简称": "",
            "变动日期": str(row.get("变动日期", ""))[:10],
            "变动人": row.get("变动人", ""),
            "变动数量": str(row.get("变动数量", "")),
            "成交均价": str(row.get("交易均价", "")),
            "变动后持股数": str(row.get("剩余股数", "")),
            "与高管关系": row.get("与公司高管关系", ""),
            "董监高人员姓名": row.get("变动人", ""),
            "职务": row.get("与公司高管关系", ""),
            "变动途径": row.get("股份变动途径", ""),
            "_source": "ths_management_change",
        })
    return records


def _fetch_ggcg_via_em_full(symbol: str, ak_module: Any) -> list[dict[str, Any]]:
    """源C: 原 stock_ggcg_em(symbol='全部') 全量下载后过滤，较慢但兼容性最好。"""
    frame = _safe_ak_call(
        lambda: ak_module.stock_ggcg_em(symbol="全部"),
        timeout_sec=30,
    )
    records = _filter_by_symbol(_to_records(frame, limit=50000), symbol=symbol)[:120]
    # 添加来源标记
    for rec in records:
        rec["_source"] = "stock_ggcg_em"
    return records


def _fetch_ggcg_multi_source(
    symbol: str, ak_module: Any
) -> tuple[list[dict[str, Any]], str, str]:
    """
    按优先级尝试3个数据源获取高管增减持数据。

    返回: (records, source_name, error_msg)
    - records: 标准化后的记录列表
    - source_name: 成功的源名称 (空字符串=全部失败)
    - error_msg: 全部失败时的汇总错误信息
    """
    errors: list[str] = []

    # ── 源A: 东方财富 f10 cgbd (快速，按个股) ──
    try:
        records = _fetch_ggcg_via_f10(symbol)
        return records, "eastmoney_f10_cgbd", ""
    except Exception as exc:
        errors.append(f"f10_cgbd: {exc}")

    # ── 源B: 同花顺 akshare (按个股) ──
    try:
        records = _fetch_ggcg_via_ths(symbol, ak_module)
        return records, "ths_management_change", ""
    except Exception as exc:
        errors.append(f"ths: {exc}")

    # ── 源C: 原 stock_ggcg_em 全量 (最慢) ──
    try:
        records = _fetch_ggcg_via_em_full(symbol, ak_module)
        return records, "stock_ggcg_em", ""
    except Exception as exc:
        errors.append(f"ggcg_em: {exc}")

    return [], "", " | ".join(errors)


# ---------------------------------------------------------------------------
# 概念题材标签 + 题材亮点 + 公司基本信息 — 东财 datacenter 直读接口
# ---------------------------------------------------------------------------

_DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_DC_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _dc_query(report_name: str, symbol: str, columns: str = "ALL", page_size: int = 50) -> list[dict[str, Any]]:
    """datacenter 通用查询封装，返回 data 列表。"""
    params = {
        "reportName": report_name,
        "columns": columns,
        "filter": f'(SECURITY_CODE="{symbol}")',
        "pageSize": page_size,
        "pageNumber": 1,
    }
    try:
        with _without_proxy_env():
            os.environ["NO_PROXY"] = "*"
            resp = requests.get(_DATACENTER_URL, params=params, headers=_DC_HEADERS, timeout=15, verify=False)
        if resp.status_code != 200:
            logger.debug("datacenter %s HTTP%s %s", report_name, resp.status_code, symbol)
            return []
        data = resp.json()
        result = data.get("result")
        if not result or not isinstance(result, dict):
            return []
        rows = result.get("data", [])
        return rows if isinstance(rows, list) else []
    except Exception as e:
        logger.debug("datacenter %s 失败 %s: %s", report_name, symbol, e)
        return []


def _dc_query_ext(
    report_name: str, filter_expr: str, columns: str = "ALL",
    page_size: int = 50, sort_columns: str = "", sort_types: str = "-1",
) -> list[dict[str, Any]]:
    """datacenter 扩展查询（自定义 filter / sort）。"""
    params: dict[str, Any] = {
        "reportName": report_name,
        "columns": columns,
        "filter": filter_expr,
        "pageSize": page_size,
        "pageNumber": 1,
        "source": "WEB",
        "client": "WEB",
    }
    if sort_columns:
        params["sortColumns"] = sort_columns
        params["sortTypes"] = sort_types
    try:
        with _without_proxy_env():
            os.environ["NO_PROXY"] = "*"
            resp = requests.get(_DATACENTER_URL, params=params, headers=_DC_HEADERS, timeout=15, verify=False)
        if resp.status_code != 200:
            return []
        data = resp.json()
        result = data.get("result")
        if not result or not isinstance(result, dict):
            return []
        rows = result.get("data", [])
        return rows if isinstance(rows, list) else []
    except Exception as e:
        logger.debug("dc_query_ext %s failed: %s", report_name, e)
        return []


# ---------------------------------------------------------------------------
# 业绩报表 datacenter 降级 — RPT_LICO_FN_CPD
# ---------------------------------------------------------------------------

def _fetch_yjbb_datacenter(symbol: str, max_items: int = 5) -> list[dict[str, Any]]:
    """通过 datacenter RPT_LICO_FN_CPD 获取业绩概要（akshare stock_yjbb_em 的降级源）。

    返回与 akshare 字段名一致的 dict 列表，方便后续统一处理。
    """
    code = _normalize_symbol_text(symbol)
    rows = _dc_query_ext(
        "RPT_LICO_FN_CPD",
        f'(SECURITY_CODE="{code}")',
        columns="ALL",
        page_size=max_items,
        sort_columns="REPORTDATE",
        sort_types="-1",
    )
    if not rows:
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({
            "股票代码": str(r.get("SECURITY_CODE", code)),
            "股票简称": str(r.get("SECURITY_NAME_ABBR", "")),
            "REPORTDATE": str(r.get("REPORTDATE", ""))[:10],
            "最新公告日期": str(r.get("UPDATE_DATE", ""))[:10],
            "每股收益": _safe_float(str(r.get("BASIC_EPS", 0))),
            "营业收入-营业收入": _safe_float(str(r.get("TOTAL_OPERATE_INCOME", 0))),
            "营业收入-同比增长": _safe_float(str(r.get("YSTZ", 0))),
            "营业收入-季度环比增长": _safe_float(str(r.get("YSHZ", 0))),
            "净利润-净利润": _safe_float(str(r.get("PARENT_NETPROFIT", 0))),
            "净利润-同比增长": _safe_float(str(r.get("SJLTZ", 0))),
            "净利润-季度环比增长": _safe_float(str(r.get("SJLHZ", 0))),
            "每股净资产": _safe_float(str(r.get("BPS", 0))),
            "净资产收益率": _safe_float(str(r.get("WEIGHTAVG_ROE", 0))),
            "每股经营现金流量": _safe_float(str(r.get("MGJYXJJE", 0))),
            "销售毛利率": _safe_float(str(r.get("XSMLL", 0))),
            "_source": "datacenter_RPT_LICO_FN_CPD",
        })
    return out


# ---------------------------------------------------------------------------
# 财务指标 datacenter 降级 — RPT_F10_FINANCE_MAINFINADATA
# ---------------------------------------------------------------------------

def _fetch_financial_indicator_datacenter(symbol: str, max_items: int = 5) -> list[dict[str, Any]]:
    """通过 datacenter RPT_F10_FINANCE_MAINFINADATA 获取主要财务指标
    （akshare stock_financial_analysis_indicator_em 的降级源）。
    """
    code = _normalize_symbol_text(symbol)
    rows = _dc_query_ext(
        "RPT_F10_FINANCE_MAINFINADATA",
        f'(SECURITY_CODE="{code}")',
        columns="ALL",
        page_size=max_items,
        sort_columns="REPORT_DATE",
        sort_types="-1",
    )
    if not rows:
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({
            "SECURITY_CODE": str(r.get("SECURITY_CODE", code)),
            "SECURITY_NAME_ABBR": str(r.get("SECURITY_NAME_ABBR", "")),
            "REPORT_DATE": str(r.get("REPORT_DATE", ""))[:10],
            "REPORT_DATE_NAME": str(r.get("REPORT_DATE_NAME", "")),
            "基本每股收益": _safe_float(str(r.get("EPSJB", 0))),
            "扣非每股收益": _safe_float(str(r.get("EPSKCJB", 0))),
            "每股净资产": _safe_float(str(r.get("BPS", 0))),
            "每股经营现金流量": _safe_float(str(r.get("MGJYXJJE", 0))),
            "每股未分配利润": _safe_float(str(r.get("MGWFPLR", 0))),
            "营业总收入": _safe_float(str(r.get("TOTALOPERATEREVE", 0))),
            "毛利润": _safe_float(str(r.get("MLR", 0))),
            "归属净利润": _safe_float(str(r.get("PARENTNETPROFIT", 0))),
            "扣非净利润": _safe_float(str(r.get("KCFJCXSYJLR", 0))),
            "营业总收入同比增长": _safe_float(str(r.get("TOTALOPERATEREVETZ", 0))),
            "归属净利润同比增长": _safe_float(str(r.get("PARENTNETPROFITTZ", 0))),
            "扣非净利润同比增长": _safe_float(str(r.get("KCFJCXSYJLRTZ", 0))),
            "加权净资产收益率": _safe_float(str(r.get("ROEJQ", 0))),
            "总资产净利率": _safe_float(str(r.get("ZZCJLL", 0))),
            "销售净利率": _safe_float(str(r.get("XSJLL", 0))),
            "销售毛利率": _safe_float(str(r.get("XSMLL", 0))),
            "资产负债率": _safe_float(str(r.get("ZCFZL", 0))),
            "流动比率": _safe_float(str(r.get("LD", 0))),
            "速动比率": _safe_float(str(r.get("SD", 0))),
            "_source": "datacenter_RPT_F10_FINANCE_MAINFINADATA",
        })
    return out


# ---------------------------------------------------------------------------
# 股东户数 datacenter 降级 — RPT_F10_EH_HOLDERSNUM
# ---------------------------------------------------------------------------

def _fetch_gdhs_datacenter(symbol: str, max_items: int = 5) -> list[dict[str, Any]]:
    """通过 datacenter 获取股东户数变化（akshare stock_zh_a_gdhs 的降级源）。"""
    code = _normalize_symbol_text(symbol)
    rows = _dc_query_ext(
        "RPT_HOLDERNUM_DET",
        f'(SECURITY_CODE="{code}")',
        columns="ALL",
        page_size=max_items,
        sort_columns="END_DATE",
        sort_types="-1",
    )
    if not rows:
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({
            "股票代码": str(r.get("SECURITY_CODE", code)),
            "股票简称": str(r.get("SECURITY_NAME_ABBR", "")),
            "截止日期": str(r.get("END_DATE", ""))[:10],
            "股东户数": int(float(r.get("HOLDER_NUM", 0) or 0)),
            "较上期变化": _safe_float(str(r.get("HOLDER_NUM_CHANGE", 0) or 0)),
            "较上期变化比率": _safe_float(str(r.get("HOLDER_NUM_RATIO", 0) or 0)),
            "户均持股数量": _safe_float(str(r.get("AVG_HOLD_NUM", 0) or 0)),
            "户均持股市值": _safe_float(str(r.get("AVG_MARKET_CAP", 0) or 0)),
            "_source": "datacenter_RPT_HOLDERNUM_DET",
        })
    return out


def _fetch_concept_tags_em(symbol: str) -> list[dict[str, str]]:
    """获取个股所属东方财富概念题材标签（含入选理由）。"""
    rows = _dc_query(
        "RPT_F10_CORETHEME_BOARDTYPE", symbol,
        columns="BOARD_CODE,BOARD_NAME,BOARD_TYPE,SELECTED_BOARD_REASON,IS_PRECISE",
    )
    return [
        {
            "board_code": str(row.get("BOARD_CODE", "")),
            "board_name": str(row.get("BOARD_NAME", "")),
            "board_type": str(row.get("BOARD_TYPE", "") or ""),
            "is_precise": str(row.get("IS_PRECISE", "")),
            "reason": str(row.get("SELECTED_BOARD_REASON", "") or ""),
        }
        for row in rows
        if row.get("BOARD_NAME")
    ]


def _fetch_theme_highlights_em(symbol: str) -> list[dict[str, str]]:
    """获取题材亮点：所属板块、经营范围、主营业务、竞争优势等（RPT_F10_CORETHEME_CONTENT）。"""
    rows = _dc_query("RPT_F10_CORETHEME_CONTENT", symbol)
    return [
        {
            "keyword": str(row.get("KEYWORD", "")),
            "content": str(row.get("MAINPOINT_CONTENT", "") or ""),
        }
        for row in rows
        if row.get("KEYWORD") and row.get("MAINPOINT_CONTENT")
    ]


def _fetch_company_profile_em(symbol: str) -> dict[str, str]:
    """获取公司基本信息：所属行业、概念、主营、简介、行业板块代码（RPT_F10_ORG_BASICINFO）。"""
    rows = _dc_query(
        "RPT_F10_ORG_BASICINFO", symbol,
        columns="SECURITY_CODE,EM2016,BLGAINIAN,MAIN_BUSINESS,ORG_PROFIE,"
                "BOARD_CODE_BK_1LEVEL,BOARD_NAME_1LEVEL,"
                "BOARD_CODE_BK_2LEVEL,BOARD_NAME_2LEVEL,"
                "BOARD_CODE_BK_3LEVEL,BOARD_NAME_3LEVEL",
        page_size=1,
    )
    if not rows:
        return {}
    r = rows[0]
    board_code_l1 = str(r.get("BOARD_CODE_BK_1LEVEL", "") or "")
    board_name_l1 = str(r.get("BOARD_NAME_1LEVEL", "") or "")
    board_code_l2 = str(r.get("BOARD_CODE_BK_2LEVEL", "") or "")
    board_name_l2 = str(r.get("BOARD_NAME_2LEVEL", "") or "")
    board_code_l3 = str(r.get("BOARD_CODE_BK_3LEVEL", "") or "")
    board_name_l3 = str(r.get("BOARD_NAME_3LEVEL", "") or "")

    # 业务归因优先：三级行业(更贴近主营) > 二级行业 > 一级行业
    board_code = board_code_l3 or board_code_l2 or board_code_l1
    board_name = board_name_l3 or board_name_l2 or board_name_l1

    return {
        "industry": str(r.get("EM2016", "") or ""),
        "concepts": str(r.get("BLGAINIAN", "") or ""),
        "main_business": str(r.get("MAIN_BUSINESS", "") or ""),
        "profile": str(r.get("ORG_PROFIE", "") or "").strip(),
        "board_code": board_code,
        "board_name": board_name,
        "board_code_l1": board_code_l1,
        "board_name_l1": board_name_l1,
        "board_code_l2": board_code_l2,
        "board_name_l2": board_name_l2,
        "board_code_l3": board_code_l3,
        "board_name_l3": board_name_l3,
    }


# ---------------------------------------------------------------------------
# 板块 / 基准指数 K 线 — 用于行业强弱对比
# ---------------------------------------------------------------------------

_KLINE_HOSTS = [
    "push2test.eastmoney.com",
    "push2his.eastmoney.com",
    "push2.eastmoney.com",
    "80.push2.eastmoney.com",
    "push2test.eastmoney.com",         # 重试一次当前可用域名
]
_KLINE_PATH = "/api/qt/stock/kline/get"
_BOARD_LIST_PATH = "/api/qt/clist/get"
_OFFICIAL_BOARD_LIST_CACHE: dict[str, Any] = {"expire_ts": 0.0, "rows": []}
_TDX_CACHE: dict[str, Any] = {"expire_ts": 0.0, "reader": None, "hy": None, "zs": None}


def _parse_kline_lines(klines: list[str], days: int) -> list[dict[str, Any]]:
    """将 push2 返回的 kline 字符串列表解析为紧凑 OHLCV 记录。"""
    records: list[dict[str, Any]] = []
    for line in klines:
        parts = line.split(",")
        if len(parts) >= 9:
            records.append({
                "日期": parts[0],
                "开盘": _safe_float(parts[1]),
                "收盘": _safe_float(parts[2]),
                "最高": _safe_float(parts[3]),
                "最低": _safe_float(parts[4]),
                "成交量": int(float(parts[5])) if parts[5] else 0,
                "涨跌幅": 0.0,
            })
    # 某些域名/指数返回字段位可能与股票不同，统一按前收盘重算涨跌幅更稳妥
    for i in range(1, len(records)):
        prev_close = records[i - 1]["收盘"]
        if prev_close:
            records[i]["涨跌幅"] = round((records[i]["收盘"] - prev_close) / prev_close * 100, 4)
    return records[-days:]


def _fetch_kline_by_secid(secid: str, days: int = 90, ref_date: date | None = None) -> list[dict[str, Any]]:
    """调用 EastMoney push2 系列 kline 接口（多域名重试），返回紧凑 OHLCV 记录。

    secid 格式: '1.000300' (沪指/CSI300), '0.399006' (创业板指), '90.BK0473' (行业板块)
    自动在 push2test / push2his / push2 / 80.push2 之间切换，提高可用性。
    """
    ref = ref_date or date.today()
    start_str = (ref - timedelta(days=days + 30)).strftime("%Y%m%d")
    end_str = ref.strftime("%Y%m%d")
    params_base = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "beg": start_str,
        "end": end_str,
        "lmt": str(days + 20),
    }
    params_variants = [
        {**params_base, "ut": "fa5fd1943c7b386f172d6893dbbd1d0c", "_": str(int(time.time() * 1000))},
        dict(params_base),
    ]
    last_err = ""
    for host in _KLINE_HOSTS:
        url = f"https://{host}{_KLINE_PATH}"
        for i, params in enumerate(params_variants, start=1):
            try:
                with _without_proxy_env():
                    os.environ["NO_PROXY"] = "*"
                    resp = requests.get(url, params=params, headers=_DC_HEADERS, timeout=12, verify=False)
                data = resp.json()
                klines = (data.get("data") or {}).get("klines") or []
                if klines:
                    records = _parse_kline_lines(klines, days)
                    if records:
                        logger.debug("kline OK via %s secid=%s rows=%d variant=%d", host, secid, len(records), i)
                        return records
                last_err = f"{host}: 返回0行(v{i})"
            except Exception as e:
                last_err = f"{host}: {type(e).__name__}(v{i})"
                logger.debug("kline_by_secid %s secid=%s variant=%d: %s", host, secid, i, e)
    logger.warning("kline_by_secid 所有域名均失败 secid=%s last=%s", secid, last_err)
    return []


def _normalize_board_text(text: str) -> str:
    s = str(text or "").strip()
    for x in ["Ⅰ", "Ⅱ", "Ⅲ", "IV", "III", "II", "I", "行业", "板块", "（申万）", "(申万)"]:
        s = s.replace(x, "")
    return s.strip()


def _fetch_official_industry_boards() -> list[dict[str, str]]:
    """获取东财官方行业板块列表（可交易K线的BK代码），带短时缓存。"""
    now = time.time()
    if now < float(_OFFICIAL_BOARD_LIST_CACHE.get("expire_ts", 0)):
        rows = _OFFICIAL_BOARD_LIST_CACHE.get("rows", [])
        return rows if isinstance(rows, list) else []

    params = {
        "pn": "1",
        "pz": "1000",
        "po": "1",
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:90 t:2 f:!50",
        "fields": "f12,f14",
    }
    rows: list[dict[str, str]] = []
    for host in _KLINE_HOSTS:
        url = f"https://{host}{_BOARD_LIST_PATH}"
        try:
            with _without_proxy_env():
                os.environ["NO_PROXY"] = "*"
                resp = requests.get(url, params=params, headers=_DC_HEADERS, timeout=12, verify=False)
            data = resp.json()
            diff = ((data.get("data") or {}).get("diff") or [])
            for item in diff:
                code = str(item.get("f12", "") or "").strip()
                name = str(item.get("f14", "") or "").strip()
                if code and name:
                    rows.append({"code": code, "name": name})
            if rows:
                break
        except Exception:
            continue

    _OFFICIAL_BOARD_LIST_CACHE["rows"] = rows
    _OFFICIAL_BOARD_LIST_CACHE["expire_ts"] = now + 3600
    return rows


def _remap_to_official_board(company_profile: dict[str, str]) -> tuple[str, str]:
    """将细分行业名映射到东财可交易官方行业板块（仅官方映射，不合成）。"""
    official_rows = _fetch_official_industry_boards()
    if not official_rows:
        return "", ""

    official_by_name = {r["name"]: r["code"] for r in official_rows if r.get("name") and r.get("code")}

    # 常见细分行业 → 东财可交易行业板块
    keyword_map = [
        ("白酒", "酿酒行业"),
        ("旅游零售", "商业百货"),
        ("商贸零售", "商业百货"),
        ("电动乘用车", "汽车整车"),
        ("乘用车", "汽车整车"),
        ("股份制银行", "银行"),
        ("城商行", "银行"),
        ("农商行", "银行"),
        ("证券", "证券"),
        ("保险", "保险"),
        ("锂电", "电池"),
        ("电池", "电池"),
        ("光伏", "光伏设备"),
        ("家电", "家电行业"),
        ("医药", "医药商业"),
    ]

    text_candidates = [
        company_profile.get("board_name_l3", ""),
        company_profile.get("board_name_l2", ""),
        company_profile.get("board_name_l1", ""),
        company_profile.get("board_name", ""),
    ]
    em_industry = str(company_profile.get("industry", "") or "")
    if em_industry:
        text_candidates.extend([x for x in em_industry.split("-") if x])
    normalized = [_normalize_board_text(x) for x in text_candidates if str(x).strip()]

    # 1) 关键字映射
    joined = " ".join(normalized)
    for kw, target_name in keyword_map:
        if kw in joined and target_name in official_by_name:
            return official_by_name[target_name], target_name

    # 2) 归一化名称包含匹配
    norm_official = [(_normalize_board_text(r["name"]), r["name"], r["code"]) for r in official_rows]
    for cand in normalized:
        for nname, raw_name, code in norm_official:
            if not cand or not nname:
                continue
            if cand == nname or cand in nname or nname in cand:
                return code, raw_name

    return "", ""


def _resolve_tdx_root_dir() -> str:
    """解析通达信根目录（支持环境变量，默认 D:\\tongdaxin）。"""
    candidates = [
        os.getenv("TDX_ROOT", "").strip(),
        os.getenv("TDX_DIR", "").strip(),
        os.getenv("TDX_EXE_PATH", "").strip(),
        r"D:\tongdaxin",
    ]
    for raw in candidates:
        if not raw:
            continue
        p = raw
        if p.lower().endswith(".exe"):
            p = os.path.dirname(p)
        if p and os.path.isdir(p):
            return p
    return ""


def _load_tdx_tables() -> tuple[Any, Any, Any]:
    """加载通达信本地映射表：tdxhy.cfg(个股行业) + tdxzs3.cfg(行业指数代码)。"""
    now = time.time()
    if now < float(_TDX_CACHE.get("expire_ts", 0)) and _TDX_CACHE.get("reader") is not None:
        return _TDX_CACHE.get("reader"), _TDX_CACHE.get("hy"), _TDX_CACHE.get("zs")

    tdx_root = _resolve_tdx_root_dir()
    if not tdx_root:
        return None, None, None

    try:
        from mootdx.reader import Reader
        reader = Reader.factory(market="std", tdxdir=tdx_root)
        hy = reader.block(symbol="tdxhy.cfg")
        zs = reader.block(symbol="tdxzs3.cfg")
        if hy is None or zs is None or len(hy) == 0 or len(zs) == 0:
            return None, None, None

        hy_df = hy.copy()
        hy_df.columns = ["mkt", "stock", "tcode", "c3", "c4", "xcode"]
        hy_df["stock"] = hy_df["stock"].astype(str).str.zfill(6)

        zs_df = zs.copy()
        zs_df.columns = ["name", "index_code", "c2", "c3", "c4", "map_code"]
        zs_df["index_code"] = zs_df["index_code"].astype(str).str.strip()
        zs_df["map_code"] = zs_df["map_code"].astype(str).str.strip()

        _TDX_CACHE["reader"] = reader
        _TDX_CACHE["hy"] = hy_df
        _TDX_CACHE["zs"] = zs_df
        _TDX_CACHE["expire_ts"] = now + 3600
        return reader, hy_df, zs_df
    except Exception as e:
        logger.debug("load tdx tables failed: %s", e)
        return None, None, None


def _map_stock_to_tdx_index(symbol: str) -> tuple[str, str]:
    """个股 -> 通达信行业指数(88xxxx) 映射：优先 X 细分，再 T 精确，再 T 上级。"""
    code = _normalize_symbol_text(symbol)
    reader, hy_df, zs_df = _load_tdx_tables()
    if reader is None or hy_df is None or zs_df is None or not code:
        return "", ""

    row = hy_df[hy_df["stock"] == code]
    if row.empty:
        return "", ""
    rec = row.iloc[0]
    tcode = str(rec.get("tcode", "") or "").strip()
    xcode = str(rec.get("xcode", "") or "").strip()

    candidates: list[str] = []
    if xcode:
        candidates.append(xcode)
    if tcode:
        candidates.append(tcode)
        if len(tcode) >= 5:
            candidates.append(tcode[:5])

    seen: set[str] = set()
    for mk in candidates:
        if not mk or mk in seen:
            continue
        seen.add(mk)
        hit = zs_df[zs_df["map_code"] == mk]
        if hit.empty:
            continue
        for _, z in hit.iterrows():
            idx = str(z.get("index_code", "") or "").strip()
            name = str(z.get("name", "") or "").strip()
            if idx.startswith("88") and len(idx) == 6:
                return idx, name
    return "", ""


def _fetch_tdx_sector_kline_by_stock(symbol: str, days: int = 60, ref_date: date | None = None) -> tuple[list[dict[str, Any]], str]:
    """通达信本地兜底：个股 -> 行业指数代码 -> 日K。"""
    ref = ref_date or date.today()
    reader, _, _ = _load_tdx_tables()
    if reader is None:
        return [], ""
    idx_code, idx_name = _map_stock_to_tdx_index(symbol)
    if not idx_code:
        return [], ""

    try:
        frame = reader.daily(symbol=idx_code)
        if frame is None or (hasattr(frame, "empty") and frame.empty):
            return [], ""
        df = frame.copy().reset_index()
        if "date" not in df.columns:
            return [], ""
        df["date"] = df["date"].astype(str).str[:10]
        ref_str = ref.strftime("%Y-%m-%d")
        df = df[df["date"] <= ref_str]
        if df.empty:
            return [], ""

        records: list[dict[str, Any]] = []
        prev_close = 0.0
        for _, r in df.iterrows():
            close_v = _safe_float(r.get("close", 0))
            pct = 0.0
            if prev_close:
                pct = round((close_v - prev_close) / prev_close * 100, 4)
            prev_close = close_v
            records.append({
                "日期": str(r.get("date", ""))[:10],
                "开盘": _safe_float(r.get("open", 0)),
                "收盘": close_v,
                "最高": _safe_float(r.get("high", 0)),
                "最低": _safe_float(r.get("low", 0)),
                "成交量": int(float(r.get("volume", 0) or 0)),
                "涨跌幅": pct,
            })
        return records[-days:], idx_name
    except Exception as e:
        logger.debug("tdx sector kline failed symbol=%s: %s", symbol, e)
        return [], ""


def _safe_float(s: str) -> float:
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


_TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"


def _fetch_kline_tencent(tencent_code: str, days: int = 60, ref_date: date | None = None) -> list[dict[str, Any]]:
    """腾讯财经 K 线接口（用作 CSI300 降级源）。

    tencent_code 格式: 'sh000300' (沪深 300), 'sz399006' (创业板指)
    """
    ref = ref_date or date.today()
    start_str = (ref - timedelta(days=days + 40)).strftime("%Y-%m-%d")
    params_str = f"{tencent_code},day,{start_str},,{days + 20},qfq"
    try:
        with _without_proxy_env():
            os.environ["NO_PROXY"] = "*"
            resp = requests.get(
                _TENCENT_KLINE_URL,
                params={"param": params_str},
                headers={"User-Agent": _DC_HEADERS.get("User-Agent", "")},
                timeout=15, verify=False,
            )
        body = resp.json()
        # 结构: {"data": {"sh000300": {"day": [[date,open,close,high,low,vol], ...]}}}
        inner = body.get("data", {})
        ticker_data = list(inner.values())[0] if inner else {}
        raw_lines = ticker_data.get("day", []) or ticker_data.get("qfqday", [])
        records: list[dict[str, Any]] = []
        ref_str = ref.strftime("%Y-%m-%d")
        for row in raw_lines:
            if len(row) < 6:
                continue
            d = str(row[0])
            if d > ref_str:
                continue
            o, c, h, lo = _safe_float(row[1]), _safe_float(row[2]), _safe_float(row[3]), _safe_float(row[4])
            vol = int(float(row[5])) if row[5] else 0
            # 计算涨跌幅需前值，后面统一补
            records.append({"日期": d, "开盘": o, "收盘": c, "最高": h, "最低": lo, "成交量": vol, "涨跌幅": 0.0})
        # 补算涨跌幅
        for i in range(1, len(records)):
            prev_close = records[i - 1]["收盘"]
            if prev_close:
                records[i]["涨跌幅"] = round((records[i]["收盘"] - prev_close) / prev_close * 100, 2)
        return records[-days:]
    except Exception as e:
        logger.debug("kline_tencent failed code=%s: %s", tencent_code, e)
        return []


def _fetch_sector_kline_akshare(
    board_name: str, ak_module: Any, max_days: int = 60, ref_date: date | None = None,
) -> list[dict[str, Any]]:
    """通过 akshare stock_board_industry_hist_em 获取行业板块 K 线（降级源）。"""
    if not board_name:
        return []
    ref = ref_date or date.today()
    start_str = (ref - timedelta(days=max_days + 30)).strftime("%Y%m%d")
    end_str = ref.strftime("%Y%m%d")
    try:
        frame = _safe_ak_call(
            lambda: ak_module.stock_board_industry_hist_em(
                symbol=board_name, period="日k",
                start_date=start_str, end_date=end_str, adjust="",
            ),
            timeout_sec=12,
        )
        raw = _to_records(frame, limit=max_days + 10)
        records: list[dict[str, Any]] = []
        for r in raw:
            d = str(r.get("日期", "")).strip()[:10]
            if not d:
                continue
            records.append({
                "日期": d,
                "开盘": _safe_float(str(r.get("开盘", 0))),
                "收盘": _safe_float(str(r.get("收盘", 0))),
                "最高": _safe_float(str(r.get("最高", 0))),
                "最低": _safe_float(str(r.get("最低", 0))),
                "成交量": int(float(r.get("成交量", 0) or 0)),
                "涨跌幅": _safe_float(str(r.get("涨跌幅", 0))),
            })
        return records[-max_days:]
    except Exception as e:
        logger.debug("akshare sector kline failed board=%s: %s", board_name, e)
        return []


def _fetch_sector_and_benchmark_klines(
    symbol: str,
    company_profile: dict[str, str],
    ak_module: Any,
    ref_date: date,
    max_days: int = 60,
) -> dict[str, Any]:
    """获取个股所属行业板块 + 沪深 300 的近 N 日 K 线，用于行业强弱对比。

    板块代码直接从 company_profile['board_code'] / ['board_name'] 读取
    （由 _fetch_company_profile_em 经 RPT_F10_ORG_BASICINFO 返回），无需 akshare 板块列表。

        K 线数据源优先级:
            板块: push2(多域名重试) → akshare stock_board_industry_hist_em（官方接口）
                 → 通达信本地行业指数(个股映射)
      CSI300: push2(多域名重试) → 腾讯财经

    返回 dict:
      sector_name: str           — 行业板块名
      sector_kline_60d: list     — 行业板块日 K
      csi300_kline_60d: list     — 沪深 300 日 K
    """
    result: dict[str, Any] = {
        "sector_name": "",
        "sector_kline_60d": [],
        "csi300_kline_60d": [],
    }

    # ── 1. 行业板块 K 线（优先主营业务对应三级行业） ──
    board_candidates = [
        (company_profile.get("board_code_l3", ""), company_profile.get("board_name_l3", ""), "L3"),
        (company_profile.get("board_code_l2", ""), company_profile.get("board_name_l2", ""), "L2"),
        (company_profile.get("board_code_l1", ""), company_profile.get("board_name_l1", ""), "L1"),
        (company_profile.get("board_code", ""), company_profile.get("board_name", ""), "DEFAULT"),
    ]
    seen: set[tuple[str, str]] = set()
    normalized_candidates: list[tuple[str, str, str]] = []
    for code, name, lvl in board_candidates:
        key = (str(code or "").strip(), str(name or "").strip())
        if key[0] and key not in seen:
            seen.add(key)
            normalized_candidates.append((key[0], key[1], lvl))

    if normalized_candidates:
        # Pass 1: 先在所有层级上尝试真实板块K线(push2)
        for board_code, board_name, level_tag in normalized_candidates:
            result["sector_name"] = board_name
            secid = f"90.{board_code}"
            records = _fetch_kline_by_secid(secid, days=max_days, ref_date=ref_date)
            if records:
                result["sector_kline_60d"] = records
                logger.info("行业板块 K 线: %s (%s, %s) → %d 条", board_name, board_code, level_tag, len(records))
                break

        # Pass 1.5: push2 全失败后，映射到东财可交易官方行业板块再试一次
        if not result["sector_kline_60d"]:
            mapped_code, mapped_name = _remap_to_official_board(company_profile)
            if mapped_code:
                mapped_records = _fetch_kline_by_secid(f"90.{mapped_code}", days=max_days, ref_date=ref_date)
                if mapped_records:
                    result["sector_kline_60d"] = mapped_records
                    result["sector_name"] = mapped_name
                    logger.info("行业板块 K 线(官方映射): %s (%s) → %d 条", mapped_name, mapped_code, len(mapped_records))

        # Pass 2: push2 全部失败后，再按层级尝试 akshare 板块K线
        if not result["sector_kline_60d"]:
            for board_code, board_name, level_tag in normalized_candidates:
                logger.info("行业板块 push2 失败，尝试 akshare 板块K线: %s (%s)", board_name, level_tag)
                records = _fetch_sector_kline_akshare(board_name, ak_module, max_days, ref_date)
                if records:
                    result["sector_kline_60d"] = records
                    result["sector_name"] = board_name
                    logger.info("行业板块 K 线(akshare): %s (%s) → %d 条", board_name, level_tag, len(records))
                    break

        # Pass 3: 东财官方源都失败后，使用通达信本地行业指数（官方通达信体系）
        if not result["sector_kline_60d"]:
            tdx_records, tdx_name = _fetch_tdx_sector_kline_by_stock(symbol=symbol, days=max_days, ref_date=ref_date)
            if tdx_records:
                result["sector_kline_60d"] = tdx_records
                result["sector_name"] = tdx_name or result.get("sector_name", "")
                logger.info("行业板块 K 线(通达信本地): %s → %d 条", result["sector_name"], len(tdx_records))

        if not result["sector_kline_60d"]:
            logger.warning("行业板块 K 线所有官方源均失败(东财+通达信): %s", normalized_candidates)
    else:
        logger.warning("公司资料中未找到行业板块代码")

    # ── 2. 沪深 300 K 线 ──
    csi300_records = _fetch_kline_by_secid("1.000300", days=max_days, ref_date=ref_date)
    if csi300_records:
        result["csi300_kline_60d"] = csi300_records
    else:
        # 降级：腾讯财经
        csi300_records = _fetch_kline_tencent("sh000300", days=max_days, ref_date=ref_date)
        if csi300_records:
            result["csi300_kline_60d"] = csi300_records
            logger.info("CSI300 K 线使用腾讯源降级: %d 条", len(csi300_records))
        else:
            logger.warning("CSI300 K 线两个源均失败")

    return result


def fetch_stock_bundle(symbol: str, mode: str = "quick", *, include_headlines: bool = True, as_of_date: date | None = None) -> dict[str, Any]:
    """
    东方财富数据适配实现。
    mode:
    - quick: 快速模式，跳过全量扫描接口，优先返回几秒级结果
    - deep: 深度模式，包含全量扫描接口（可能较慢）
    include_headlines: 是否同时抓取市场要闻（scope=stock 时可设为 False 跳过）
    as_of_date: 数据基准日期（默认 None=今天）；传入历史日期时行情/公告/新闻等按该日往前抓取

    目标：统一聚合以下接口的数据，失败字段保持空值，不做臆断。
    - stock_zygc_em
    - stock_news_em
    - stock_yjbb_em
    - stock_notice_report
    - stock_financial_analysis_indicator_em
    - stock_zh_a_gdhs
    - stock_zh_a_hist
    - stock_ggcg_em
    """
    fetch_mode = (mode or "quick").strip().lower()
    ref = as_of_date or date.today()
    cache_key = _bundle_cache_key(symbol, fetch_mode, as_of_date)
    cached_bundle = _cache_get_bundle(cache_key)
    # deep + 当天场景优先实时性：避免刚发布公告被同日缓存遮蔽
    bypass_cache_for_fresh_notice = (fetch_mode == "deep") and (ref == date.today())
    if cached_bundle is not None and not bypass_cache_for_fresh_notice:
        return cached_bundle

    bundle: dict[str, Any] = {
        "symbol": symbol,
        "concept_tags": [],
        "theme_highlights": [],
        "company_profile": {},
        "sector_name": "",
        "sector_kline_60d": [],
        "csi300_kline_60d": [],
        "zygc": [],
        "news": [],
        "market_hot_news_top10": [],
        "yjbb": [],
        "notice": [],
        "notice_recent_30d_with_content": [],
        "financial_indicator": [],
        "gdhs": [],
        "hist": [],
        "ggcg": [],
        "_errors": [],
        "_diagnostics": [],
    }

    try:
        import akshare as ak
    except Exception as exc:
        bundle["_errors"].append(f"akshare导入失败: {exc}")
        return bundle

    prefixed_symbol = _symbol_with_market_prefix(symbol)
    dot_symbol = _symbol_with_dot_market(symbol)
    sina_symbol = _symbol_for_sina(symbol)
    start_date = (ref - timedelta(days=90)).strftime("%Y%m%d")
    end_date = ref.strftime("%Y%m%d")

    # ── 概念题材标签 + 题材亮点 + 公司基本信息（datacenter API，共 3 次 HTTP） ──
    start_concept = time.perf_counter()
    concept_tags = _fetch_concept_tags_em(symbol)
    bundle["concept_tags"] = concept_tags
    bundle["_diagnostics"].append({
        "endpoint": "concept_tags_datacenter",
        "ok": len(concept_tags) > 0,
        "rows": len(concept_tags),
        "duration_ms": int((time.perf_counter() - start_concept) * 1000),
        "error": "" if concept_tags else "概念接口返回空",
    })

    start_th = time.perf_counter()
    theme_highlights = _fetch_theme_highlights_em(symbol)
    bundle["theme_highlights"] = theme_highlights
    bundle["_diagnostics"].append({
        "endpoint": "theme_highlights_datacenter",
        "ok": len(theme_highlights) > 0,
        "rows": len(theme_highlights),
        "duration_ms": int((time.perf_counter() - start_th) * 1000),
        "error": "" if theme_highlights else "题材亮点接口返回空",
    })

    start_cp = time.perf_counter()
    company_profile = _fetch_company_profile_em(symbol)
    bundle["company_profile"] = company_profile
    bundle["_diagnostics"].append({
        "endpoint": "company_profile_datacenter",
        "ok": bool(company_profile),
        "rows": 1 if company_profile else 0,
        "duration_ms": int((time.perf_counter() - start_cp) * 1000),
        "error": "" if company_profile else "公司信息接口返回空",
    })

    _run_endpoint(
        bundle,
        endpoint_name="stock_zygc_em",
        action=lambda: _safe_ak_call(lambda: ak.stock_zygc_em(symbol=prefixed_symbol), timeout_sec=8),
        target_key="zygc",
        limit=120,
    )
    bundle["zygc"] = _keep_recent_zygc_records(bundle.get("zygc", []), max_days=365, max_items=80, ref_date=ref)
    _update_diagnostic_rows(
        bundle,
        "stock_zygc_em",
        len(bundle.get("zygc", [])),
        note="仅保留近1年主营构成，若无则降级到最新报告期",
    )

    _run_endpoint(
        bundle,
        endpoint_name="stock_news_em",
        action=lambda: _safe_ak_call(lambda: ak.stock_news_em(symbol=symbol), timeout_sec=8),
        target_key="news",
        limit=200,
    )
    if include_headlines:
        start_hotnews = time.perf_counter()
        hot_news, hot_news_meta = _fetch_market_hot_news_merged(max_items_per_source=5, max_total=10)
        bundle["market_hot_news_top10"] = hot_news
        try:
            archive_market_hot_news(news_rows=hot_news, target_day=ref)
        except Exception:
            pass
        bundle["_diagnostics"].append(
            {
                "endpoint": "market_hot_news_merged_top5_each",
                "ok": len(hot_news) > 0,
                "rows": len(hot_news),
                "duration_ms": int((time.perf_counter() - start_hotnews) * 1000),
                "error": (
                    ""
                    if hot_news
                    else "东财要闻获取失败，且同花顺补充失败"
                ),
                "meta": hot_news_meta,
            }
        )
    else:
        bundle["_diagnostics"].append(
            {"endpoint": "market_hot_news_merged_top5_each", "ok": True, "rows": 0,
             "duration_ms": 0, "error": "已跳过(scope=stock)"}
        )

    if fetch_mode == "deep":
        yjbb_records: list[dict[str, Any]] = []
        yjbb_errors: list[str] = []
        yjbb_source = "akshare"
        start_yjbb = time.perf_counter()
        for quarter_end in _recent_quarter_ends(max_items=4, ref_date=ref):
            try:
                frame = _safe_ak_call(lambda: ak.stock_yjbb_em(date=quarter_end), timeout_sec=20)
                records = _filter_by_symbol(_to_records(frame, limit=50000), symbol=symbol)
                if records:
                    yjbb_records = records[:60]
                    break
            except Exception as exc:
                yjbb_errors.append(f"{quarter_end}:{exc}")
                if "请求超时" in str(exc):
                    break
        # ── akshare 失败 → datacenter RPT_LICO_FN_CPD 降级 ──
        if not yjbb_records:
            dc_rows = _fetch_yjbb_datacenter(symbol)
            if dc_rows:
                yjbb_records = dc_rows
                yjbb_source = "datacenter_RPT_LICO_FN_CPD"
                logger.info("业绩报表 akshare 失败，datacenter 降级成功: %s → %d 条", symbol, len(dc_rows))
        if (not yjbb_records) and (not yjbb_errors):
            yjbb_errors.append("最近4个报告期未命中该股票业绩报表")
        bundle["yjbb"] = _keep_latest_period_records(
            yjbb_records,
            date_keys=["最新公告日期", "公告日期", "REPORT_DATE", "REPORTDATE", "报告日期", "日期"],
            max_items=2,
        )
        bundle["_diagnostics"].append(
            {
                "endpoint": f"stock_yjbb_em({yjbb_source})",
                "ok": len(yjbb_records) > 0,
                "rows": len(bundle["yjbb"]),
                "duration_ms": int((time.perf_counter() - start_yjbb) * 1000),
                "error": " | ".join(yjbb_errors[:3]) if yjbb_errors and not yjbb_records else "",
            }
        )
        if not yjbb_records and yjbb_errors:
            bundle["_errors"].append(f"stock_yjbb_em失败: {yjbb_errors[0]}")
    else:
        bundle["_diagnostics"].append(
            {"endpoint": "stock_yjbb_em", "ok": True, "rows": 0, "duration_ms": 0, "error": "quick模式已跳过"}
        )

    if fetch_mode == "deep":
        notice_records: list[dict[str, Any]] = []
        notice_api_ok_count = 0
        notice_api_fail_count = 0
        fallback_notice_records: list[dict[str, Any]] = []
        start_notice = time.perf_counter()
        notice_categories = ["重大事项", "财务报告", "风险提示"]
        for day in _recent_dates(days=7, ref_date=ref):
            for category in notice_categories:
                try:
                    frame = _safe_ak_call(
                        lambda c=category, d=day: ak.stock_notice_report(symbol=c, date=d),
                        timeout_sec=10,
                    )
                    notice_api_ok_count += 1
                    records = _filter_by_symbol(_to_records(frame, limit=30000), symbol=symbol)
                    if records:
                        notice_records.extend(records)
                        if len(notice_records) >= 80:
                            break
                except (KeyError, ValueError, TypeError):
                    # akshare 内部解析异常 (如某些日期无数据时 KeyError) — 跳过该日期/类别
                    notice_api_fail_count += 1
                except Exception as exc:
                    notice_api_fail_count += 1
                    if "请求超时" in str(exc):
                        break
            if len(notice_records) >= 80:
                break

        # 无论 ak 是否有结果，都用东财公告中心做一次补充，避免最新公告漏抓
        fallback_notice_records = _fetch_notice_by_symbol_eastmoney(
            symbol=symbol,
            max_items=120,
            max_days=180,
            max_pages=8,
            ref_date=ref,
        )
        ak_notice_count = len(notice_records)
        if fallback_notice_records:
            notice_records = _merge_notice_records(notice_records, fallback_notice_records, max_items=120)
        merged_notice_count = len(notice_records)
        merged_added = max(0, merged_notice_count - ak_notice_count)

        # 判定: API 调用本身是否成功（而非是否匹配到个股）
        api_reachable = (notice_api_ok_count > 0) or bool(fallback_notice_records)
        notice_note = ""
        if not notice_records:
            if api_reachable:
                notice_note = f"API正常({notice_api_ok_count}次成功)，但{symbol}近7天无公告"
            else:
                notice_note = f"全部{notice_api_fail_count}次调用失败"
        if fallback_notice_records and notice_records:
            restruct_count = sum(1 for item in fallback_notice_records if item.get("重组相关"))
            notice_note = (
                f"Eastmoney公告中心对齐完成，新增{merged_added}条，合并后{merged_notice_count}条"
                f"(东财补充池{len(fallback_notice_records)}条，重组相关{restruct_count}条)"
            )
        bundle["notice"] = notice_records[:120]
        bundle["notice_recent_30d_with_content"] = _enrich_recent_notice_with_content(
            bundle["notice"],
            max_days=30,
            max_items=30,
            ref_date=ref,
        )
        bundle["_diagnostics"].append(
            {
                "endpoint": "stock_notice_report",
                "ok": api_reachable,
                "rows": len(bundle["notice"]),
                "duration_ms": int((time.perf_counter() - start_notice) * 1000),
                "error": (
                    f"{notice_note} | 近30天公告正文补充{len(bundle.get('notice_recent_30d_with_content', []))}条"
                    if notice_note
                    else f"近30天公告正文补充{len(bundle.get('notice_recent_30d_with_content', []))}条"
                ),
            }
        )
        if not api_reachable:
            bundle["_errors"].append(f"stock_notice_report失败: {notice_note}")
    else:
        bundle["_diagnostics"].append(
            {"endpoint": "stock_notice_report", "ok": True, "rows": 0, "duration_ms": 0, "error": "quick模式已跳过"}
        )

    _run_endpoint(
        bundle,
        endpoint_name="stock_financial_analysis_indicator_em",
        action=lambda: _safe_ak_call(
            lambda: ak.stock_financial_analysis_indicator_em(symbol=dot_symbol, indicator="按报告期"),
            timeout_sec=8,
        ),
        target_key="financial_indicator",
        limit=120,
    )
    # ── akshare 失败 → datacenter RPT_F10_FINANCE_MAINFINADATA 降级 ──
    if not bundle.get("financial_indicator"):
        dc_fi = _fetch_financial_indicator_datacenter(symbol, max_items=5)
        if dc_fi:
            bundle["financial_indicator"] = dc_fi
            logger.info("财务指标 akshare 失败，datacenter 降级成功: %s → %d 条", symbol, len(dc_fi))
    bundle["financial_indicator"] = _keep_latest_period_records(
        bundle.get("financial_indicator", []),
        date_keys=["REPORT_DATE", "NOTICE_DATE", "UPDATE_DATE", "报告日期", "日期"],
        max_items=2,
    )
    _update_diagnostic_rows(
        bundle,
        "stock_financial_analysis_indicator_em",
        len(bundle.get("financial_indicator", [])),
        note="仅保留最近2个报告期",
    )

    if fetch_mode == "deep":
        gdhs_records: list[dict[str, Any]] = []
        gdhs_errors: list[str] = []
        gdhs_source = "akshare"
        start_gdhs = time.perf_counter()
        for quarter_end in _recent_quarter_ends(max_items=4, ref_date=ref):
            try:
                frame = _safe_ak_call(lambda: ak.stock_zh_a_gdhs(symbol=quarter_end), timeout_sec=20)
                records = _filter_by_symbol(_to_records(frame, limit=50000), symbol=symbol)
                if records:
                    gdhs_records.extend(records)
            except Exception as exc:
                gdhs_errors.append(f"{quarter_end}:{exc}")
                if "请求超时" in str(exc):
                    break
        # ── akshare 失败 → datacenter RPT_F10_EH_HOLDERSNUM 降级 ──
        if not gdhs_records:
            dc_gdhs = _fetch_gdhs_datacenter(symbol, max_items=5)
            if dc_gdhs:
                gdhs_records = dc_gdhs
                gdhs_source = "datacenter_HOLDERSNUM"
                logger.info("股东户数 akshare 失败，datacenter 降级成功: %s → %d 条", symbol, len(dc_gdhs))
        if (not gdhs_records) and (not gdhs_errors):
            gdhs_errors.append("最近4个报告期未命中该股票股东户数")
        bundle["gdhs"] = gdhs_records[:120]
        bundle["_diagnostics"].append(
            {
                "endpoint": f"stock_zh_a_gdhs({gdhs_source})",
                "ok": len(gdhs_records) > 0,
                "rows": len(bundle["gdhs"]),
                "duration_ms": int((time.perf_counter() - start_gdhs) * 1000),
                "error": " | ".join(gdhs_errors[:3]) if gdhs_errors and not gdhs_records else "",
            }
        )
        if not gdhs_records and gdhs_errors:
            bundle["_errors"].append(f"stock_zh_a_gdhs失败: {gdhs_errors[0]}")
    else:
        bundle["_diagnostics"].append(
            {"endpoint": "stock_zh_a_gdhs", "ok": True, "rows": 0, "duration_ms": 0, "error": "quick模式已跳过"}
        )

    start_hist = time.perf_counter()
    hist_errors: list[str] = []
    hist_records: list[dict[str, Any]] = []
    for adjust_mode in ["qfq", ""]:
        try:
            frame = _safe_ak_call(
                lambda m=adjust_mode: ak.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust=m,
                ),
                timeout_sec=8,
            )
            hist_records = _to_records(frame, limit=80)
            if hist_records:
                break
        except Exception as exc:
            hist_errors.append(f"adjust={adjust_mode}:{exc}")

    if not hist_records:
        try:
            frame = _safe_ak_call(
                lambda: ak.stock_zh_a_daily(
                    symbol=sina_symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="",
                ),
                timeout_sec=12,
            )
            if hasattr(frame, "reset_index"):
                frame = frame.reset_index()
            hist_records = _to_records(frame, limit=80)
        except Exception as exc:
            hist_errors.append(f"sina_daily:{exc}")

    bundle["hist"] = _keep_recent_hist_records(hist_records, max_days=60, max_items=60, ref_date=ref)
    hist_ok = len(hist_records) > 0
    bundle["_diagnostics"].append(
        {
            "endpoint": "stock_zh_a_hist",
            "ok": hist_ok,
            "rows": len(hist_records),
            "duration_ms": int((time.perf_counter() - start_hist) * 1000),
            "error": " | ".join(hist_errors[:2]) if (not hist_ok and hist_errors) else "",
        }
    )
    if (not hist_ok) and hist_errors:
        first_error = hist_errors[0]
        network_block_keywords = ["RemoteDisconnected", "Connection aborted", "ProxyError"]
        if any(key in first_error for key in network_block_keywords):
            if fetch_mode == "deep":
                bundle["_errors"].append(f"stock_zh_a_hist降级: 网络阻断，已尝试备用源仍失败: {first_error}")
        else:
            bundle["_errors"].append(f"stock_zh_a_hist失败: {first_error}")

    # ── 行业板块 + 沪深 300 K 线（用于行业强弱对比） ──
    start_bench = time.perf_counter()
    bench_data = _fetch_sector_and_benchmark_klines(
        symbol=symbol,
        company_profile=bundle.get("company_profile", {}),
        ak_module=ak,
        ref_date=ref,
        max_days=60,
    )
    bundle["sector_name"] = bench_data.get("sector_name", "")
    bundle["sector_kline_60d"] = bench_data.get("sector_kline_60d", [])
    bundle["csi300_kline_60d"] = bench_data.get("csi300_kline_60d", [])
    sector_ok = bool(bundle["sector_kline_60d"])
    csi300_ok = bool(bundle["csi300_kline_60d"])
    bench_note_parts = []
    if bundle["sector_name"]:
        bench_note_parts.append(f"板块={bundle['sector_name']}")
    if not sector_ok:
        bench_note_parts.append("行业板块K线获取失败")
    if not csi300_ok:
        bench_note_parts.append("沪深300K线获取失败")
    bundle["_diagnostics"].append({
        "endpoint": "sector_benchmark_kline",
        "ok": sector_ok or csi300_ok,
        "rows": len(bundle["sector_kline_60d"]) + len(bundle["csi300_kline_60d"]),
        "duration_ms": int((time.perf_counter() - start_bench) * 1000),
        "error": "; ".join(bench_note_parts) if bench_note_parts else "",
    })

    if fetch_mode == "deep":
        start_ggcg = time.perf_counter()
        ggcg_records, ggcg_source, ggcg_error = _fetch_ggcg_multi_source(symbol, ak)
        ggcg_api_ok = ggcg_source != ""
        ggcg_note = ""
        if ggcg_api_ok and not ggcg_records:
            ggcg_note = f"{ggcg_source}正常，但{symbol}近期无高管增减持记录"
        elif not ggcg_api_ok:
            is_timeout = "请求超时" in ggcg_error
            ggcg_note = ggcg_error + ("(非核心，已降级)" if is_timeout else "")
        bundle["ggcg"] = ggcg_records
        # ggcg 为辅助数据：超时只标 WARN(ok=True, rows=0), 非超时错误才标 ERR
        is_soft_fail = (not ggcg_api_ok) and ("请求超时" in ggcg_error)
        endpoint_label = ggcg_source or "ggcg_multi_source"
        bundle["_diagnostics"].append(
            {
                "endpoint": endpoint_label,
                "ok": ggcg_api_ok or is_soft_fail,
                "rows": len(ggcg_records),
                "duration_ms": int((time.perf_counter() - start_ggcg) * 1000),
                "error": ggcg_note,
            }
        )
        if not ggcg_api_ok and not is_soft_fail:
            bundle["_errors"].append(f"高管增减持(3源均失败): {ggcg_error}")
    else:
        bundle["_diagnostics"].append(
            {"endpoint": "ggcg_multi_source", "ok": True, "rows": 0, "duration_ms": 0, "error": "quick模式已跳过"}
        )

    stock_names = _extract_stock_name_candidates(bundle)
    raw_news = bundle.get("news", [])
    filtered_news = _filter_news_by_relevance(raw_news, symbol=symbol, stock_names=stock_names)
    filtered_news = _keep_recent_records(
        filtered_news,
        date_keys=["发布时间", "日期", "publish_date", "report_date"],
        max_days=30,
        max_items=30,
        ref_date=ref,
    )
    bundle["news"] = filtered_news
    _update_diagnostic_rows(
        bundle,
        "stock_news_em",
        len(bundle.get("news", [])),
        note=f"名称相关过滤后保留{len(bundle.get('news', []))}条",
    )

    _cache_set_bundle(cache_key, bundle)
    return bundle
