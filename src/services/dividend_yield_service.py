from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
import os
import re
from statistics import median
from typing import Any
import requests
from bs4 import BeautifulSoup


SOURCE_AUTHORITY_ORDER = [
    "eastmoney_quote_page",
    "ths_basic_page",
    "sina_finance_page",
    "xueqiu_page",
    "baidu_gushitong",
]

SOURCE_DIVIDEND_AUTHORITY_ORDER = [
    "ths_bonus",
    "cninfo",
    "eastmoney_fhps_detail",
    "sina_history_dividend",
]


@dataclass
class DividendEvidence:
    source: str
    ex_date: str
    report_period: str
    cash_dividend_per_10_shares: float
    cash_dividend_per_share: float
    currency: str
    description: str
    raw: dict[str, Any]


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", raw)
    if not m:
        m = re.search(r"(\d{4})(\d{2})(\d{2})", raw)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception:
        return None


def _quarter_key(d: date) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{d.year}Q{q}"


def _recent_quarter_keys(as_of: date, count: int = 4) -> list[str]:
    out: list[str] = []
    y = as_of.year
    q = (as_of.month - 1) // 3 + 1
    for _ in range(max(1, count)):
        out.append(f"{y}Q{q}")
        q -= 1
        if q <= 0:
            q = 4
            y -= 1
    return out


def _quarter_start_from_key(key: str) -> date | None:
    m = re.match(r"^(\d{4})Q([1-4])$", str(key))
    if not m:
        return None
    year = int(m.group(1))
    quarter = int(m.group(2))
    month = 3 * (quarter - 1) + 1
    return date(year, month, 1)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (float, int)):
        return float(value)
    raw = str(value).strip()
    if not raw or raw.lower() == "nan":
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _parse_currency(text: str) -> str:
    if "港元" in text or "HKD" in text.upper():
        return "HKD"
    return "CNY"


def _parse_dividend_per10_from_text(text: str) -> float | None:
    raw = str(text or "").replace(" ", "")
    if not raw:
        return None
    if ("不分配" in raw) or ("不派息" in raw):
        return None
    patterns = [
        r"每?10股?派([0-9]+(?:\.[0-9]+)?)",
        r"10派([0-9]+(?:\.[0-9]+)?)",
        r"派([0-9]+(?:\.[0-9]+)?)元",
        r"派([0-9]+(?:\.[0-9]+)?)港元",
    ]
    for pat in patterns:
        m = re.search(pat, raw, flags=re.IGNORECASE)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                continue
    return None


def _normalize_symbol(symbol: str) -> str:
    code = str(symbol).strip().upper()
    code = code.replace("SH", "").replace("SZ", "").replace("BJ", "")
    code = code.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    digits = "".join(ch for ch in code if ch.isdigit())
    return digits.zfill(6) if digits else code


def _sina_symbol(symbol: str) -> str:
    code = _normalize_symbol(symbol)
    if code.startswith(("6", "9")):
        return f"sh{code}"
    if code.startswith(("0", "3")):
        return f"sz{code}"
    if code.startswith(("8", "4")):
        return f"bj{code}"
    return code.lower()


def _build_evidence(
    source: str,
    ex_date: date,
    per10: float,
    description: str,
    report_period: str = "",
    currency: str = "CNY",
    raw: dict[str, Any] | None = None,
) -> DividendEvidence:
    return DividendEvidence(
        source=source,
        ex_date=ex_date.isoformat(),
        report_period=report_period,
        cash_dividend_per_10_shares=float(per10),
        cash_dividend_per_share=float(per10) / 10.0,
        currency=currency,
        description=description,
        raw=raw or {},
    )


def _fetch_cninfo(symbol: str) -> list[DividendEvidence]:
    import akshare as ak

    out: list[DividendEvidence] = []
    try:
        df = ak.stock_dividend_cninfo(symbol=symbol)
    except Exception:
        return out
    if df is None or getattr(df, "empty", True):
        return out
    records = df.fillna("").to_dict(orient="records")
    for rec in records:
        ex = _parse_date(rec.get("除权日", ""))
        if not ex:
            continue
        desc = str(rec.get("实施方案分红说明", ""))
        per10 = _to_float(rec.get("派息比例", None))
        if per10 is None:
            per10 = _parse_dividend_per10_from_text(desc)
        if per10 is None or per10 <= 0:
            continue
        out.append(
            _build_evidence(
                source="cninfo",
                ex_date=ex,
                per10=per10,
                description=desc or f"派息比例={per10}",
                report_period=str(rec.get("报告时间", "")),
                currency=_parse_currency(desc),
                raw=rec,
            )
        )
    return out


def _fetch_em_detail(symbol: str) -> list[DividendEvidence]:
    import akshare as ak

    out: list[DividendEvidence] = []
    try:
        df = ak.stock_fhps_detail_em(symbol=symbol)
    except Exception:
        return out
    if df is None or getattr(df, "empty", True):
        return out
    records = df.fillna("").to_dict(orient="records")
    for rec in records:
        ex = _parse_date(rec.get("除权除息日", ""))
        if not ex:
            continue
        desc = str(rec.get("现金分红-现金分红比例描述", ""))
        per10 = _to_float(rec.get("现金分红-现金分红比例", None))
        if per10 is None:
            per10 = _parse_dividend_per10_from_text(desc)
        if per10 is None or per10 <= 0:
            continue
        out.append(
            _build_evidence(
                source="eastmoney_fhps_detail",
                ex_date=ex,
                per10=per10,
                description=desc or f"现金分红比例={per10}",
                report_period=str(rec.get("报告期", "")),
                currency=_parse_currency(desc),
                raw=rec,
            )
        )
    return out


def _fetch_ths_detail(symbol: str) -> list[DividendEvidence]:
    import akshare as ak

    out: list[DividendEvidence] = []
    try:
        df = ak.stock_fhps_detail_ths(symbol=symbol)
    except Exception:
        return out
    if df is None or getattr(df, "empty", True):
        return out
    records = df.fillna("").to_dict(orient="records")
    for rec in records:
        ex = _parse_date(rec.get("A股除权除息日", ""))
        if not ex:
            continue
        desc = str(rec.get("分红方案说明", ""))
        per10 = _parse_dividend_per10_from_text(desc)
        if per10 is None or per10 <= 0:
            continue
        out.append(
            _build_evidence(
                source="ths_bonus",
                ex_date=ex,
                per10=per10,
                description=desc,
                report_period=str(rec.get("报告期", "")),
                currency=_parse_currency(desc),
                raw=rec,
            )
        )
    return out


def _fetch_sina_dividend_detail(symbol: str) -> list[DividendEvidence]:
    import akshare as ak

    out: list[DividendEvidence] = []
    try:
        df = ak.stock_history_dividend_detail(symbol=symbol, indicator="分红")
    except Exception:
        return out
    if df is None or getattr(df, "empty", True):
        return out

    records = df.fillna("").to_dict(orient="records")
    for rec in records:
        ex = _parse_date(rec.get("除权除息日", ""))
        if not ex:
            continue
        per10 = _to_float(rec.get("派息", None))
        if per10 is None or per10 <= 0:
            continue
        out.append(
            _build_evidence(
                source="sina_history_dividend",
                ex_date=ex,
                per10=per10,
                description=f"派息={per10}",
                report_period=str(rec.get("公告日期", "")),
                currency="CNY",
                raw=rec,
            )
        )
    return out


def _fetch_baidu_on_date(symbol: str, target: date) -> list[DividendEvidence]:
    import akshare as ak

    out: list[DividendEvidence] = []
    ds = target.strftime("%Y%m%d")
    try:
        df = ak.news_trade_notify_dividend_baidu(date=ds)
    except Exception:
        return out
    if df is None or getattr(df, "empty", True):
        return out
    code = _normalize_symbol(symbol)
    records = df.fillna("").to_dict(orient="records")
    for rec in records:
        rec_code = _normalize_symbol(rec.get("股票代码", ""))
        if rec_code != code:
            continue
        ex = _parse_date(rec.get("除权日", ""))
        if not ex:
            continue
        desc = str(rec.get("分红", ""))
        per10 = _parse_dividend_per10_from_text(desc)
        # 百度日历中的"分红"通常是 10 派金额，若描述里缺少"10派"则按同口径处理
        if per10 is None:
            numeric = _to_float(re.sub(r"[^0-9.]+", "", desc))
            per10 = numeric
        if per10 is None or per10 <= 0:
            continue
        out.append(
            _build_evidence(
                source="baidu_dividend_calendar",
                ex_date=ex,
                per10=per10,
                description=desc,
                report_period=str(rec.get("报告期", "")),
                currency=_parse_currency(desc),
                raw=rec,
            )
        )
    return out


def _group_events(evidences: list[DividendEvidence]) -> list[dict[str, Any]]:
    grouped: dict[str, list[DividendEvidence]] = {}
    for ev in evidences:
        grouped.setdefault(ev.ex_date, []).append(ev)
    events: list[dict[str, Any]] = []
    for ex_date, rows in grouped.items():
        values = [x.cash_dividend_per_10_shares for x in rows]
        cons = float(median(values))
        spread = (max(values) - min(values)) if values else 0.0
        confidence = "high"
        if len(rows) < 2:
            confidence = "low"
        elif spread > max(0.02, cons * 0.03):
            confidence = "medium"
        events.append(
            {
                "ex_date": ex_date,
                "cash_dividend_per_10_shares_consensus": cons,
                "cash_dividend_per_share_consensus": cons / 10.0,
                "sources": sorted(list({x.source for x in rows})),
                "source_count": len(rows),
                "spread": spread,
                "confidence": confidence,
                "evidences": [asdict(x) for x in rows],
            }
        )
    events.sort(key=lambda x: x["ex_date"])
    return events


def _filter_ttm_events(events: list[dict[str, Any]], as_of: date, ttm_days: int) -> list[dict[str, Any]]:
    window_days = max(30, int(ttm_days))
    window_start = as_of - timedelta(days=window_days)
    picked: list[dict[str, Any]] = []
    for event in events:
        ex_date = _parse_date(event.get("ex_date", ""))
        if not ex_date:
            continue
        if window_start <= ex_date <= as_of:
            picked.append(event)
    picked.sort(key=lambda item: str(item.get("ex_date", "")))
    return picked


def _yield_pct(cash_dividend_per_share: float | None, price: float | None) -> float | None:
    if cash_dividend_per_share is None or price is None:
        return None
    try:
        if float(price) <= 0:
            return None
        return round(float(cash_dividend_per_share) / float(price) * 100.0, 4)
    except Exception:
        return None


def _pick_event_for_date(
    events: list[dict[str, Any]],
    target_date: date,
    strict: bool,
    nearby_days: int,
) -> tuple[dict[str, Any] | None, str]:
    if not events:
        return None, "none"
    target_str = target_date.isoformat()
    exact = [e for e in events if e["ex_date"] == target_str]
    if exact:
        return exact[-1], "exact"
    if strict:
        return None, "strict_no_exact"
    anchor = target_date
    candidates: list[tuple[int, dict[str, Any]]] = []
    for ev in events:
        d = _parse_date(ev["ex_date"])
        if not d:
            continue
        gap = abs((d - anchor).days)
        if gap <= max(0, nearby_days):
            candidates.append((gap, ev))
    if not candidates:
        before = []
        for ev in events:
            d = _parse_date(ev["ex_date"])
            if d and d <= anchor:
                before.append(ev)
        if before:
            before.sort(key=lambda x: x["ex_date"])
            return before[-1], "latest_before"
        return None, "no_nearby"
    candidates.sort(key=lambda x: (x[0], x[1]["ex_date"]), reverse=False)
    return candidates[0][1], "nearby"


def _pick_latest_event(events: list[dict[str, Any]], anchor_date: date) -> tuple[dict[str, Any] | None, str]:
    if not events:
        return None, "none"
    before: list[dict[str, Any]] = []
    for ev in events:
        d = _parse_date(ev.get("ex_date", ""))
        if d and d <= anchor_date:
            before.append(ev)
    if before:
        before.sort(key=lambda x: x.get("ex_date", ""))
        return before[-1], "latest_event"
    events_sorted = sorted(events, key=lambda x: x.get("ex_date", ""))
    return events_sorted[-1], "latest_event_no_before"


def _fetch_price(symbol: str, target_date: date) -> tuple[float | None, str, str]:
    import akshare as ak

    start = (target_date - timedelta(days=20)).strftime("%Y%m%d")
    end = target_date.strftime("%Y%m%d")
    try:
        df = ak.stock_zh_a_hist(
            symbol=_normalize_symbol(symbol),
            period="daily",
            start_date=start,
            end_date=end,
            adjust="",
        )
        if df is not None and (not df.empty):
            recs = df.fillna("").to_dict(orient="records")
            best_date = ""
            best_price: float | None = None
            for rec in recs:
                d = _parse_date(rec.get("日期", rec.get("date", "")))
                if not d or d > target_date:
                    continue
                px = _to_float(rec.get("收盘", rec.get("close", None)))
                if px is None:
                    continue
                if (best_date == "") or (d.isoformat() > best_date):
                    best_date = d.isoformat()
                    best_price = px
            if best_price is not None:
                return best_price, best_date, "stock_zh_a_hist"
    except Exception:
        pass

    try:
        df = ak.stock_zh_a_daily(
            symbol=_sina_symbol(symbol),
            start_date=start,
            end_date=end,
            adjust="",
        )
        if df is not None and (not df.empty):
            if hasattr(df, "reset_index"):
                df = df.reset_index()
            recs = df.fillna("").to_dict(orient="records")
            best_date = ""
            best_price: float | None = None
            for rec in recs:
                d = _parse_date(rec.get("date", rec.get("日期", "")))
                if not d or d > target_date:
                    continue
                px = _to_float(rec.get("close", rec.get("收盘", None)))
                if px is None:
                    continue
                if (best_date == "") or (d.isoformat() > best_date):
                    best_date = d.isoformat()
                    best_price = px
            if best_price is not None:
                return best_price, best_date, "stock_zh_a_daily"
    except Exception:
        pass
    return None, "", ""


def _extract_number_from_text(text: str, patterns: list[str]) -> float | None:
    raw = str(text or "")
    if not raw:
        return None
    for pat in patterns:
        m = re.search(pat, raw, flags=re.IGNORECASE)
        if not m:
            continue
        try:
            return float(m.group(1))
        except Exception:
            continue
    return None


def _fetch_ready_yield_from_ths(symbol: str) -> tuple[float | None, str, str, dict[str, Any]]:
    code = _normalize_symbol(symbol)
    if not code:
        return None, "", "", {}

    url = f"https://basic.10jqka.com.cn/{code}/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Referer": "https://www.10jqka.com.cn/",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        try:
            if resp.apparent_encoding:
                resp.encoding = resp.apparent_encoding
        except Exception:
            pass
        html = resp.text
    except Exception:
        return None, "", "", {}

    soup = BeautifulSoup(html, "lxml")

    # 方案1: 结构化标签邻接提取
    label_candidates = ["股息率TTM", "股息率(TTM)", "股息率（TTM）", "股息率"]
    for label in label_candidates:
        node = soup.find(string=lambda x: isinstance(x, str) and label in x)
        if not node:
            continue
        parent = getattr(node, "parent", None)
        if not parent:
            continue
        neighbor_texts: list[str] = []
        for sibling in [parent, parent.find_next_sibling(), parent.find_next("td"), parent.find_next("span")]:
            if sibling is None:
                continue
            txt = sibling.get_text(" ", strip=True) if hasattr(sibling, "get_text") else str(sibling)
            if txt:
                neighbor_texts.append(txt)
        joined = " | ".join(neighbor_texts)
        value = _extract_number_from_text(joined, [r"([0-9]+(?:\.[0-9]+)?)\s*%"])
        if value is not None:
            return value, date.today().isoformat(), "ths_basic_page", {"url": url, "label": label, "snippet": joined[:240]}

    # 方案2: 全文正则兜底
    all_text = soup.get_text(" ", strip=True)
    value = _extract_number_from_text(
        all_text,
        [
            r"股息率TTM[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
            r"股息率\(TTM\)[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
            r"股息率（TTM）[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
            r"股息率[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
        ],
    )
    if value is not None:
        return value, date.today().isoformat(), "ths_basic_page_regex", {"url": url}
    return None, "", "", {}


def _symbol_with_market_tag(symbol: str) -> str:
    code = _normalize_symbol(symbol)
    if code.startswith(("6", "9")):
        return f"sh{code}"
    if code.startswith(("0", "3")):
        return f"sz{code}"
    if code.startswith(("8", "4")):
        return f"bj{code}"
    return code


def _fetch_ready_yield_from_eastmoney_page(symbol: str) -> tuple[float | None, str, str, dict[str, Any]]:
    code = _normalize_symbol(symbol)
    market_code = _symbol_with_market_tag(code)
    if not market_code:
        return None, "", "", {}
    url = f"https://quote.eastmoney.com/{market_code}.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        html = resp.text
    except Exception:
        return None, "", "", {}

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    value = _extract_number_from_text(
        text,
        [
            r"股息率TTM[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
            r"股息率\(TTM\)[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
            r"股息率（TTM）[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
        ],
    )
    if value is not None:
        return value, date.today().isoformat(), "eastmoney_quote_page", {"url": url}
    return None, "", "", {}


def _fetch_ready_yield_from_baidu(symbol: str, as_of: date | None = None) -> tuple[float | None, str, str, dict[str, Any]]:
    code = _normalize_symbol(symbol)
    if not code:
        return None, "", "", {}

    url = "https://gushitong.baidu.com/opendata"
    params = {
        "openapi": "1",
        "dspName": "iphone",
        "tn": "tangram",
        "client": "app",
        "query": "股息率",
        "code": code,
        "word": "",
        "resource_id": "51171",
        "market": "ab",
        "tag": "股息率",
        "chart_select": "近一年",
        "industry_select": "",
        "skip_industry": "1",
        "finClientType": "pc",
    }
    try:
        resp = requests.get(url, params=params, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return None, "", "", {}

    try:
        body = payload["Result"][0]["DisplayData"]["resultData"]["tplData"]["result"]["chartInfo"][0]["body"]
    except Exception:
        return None, "", "", {}

    anchor = as_of or date.today()
    best_date = ""
    best_val: float | None = None
    for row in body or []:
        d = _parse_date((row or {}).get("date", ""))
        v = _to_float((row or {}).get("value", None))
        if d is None or v is None:
            continue
        if d > anchor:
            continue
        iso = d.isoformat()
        if (best_date == "") or (iso > best_date):
            best_date = iso
            best_val = float(v)
    if best_val is None:
        # 若截止日期前没有数据，退化为最新值
        for row in body or []:
            d = _parse_date((row or {}).get("date", ""))
            v = _to_float((row or {}).get("value", None))
            if d is None or v is None:
                continue
            iso = d.isoformat()
            if (best_date == "") or (iso > best_date):
                best_date = iso
                best_val = float(v)
    if best_val is None:
        return None, "", "", {}
    return best_val, (best_date or date.today().isoformat()), "baidu_gushitong", {"url": url, "query": "股息率"}


def _fetch_ready_yield_from_sina(symbol: str) -> tuple[float | None, str, str, dict[str, Any]]:
    code = _normalize_symbol(symbol)
    market_code = _symbol_with_market_tag(code)
    if not market_code:
        return None, "", "", {}
    url = f"https://finance.sina.com.cn/realstock/company/{market_code}/nc.shtml"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Referer": "https://finance.sina.com.cn/",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        try:
            if resp.apparent_encoding:
                resp.encoding = resp.apparent_encoding
        except Exception:
            pass
        html = resp.text
    except Exception:
        return None, "", "", {}

    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    value = _extract_number_from_text(
        text,
        [
            r"股息率TTM[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
            r"股息率\(TTM\)[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
            r"股息率（TTM）[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
            r"股息率[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
        ],
    )
    if value is not None:
        return value, date.today().isoformat(), "sina_finance_page", {"url": url}
    return None, "", "", {}


def _fetch_ready_yield_from_xueqiu(symbol: str) -> tuple[float | None, str, str, dict[str, Any]]:
    code = _normalize_symbol(symbol)
    market_code = _symbol_with_market_tag(code).upper()
    if not market_code:
        return None, "", "", {}
    url = f"https://xueqiu.com/S/{market_code.upper()}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Referer": "https://xueqiu.com/",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        html = resp.text
    except Exception:
        return None, "", "", {}

    value = _extract_number_from_text(
        html,
        [
            r'"dividend_yield"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
            r'"dividend_yield_ttm"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
            r"股息率TTM[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
            r"股息率[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)\s*%",
        ],
    )
    if value is not None:
        return value, date.today().isoformat(), "xueqiu_page", {"url": url}
    return None, "", "", {}


def _collect_ready_made_yields(symbol: str) -> list[dict[str, Any]]:
    fetchers = [
        _fetch_ready_yield_from_ths,
        _fetch_ready_yield_from_eastmoney_page,
        _fetch_ready_yield_from_baidu,
        _fetch_ready_yield_from_sina,
        _fetch_ready_yield_from_xueqiu,
    ]
    rows: list[dict[str, Any]] = []
    for fetcher in fetchers:
        try:
            value, as_of, source, raw = fetcher(symbol)
        except Exception:
            value, as_of, source, raw = None, "", "", {}
        if (value is None) or (not source):
            continue
        rows.append(
            {
                "source": source,
                "yield_pct": float(value),
                "as_of": as_of or date.today().isoformat(),
                "raw": raw or {},
            }
        )
    rows.sort(key=lambda x: str(x.get("source", "")))
    return rows


def _collect_ready_made_yield_attempts(symbol: str, as_of: date | None = None) -> list[dict[str, Any]]:
    fetchers: list[tuple[str, Any]] = [
        ("ths_basic_page", _fetch_ready_yield_from_ths),
        ("eastmoney_quote_page", _fetch_ready_yield_from_eastmoney_page),
        ("baidu_gushitong", lambda s: _fetch_ready_yield_from_baidu(s, as_of=as_of)),
        ("sina_finance_page", _fetch_ready_yield_from_sina),
        ("xueqiu_page", _fetch_ready_yield_from_xueqiu),
    ]
    attempts: list[dict[str, Any]] = []
    for name, fetcher in fetchers:
        try:
            value, as_of, source, raw = fetcher(symbol)
            if (value is None) or (not source):
                attempts.append(
                    {
                        "source": name,
                        "ok": False,
                        "yield_pct": None,
                        "as_of": "",
                        "error": "未命中数值",
                        "raw": raw or {},
                    }
                )
            else:
                attempts.append(
                    {
                        "source": source,
                        "ok": True,
                        "yield_pct": float(value),
                        "as_of": as_of or date.today().isoformat(),
                        "error": "",
                        "raw": raw or {},
                    }
                )
        except Exception as exc:
            attempts.append(
                {
                    "source": name,
                    "ok": False,
                    "yield_pct": None,
                    "as_of": "",
                    "error": str(exc),
                    "raw": {},
                }
            )
    return attempts


def _source_priority(source: str) -> int:
    try:
        return SOURCE_AUTHORITY_ORDER.index(str(source))
    except Exception:
        return 999


def _pick_by_authority(source_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not source_rows:
        return None
    ranked = sorted(source_rows, key=lambda x: (_source_priority(str(x.get("source", ""))), str(x.get("source", ""))))
    return ranked[0]


def _summarize_dividend_evidences(
    evidences: list[DividendEvidence],
    as_of: date,
    ttm_days: int,
) -> dict[str, Any]:
    quarter_keys = _recent_quarter_keys(as_of, count=4)
    quarter_set = set(quarter_keys)
    oldest_start = _quarter_start_from_key(quarter_keys[-1])
    window_start = oldest_start or (as_of - timedelta(days=365))
    grouped: dict[str, dict[str, Any]] = {}
    for ev in evidences:
        ex = _parse_date(ev.ex_date)
        if not ex:
            continue
        if ex > as_of:
            continue
        if _quarter_key(ex) not in quarter_set:
            continue
        source = str(ev.source or "")
        if not source:
            continue
        slot = grouped.setdefault(
            source,
            {
                "source": source,
                "per10_sum": 0.0,
                "per_share_sum": 0.0,
                "event_count": 0,
                "latest_ex_date": "",
            },
        )
        per10 = float(ev.cash_dividend_per_10_shares or 0.0)
        per_share = float(ev.cash_dividend_per_share or 0.0)
        slot["per10_sum"] = float(slot["per10_sum"]) + per10
        slot["per_share_sum"] = float(slot["per_share_sum"]) + per_share
        slot["event_count"] = int(slot["event_count"]) + 1
        ex_iso = ex.isoformat()
        if (not slot["latest_ex_date"]) or (ex_iso > str(slot["latest_ex_date"])):
            slot["latest_ex_date"] = ex_iso

    rows = list(grouped.values())
    rows.sort(key=lambda x: (_dividend_source_priority(str(x.get("source", ""))), str(x.get("source", ""))))
    distinct_per_share = sorted(
        set(
            round(float(item.get("per_share_sum", 0.0)), 6)
            for item in rows
            if item.get("per_share_sum") is not None
        )
    )
    return {
        "window_start": window_start.isoformat(),
        "window_end": as_of.isoformat(),
        "window_type": "recent_4_quarters",
        "quarters": quarter_keys,
        "source_rows": rows,
        "distinct_per_share_sum": distinct_per_share,
        "has_diff": len(distinct_per_share) > 1,
    }


def _dividend_source_priority(source: str) -> int:
    try:
        return SOURCE_DIVIDEND_AUTHORITY_ORDER.index(str(source))
    except Exception:
        return 999


def _pick_dividend_source_by_authority(source_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not source_rows:
        return None
    ranked = sorted(
        [item for item in source_rows if float(item.get("per_share_sum", 0.0)) > 0],
        key=lambda x: (_dividend_source_priority(str(x.get("source", ""))), str(x.get("source", ""))),
    )
    if not ranked:
        return None
    return ranked[0]


def _pick_dividend_source_with_consensus(source_rows: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    valid = [item for item in source_rows if float(item.get("per_share_sum", 0.0)) > 0]
    if not valid:
        return None, {"mode": "none", "distinct": [], "group_sizes": {}}

    groups: dict[float, list[dict[str, Any]]] = {}
    for item in valid:
        key = round(float(item.get("per_share_sum", 0.0)), 6)
        groups.setdefault(key, []).append(item)

    group_sizes = {str(k): len(v) for k, v in groups.items()}
    distinct = sorted(groups.keys())
    consensus_groups = [(k, v) for k, v in groups.items() if len(v) >= 2]
    if consensus_groups:
        consensus_groups.sort(key=lambda kv: (-len(kv[1]), _dividend_source_priority(str(kv[1][0].get("source", "")))))
        _, candidates = consensus_groups[0]
        candidates_sorted = sorted(
            candidates,
            key=lambda x: (_dividend_source_priority(str(x.get("source", ""))), str(x.get("source", ""))),
        )
        pick = dict(candidates_sorted[0])
        pick["selection_mode"] = "consensus"
        pick["consensus_sources"] = [str(item.get("source", "")) for item in candidates_sorted]
        return pick, {"mode": "consensus", "distinct": distinct, "group_sizes": group_sizes}

    pick = _pick_dividend_source_by_authority(valid)
    if pick is not None:
        pick = dict(pick)
        pick["selection_mode"] = "authority"
        pick["consensus_sources"] = [str(pick.get("source", ""))]
    return pick, {"mode": "authority", "distinct": distinct, "group_sizes": group_sizes}


def _fetch_dividend_evidences_multi_round(symbol: str, rounds: int = 2) -> tuple[list[DividendEvidence], dict[str, Any]]:
    fetchers = [_fetch_em_detail, _fetch_ths_detail, _fetch_cninfo, _fetch_sina_dividend_detail]
    all_rows: list[DividendEvidence] = []
    round_stats: list[dict[str, Any]] = []
    seen: set[tuple[str, str, float]] = set()

    for idx in range(max(1, rounds)):
        stat = {"round": idx + 1, "source_counts": {}}
        for fn in fetchers:
            try:
                rows = fn(symbol)
            except Exception:
                rows = []
            source_name = rows[0].source if rows else fn.__name__.replace("_fetch_", "")
            stat["source_counts"][source_name] = len(rows)
            for ev in rows:
                key = (str(ev.source), str(ev.ex_date), round(float(ev.cash_dividend_per_10_shares or 0.0), 6))
                if key in seen:
                    continue
                seen.add(key)
                all_rows.append(ev)
        round_stats.append(stat)
    return all_rows, {"rounds": round_stats, "round_count": max(1, rounds)}


def backtest_ready_yield_coverage(symbol: str, years: int = 5) -> dict[str, Any]:
    code = _normalize_symbol(symbol)
    today = date.today()
    rows: list[dict[str, Any]] = []

    for offset in range(max(1, int(years))):
        y = today.year - offset
        as_of = date(y, 12, 31)
        result = calculate_dividend_yield(
            symbol=code,
            query_date=as_of,
            future_price=None,
            use_latest_event=False,
            strict_date=False,
            nearby_days=10,
            ttm_days=365,
        )
        attempts = ((result.get("calculation_trace") or {}).get("ready_sources") or [])
        success = [item for item in attempts if item.get("ok")]
        values = [float(item.get("yield_pct")) for item in success if item.get("yield_pct") is not None]
        distinct = sorted(set(round(v, 6) for v in values))
        authority_pick = ((result.get("calculation_trace") or {}).get("authority_pick") or {})

        rows.append(
            {
                "year": y,
                "as_of": as_of.isoformat(),
                "attempted_sources": [item.get("source", "") for item in attempts],
                "success_sources": [item.get("source", "") for item in success],
                "success_count": len(success),
                "all_covered": len(success) == len(attempts) if attempts else False,
                "distinct_yield_values": distinct,
                "has_diff_across_sources": len(distinct) > 1,
                "authority_pick_source": authority_pick.get("source", ""),
                "authority_pick_yield": authority_pick.get("yield_pct", None),
                "warnings": (result.get("validation") or {}).get("warnings", []),
            }
        )

    rows.sort(key=lambda x: x.get("year", 0), reverse=True)
    return {
        "symbol": code,
        "years": max(1, int(years)),
        "authority_order": SOURCE_AUTHORITY_ORDER,
        "rows": rows,
    }


def calculate_dividend_yield(
    symbol: str,
    query_date: str | date,
    future_price: float | None = None,
    use_latest_event: bool = False,
    strict_date: bool = False,
    nearby_days: int = 10,
    ttm_days: int = 365,
) -> dict[str, Any]:
    if isinstance(query_date, date):
        qd = query_date
    else:
        parsed = _parse_date(query_date)
        if not parsed:
            raise ValueError(f"invalid query_date: {query_date}")
        qd = parsed

    code = _normalize_symbol(symbol)
    price, price_date, price_source = _fetch_price(code, qd)
    warnings: list[str] = []

    dividend_evidences, dividend_round_meta = _fetch_dividend_evidences_multi_round(code, rounds=2)
    grouped_events = _group_events(dividend_evidences)
    dividend_summary = _summarize_dividend_evidences(dividend_evidences, as_of=qd, ttm_days=int(ttm_days))
    dividend_source_rows = dividend_summary.get("source_rows", []) if isinstance(dividend_summary, dict) else []
    dividend_pick, dividend_pick_meta = _pick_dividend_source_with_consensus(dividend_source_rows)

    if use_latest_event:
        selected_event, selected_mode = _pick_latest_event(grouped_events, qd)
    else:
        selected_event, selected_mode = _pick_event_for_date(grouped_events, qd, strict_date, nearby_days)

    ttm_window_days = max(30, int(ttm_days))
    ttm_events = _filter_ttm_events(grouped_events, qd, ttm_window_days)
    ttm_per_share_sum = round(
        sum(float(item.get("cash_dividend_per_share_consensus", 0.0) or 0.0) for item in ttm_events),
        6,
    )

    selected_per_share: float | None = None
    if selected_event:
        selected_per_share = float(selected_event.get("cash_dividend_per_share_consensus", 0.0) or 0.0)

    selected_event_yield_close = _yield_pct(selected_per_share, price)
    ttm_yield_close = _yield_pct(ttm_per_share_sum, price)
    selected_event_yield_future = _yield_pct(selected_per_share, future_price)
    ttm_yield_future = _yield_pct(ttm_per_share_sum, future_price)

    source_attempts = _collect_ready_made_yield_attempts(code, as_of=qd)
    source_rows = [item for item in source_attempts if item.get("ok")]
    ready_values = [float(item.get("yield_pct")) for item in source_rows if item.get("yield_pct") is not None]
    ready_ttm_yield: float | None = float(median(ready_values)) if ready_values else None
    ready_source = "multi_source_consensus_median" if ready_values else ""
    ready_date = source_rows[0].get("as_of", qd.isoformat()) if source_rows else ""
    ready_raw = {"sources": source_rows}
    authority_pick = _pick_by_authority(source_rows)
    if authority_pick and authority_pick.get("yield_pct") is not None:
        ready_ttm_yield = float(authority_pick.get("yield_pct"))
        ready_source = str(authority_pick.get("source", ""))
        ready_date = str(authority_pick.get("as_of", ready_date))

    ttm_fallback_used = False
    if ttm_yield_close is None and ready_ttm_yield is not None:
        ttm_yield_close = ready_ttm_yield
        ttm_fallback_used = True
    if future_price is not None and ttm_yield_future is None and ready_ttm_yield is not None and price is not None and float(price) > 0:
        ttm_yield_future = round(float(ready_ttm_yield) * float(price) / float(future_price), 4)
        ttm_fallback_used = True

    if selected_event is None:
        if use_latest_event:
            warnings.append("未找到可用的最新分红事件，本次仅保留 TTM 结果作为参考。")
        elif strict_date:
            warnings.append("严格匹配除权日时未命中对应分红事件，请检查查询日期。")
        else:
            warnings.append("查询日期附近未找到匹配分红事件，本次仅保留 TTM 结果。")

    if price is None or float(price) <= 0:
        warnings.append("未获取到有效收盘价，部分收益率结果可能为空。")

    if ready_ttm_yield is None:
        warnings.append("外部现成 TTM 股息率暂无可用数据。")
    elif len(source_rows) == 1:
        warnings.append("现成 TTM 股息率仅命中 1 个来源，建议结合分红事件复核。")

    if len(ready_values) >= 2:
        distinct_vals = sorted(set(round(v, 6) for v in ready_values))
        if len(distinct_vals) > 1:
            warnings.append(f"现成 TTM 股息率多来源存在差异：{distinct_vals}，已按权威顺序择优。")

    if (
        ready_ttm_yield is not None
        and ttm_yield_close is not None
        and abs(float(ready_ttm_yield) - float(ttm_yield_close)) > 0.35
    ):
        warnings.append(
            f"现成 TTM={ready_ttm_yield:.4f}% 与事件重建 TTM={ttm_yield_close:.4f}% 差异较大，建议人工复核。"
        )

    if dividend_summary.get("has_diff"):
        if str(dividend_pick_meta.get("mode", "")) == "consensus":
            warnings.append(
                f"分红对盘存在多源差异，但以下来源已形成共识：{(dividend_pick or {}).get('consensus_sources', [])}。"
            )
        else:
            warnings.append(
                f"分红对盘存在差异：{dividend_summary.get('distinct_per_share_sum', [])}，当前按分红权威顺序 {SOURCE_DIVIDEND_AUTHORITY_ORDER} 选源。"
            )

    selected_event_output = selected_event or {
        "ex_date": ready_date or qd.isoformat(),
        "cash_dividend_per_share_consensus": None,
        "source": ready_source,
        "source_count": len(source_rows),
        "confidence": "reference_only",
        "evidences": [],
    }

    pick_mode = "ready_made_yield"
    if selected_event is not None:
        pick_mode = "latest_event" if use_latest_event else str(selected_mode or "event")
    elif ttm_yield_close is not None:
        pick_mode = "ttm_only"

    result: dict[str, Any] = {
        "symbol": code,
        "query_date": qd.isoformat(),
        "pick_mode": pick_mode,
        "price": {
            "close_price": price,
            "price_date": price_date,
            "source": price_source,
        },
        "selected_event": selected_event_output,
        "yields": {
            "selected_event_yield_pct_at_close": selected_event_yield_close,
            "selected_event_yield_pct_at_future_price": selected_event_yield_future,
            "future_price_input": future_price,
            "ttm_yield_pct_at_close": ttm_yield_close,
            "ttm_yield_pct_at_future_price": ttm_yield_future,
            "ready_made_ttm_yield_pct": ready_ttm_yield,
        },
        "ttm": {
            "days": ttm_window_days,
            "window_start": (qd - timedelta(days=ttm_window_days)).isoformat(),
            "window_end": qd.isoformat(),
            "window_type": "rolling_days",
            "quarters": dividend_summary.get("quarters", []),
            "cash_dividend_per_share_total": ttm_per_share_sum,
            "event_count": len(ttm_events),
            "events": ttm_events,
        },
        "validation": {
            "source_counts": {item.get("source", ""): 1 for item in source_rows},
            "total_evidence_count": len(dividend_evidences),
            "event_count": len(grouped_events),
            "warnings": warnings,
            "ttm_fallback_used": ttm_fallback_used,
        },
        "dividend_per_share_reconciliation": {
            "authority_order": SOURCE_DIVIDEND_AUTHORITY_ORDER,
            "window_start": dividend_summary.get("window_start", ""),
            "window_end": dividend_summary.get("window_end", qd.isoformat()),
            "window_type": dividend_summary.get("window_type", "recent_4_quarters"),
            "quarters": dividend_summary.get("quarters", []),
            "source_rows": dividend_source_rows,
            "authority_pick": dividend_pick or {},
            "selection_mode": dividend_pick_meta.get("mode", "none"),
            "consensus_sources": (dividend_pick or {}).get("consensus_sources", []),
            "round_validation": dividend_round_meta,
            "has_diff": bool(dividend_summary.get("has_diff", False)),
        },
        "calculation_trace": {
            "selected_event_formula": {
                "formula": "selected_dividend_per_share / price * 100",
                "selected_dividend_per_share": selected_per_share,
                "close_price": price,
                "future_price": future_price,
                "result_pct_at_close": selected_event_yield_close,
                "result_pct_at_future_price": selected_event_yield_future,
                "mode": selected_mode,
            },
            "ttm_formula": {
                "formula": "sum(ttm_dividend_per_share_events) / price * 100",
                "ttm_per_share_sum": ttm_per_share_sum,
                "close_price": price,
                "future_price": future_price,
                "result_pct_at_close": ttm_yield_close,
                "result_pct_at_future_price": ttm_yield_future,
                "window_days": ttm_window_days,
                "window_type": "rolling_days",
                "event_count": len(ttm_events),
                "fallback_used": ttm_fallback_used,
            },
            "raw_source": ready_raw,
            "authority_order": SOURCE_AUTHORITY_ORDER,
            "authority_pick": authority_pick or {},
            "ready_sources": source_attempts,
            "dividend_reconciliation": {
                "authority_order": SOURCE_DIVIDEND_AUTHORITY_ORDER,
                "source_rows": dividend_source_rows,
                "authority_pick": dividend_pick or {},
                "selection_mode": dividend_pick_meta.get("mode", "none"),
                "round_validation": dividend_round_meta,
            },
        },
    }
    return result

