from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.collectors.eastmoney_adapter import fetch_stock_bundle, fetch_market_headlines
from src.collectors.jiuyangongshe_collector import fetch_daily_reports_for_user
from src.services.symbol_resolver import resolve_stock_input


# --------------- 日缓存（研报+要闻） ---------------

def _daily_cache_path(out_dir: str | Path, report_date: date) -> Path:
    """返回当日研报+要闻缓存文件路径。"""
    return Path(out_dir) / f"_daily_reports_cache_{report_date.isoformat()}.json"


def _save_daily_cache(
    cache_path: Path,
    reports: list[dict[str, Any]],
    headlines: list[dict[str, Any]],
    headlines_meta: dict[str, Any],
    target_user: str,
    report_target_date: date,
    window_days: int,
) -> None:
    data = {
        "_cache_version": 1,
        "report_target_date": report_target_date.isoformat(),
        "target_user": target_user,
        "window_days": window_days,
        "reports": reports,
        "headlines": headlines,
        "headlines_meta": headlines_meta,
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _load_daily_cache(
    cache_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]] | None:
    """读取日缓存，返回 (reports, headlines, headlines_meta) 或 None。"""
    if not cache_path.exists():
        return None
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        return (
            data.get("reports", []),
            data.get("headlines", []),
            data.get("headlines_meta", {}),
        )
    except Exception:
        return None


def _dump_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _load_prompt_text(prompt_path: Path) -> tuple[str, str]:
    """读取提示词内容，返回 (text, error)。"""
    try:
        text = prompt_path.read_text(encoding="utf-8")
        return text, ""
    except Exception as exc:
        return "", f"提示词读取失败: {exc}"


def _attach_prompt_to_payload(payload: dict[str, Any], prompt_path: Path, prompt_text: str, prompt_error: str = "") -> None:
    """将提示词附加到导出 payload，便于单文件直接喂给 AI。"""
    payload["llm_instruction"] = {
        "source_path": str(prompt_path),
        "attached": bool(prompt_text),
        "error": prompt_error,
        "content": prompt_text,
    }


def _parse_date(value: Any) -> date | None:
    raw = str(value).strip()
    if not raw:
        return None
    m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", raw)
    if not m:
        m = re.search(r"(\d{4})(\d{2})(\d{2})", raw)
    if not m:
        return None
    try:
        y = int(m.group(1))
        mo = int(m.group(2))
        d = int(m.group(3))
        return date(y, mo, d)
    except Exception:
        return None


def _keep_recent_records(
    records: list[dict[str, Any]],
    date_keys: list[str],
    max_days: int,
    max_items: int,
    ref_date: date | None = None,
) -> list[dict[str, Any]]:
    if not records:
        return []
    anchor = ref_date or date.today()
    with_date: list[tuple[date, dict[str, Any]]] = []
    no_date: list[dict[str, Any]] = []
    for rec in records:
        rec_date = None
        for key in date_keys:
            rec_date = _parse_date(rec.get(key, ""))
            if rec_date:
                break
        if rec_date:
            with_date.append((rec_date, rec))
        else:
            no_date.append(rec)
    with_date.sort(key=lambda x: x[0], reverse=True)
    recent = [rec for rec_date, rec in with_date if 0 <= (anchor - rec_date).days <= max_days]
    if recent:
        return recent[:max_items]
    fallback = [rec for _, rec in with_date[:max_items]]
    if len(fallback) < max_items and no_date:
        fallback.extend(no_date[: max_items - len(fallback)])
    return fallback[:max_items]


def _normalize_code(value: Any) -> str:
    raw = str(value).strip().upper()
    raw = raw.replace("SH", "").replace("SZ", "").replace("BJ", "")
    raw = raw.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    return "".join(ch for ch in raw if ch.isdigit())[-6:]


def _extract_stock_aliases(symbol: str, bundle: dict[str, Any]) -> list[str]:
    aliases = {symbol, _normalize_code(symbol)}
    for section in ("financial_indicator", "yjbb", "news", "notice"):
        for rec in bundle.get(section, [])[:50]:
            if not isinstance(rec, dict):
                continue
            for key in ("SECURITY_NAME_ABBR", "股票简称", "证券简称", "名称", "name"):
                val = str(rec.get(key, "")).strip()
                if val and len(val) <= 20 and not any(ch.isdigit() for ch in val):
                    aliases.add(val)
    return sorted([x for x in aliases if x], key=len, reverse=True)


def _report_hits_stock(report: dict[str, Any], aliases: list[str]) -> bool:
    text = f"{report.get('title', '')}\n{report.get('content', '')}"
    return any(alias in text for alias in aliases if alias)


def _pick_fields(record: dict[str, Any], fields: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in fields:
        if key in record:
            out[key] = record.get(key)
    return out


def _compact_financial(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        "SECURITY_CODE",
        "SECURITY_NAME_ABBR",
        "REPORT_DATE",
        "REPORT_TYPE",
        "NOTICE_DATE",
        "EPSJB",
        "TOTALOPERATEREVE",
        "PARENTNETPROFIT",
        "TOTALOPERATEREVETZ",
        "PARENTNETPROFITTZ",
        "ROEJQ",
        "XSMLL",
        "LD",
        "SD",
        "ZCFZL",
    ]
    return [_pick_fields(rec, fields) for rec in records[:2]]


def _compact_hist(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    for rec in records[:60]:
        date_val = rec.get("date", rec.get("日期", rec.get("交易日期", "")))
        compacted.append(
            {
                "date": date_val,
                "open": rec.get("open", rec.get("开盘", "")),
                "high": rec.get("high", rec.get("最高", "")),
                "low": rec.get("low", rec.get("最低", "")),
                "close": rec.get("close", rec.get("收盘", "")),
                "volume": rec.get("volume", rec.get("成交量", "")),
                "amount": rec.get("amount", rec.get("成交额", "")),
                "pct_change": rec.get("涨跌幅", rec.get("pct_chg", rec.get("涨跌幅(%)", ""))),
            }
        )
    return compacted


def _compact_news(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for rec in records[:30]:
        output.append(
            {
                "date": rec.get("发布时间", rec.get("日期", rec.get("publish_date", ""))),
                "title": rec.get("新闻标题", rec.get("title", "")),
                "content": rec.get("新闻内容", rec.get("content", "")),
                "source": rec.get("文章来源", rec.get("source", "")),
                "url": rec.get("新闻链接", rec.get("url", "")),
            }
        )
    return output


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _extract_date_close_map(records: list[dict[str, Any]]) -> dict[str, float]:
    date_keys = ["date", "日期", "交易日期", "时间"]
    close_keys = ["close", "收盘", "收盘价"]
    out: dict[str, float] = {}
    for rec in records or []:
        if not isinstance(rec, dict):
            continue
        raw_date = ""
        for k in date_keys:
            val = rec.get(k)
            if val not in (None, ""):
                raw_date = str(val).strip()
                break
        if not raw_date:
            continue
        day = raw_date[:10]
        close_val: float | None = None
        for k in close_keys:
            close_val = _to_float(rec.get(k))
            if close_val is not None:
                break
        if close_val is None or close_val <= 0:
            continue
        out[day] = close_val
    return out


def _sample_stdev(values: list[float]) -> float | None:
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    var = sum((x - mean) ** 2 for x in values) / (n - 1)
    return math.sqrt(var)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    arr = sorted(values)
    n = len(arr)
    mid = n // 2
    if n % 2 == 1:
        return arr[mid]
    return (arr[mid - 1] + arr[mid]) / 2.0


def _robust_sigma_mad(values: list[float]) -> float | None:
    med = _median(values)
    if med is None:
        return None
    abs_dev = [abs(x - med) for x in values]
    mad = _median(abs_dev)
    if mad is None:
        return None
    return 1.4826 * mad


def _win_rate(values: list[float]) -> float | None:
    if not values:
        return None
    wins = sum(1 for v in values if v > 0)
    return wins / len(values)


def _information_ratio(values: list[float]) -> float | None:
    if not values:
        return None
    sigma = _sample_stdev(values)
    if sigma is None or sigma <= 0:
        return None
    mean_val = sum(values) / len(values)
    return mean_val / sigma


def _inverse_3x3(mat: list[list[float]]) -> list[list[float]] | None:
    if len(mat) != 3 or any(len(row) != 3 for row in mat):
        return None
    a, b, c = mat[0]
    d, e, f = mat[1]
    g, h, i = mat[2]

    det = (
        a * (e * i - f * h)
        - b * (d * i - f * g)
        + c * (d * h - e * g)
    )
    if abs(det) < 1e-12:
        return None

    inv = [
        [(e * i - f * h) / det, (c * h - b * i) / det, (b * f - c * e) / det],
        [(f * g - d * i) / det, (a * i - c * g) / det, (c * d - a * f) / det],
        [(d * h - e * g) / det, (b * g - a * h) / det, (a * e - b * d) / det],
    ]
    return inv


def _mat_vec_mul(mat: list[list[float]], vec: list[float]) -> list[float]:
    return [sum(row[j] * vec[j] for j in range(len(vec))) for row in mat]


def _compute_alpha_regression(
    return_series: list[dict[str, Any]],
    window: int,
) -> dict[str, Any]:
    if len(return_series) < window:
        return {
            "available": False,
            "window": window,
            "n": len(return_series),
            "reason": f"收益序列长度{len(return_series)}小于窗口{window}",
            "alpha": None,
            "beta_sector": None,
            "beta_index": None,
            "t_alpha": None,
            "r2": None,
        }

    tail = return_series[-window:]
    x_rows: list[list[float]] = []
    y_vals: list[float] = []
    for rec in tail:
        rs = _to_float(rec.get("stock_return"))
        rb = _to_float(rec.get("sector_return"))
        ri = _to_float(rec.get("index_return"))
        if rs is None or rb is None or ri is None:
            continue
        x_rows.append([1.0, rb, ri])
        y_vals.append(rs)

    n = len(y_vals)
    k = 3
    if n <= k:
        return {
            "available": False,
            "window": window,
            "n": n,
            "reason": "有效样本不足，无法做三变量回归",
            "alpha": None,
            "beta_sector": None,
            "beta_index": None,
            "t_alpha": None,
            "r2": None,
        }

    xtx = [[0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0, 0.0, 0.0]]
    xty = [0.0, 0.0, 0.0]
    for row, y in zip(x_rows, y_vals):
        for r in range(3):
            xty[r] += row[r] * y
            for c in range(3):
                xtx[r][c] += row[r] * row[c]

    xtx_inv = _inverse_3x3(xtx)
    if xtx_inv is None:
        return {
            "available": False,
            "window": window,
            "n": n,
            "reason": "回归矩阵奇异，无法稳定求解",
            "alpha": None,
            "beta_sector": None,
            "beta_index": None,
            "t_alpha": None,
            "r2": None,
        }

    beta = _mat_vec_mul(xtx_inv, xty)
    alpha, beta_sector, beta_index = beta

    sse = 0.0
    y_mean = sum(y_vals) / n
    sst = 0.0
    for row, y in zip(x_rows, y_vals):
        y_hat = alpha + beta_sector * row[1] + beta_index * row[2]
        sse += (y - y_hat) ** 2
        sst += (y - y_mean) ** 2

    df = n - k
    t_alpha: float | None = None
    if df > 0:
        s2 = sse / df
        var_alpha = s2 * xtx_inv[0][0]
        if var_alpha > 0:
            se_alpha = math.sqrt(var_alpha)
            if se_alpha > 0:
                t_alpha = alpha / se_alpha

    r2: float | None
    if sst > 0:
        r2 = 1.0 - sse / sst
    else:
        r2 = None

    return {
        "available": True,
        "window": window,
        "n": n,
        "reason": "",
        "alpha": alpha,
        "beta_sector": beta_sector,
        "beta_index": beta_index,
        "t_alpha": t_alpha,
        "r2": r2,
    }


def _compute_relative_strength_metrics(
    stock_hist: list[dict[str, Any]],
    sector_hist: list[dict[str, Any]],
    index_hist: list[dict[str, Any]],
) -> dict[str, Any]:
    stock_map = _extract_date_close_map(stock_hist)
    sector_map = _extract_date_close_map(sector_hist)
    index_map = _extract_date_close_map(index_hist)

    common_dates = sorted(set(stock_map) & set(sector_map) & set(index_map))
    if len(common_dates) < 2:
        return {
            "computable": False,
            "reason": "共同交易日不足2天，无法计算D序列与σ",
            "common_dates_count": len(common_dates),
            "D_series": [],
            "sigma": {"D1": None, "D2": None, "D3": None},
            "win_rate": {"10": {"D1": None, "D2": None, "D3": None}, "20": {"D1": None, "D2": None, "D3": None}},
            "information_ratio": {"10": {"D1": None, "D2": None, "D3": None}, "20": {"D1": None, "D2": None, "D3": None}},
            "rolling_alpha": {
                "available": False,
                "window": None,
                "fallback_used": False,
                "reason": "共同交易日不足",
                "alpha": None,
                "beta_sector": None,
                "beta_index": None,
                "t_alpha": None,
                "r2": None,
                "window20": {"available": False, "window": 20, "n": 0, "reason": "共同交易日不足", "alpha": None, "beta_sector": None, "beta_index": None, "t_alpha": None, "r2": None},
                "window10": {"available": False, "window": 10, "n": 0, "reason": "共同交易日不足", "alpha": None, "beta_sector": None, "beta_index": None, "t_alpha": None, "r2": None},
            },
            "z_scores": {"z5": {"D1": None, "D2": None, "D3": None}, "z10": {"D1": None, "D2": None, "D3": None}, "z20": {"D1": None, "D2": None, "D3": None}},
        }

    d_series: list[dict[str, Any]] = []
    return_series: list[dict[str, Any]] = []
    for prev_day, cur_day in zip(common_dates[:-1], common_dates[1:]):
        ps0, ps1 = stock_map[prev_day], stock_map[cur_day]
        pb0, pb1 = sector_map[prev_day], sector_map[cur_day]
        pi0, pi1 = index_map[prev_day], index_map[cur_day]
        if min(ps0, pb0, pi0) <= 0:
            continue
        rs = ps1 / ps0 - 1.0
        rb = pb1 / pb0 - 1.0
        ri = pi1 / pi0 - 1.0
        d1 = rs - rb
        d2 = rs - ri
        d3 = rb - ri
        d_series.append(
            {
                "date": cur_day,
                "D1_stock_vs_sector": d1,
                "D2_stock_vs_index": d2,
                "D3_sector_vs_index": d3,
            }
        )
        return_series.append(
            {
                "date": cur_day,
                "stock_return": rs,
                "sector_return": rb,
                "index_return": ri,
            }
        )

    d1_vals = [x["D1_stock_vs_sector"] for x in d_series]
    d2_vals = [x["D2_stock_vs_index"] for x in d_series]
    d3_vals = [x["D3_sector_vs_index"] for x in d_series]

    sigma_d1 = _sample_stdev(d1_vals)
    sigma_d2 = _sample_stdev(d2_vals)
    sigma_d3 = _sample_stdev(d3_vals)

    robust_sigma_d1 = _robust_sigma_mad(d1_vals)
    robust_sigma_d2 = _robust_sigma_mad(d2_vals)
    robust_sigma_d3 = _robust_sigma_mad(d3_vals)

    def _window_stats(n: int) -> dict[str, Any]:
        if len(d_series) < n:
            return {
                "window": n,
                "available": False,
                "reason": f"D序列长度{len(d_series)}小于窗口{n}",
                "excess": {"D1": None, "D2": None, "D3": None},
                "z": {"D1": None, "D2": None, "D3": None},
                "win_rate": {"D1": None, "D2": None, "D3": None},
                "information_ratio": {"D1": None, "D2": None, "D3": None},
            }
        tail = d_series[-n:]
        d1_tail = [x["D1_stock_vs_sector"] for x in tail]
        d2_tail = [x["D2_stock_vs_index"] for x in tail]
        d3_tail = [x["D3_sector_vs_index"] for x in tail]
        ex_d1 = sum(x["D1_stock_vs_sector"] for x in tail)
        ex_d2 = sum(x["D2_stock_vs_index"] for x in tail)
        ex_d3 = sum(x["D3_sector_vs_index"] for x in tail)

        def _z(excess: float, sigma: float | None) -> float | None:
            if sigma is None or sigma <= 0:
                return None
            return excess / (sigma * math.sqrt(n))

        return {
            "window": n,
            "available": True,
            "reason": "",
            "excess": {"D1": ex_d1, "D2": ex_d2, "D3": ex_d3},
            "z": {
                "D1": _z(ex_d1, sigma_d1),
                "D2": _z(ex_d2, sigma_d2),
                "D3": _z(ex_d3, sigma_d3),
            },
            "win_rate": {
                "D1": _win_rate(d1_tail),
                "D2": _win_rate(d2_tail),
                "D3": _win_rate(d3_tail),
            },
            "information_ratio": {
                "D1": _information_ratio(d1_tail),
                "D2": _information_ratio(d2_tail),
                "D3": _information_ratio(d3_tail),
            },
        }

    w5 = _window_stats(5)
    w10 = _window_stats(10)
    w20 = _window_stats(20)

    reg20 = _compute_alpha_regression(return_series, window=20)
    reg10 = _compute_alpha_regression(return_series, window=10)
    if reg20.get("available"):
        rolling_alpha = {
            "available": True,
            "window": 20,
            "fallback_used": False,
            "reason": "",
            "alpha": reg20.get("alpha"),
            "beta_sector": reg20.get("beta_sector"),
            "beta_index": reg20.get("beta_index"),
            "t_alpha": reg20.get("t_alpha"),
            "r2": reg20.get("r2"),
            "window20": reg20,
            "window10": reg10,
        }
    elif reg10.get("available"):
        rolling_alpha = {
            "available": True,
            "window": 10,
            "fallback_used": True,
            "reason": "20日窗口不可用，已降级到10日",
            "alpha": reg10.get("alpha"),
            "beta_sector": reg10.get("beta_sector"),
            "beta_index": reg10.get("beta_index"),
            "t_alpha": reg10.get("t_alpha"),
            "r2": reg10.get("r2"),
            "window20": reg20,
            "window10": reg10,
        }
    else:
        rolling_alpha = {
            "available": False,
            "window": None,
            "fallback_used": False,
            "reason": "20日和10日窗口均无法完成回归",
            "alpha": None,
            "beta_sector": None,
            "beta_index": None,
            "t_alpha": None,
            "r2": None,
            "window20": reg20,
            "window10": reg10,
        }

    return {
        "computable": True,
        "reason": "",
        "common_dates_count": len(common_dates),
        "common_date_range": {
            "start": common_dates[0],
            "end": common_dates[-1],
        },
        "return_points_count": len(d_series),
        "D_series": d_series,
        "sigma": {
            "D1": sigma_d1,
            "D2": sigma_d2,
            "D3": sigma_d3,
        },
        "robust_sigma_mad": {
            "D1": robust_sigma_d1,
            "D2": robust_sigma_d2,
            "D3": robust_sigma_d3,
        },
        "win_rate": {
            "10": w10.get("win_rate", {"D1": None, "D2": None, "D3": None}),
            "20": w20.get("win_rate", {"D1": None, "D2": None, "D3": None}),
        },
        "information_ratio": {
            "10": w10.get("information_ratio", {"D1": None, "D2": None, "D3": None}),
            "20": w20.get("information_ratio", {"D1": None, "D2": None, "D3": None}),
        },
        "rolling_alpha": rolling_alpha,
        "window_stats": {
            "5": w5,
            "10": w10,
            "20": w20,
        },
        "z_scores": {
            "z5": w5.get("z", {"D1": None, "D2": None, "D3": None}),
            "z10": w10.get("z", {"D1": None, "D2": None, "D3": None}),
            "z20": w20.get("z", {"D1": None, "D2": None, "D3": None}),
        },
    }


def _build_stock_context(symbol: str, bundle: dict[str, Any], ref_date: date | None = None) -> tuple[dict[str, Any], list[str], dict[str, Any]]:
    """构建单只股票的 stock_context、aliases 列表和诊断计数。

    返回 (stock_context_dict, aliases, counts_dict)。
    """
    aliases = _extract_stock_aliases(symbol, bundle)
    stock_news = _keep_recent_records(
        bundle.get("news", []),
        date_keys=["发布时间", "日期", "publish_date", "report_date"],
        max_days=30,
        max_items=30,
        ref_date=ref_date,
    )
    rs_metrics = _compute_relative_strength_metrics(
        bundle.get("hist", []),
        bundle.get("sector_kline_60d", []),
        bundle.get("csi300_kline_60d", []),
    )
    ctx: dict[str, Any] = {
        "aliases": aliases,
        "company_profile": bundle.get("company_profile", {}),
        "concept_tags": bundle.get("concept_tags", []),
        "theme_highlights": bundle.get("theme_highlights", []),
        "zygc_12m": bundle.get("zygc", []),
        "financial_recent": _compact_financial(bundle.get("financial_indicator", [])),
        "price_last_60d": _compact_hist(bundle.get("hist", [])),
        "sector_name": bundle.get("sector_name", ""),
        "sector_kline_60d": bundle.get("sector_kline_60d", []),
        "csi300_kline_60d": bundle.get("csi300_kline_60d", []),
        "news_last_30d_relevant": _compact_news(stock_news),
        "headlines_top5_merged": bundle.get("market_hot_news_top10", []),
        "eastmoney_headlines_top10": bundle.get("market_hot_news_top10", []),
        "earnings_brief": bundle.get("yjbb", []),
        "notice_recent_30d_with_content": bundle.get("notice_recent_30d_with_content", []),
        "gdhs_recent": bundle.get("gdhs", []),
        "ggcg_recent": bundle.get("ggcg", []),
        "relative_strength_metrics": rs_metrics,
    }
    counts: dict[str, Any] = {
        "company_profile": 1 if bundle.get("company_profile") else 0,
        "concept_tags": len(bundle.get("concept_tags", [])),
        "theme_highlights": len(bundle.get("theme_highlights", [])),
        "zygc_12m": len(bundle.get("zygc", [])),
        "financial_recent": len(bundle.get("financial_indicator", [])),
        "price_last_60d": len(bundle.get("hist", [])),
        "sector_kline_60d": len(bundle.get("sector_kline_60d", [])),
        "csi300_kline_60d": len(bundle.get("csi300_kline_60d", [])),
        "news_last_30d_relevant": len(stock_news),
        "headlines_top5_merged": len(bundle.get("market_hot_news_top10", [])),
        "eastmoney_headlines_top10": len(bundle.get("market_hot_news_top10", [])),
        "notice_recent_30d_with_content": len(bundle.get("notice_recent_30d_with_content", [])),
        "relative_strength_metrics": 1 if rs_metrics.get("computable") else 0,
    }
    return ctx, aliases, counts


def _build_ai_payload(
    symbol: str,
    mode: str,
    target_date: date,
    target_user: str,
    strict_date: bool,
    reports: list[dict[str, Any]],
    bundle: dict[str, Any],
) -> dict[str, Any]:
    ctx, aliases, counts = _build_stock_context(symbol, bundle, ref_date=target_date)
    report_items: list[dict[str, Any]] = []
    for rep in reports:
        report_items.append(
            {
                "title": rep.get("title", ""),
                "report_date": rep.get("report_date", ""),
                "source_url": rep.get("source_url", ""),
                "matched_to_stock": _report_hits_stock(rep, aliases),
                "raw_content_text": rep.get("content", ""),
                "raw_content_html": rep.get("content_html", ""),
            }
        )

    counts["reports_today"] = len(report_items)
    payload: dict[str, Any] = {
        "meta": {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "target_date": target_date.isoformat(),
            "target_user": target_user,
            "symbol": symbol,
            "mode": mode,
            "strict_date": strict_date,
            "purpose": "供LLM直接读取的原始整合输入，不包含自动分析结论",
        },
        "input_schema_note": {
            "reports_today": "九阳公社当天研报原文，raw_content_text 保留段落换行，raw_content_html 保留网页原始结构",
            "stock_context": "东方财富接口的近期个股上下文，已做时间收敛（近一年/近一月）",
        },
        "reports_today": report_items,
        "stock_context": ctx,
        "diagnostics": {
            "eastmoney": bundle.get("_diagnostics", []),
            "errors": bundle.get("_errors", []),
            "counts": counts,
        },
    }
    return payload


def _build_markdown(payload: dict[str, Any]) -> str:
    meta = payload["meta"]
    reports = payload.get("reports_today", [])
    diag = payload.get("diagnostics", {})

    # 判断是否多股票格式
    stock_contexts: dict[str, Any] | None = payload.get("stock_contexts")
    single_stock: dict[str, Any] | None = payload.get("stock_context")

    lines: list[str] = []
    # 标题
    if stock_contexts:
        names = list(stock_contexts.keys())
        lines.append(f"# AI 输入包（多股票）- {', '.join(names)}")
    else:
        lines.append(f"# AI 输入包（非自动分析）- {meta.get('symbol', '')}")
    lines.append("")
    lines.append(f"- 生成时间: {meta.get('generated_at', '')}")
    lines.append(f"- 目标日期: {meta.get('target_date', '')}")
    lines.append(f"- 目标用户: {meta.get('target_user', '')}")
    if stock_contexts:
        symbols = meta.get("symbols", [])
        lines.append(f"- 股票列表: {', '.join(symbols)}")
    else:
        lines.append(f"- 股票: {meta.get('symbol', '')}")
    lines.append(f"- 东财模式: {meta.get('mode', '')}")
    lines.append(f"- 严格当天: {meta.get('strict_date', False)}")
    lines.append("")

    # 研报部分
    lines.append("## 上半部分：当天研报原文（保留格式）")
    lines.append(f"- 抓取条数: {len(reports)}")
    lines.append("")
    for idx, rep in enumerate(reports, start=1):
        lines.append(f"### 研报 {idx}")
        lines.append(f"- 标题: {rep.get('title', '')}")
        lines.append(f"- 日期: {rep.get('report_date', '')}")
        lines.append(f"- 链接: {rep.get('source_url', '')}")
        lines.append(f"- 命中该股票: {rep.get('matched_to_stock', False)}")
        lines.append("")
        lines.append("```text")
        lines.append(str(rep.get("raw_content_text", "")))
        lines.append("```")
        lines.append("")

    # 个股上下文——多股票 vs 单股票
    _stock_data_keys = [
        "company_profile",
        "concept_tags",
        "theme_highlights",
        "zygc_12m",
        "financial_recent",
        "price_last_60d",
        "sector_name",
        "sector_kline_60d",
        "csi300_kline_60d",
        "news_last_30d_relevant",
        "headlines_top5_merged",
        "eastmoney_headlines_top10",
        "earnings_brief",
        "notice_recent_30d_with_content",
        "gdhs_recent",
        "ggcg_recent",
        "relative_strength_metrics",
    ]

    def _render_stock_section(stock: dict[str, Any], section_counts: dict[str, Any], heading_level: str = "##") -> None:
        lines.append(f"{heading_level} 指定个股近期上下文")
        lines.append(f"- 公司概况(行业+主营+简介): {section_counts.get('company_profile', 0)} 条")
        lines.append(f"- 概念题材标签(东财含入选理由): {section_counts.get('concept_tags', 0)} 条")
        lines.append(f"- 题材亮点(经营范围+主营+竞争优势): {section_counts.get('theme_highlights', 0)} 条")
        lines.append(f"- 主营构成(12个月): {section_counts.get('zygc_12m', 0)} 条")
        lines.append(f"- 财务摘要(最近期): {section_counts.get('financial_recent', 0)} 条")
        lines.append(f"- 股价(日K最近60天): {section_counts.get('price_last_60d', 0)} 条")
        lines.append(f"- 行业板块K线(60天): {section_counts.get('sector_kline_60d', 0)} 条")
        lines.append(f"- 沪深300K线(60天): {section_counts.get('csi300_kline_60d', 0)} 条")
        lines.append(f"- 新闻(近30天相关): {section_counts.get('news_last_30d_relevant', 0)} 条")
        lines.append(f"- 要闻(东财+同花顺，前5去重): {section_counts.get('headlines_top5_merged', section_counts.get('eastmoney_headlines_top10', 0))} 条")
        lines.append(f"- 公告正文(近30天): {section_counts.get('notice_recent_30d_with_content', 0)} 条")
        lines.append(f"- 强弱统计(D/σ/z): {section_counts.get('relative_strength_metrics', 0)} 组")
        lines.append("")
        for key in _stock_data_keys:
            records = stock.get(key, [] if key not in ("company_profile", "sector_name") else ("" if key == "sector_name" else {}))
            if isinstance(records, str):
                # sector_name 等标量字段
                lines.append(f"{'#' * (len(heading_level) + 1)} {key}")
                lines.append(records if records else "(空)")
                lines.append("")
                continue
            count = len(records) if isinstance(records, list) else (1 if records else 0)
            lines.append(f"{'#' * (len(heading_level) + 1)} {key} ({count} 条)")
            if not records:
                lines.append("(空)")
                lines.append("")
                continue
            lines.append("```json")
            lines.append(json.dumps(records, ensure_ascii=False, indent=2, default=str))
            lines.append("```")
            lines.append("")

    if stock_contexts:
        # 多股票：每只股票一个大段
        per_stock_diag = diag.get("per_stock", {})
        for stock_name, stock_data in stock_contexts.items():
            lines.append(f"## 下半部分：{stock_name} 近期上下文")
            s_counts = per_stock_diag.get(stock_name, {}).get("counts", {})
            _render_stock_section(stock_data, s_counts, heading_level="##")
    elif single_stock:
        lines.append("## 下半部分：指定个股近期上下文")
        counts = diag.get("counts", {})
        _render_stock_section(single_stock, counts, heading_level="##")

    lines.append("## 接口诊断")
    diagnostics = diag.get("eastmoney", [])
    if diagnostics:
        for item in diagnostics:
            lines.append(
                f"- {item.get('endpoint', '')}: ok={item.get('ok')}, rows={item.get('rows', 0)}, "
                f"duration_ms={item.get('duration_ms', 0)}, error={item.get('error', '')}"
            )
    else:
        lines.append("- (无)")
    lines.append("")
    if diag.get("errors"):
        lines.append("## 硬错误")
        for err in diag.get("errors", []):
            lines.append(f"- {err}")
        lines.append("")

    llm_instruction = payload.get("llm_instruction", {})
    if llm_instruction:
        lines.append("## 附带提示词（可直接喂给AI）")
        lines.append(f"- 来源: {llm_instruction.get('source_path', '')}")
        lines.append(f"- 已附带: {llm_instruction.get('attached', False)}")
        if llm_instruction.get("error"):
            lines.append(f"- 读取错误: {llm_instruction.get('error', '')}")
        lines.append("")
        content = str(llm_instruction.get("content", ""))
        if content:
            lines.append("```markdown")
            lines.append(content)
            lines.append("```")
            lines.append("")
    return "\n".join(lines)


def _parse_target_date(value: str | None) -> date:
    if not value:
        return date.today()
    d = _parse_date(value)
    if not d:
        raise ValueError(f"非法日期: {value}")
    return d


def _weekend_to_friday(d: date) -> date:
    # 周六(5)回退1天；周日(6)回退2天
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d - timedelta(days=2)
    return d


def main() -> None:
    parser = argparse.ArgumentParser(
        description="自动抓取当天研报 + 指定个股近期数据，并导出可直接喂给AI的输入文件"
    )
    parser.add_argument("--symbol", default="", help="股票代码，例如 600120（scope=reports时可省略）")
    parser.add_argument("--symbols", default="", help="多股票，逗号分隔，例如 600120,000001（优先级高于 --symbol）")
    parser.add_argument("--mode", default="deep", choices=["quick", "deep"], help="东方财富抓取模式")
    parser.add_argument("--target-user", default="盘前纪要", help="九阳公社目标用户名")
    parser.add_argument("--target-date", default="", help="目标日期，默认今天，格式 YYYY-MM-DD")
    parser.add_argument("--window-days", type=int, default=1, help="提取最近 N 篇研报（篇数），默认 1")
    parser.add_argument(
        "--disable-weekend-shift",
        action="store_true",
        help="关闭周末自动回退到周五研报日（默认开启）",
    )
    parser.add_argument(
        "--allow-fallback",
        action="store_true",
        help="若当天无研报，允许回退到最近一篇（默认不回退）",
    )
    parser.add_argument("--out-prefix", default="ai_input_bundle", help="输出文件名前缀")
    parser.add_argument("--out-dir", default="tools", help="输出目录")
    parser.add_argument(
        "--include-prompt",
        action="store_true",
        default=True,
        help="将提示词文件内容附加到输出 JSON/MD（默认开启）",
    )
    parser.add_argument(
        "--no-include-prompt",
        action="store_false",
        dest="include_prompt",
        help="不在输出中附带提示词",
    )
    parser.add_argument(
        "--prompt-path",
        default="prompts/gemini_custom_instruction.md",
        help="要附加到输出文件中的提示词路径（相对项目根目录）",
    )
    parser.add_argument(
        "--scope",
        default="all",
        choices=["reports", "stock", "all"],
        help="拉取范围: reports=仅研报+要闻, stock=仅个股数据, all=全部",
    )
    parser.add_argument(
        "--overwrite-latest",
        action="store_true",
        help="覆盖写固定 latest 文件名，避免每天生成新文件",
    )
    args = parser.parse_args()

    os.environ.setdefault("TQDM_DISABLE", "1")

    target_date = _parse_target_date(args.target_date)
    report_target_date = target_date if args.disable_weekend_shift else _weekend_to_friday(target_date)
    strict_date = not args.allow_fallback
    scope = args.scope  # reports / stock / all
    is_historical = target_date != date.today()  # 输入了历史日期时跳过抓取实时要闻

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_path = _daily_cache_path(out_dir, report_target_date)
    prompt_path = Path(args.prompt_path)
    if not prompt_path.is_absolute():
        prompt_path = (PROJECT_ROOT / prompt_path).resolve()
    prompt_text = ""
    prompt_error = ""
    if args.include_prompt:
        prompt_text, prompt_error = _load_prompt_text(prompt_path)

    # ---- scope=reports: 只拉研报+要闻，不需要股票代码 ----
    if scope == "reports":
        reports = fetch_daily_reports_for_user(
            target_user=args.target_user,
            target_date=report_target_date,
            window_days=max(1, args.window_days),
            strict_date=strict_date,
        )
        # 历史日期不抓实时要闻
        if is_historical:
            print(f"历史日期模式（{target_date}）：跳过抓取实时要闻", file=sys.stderr)
            headlines, headlines_meta = [], {}
        else:
            headlines, headlines_meta = fetch_market_headlines(max_items_per_source=5, max_total=10)
        _save_daily_cache(cache_path, reports, headlines, headlines_meta,
                          args.target_user, report_target_date, args.window_days)
        # 输出一个精简 payload（不含个股上下文）
        payload: dict[str, Any] = {
            "meta": {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "target_date": report_target_date.isoformat(),
                "report_target_date": report_target_date.isoformat(),
                "request_date": target_date.isoformat(),
                "target_user": args.target_user,
                "scope": "reports",
                "is_historical": is_historical,
                "purpose": "仅包含研报" + ("，历史日期已跳过要闻" if is_historical else "+要闻") + "，个股数据请另行 scope=stock 获取",
            },
            "reports_today": [{
                "title": r.get("title", ""),
                "report_date": r.get("report_date", ""),
                "source_url": r.get("source_url", ""),
                "raw_content_text": r.get("content", ""),
            } for r in reports],
            "headlines": headlines,
            "headlines_meta": headlines_meta,
        }
        _attach_prompt_to_payload(payload, prompt_path, prompt_text, prompt_error)
        base = f"研报要闻_{report_target_date.isoformat()}"
        json_path = out_dir / f"{base}.json"
        md_path = out_dir / f"{base}.md"
        _dump_json(json_path, payload)
        md_path.write_text(_build_markdown(payload), encoding="utf-8")
        print(str(json_path.resolve()))
        print(str(md_path.resolve()))
        return

    # ---- scope=stock / scope=all: 需要股票代码 ----
    # 解析多股票 --symbols 或单股票 --symbol
    symbol_list: list[str] = []
    if args.symbols.strip():
        symbol_list = [s.strip() for s in args.symbols.split(",") if s.strip()]
    elif args.symbol.strip():
        symbol_list = [args.symbol.strip()]

    if not symbol_list:
        raise ValueError("scope=stock/all 需要提供 --symbol 或 --symbols 参数")

    # 逐个解析
    resolved_pairs: list[tuple[str, str]] = []  # [(code, name), ...]
    for _raw_sym in symbol_list:
        resolved = resolve_stock_input(_raw_sym)
        if not resolved.get("ok"):
            raise ValueError(str(resolved.get("message", f"无法解析股票: {_raw_sym}")))
        _code = str(resolved.get("code", "")).strip()
        _name = str(resolved.get("name", "")).strip()
        resolved_pairs.append((_code, _name))

    is_multi = len(resolved_pairs) > 1

    if is_multi:
        # ======= 多股票拆分导出模式 =======
        # 拉研报（仅第一次或 scope=all 时）
        headlines_merged: list[dict[str, Any]] = []
        if scope == "all":
            reports = fetch_daily_reports_for_user(
                target_user=args.target_user,
                target_date=report_target_date,
                window_days=max(1, args.window_days),
                strict_date=strict_date,
            )
        else:
            # scope=stock: 从缓存加载研报+要闻
            cached = _load_daily_cache(cache_path)
            if cached:
                reports, headlines_merged, _ = cached
            else:
                print("WARNING: 未找到当日研报缓存，请先执行 scope=reports。本次输出将不含研报。", file=sys.stderr)
                reports = []

        per_stock_bundles: list[tuple[str, str, dict[str, Any]]] = []

        for _pair_idx, (_code, _name) in enumerate(resolved_pairs):
            print(f"正在抓取 [{_pair_idx + 1}/{len(resolved_pairs)}] {_code} {_name} ...", file=sys.stderr)
            # 第一只股票拉 headline（仅非历史日期），后续不重复拉
            _include_hl = (_pair_idx == 0) and (scope == "all") and (not is_historical)
            _bundle = fetch_stock_bundle(_code, mode=args.mode, include_headlines=_include_hl, as_of_date=report_target_date)
            if _pair_idx == 0 and scope == "all":
                if not is_historical:
                    headlines_merged = _bundle.get("market_hot_news_top10", [])
                # scope=all 时顺便保存缓存
                _save_daily_cache(cache_path, reports, headlines_merged, {},
                                  args.target_user, report_target_date, args.window_days)

            # 补充 headline 到每只股票（引用同一份）
            if headlines_merged:
                _bundle["market_hot_news_top10"] = headlines_merged

            # 兜底名称提取
            if not _name:
                _name_keys_inner = ["股票简称", "证券简称", "名称", "SECURITY_NAME_ABBR"]
                for _sec in ("yjbb", "financial_indicator", "gdhs", "ggcg", "notice", "news"):
                    for _rec in _bundle.get(_sec, [])[:5]:
                        if not isinstance(_rec, dict):
                            continue
                        for _k in _name_keys_inner:
                            _v = str(_rec.get(_k, "")).strip()
                            if _v and _v.lower() != "none" and len(_v) <= 20:
                                _name = _v
                                break
                        if _name:
                            break
                    if _name:
                        break

            resolved_name = _name or _code
            per_stock_bundles.append((_code, resolved_name, _bundle))

        # 多股票：逐股输出文件（共享同日研报/要闻，各自个股上下文）
        for _code, _name, _bundle in per_stock_bundles:
            payload = _build_ai_payload(
                symbol=_code,
                mode=args.mode,
                target_date=report_target_date,
                target_user=args.target_user,
                strict_date=strict_date,
                reports=reports,
                bundle=_bundle,
            )
            payload["meta"]["request_date"] = target_date.isoformat()
            payload["meta"]["report_target_date"] = report_target_date.isoformat()
            payload["meta"]["symbol_input"] = args.symbols or args.symbol
            payload["meta"]["symbol_name"] = _name
            payload["meta"]["scope"] = scope
            payload["meta"]["multi_source_symbols"] = [c for c, _ in resolved_pairs]
            payload["meta"]["multi_output"] = True
            _attach_prompt_to_payload(payload, prompt_path, prompt_text, prompt_error)

            safe_name = _name or _code
            for _ch in r'\/:*?"<>|':
                safe_name = safe_name.replace(_ch, "_")
            base = f"{safe_name}_{report_target_date.isoformat()}"
            json_path = out_dir / f"{base}.json"
            md_path = out_dir / f"{base}.md"

            _dump_json(json_path, payload)
            md_path.write_text(_build_markdown(payload), encoding="utf-8")
            print(str(json_path.resolve()))
            print(str(md_path.resolve()))
        return

    # ======= 单股票模式（原逻辑） =======
    resolved_symbol, resolved_name = resolved_pairs[0]

    if scope == "stock":
        # 仅拉个股数据，跳过要闻
        bundle = fetch_stock_bundle(resolved_symbol, mode=args.mode, include_headlines=False, as_of_date=report_target_date)
        # 尝试从日缓存加载研报+要闻（历史日期不加载要闻）
        cached = _load_daily_cache(cache_path)
        if cached:
            reports_from_cache, headlines_from_cache, _ = cached
            if not is_historical:
                bundle["market_hot_news_top10"] = headlines_from_cache
            reports = reports_from_cache
        else:
            print("WARNING: 未找到当日研报缓存，请先执行 scope=reports。本次输出将不含研报与要闻。", file=sys.stderr)
            reports = []
    else:
        # scope=all: 原始逻辑——全部拉取
        reports = fetch_daily_reports_for_user(
            target_user=args.target_user,
            target_date=report_target_date,
            window_days=max(1, args.window_days),
            strict_date=strict_date,
        )
        # 历史日期不抓实时要闻
        _include_hl = not is_historical
        if is_historical:
            print(f"历史日期模式（{target_date}）：跳过抓取实时要闻", file=sys.stderr)
        bundle = fetch_stock_bundle(resolved_symbol, mode=args.mode, include_headlines=_include_hl, as_of_date=report_target_date)
        # 顺便保存日缓存（副产品）
        _hl = bundle.get("market_hot_news_top10", []) if not is_historical else []
        _save_daily_cache(cache_path, reports, _hl, {},
                          args.target_user, report_target_date, args.window_days)

    payload = _build_ai_payload(
        symbol=resolved_symbol,
        mode=args.mode,
        target_date=report_target_date,
        target_user=args.target_user,
        strict_date=strict_date,
        reports=reports,
        bundle=bundle,
    )
    payload["meta"]["request_date"] = target_date.isoformat()
    payload["meta"]["report_target_date"] = report_target_date.isoformat()
    payload["meta"]["symbol_input"] = args.symbol
    payload["meta"]["symbol_name"] = resolved_name
    payload["meta"]["scope"] = scope
    _attach_prompt_to_payload(payload, prompt_path, prompt_text, prompt_error)

    # 兜底：如果 resolver 没返回名称，从 bundle 数据中提取
    if not resolved_name:
        _name_keys = ["股票简称", "证券简称", "名称", "SECURITY_NAME_ABBR"]
        for _sec in ("yjbb", "financial_indicator", "gdhs", "ggcg", "notice", "news"):
            for _rec in bundle.get(_sec, [])[:5]:
                if not isinstance(_rec, dict):
                    continue
                for _k in _name_keys:
                    _v = str(_rec.get(_k, "")).strip()
                    if _v and _v.lower() != "none" and len(_v) <= 20:
                        resolved_name = _v
                        break
                if resolved_name:
                    break
            if resolved_name:
                break

    # 文件名：股票名称_分析日期.json / .md
    safe_name = resolved_name or resolved_symbol
    # 去除 Windows 文件名非法字符
    for _ch in r'\/:*?"<>|':
        safe_name = safe_name.replace(_ch, "_")
    base = f"{safe_name}_{report_target_date.isoformat()}"
    json_path = out_dir / f"{base}.json"
    md_path = out_dir / f"{base}.md"

    _dump_json(json_path, payload)
    md_path.write_text(_build_markdown(payload), encoding="utf-8")

    print(str(json_path.resolve()))
    print(str(md_path.resolve()))


if __name__ == "__main__":
    main()
