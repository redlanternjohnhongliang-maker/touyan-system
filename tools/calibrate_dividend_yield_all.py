from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.dividend_yield_service import (
    SOURCE_AUTHORITY_ORDER,
    _fetch_ready_yield_from_eastmoney_page,
    _fetch_ready_yield_from_ths,
)


def _normalize_code(value: Any) -> str:
    raw = str(value or "").strip().upper()
    raw = raw.replace("SH", "").replace("SZ", "").replace("BJ", "")
    raw = raw.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _market_symbol(code: str) -> str:
    if code.startswith(("6", "9")):
        return f"sh{code}"
    if code.startswith(("0", "3")):
        return f"sz{code}"
    if code.startswith(("8", "4")):
        return f"bj{code}"
    return code


def _to_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _parse_per10_from_text(text: str) -> float | None:
    raw = str(text or "").replace(" ", "")
    if not raw:
        return None
    for pat in [
        r"姣?10鑲?娲?[0-9]+(?:\.[0-9]+)?)",
        r"10娲?[0-9]+(?:\.[0-9]+)?)",
        r"娲?[0-9]+(?:\.[0-9]+)?)鍏?,
    ]:
        m = __import__("re").search(pat, raw)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                continue
    return None


def _quarter_key(ts: pd.Timestamp) -> str:
    q = (int(ts.month) - 1) // 3 + 1
    return f"{int(ts.year)}Q{q}"


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


def _fetch_all_dividend_stocks() -> pd.DataFrame:
    import akshare as ak

    try:
        df = ak.stock_history_dividend()
        if df is not None and (not df.empty):
            out = df.copy()
            out["浠ｇ爜"] = out["浠ｇ爜"].astype(str).map(_normalize_code)
            out = out[out["浠ｇ爜"].str.len() == 6]
            return out.drop_duplicates(subset=["浠ｇ爜"]).reset_index(drop=True)
    except Exception:
        pass

    # 鍏滃簳: 鍒嗙孩娓呭崟椤甸潰澶辨晥鏃讹紝閫€鍖栦负鍏ˋ鑲＄エ姹狅紙鍚庣画閫氳繃鑷畻缁撴灉璇嗗埆鏄惁鏈夊垎绾級
    try:
        all_df = ak.stock_info_a_code_name()
    except Exception:
        return pd.DataFrame(columns=["浠ｇ爜", "鍚嶇О"])
    if all_df is None or all_df.empty:
        return pd.DataFrame(columns=["浠ｇ爜", "鍚嶇О"])
    out = all_df.copy()
    code_col = "code" if "code" in out.columns else ("浠ｇ爜" if "浠ｇ爜" in out.columns else out.columns[0])
    name_col = "name" if "name" in out.columns else ("鍚嶇О" if "鍚嶇О" in out.columns else out.columns[min(1, len(out.columns)-1)])
    out = out.rename(columns={code_col: "浠ｇ爜", name_col: "鍚嶇О"})
    out["浠ｇ爜"] = out["浠ｇ爜"].astype(str).map(_normalize_code)
    out = out[out["浠ｇ爜"].str.len() == 6]
    return out[["浠ｇ爜", "鍚嶇О"]].drop_duplicates(subset=["浠ｇ爜"]).reset_index(drop=True)


def _fetch_sina_prices(codes: list[str]) -> dict[str, float]:
    if not codes:
        return {}
    headers = {
        "Referer": "https://finance.sina.com.cn",
        "User-Agent": "Mozilla/5.0",
    }
    out: dict[str, float] = {}
    batch_size = 180
    for i in range(0, len(codes), batch_size):
        batch = codes[i : i + batch_size]
        sym_list = ",".join(_market_symbol(c) for c in batch)
        url = f"https://hq.sinajs.cn/list={sym_list}"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.encoding = "gbk"
            text = r.text
        except Exception:
            continue
        for line in text.splitlines():
            # var hq_str_sh600795="鍥界數鐢靛姏,4.840,4.820,4.910,..."
            if "hq_str_" not in line or "=\"" not in line:
                continue
            left, right = line.split("=\"", 1)
            sym = left.split("hq_str_")[-1].strip()
            parts = right.rstrip("\";").split(",")
            if len(parts) < 4:
                continue
            px = _to_float(parts[3])  # 褰撳墠浠?
            if px is None or px <= 0:
                continue
            code = _normalize_code(sym)
            if code:
                out[code] = px
    return out


def _calc_self_ttm_yield(symbol: str, as_of: date, price: float, ttm_days: int = 365) -> tuple[float | None, dict[str, Any]]:
    import akshare as ak

    if price <= 0:
        return None, {"reason": "invalid_price"}
    quarter_keys = _recent_quarter_keys(as_of, count=4)
    quarter_set = set(quarter_keys)
    end = pd.Timestamp(as_of)

    # 涓绘簮: 鏂版氮鍘嗗彶鍒嗙孩鏄庣粏
    try:
        df = ak.stock_history_dividend_detail(symbol=symbol, indicator="鍒嗙孩")
        if df is not None and (not df.empty):
            x = df.copy()
            if ("闄ゆ潈闄ゆ伅鏃? in x.columns) and ("娲炬伅" in x.columns):
                x["闄ゆ潈闄ゆ伅鏃?] = pd.to_datetime(x["闄ゆ潈闄ゆ伅鏃?], errors="coerce")
                x["娲炬伅"] = pd.to_numeric(x["娲炬伅"], errors="coerce")
                x = x.dropna(subset=["闄ゆ潈闄ゆ伅鏃?, "娲炬伅"])
                x = x[x["闄ゆ潈闄ゆ伅鏃?] <= end]
                x = x[x["闄ゆ潈闄ゆ伅鏃?].map(lambda d: _quarter_key(pd.Timestamp(d)) in quarter_set)]
                x = x[x["娲炬伅"] > 0]
                if not x.empty:
                    per10_sum = float(x["娲炬伅"].sum())
                    per_share_sum = per10_sum / 10.0
                    y = per_share_sum / float(price) * 100.0
                    return y, {
                        "source": "stock_history_dividend_detail",
                        "window_type": "recent_4_quarters",
                        "quarters": quarter_keys,
                        "dividend_rows": int(len(x)),
                        "per10_sum": per10_sum,
                        "per_share_sum": per_share_sum,
                        "price": float(price),
                    }
    except Exception:
        pass

    # 鍏滃簳: 宸ㄦ疆鍒嗙孩
    try:
        df2 = ak.stock_dividend_cninfo(symbol=symbol)
    except Exception as exc:
        return None, {"reason": f"dividend_api_error:{exc}"}
    if df2 is None or df2.empty:
        return None, {"reason": "no_dividend_rows"}

    y = df2.copy()
    if "闄ゆ潈鏃? not in y.columns:
        return None, {"reason": "missing_ex_date_cninfo"}
    y["闄ゆ潈鏃?] = pd.to_datetime(y["闄ゆ潈鏃?], errors="coerce")
    y = y.dropna(subset=["闄ゆ潈鏃?])
    y = y[y["闄ゆ潈鏃?] <= end]
    y = y[y["闄ゆ潈鏃?].map(lambda d: _quarter_key(pd.Timestamp(d)) in quarter_set)]
    if y.empty:
        return None, {"reason": "no_dividend_in_ttm"}

    per10_vals: list[float] = []
    for _, rec in y.iterrows():
        val = _to_float(rec.get("娲炬伅姣斾緥", None))
        if val is None:
            val = _parse_per10_from_text(rec.get("瀹炴柦鏂规鍒嗙孩璇存槑", ""))
        if val is not None and val > 0:
            per10_vals.append(float(val))
    if not per10_vals:
        return None, {"reason": "no_valid_cash_dividend_cninfo"}

    per10_sum = float(sum(per10_vals))
    per_share_sum = per10_sum / 10.0
    ret = per_share_sum / float(price) * 100.0
    return ret, {
        "source": "stock_dividend_cninfo",
        "window_type": "recent_4_quarters",
        "quarters": quarter_keys,
        "dividend_rows": int(len(per10_vals)),
        "per10_sum": per10_sum,
        "per_share_sum": per_share_sum,
        "price": float(price),
    }


def _fetch_reference_yields(symbol: str) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for source_name, fn in [
        ("eastmoney_quote_page", _fetch_ready_yield_from_eastmoney_page),
        ("ths_basic_page", _fetch_ready_yield_from_ths),
    ]:
        try:
            val, as_of, src, raw = fn(symbol)
            if val is None:
                refs.append({"source": source_name, "ok": False, "yield_pct": None, "as_of": "", "raw": raw or {}})
            else:
                refs.append(
                    {
                        "source": src or source_name,
                        "ok": True,
                        "yield_pct": float(val),
                        "as_of": as_of,
                        "raw": raw or {},
                    }
                )
        except Exception:
            refs.append({"source": source_name, "ok": False, "yield_pct": None, "as_of": "", "raw": {}})
    return refs


def _authority_pick(refs: list[dict[str, Any]]) -> dict[str, Any] | None:
    oks = [x for x in refs if x.get("ok") and x.get("yield_pct") is not None]
    if not oks:
        return None
    order = {name: idx for idx, name in enumerate(SOURCE_AUTHORITY_ORDER)}
    oks.sort(key=lambda x: order.get(str(x.get("source", "")), 999))
    return oks[0]


def _linear_fit(xs: list[float], ys: list[float]) -> dict[str, Any]:
    if len(xs) < 2 or len(xs) != len(ys):
        return {"ok": False}
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    den = sum((x - x_mean) ** 2 for x in xs)
    if den == 0:
        return {"ok": False}
    a = num / den
    b = y_mean - a * x_mean
    preds = [a * x + b for x in xs]
    mae_before = sum(abs(y - x) for x, y in zip(xs, ys)) / len(xs)
    mae_after = sum(abs(y - p) for y, p in zip(ys, preds)) / len(ys)
    return {
        "ok": True,
        "slope": a,
        "intercept": b,
        "sample_n": len(xs),
        "mae_before": mae_before,
        "mae_after": mae_after,
    }


@dataclass
class Args:
    max_stocks: int
    workers: int
    ttm_days: int
    out: str


def run(args: Args) -> dict[str, Any]:
    base_df = _fetch_all_dividend_stocks()
    if args.max_stocks > 0:
        base_df = base_df.head(args.max_stocks)

    codes = [str(x) for x in base_df["浠ｇ爜"].tolist()]
    names = {str(row["浠ｇ爜"]): str(row.get("鍚嶇О", "")) for _, row in base_df.iterrows()}

    prices = _fetch_sina_prices(codes)
    as_of = date.today()

    rows: list[dict[str, Any]] = []
    self_ok = 0
    ref_ok = 0

    def process(code: str) -> dict[str, Any]:
        name = names.get(code, "")
        px = prices.get(code)
        if px is None or px <= 0:
            return {
                "symbol": code,
                "name": name,
                "price": None,
                "self_yield_pct": None,
                "self_trace": {"reason": "missing_price"},
                "refs": [],
                "authority_ref": None,
                "diff_vs_authority": None,
                "issue": "missing_price",
            }

        self_yield, self_trace = _calc_self_ttm_yield(code, as_of=as_of, price=px, ttm_days=args.ttm_days)
        refs = _fetch_reference_yields(code)
        authority = _authority_pick(refs)

        diff = None
        issue = "ok"
        if self_yield is None:
            issue = str((self_trace or {}).get("reason", "self_calc_missing"))
        if authority is None:
            issue = (issue + "|ref_missing") if issue != "ok" else "ref_missing"
        if (self_yield is not None) and (authority is not None):
            diff = float(self_yield) - float(authority.get("yield_pct"))
            if abs(diff) >= 1.0:
                issue = "large_diff"

        return {
            "symbol": code,
            "name": name,
            "price": px,
            "self_yield_pct": self_yield,
            "self_trace": self_trace,
            "refs": refs,
            "authority_ref": authority,
            "diff_vs_authority": diff,
            "issue": issue,
        }

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        future_map = {pool.submit(process, c): c for c in codes}
        done = 0
        total = len(codes)
        for fut in as_completed(future_map):
            done += 1
            rec = fut.result()
            rows.append(rec)
            if rec.get("self_yield_pct") is not None:
                self_ok += 1
            if rec.get("authority_ref") is not None:
                ref_ok += 1
            if done % 100 == 0:
                print(f"progress: {done}/{total}")

    rows.sort(key=lambda x: x.get("symbol", ""))

    fit_x: list[float] = []
    fit_y: list[float] = []
    per_source_pairs: dict[str, tuple[list[float], list[float]]] = {}

    for rec in rows:
        s = rec.get("self_yield_pct")
        a = rec.get("authority_ref") or {}
        r = a.get("yield_pct") if isinstance(a, dict) else None
        src = a.get("source") if isinstance(a, dict) else None
        if (s is None) or (r is None) or (src is None):
            continue
        fit_x.append(float(s))
        fit_y.append(float(r))
        xs, ys = per_source_pairs.setdefault(str(src), ([], []))
        xs.append(float(s))
        ys.append(float(r))

    source_fits = {src: _linear_fit(xs, ys) for src, (xs, ys) in per_source_pairs.items()}
    global_fit = _linear_fit(fit_x, fit_y)

    issue_counts: dict[str, int] = {}
    for rec in rows:
        issue = str(rec.get("issue", "")) or "unknown"
        issue_counts[issue] = issue_counts.get(issue, 0) + 1

    return {
        "meta": {
            "as_of": as_of.isoformat(),
            "authority_order": ["eastmoney_quote_page", "ths_basic_page"],
            "universe_count": len(codes),
            "self_ok_count": self_ok,
            "ref_ok_count": ref_ok,
            "ttm_days": int(args.ttm_days),
        },
        "calibration": {
            "global_fit": global_fit,
            "per_source_fit": source_fits,
        },
        "diagnostics": {
            "issue_counts": issue_counts,
        },
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="鍏ㄥ競鍦哄垎绾㈣偂鑲℃伅鐜囪嚜绠?vs 涓滆储/鍚岃姳椤虹幇鎴愬€兼牎鍑?)
    parser.add_argument("--max-stocks", type=int, default=300, help="鏈€澶ц偂绁ㄦ暟锛?=鍏ㄩ儴")
    parser.add_argument("--workers", type=int, default=8, help="骞跺彂绾跨▼鏁?)
    parser.add_argument("--ttm-days", type=int, default=365, help="鑷畻TTM绐楀彛澶╂暟")
    parser.add_argument("--out", default="tools/dividend_yield_calibration_latest.json", help="杈撳嚭璺緞")
    args_ns = parser.parse_args()

    args = Args(
        max_stocks=max(0, int(args_ns.max_stocks)),
        workers=max(1, int(args_ns.workers)),
        ttm_days=max(90, int(args_ns.ttm_days)),
        out=str(args_ns.out),
    )

    result = run(args)

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = PROJECT_ROOT / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out_path)


if __name__ == "__main__":
    # 绂佺敤浠ｇ悊鐜锛屽噺灏戣繛鎺ュ共鎵?
    for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"]:
        os.environ.pop(key, None)
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"
    main()

