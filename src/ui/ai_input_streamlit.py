from __future__ import annotations

import calendar
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import io
import queue
from pathlib import Path
import json
import os
import re
import time
import zipfile
from typing import Any

import streamlit as st
import pandas as pd

from src.services.ai_input_runner_live import PROJECT_ROOT, AiInputExportResult, run_ai_input_export
from src.services.dividend_yield_service import calculate_dividend_yield
from src.services.symbol_resolver import resolve_stock_input, search_stock_suggestions, warmup_search_api


_MONTH_LABELS = [
    "1月", "2月", "3月", "4月", "5月", "6月",
    "7月", "8月", "9月", "10月", "11月", "12月",
]


def _date_selector(label: str, default: date, key_prefix: str, disabled: bool = False) -> date:
    """用年/月/日三个 selectbox 替代 st.date_input，避免 locale 乱码。"""
    st.caption(label)
    c_y, c_m, c_d = st.columns(3)
    years = list(range(2015, date.today().year + 2))
    with c_y:
        year = st.selectbox(
            "年", years,
            index=years.index(default.year) if default.year in years else len(years) - 1,
            key=f"{key_prefix}_y", disabled=disabled,
        )
    with c_m:
        month = st.selectbox(
            "月", list(range(1, 13)),
            index=default.month - 1,
            key=f"{key_prefix}_m", disabled=disabled,
            format_func=lambda m: _MONTH_LABELS[m - 1],
        )
    max_day = calendar.monthrange(year, month)[1]
    day_default = min(default.day, max_day)
    with c_d:
        day = st.selectbox(
            "日", list(range(1, max_day + 1)),
            index=day_default - 1,
            key=f"{key_prefix}_d", disabled=disabled,
        )
    return date(year, month, day)


def _inject_styles() -> None:
    # 搜索 API 预热只需执行一次
    if not st.session_state.get("_warmup_started_v8"):
        st.session_state["_warmup_started_v8"] = True
        import threading
        threading.Thread(target=warmup_search_api, daemon=True).start()

    # 样式必须每次 rerun 都注入，因为 Streamlit 每次重建页面 DOM
    st.markdown(
        """
<style>
:root {
  --bg-0: #030815;
  --bg-1: #071325;
  --bg-2: #0c1d37;
  --surface: rgba(7, 18, 36, 0.90);
  --surface-2: rgba(10, 24, 48, 0.96);
  --line: rgba(90, 220, 255, 0.28);
  --line-strong: rgba(90, 220, 255, 0.46);
  --text: #f5fbff;
  --muted: #e3ecff;
  --cyan: #5ce6ff;
  --blue: #77a8ff;
  --purple: #9b8cff;
  --pink: #ff61dc;
  --glow: 0 0 0 1px rgba(92, 230, 255, 0.18), 0 0 34px rgba(92, 230, 255, 0.14);
  --shadow: 0 18px 60px rgba(0, 0, 0, 0.40);
}

html, body, [class*="css"] {
  font-family: "Inter", "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
  color: var(--text);
}

[data-testid="stAppViewContainer"] {
  background:
    radial-gradient(circle at 12% 12%, rgba(92, 230, 255, 0.16), transparent 24%),
    radial-gradient(circle at 88% 8%, rgba(155, 140, 255, 0.18), transparent 22%),
    radial-gradient(circle at 50% 100%, rgba(255, 97, 220, 0.10), transparent 26%),
    linear-gradient(135deg, var(--bg-0) 0%, var(--bg-1) 44%, var(--bg-2) 100%);
}

.block-container {
  max-width: 1240px;
  padding-top: 1.35rem;
  padding-bottom: 2.8rem;
}

[data-testid="stHeader"] {
  background: rgba(0,0,0,0);
}

[data-testid="stSidebar"] {
  background: linear-gradient(180deg, rgba(6, 16, 30, 0.98), rgba(10, 24, 43, 0.96));
  border-right: 1px solid var(--line);
}

[data-testid="stSidebar"] * {
  color: var(--text) !important;
}

h1, h2, h3, label, p, li, span, div {
  color: inherit;
}

label,
[data-testid="stWidgetLabel"] p {
  color: #ffffff !important;
  font-weight: 700 !important;
  font-size: 0.98rem !important;
}

h2 {
  font-size: 1.26rem !important;
  font-weight: 800 !important;
  letter-spacing: -0.02em;
}

p, li, .stCaption, .stMarkdown, label {
  line-height: 1.72;
}

small, .stCaption {
  color: var(--muted) !important;
  font-size: 0.92rem !important;
}

.hero {
  position: relative;
  overflow: hidden;
  background:
    linear-gradient(135deg, rgba(9, 21, 40, 0.96), rgba(11, 30, 58, 0.90)),
    linear-gradient(90deg, rgba(92, 230, 255, 0.10), rgba(155, 140, 255, 0.08));
  border: 1px solid var(--line);
  border-radius: 26px;
  padding: 28px 30px 24px;
  box-shadow: var(--shadow);
  margin-bottom: 18px;
}

.hero::before {
  content: "";
  position: absolute;
  inset: 0;
  background:
    radial-gradient(circle at 0% 0%, rgba(92, 230, 255, 0.18), transparent 28%),
    radial-gradient(circle at 100% 0%, rgba(155, 140, 255, 0.22), transparent 24%);
  pointer-events: none;
}

.hero .eyebrow {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 12px;
  border-radius: 999px;
  border: 1px solid var(--line-strong);
  background: rgba(92, 230, 255, 0.14);
  color: #c6f7ff;
  font-size: 0.80rem;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  box-shadow: 0 0 16px rgba(92, 230, 255, 0.12);
}

.hero h2 {
  margin: 16px 0 0 0;
  font-size: 2rem !important;
  font-weight: 900 !important;
  color: #ffffff;
  text-shadow: 0 0 24px rgba(94, 160, 255, 0.18);
}

.hero p {
  margin: 12px 0 0 0;
  max-width: 800px;
  color: #e2eeff;
  font-size: 1rem;
}

.hero-badges {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 18px;
}

.hero-badges span {
  display: inline-flex;
  align-items: center;
  padding: 8px 14px;
  border-radius: 999px;
  background: rgba(255,255,255,0.08);
  border: 1px solid rgba(255,255,255,0.16);
  color: #ffffff;
  font-size: 0.88rem;
  font-weight: 700;
  backdrop-filter: blur(8px);
}

.panel {
  background: linear-gradient(180deg, rgba(7, 17, 32, 0.94), rgba(9, 22, 40, 0.90));
  border: 1px solid var(--line);
  border-radius: 22px;
  padding: 18px 18px 8px 18px;
  box-shadow: var(--shadow);
  backdrop-filter: blur(14px);
}

.card {
  background: linear-gradient(180deg, rgba(12, 27, 50, 0.96), rgba(9, 18, 34, 0.94));
  border-radius: 16px;
  border: 1px solid var(--line);
  padding: 14px 16px;
  margin: 8px 0;
  box-shadow: var(--glow);
}

.card .k {
  font-size: 0.82rem;
  color: #d7e7ff;
  margin-bottom: 6px;
}

.card .v {
  font-size: 1.82rem;
  font-weight: 900;
  color: #ffffff;
}

.path-box,
[data-testid="stExpander"] {
  background: rgba(8, 19, 36, 0.86);
  border: 1px solid rgba(92, 230, 255, 0.18);
  border-radius: 14px;
}

.path-box {
  padding: 12px 14px;
}

.warn-note {
  border-left: 4px solid var(--pink);
  padding: 10px 12px;
  background: rgba(255, 97, 220, 0.10);
  color: #ffe3fa;
  border-radius: 8px;
}

[data-testid="stRadio"] > div {
  gap: 12px;
}

[data-testid="stRadio"] label {
  background: rgba(255,255,255,0.12);
  border: 1px solid rgba(255,255,255,0.24);
  border-radius: 999px;
  padding: 8px 14px;
}

[data-testid="stRadio"] label * {
  color: #ffffff !important;
}

button[data-baseweb="tab"] {
  height: 44px;
  border-radius: 999px;
  padding: 0 16px;
  background: rgba(255,255,255,0.08);
  border: 1px solid rgba(255,255,255,0.14);
}

button[data-baseweb="tab"] p {
  color: #eef6ff !important;
  font-size: 0.94rem;
  font-weight: 800;
}

button[data-baseweb="tab"][aria-selected="true"] {
  background: linear-gradient(90deg, rgba(92, 230, 255, 0.28), rgba(155, 140, 255, 0.28));
  border-color: var(--line-strong);
  box-shadow: 0 0 18px rgba(92, 230, 255, 0.18);
}

.stButton > button,
.stDownloadButton > button {
  border-radius: 14px;
  border: 1px solid rgba(116, 232, 255, 0.42);
  min-height: 44px;
  height: auto;
  padding: 0.58rem 0.95rem;
  font-weight: 800;
  font-size: 0.98rem;
  color: #ffffff;
  background: linear-gradient(180deg, rgba(25, 53, 92, 0.98), rgba(8, 20, 40, 0.99));
  box-shadow: 0 10px 28px rgba(0, 0, 0, 0.26);
}

.stButton > button:hover,
.stDownloadButton > button:hover {
  border-color: rgba(116, 232, 255, 0.72);
  box-shadow: 0 0 22px rgba(92, 230, 255, 0.24);
}

.stButton > button[kind="primary"] {
  background: linear-gradient(90deg, rgba(92, 230, 255, 0.98), rgba(119, 168, 255, 0.98));
  color: #05101f;
  border: none;
}

[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stDateInput"] input,
textarea,
[data-baseweb="select"] > div {
  border-radius: 14px !important;
  border: 1px solid rgba(116, 232, 255, 0.44) !important;
  background: rgba(7, 18, 34, 0.98) !important;
  color: #ffffff !important;
  font-size: 1rem !important;
  box-shadow: inset 0 0 0 1px rgba(255,255,255,0.06);
}

[data-testid="stTextInput"] input:focus,
[data-testid="stNumberInput"] input:focus,
textarea:focus,
[data-baseweb="select"] > div:focus-within {
  border-color: rgba(116, 232, 255, 0.82) !important;
  box-shadow: 0 0 0 1px rgba(116, 232, 255, 0.52), 0 0 0 4px rgba(116, 232, 255, 0.14) !important;
}

input::placeholder,
textarea::placeholder {
  color: #d7e3f8 !important;
}

[data-baseweb="select"] * {
  color: #ffffff !important;
}

.stMetric {
  background: linear-gradient(180deg, rgba(12, 28, 50, 0.96), rgba(8, 18, 31, 0.94));
  border: 1px solid var(--line);
  border-radius: 16px;
  padding: 12px 14px;
  box-shadow: var(--glow);
}

[data-testid="stMetricLabel"] *,
[data-testid="stMetricValue"] * {
  color: #ffffff !important;
}

[data-testid="stMetricLabel"] * {
  font-size: 0.96rem !important;
  font-weight: 700 !important;
}

[data-testid="stDataFrame"],
.stDataFrame {
  border: 1px solid rgba(116, 232, 255, 0.18);
  border-radius: 14px;
  background: rgba(8, 19, 36, 0.86);
}

[data-testid="stDataFrame"] * {
  color: #f5fbff !important;
}

.stInfo, .stSuccess, .stWarning, .stError {
  border-radius: 16px;
}

.stInfo {
  background: rgba(92, 230, 255, 0.10);
}

.stWarning {
  background: rgba(255, 176, 0, 0.12);
}

.stSuccess {
  background: rgba(60, 220, 150, 0.12);
}

code, pre, .stCode {
  font-family: "JetBrains Mono", "Cascadia Code", Consolas, monospace !important;
}

@media (max-width: 768px) {
  .hero {
    padding: 20px 18px 18px;
    border-radius: 18px;
  }
  .hero h2 {
    font-size: 1.45rem !important;
  }
  .panel {
    padding: 14px 14px 4px;
  }
}
</style>
        """,
        unsafe_allow_html=True,
    )

def _safe_counts(payload: dict[str, Any]) -> dict[str, Any]:
    diag = payload.get("diagnostics", {}) if isinstance(payload, dict) else {}
    # 单股票格式
    if "counts" in diag:
        return diag["counts"]
    # 多股票格式：合并各股票的 counts
    per_stock = diag.get("per_stock", {})
    if per_stock:
        merged: dict[str, int] = {}
        for _stock_diag in per_stock.values():
            for k, v in _stock_diag.get("counts", {}).items():
                merged[k] = merged.get(k, 0) + (v if isinstance(v, int) else 0)
        return merged
    return {}


def _fix_mojibake_text(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    if re.search(r"[\u4e00-\u9fff]", text):
        return text
    if not re.search(r"[¶·ÐÑÒÓÊËÎÏ×Ø¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¸¹º»¼½¾¿]", text):
        return text
    try:
        fixed = text.encode("latin1", errors="ignore").decode("gbk", errors="ignore")
        if re.search(r"[\u4e00-\u9fff]", fixed):
            return fixed
    except Exception:
        pass
    return text


def _render_reports(payload: dict[str, Any]) -> None:
    reports = payload.get("reports_today", [])
    st.subheader("上半部分：研报原文")
    if not reports:
        st.info("当前研报目标日无数据（周末默认会回退到周五，可在侧边栏关闭）。")
        return

    _ver = st.session_state.get("_report_render_ver", 0)
    for idx, rep in enumerate(reports, start=1):
        title = str(rep.get("title", "") or f"研报{idx}")
        with st.expander(f"{idx}. {title}", expanded=(idx == 1)):
            c1, c2, c3 = st.columns(3)
            c1.caption(f"日期: {rep.get('report_date', '')}")
            c2.caption(f"命中个股: {rep.get('matched_to_stock', False)}")
            c3.caption(f"链接: {rep.get('source_url', '')}")
            st.text_area(
                label=f"正文 {idx}",
                value=str(rep.get("raw_content_text", "")),
                height=260,
                key=f"report_text_v{_ver}_{idx}",
            )


def _render_stock_context(payload: dict[str, Any]) -> None:
    section_map = [
        ("company_profile", "公司概况（行业+主营+简介）"),
        ("concept_tags", "概念题材标签（含入选理由）"),
        ("theme_highlights", "题材亮点（经营范围+竞争优势）"),
        ("zygc_12m", "主营构成（近12个月）"),
        ("financial_recent", "财务摘要（最近2期）"),
        ("price_last_60d", "股价（日K近60天）"),
        ("sector_kline_60d", "行业板块K线（60天）"),
        ("csi300_kline_60d", "沪深300 K线（60天）"),
        ("news_last_30d_relevant", "相关新闻（近30天）"),
        ("headlines_top5_merged", "要闻（东财+同花顺，前5去重）"),
        ("earnings_brief", "业绩简表"),
        ("notice_recent_30d_with_content", "公告主要内容（近30天）"),
        ("gdhs_recent", "股东户数"),
        ("ggcg_recent", "高管增减持"),
    ]

    # 多股票格式
    stock_contexts: dict[str, Any] | None = payload.get("stock_contexts")
    if stock_contexts:
        for stock_label, stock_data in stock_contexts.items():
            st.subheader(f"个股上下文：{stock_label}")
            if not stock_data.get("headlines_top5_merged") and stock_data.get("eastmoney_headlines_top10"):
                stock_data["headlines_top5_merged"] = stock_data.get("eastmoney_headlines_top10", [])
            for key, title in section_map:
                records = stock_data.get(key, [])
                with st.expander(f"[{stock_label}] {title} | {len(records)} 条", expanded=False):
                    if not records:
                        st.caption("空")
                        continue
                    if key == "price_last_60d":
                        try:
                            st.dataframe(pd.DataFrame(records), width="stretch", height=280)
                        except Exception:
                            st.json(records)
                    else:
                        st.json(records)
        return

    # 单股票格式
    stock = payload.get("stock_context", {})
    if not stock.get("headlines_top5_merged") and stock.get("eastmoney_headlines_top10"):
        stock["headlines_top5_merged"] = stock.get("eastmoney_headlines_top10", [])
    st.subheader("下半部分：个股近期上下文")

    for key, title in section_map:
        records = stock.get(key, [])
        with st.expander(f"{title} | {len(records)} 条", expanded=False):
            if not records:
                st.caption("空")
                continue
            if key == "price_last_60d":
                try:
                    st.dataframe(pd.DataFrame(records), width="stretch", height=280)
                except Exception:
                    st.json(records)
            else:
                st.json(records)


def _render_diagnostics(payload: dict[str, Any]) -> None:
    diag = payload.get("diagnostics", {})
    st.subheader("接口诊断")
    rows = diag.get("eastmoney", [])
    if rows:
        try:
            st.dataframe(pd.DataFrame(rows), width="stretch", height=280)
        except Exception:
            st.json(rows)
    else:
        st.caption("无诊断信息")

    errors = diag.get("errors", [])
    if errors:
        st.warning("存在硬错误（其他字段仍可展示）")
        for err in errors:
            st.text(err)


def _render_downloads(result: AiInputExportResult) -> None:
    json_paths = getattr(result, "json_paths", None) or [result.json_path]
    md_paths = getattr(result, "md_paths", None) or [result.md_path]
    md_by_stem = {p.stem: p for p in md_paths}

    if len(json_paths) <= 1:
        json_text = result.json_path.read_text(encoding="utf-8")
        md_text = result.md_path.read_text(encoding="utf-8")
        c1, c2 = st.columns(2)
        c1.download_button(
            label="下载 JSON",
            data=json_text,
            file_name=result.json_path.name,
            mime="application/json",
            width="stretch",
        )
        c2.download_button(
            label="下载 Markdown",
            data=md_text,
            file_name=result.md_path.name,
            mime="text/markdown",
            width="stretch",
        )
        return

    st.caption(f"检测到 {len(json_paths)} 个个股文件，可分别下载：")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for jp in json_paths:
            if jp.exists():
                zf.writestr(jp.name, jp.read_text(encoding="utf-8"))
        for mp in md_paths:
            if mp.exists():
                zf.writestr(mp.name, mp.read_text(encoding="utf-8"))
    zip_bytes = zip_buffer.getvalue()
    st.download_button(
        label="一键下载全部（ZIP）",
        data=zip_bytes,
        file_name=f"ai_input_multi_{date.today().isoformat()}.zip",
        mime="application/zip",
        width="stretch",
        key="dl_zip_all_multi",
    )

    for idx, jp in enumerate(json_paths, start=1):
        mp = md_by_stem.get(jp.stem)
        c1, c2 = st.columns(2)
        c1.download_button(
            label=f"下载 JSON #{idx} · {jp.stem}",
            data=jp.read_text(encoding="utf-8"),
            file_name=jp.name,
            mime="application/json",
            width="stretch",
            key=f"dl_json_{jp.name}_{idx}",
        )
        if mp and mp.exists():
            c2.download_button(
                label=f"下载 Markdown #{idx}",
                data=mp.read_text(encoding="utf-8"),
                file_name=mp.name,
                mime="text/markdown",
                width="stretch",
                key=f"dl_md_{mp.name}_{idx}",
            )
        else:
            c2.caption("对应 Markdown 未找到")


def _render_kpi_cards(counts: dict[str, Any]) -> None:
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            f"""
<div class="card">
  <div class="k">研报条数</div>
  <div class="v">{int(counts.get("reports_today", 0))}</div>
</div>
            """,
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f"""
<div class="card">
  <div class="k">主营构成(12个月)</div>
  <div class="v">{int(counts.get("zygc_12m", 0))}</div>
</div>
            """,
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f"""
<div class="card">
  <div class="k">相关新闻(30天)</div>
  <div class="v">{int(counts.get("news_last_30d_relevant", 0))}</div>
</div>
            """,
            unsafe_allow_html=True,
        )


def _resolve_symbol_for_ui(input_value: str, field_label: str) -> dict[str, Any] | None:
    parsed = resolve_stock_input(input_value.strip())
    if not parsed.get("ok"):
        st.error(f"{field_label}解析失败: {parsed.get('message', '')}")
        return None
    return parsed


def _search_stocks(query: str) -> list[str]:
    """每次按键都会被 st_searchbox 调用，返回候选列表。"""
    rows = search_stock_suggestions(query, limit=12)
    return [
        f"{r['code']}  {r['name']}  ({r['pinyin'].upper()})"
        for r in rows
    ]


def _symbol_searchbox(label: str, key: str, placeholder: str, clear_on_submit: bool = True) -> str:
    """稳定的股票搜索输入框，避免输入过程中被自动重置。"""
    input_key = f"{key}__text"
    selected_key = f"{key}__selected"
    just_selected_key = f"{key}__just_selected"
    clear_pending_key = f"{key}__clear_pending"
    fill_pending_key = f"{key}__fill_pending"

    pending_fill = st.session_state.pop(fill_pending_key, None)
    pending_clear = bool(st.session_state.pop(clear_pending_key, False))
    if pending_fill is not None:
        st.session_state[input_key] = str(pending_fill)
    elif pending_clear:
        st.session_state[input_key] = ""

    typed = st.text_input(label, key=input_key, placeholder=placeholder)
    query = str(typed or "").strip()

    if query:
        suggestions = _search_stocks(query)[:8]
        if suggestions:
            st.caption("匹配结果（点击即可选中）")
        elif len(query) >= 2:
            st.caption("未找到匹配项，可继续输入完整代码或名称。")
        if suggestions:
            cols = st.columns(2)
            for idx, item in enumerate(suggestions):
                col = cols[idx % 2]
                if col.button(item, key=f"{key}__pick__{idx}", use_container_width=True):
                    st.session_state[selected_key] = item
                    if clear_on_submit:
                        st.session_state[just_selected_key] = True
                        st.session_state[clear_pending_key] = True
                    else:
                        st.session_state[fill_pending_key] = item
                    st.rerun()

    selected = str(st.session_state.pop(selected_key, "") or "").strip()
    return selected or query


def _run_export(
    symbol: str,
    mode: str,
    target_user: str,
    target_date: date,
    window_days: int,
    allow_fallback: bool,
    disable_weekend_shift: bool,
    out_prefix: str,
    out_dir: str,
    overwrite_latest: bool,
    scope: str = "all",
    symbols: str = "",
) -> None:
    # scope=reports 不需要股票代码；其他两种需要
    if scope != "reports":
        if symbols.strip():
            # 多股票模式：不做 UI 端解析，交给 export 脚本
            resolved_symbol = ""
            parsed = {}
        elif symbol.strip():
            parsed = _resolve_symbol_for_ui(symbol, "信息合并股票")
            if not parsed:
                return
            resolved_symbol = str(parsed.get("code", "")).strip()
        else:
            st.error("请输入股票代码或名称")
            return
    else:
        resolved_symbol = ""
        parsed = {}

    scope_labels = {"reports": "研报+要闻", "stock": "个股数据", "all": "全部"}
    scope_label = scope_labels.get(scope, scope)
    _is_multi = bool(symbols.strip())
    if _is_multi:
        _num_stocks = len([s for s in symbols.split(",") if s.strip()])
        scope_label += f"（{_num_stocks}只股票合并）"
    status = st.status(f"开始执行【{scope_label}】导出...", expanded=True)
    status.write("参数检查完成，准备启动抓取任务。")
    progress = st.progress(0, text="正在初始化...")
    result = None
    start_ts = time.time()
    # 不同 scope 预估时间不同；多股票按数量倍增
    _stock_count = max(1, len([s for s in symbols.split(",") if s.strip()])) if _is_multi else 1
    if scope == "reports":
        est_total_sec = 45
    elif scope == "stock":
        est_total_sec = (180 if mode == "deep" else 40) * _stock_count
    else:
        est_total_sec = (240 if mode == "deep" else 60) + (180 if mode == "deep" else 40) * max(0, _stock_count - 1)

    kwargs: dict[str, Any] = {
        "symbol": resolved_symbol,
        "symbols": symbols,
        "mode": mode,
        "target_user": target_user.strip() or "盘前纪要",
        "target_date": target_date.isoformat(),
        "window_days": int(window_days),
        "allow_fallback": allow_fallback,
        "disable_weekend_shift": disable_weekend_shift,
        "out_prefix": out_prefix.strip() or "ai_input_bundle",
        "out_dir": out_dir.strip() or "tools",
        "scope": scope,
        "timeout_sec": max(420, est_total_sec + 120),
    }
    if overwrite_latest:
        kwargs["overwrite_latest"] = True

    # 分阶段提示映射
    _STAGE_HINTS: dict[str, list[str]] = {
        "reports": [
            "阶段1/3：启动脚本与参数解析",
            "阶段2/3：抓取九阳公社研报 + 市场要闻",
            "阶段3/3：写出缓存与文件",
        ],
        "stock": [
            "阶段1/3：启动脚本与参数解析",
            "阶段2/3：抓取东方财富个股数据（含公告/财务/行情等）",
            "阶段3/3：合并日缓存（研报+要闻）并写出文件",
        ],
        "all": [
            "阶段1/5：启动脚本与参数解析",
            "阶段2/5：抓取九阳公社研报",
            "阶段3/5：抓取东方财富个股数据",
            "阶段4/5：做字段整理与匹配",
            "阶段5/5：写出 JSON/Markdown 文件",
        ],
    }
    hints = _STAGE_HINTS.get(scope, _STAGE_HINTS["all"])
    num_stages = len(hints)
    event_queue: queue.Queue[str] = queue.Queue()
    live_log_box = st.empty()
    live_logs: list[str] = []

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_ai_input_export, event_queue=event_queue, **kwargs)
        last_step = -1
        while not future.done():
            elapsed = max(0, int(time.time() - start_ts))
            pct = min(92, int(elapsed / max(1, est_total_sec) * 100))
            progress.progress(pct, text=f"抓取运行中... {pct}%（已用时 {elapsed}s）")

            step = min(pct * num_stages // 100, num_stages - 1)
            if step != last_step:
                last_step = step
                status.write(hints[step])

            while True:
                try:
                    line = event_queue.get_nowait()
                except queue.Empty:
                    break
                if not line:
                    continue
                live_logs.append(line)
                if len(live_logs) > 12:
                    live_logs = live_logs[-12:]
                live_log_box.code("\n".join(live_logs), language=None)

            time.sleep(0.25)

        try:
            while True:
                try:
                    line = event_queue.get_nowait()
                except queue.Empty:
                    break
                if not line:
                    continue
                live_logs.append(line)
                if len(live_logs) > 12:
                    live_logs = live_logs[-12:]
            if live_logs:
                live_log_box.code("\n".join(live_logs), language=None)
            result = future.result()
        except TypeError as exc:
            if ("overwrite_latest" in str(exc)) or ("overrite_latest" in str(exc)):
                status.write("检测到旧版本函数签名，自动降级重试（不传 overwrite_latest）。")
                kwargs.pop("overwrite_latest", None)
                try:
                    result = run_ai_input_export(event_queue=event_queue, **kwargs)
                except Exception as inner_exc:
                    status.update(label="导出失败", state="error")
                    st.error(str(inner_exc))
                    return
            else:
                status.update(label="导出失败", state="error")
                st.error(str(exc))
                return
        except Exception as exc:
            status.update(label="导出失败", state="error")
            st.error(str(exc))
            return

    progress.progress(100, text="导出完成 100%")
    status.update(label=f"【{scope_label}】导出完成", state="complete")
    if live_logs:
        st.caption("最近运行日志")
        live_log_box.code("\n".join(live_logs[-12:]), language=None)
    if _is_multi:
        st.caption(f"多股票拆分导出完成（{symbols}）")
    elif resolved_symbol:
        st.caption(f"信息合并已解析: {symbol} -> {resolved_symbol} {parsed.get('name', '')}")
    else:
        st.caption(f"研报+要闻已缓存到输出目录（日期: {target_date}）")
    # ── 清除旧的研报文本 widget 缓存，防止 Streamlit 显示旧内容 ──
    for _k in list(st.session_state.keys()):
        if _k.startswith("report_text_"):
            del st.session_state[_k]
    _ver = st.session_state.get("_report_render_ver", 0) + 1
    st.session_state["_report_render_ver"] = _ver
    st.session_state["ai_input_result"] = result


def _run_dividend_calc(
    symbol: str,
    query_date: date,
    future_price_text: str,
    strict_date: bool,
    ttm_days: int,
) -> None:
    if not symbol.strip():
        st.error("请输入股票代码或名称后再计算。")
        return
    parsed = _resolve_symbol_for_ui(symbol, "股息率工具")
    if not parsed:
        return
    resolved_symbol = str(parsed.get("code", "")).strip()

    text = future_price_text.strip()
    has_price = bool(text)
    future_price: float | None = None

    if has_price:
        try:
            future_price = float(text)
        except Exception:
            st.error("输入价格格式不正确，请输入数字。")
            return
        if future_price <= 0:
            st.error("输入价格必须大于 0。")
            return

    try:
        if has_price:
            anchor_date = date.today()
            dy_result = calculate_dividend_yield(
                symbol=resolved_symbol,
                query_date=anchor_date.isoformat(),
                future_price=future_price,
                use_latest_event=True,
                strict_date=False,
                ttm_days=max(30, int(ttm_days)),
            )
            dy_result["ui_mode"] = "latest_event_by_price"
            dy_result["ui_has_price"] = True
            dy_result["ui_symbol_resolved"] = f"{symbol} -> {resolved_symbol} {parsed.get('name', '')}".strip()
            dy_result["ui_note"] = (
                f"已按最近一次分红事件 + 输入价格计算，同时统计截至 {anchor_date.isoformat()} 的 TTM 股息率。"
            )
        else:
            dy_result = calculate_dividend_yield(
                symbol=resolved_symbol,
                query_date=query_date.isoformat(),
                future_price=None,
                use_latest_event=False,
                strict_date=bool(strict_date),
                ttm_days=max(30, int(ttm_days)),
            )
            dy_result["ui_mode"] = "date_based"
            dy_result["ui_has_price"] = False
            dy_result["ui_symbol_resolved"] = f"{symbol} -> {resolved_symbol} {parsed.get('name', '')}".strip()
            dy_result["ui_note"] = (
                f"已按查询日期匹配分红事件，并回看最近 {max(30, int(ttm_days))} 天统计 TTM；查询日期={query_date.isoformat()}。"
            )
    except Exception as exc:
        st.error(f"股息率计算失败：{exc}")
        return

    st.session_state["dividend_yield_result"] = dy_result


def _render_dividend_result() -> None:
    dy_result = st.session_state.get("dividend_yield_result")
    if not dy_result:
        return

    def _fmt_pct(value: Any) -> str:
        try:
            if value is None or value == "":
                return "-"
            return f"{float(value):.4f}"
        except Exception:
            return str(value)

    def _fmt_num(value: Any, digits: int = 4) -> str:
        try:
            if value is None or value == "":
                return "-"
            return f"{float(value):.{digits}f}"
        except Exception:
            return str(value)

    def _mode_label(mode: str) -> str:
        mapping = {
            "exact": "精确匹配除权日",
            "nearby": "附近日期匹配",
            "latest_before": "取查询日前最近分红",
            "strict_no_exact": "严格匹配未命中",
            "no_nearby": "附近未命中",
            "latest_event": "最新分红事件",
            "latest_event_no_before": "使用未来最近分红事件",
            "ttm_only": "仅输出 TTM 结果",
            "ready_made_yield": "仅现成 TTM 参考",
        }
        return mapping.get(str(mode or ""), str(mode or "-"))

    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("股息率测算结果")

    note = str(dy_result.get("ui_note", "")).strip()
    if note:
        st.markdown(f'<div class="warn-note">{note}</div>', unsafe_allow_html=True)

    yields = dy_result.get("yields", {}) if isinstance(dy_result, dict) else {}
    price_info = dy_result.get("price", {}) if isinstance(dy_result, dict) else {}
    calc_trace = dy_result.get("calculation_trace", {}) if isinstance(dy_result, dict) else {}
    selected_event = dy_result.get("selected_event") or {}
    close_price = price_info.get("close_price")
    ready_ttm = yields.get("ready_made_ttm_yield_pct")
    has_price_mode = bool(dy_result.get("ui_has_price"))
    ttm_future_fallback_used = bool((dy_result.get("validation") or {}).get("ttm_fallback_used", False))
    ttm_info = dy_result.get("ttm", {}) if isinstance(dy_result, dict) else {}

    c1, c2, c3 = st.columns(3)
    if has_price_mode:
        c1.metric("输入价-单次股息率(%)", _fmt_pct(yields.get("selected_event_yield_pct_at_future_price")))
        c2.metric("输入价-TTM股息率(%)", _fmt_pct(yields.get("ttm_yield_pct_at_future_price")))
        c3.metric("当前收盘-TTM股息率(%)", _fmt_pct(yields.get("ttm_yield_pct_at_close")))
    else:
        c1.metric("当前价-单次股息率(%)", _fmt_pct(yields.get("selected_event_yield_pct_at_close")))
        c2.metric("当前价-TTM股息率(%)", _fmt_pct(yields.get("ttm_yield_pct_at_close")))
        c3.metric("现成TTM参考(%)", _fmt_pct(ready_ttm))

    resolved_text = str(dy_result.get("ui_symbol_resolved", "")).strip()
    if resolved_text:
        st.caption(f"股票解析：{_fix_mojibake_text(resolved_text)}")
    st.caption(
        f"选取方式：{_mode_label(str(dy_result.get('pick_mode', '')))} | 选中除权日：{selected_event.get('ex_date', '-') or '-'} | 每股分红：{_fmt_num(selected_event.get('cash_dividend_per_share_consensus'))}"
    )
    st.caption(
        f"价格来源：{_fix_mojibake_text(price_info.get('source', '')) or '-'} | 收盘价：{_fmt_num(close_price)} | 价格日期：{price_info.get('price_date', '-') or '-'}"
    )
    if yields.get("future_price_input") is not None:
        st.caption(f"输入价格：{_fmt_num(yields.get('future_price_input'))}")
    st.caption(
        f"TTM窗口：{ttm_info.get('window_start', '-') or '-'} ~ {ttm_info.get('window_end', '-') or '-'} | 事件数：{ttm_info.get('event_count', 0)} | 每股累计分红：{_fmt_num(ttm_info.get('cash_dividend_per_share_total'))}"
    )

    selected_trace = calc_trace.get("selected_event_formula") or {}
    ttm_trace = calc_trace.get("ttm_formula") or {}
    if selected_trace:
        st.caption(
            f"单次公式：每股分红 ÷ 价格 × 100 | 每股分红={_fmt_num(selected_trace.get('selected_dividend_per_share'))} | 当前价结果={_fmt_pct(selected_trace.get('result_pct_at_close'))}% | 输入价结果={_fmt_pct(selected_trace.get('result_pct_at_future_price'))}%"
        )
    if ttm_trace:
        st.caption(
            f"TTM公式：窗口内每股分红合计 ÷ 价格 × 100 | 窗口={ttm_trace.get('window_days')}天 | 每股累计={_fmt_num(ttm_trace.get('ttm_per_share_sum'))} | 当前价结果={_fmt_pct(ttm_trace.get('result_pct_at_close'))}% | 输入价结果={_fmt_pct(ttm_trace.get('result_pct_at_future_price'))}%"
        )
    if ttm_future_fallback_used:
        st.info("TTM 结果部分使用了现成参考口径进行补足，请结合下方来源明细复核。")

    warnings = dy_result.get("validation", {}).get("warnings", [])
    for warning in warnings:
        st.warning(_fix_mojibake_text(warning))

    ready_sources = (calc_trace.get("ready_sources") or []) if isinstance(calc_trace, dict) else []
    authority_order = (calc_trace.get("authority_order") or []) if isinstance(calc_trace, dict) else []
    authority_pick = (calc_trace.get("authority_pick") or {}) if isinstance(calc_trace, dict) else {}
    if ready_sources:
        with st.expander("查看现成 TTM 参考来源", expanded=False):
            st.caption("该表用于对比外部现成 TTM 股息率来源，最终结果仍以页面顶部指标为准。")
            try:
                def _order_idx(src: str) -> int:
                    try:
                        return authority_order.index(src)
                    except Exception:
                        return 999

                sorted_rows = sorted(ready_sources, key=lambda item: (_order_idx(str(item.get("source", ""))), str(item.get("source", ""))))
                show_rows = [
                    {
                        "来源": str(item.get("source", "")),
                        "状态": "成功" if bool(item.get("ok", True)) else "失败",
                        "TTM股息率(%)": item.get("yield_pct", None),
                        "日期": str(item.get("as_of", "")),
                        "备注": _fix_mojibake_text(item.get("error", "")),
                    }
                    for item in sorted_rows[:5]
                ]
                st.dataframe(pd.DataFrame(show_rows), width="stretch", height=180)
            except Exception:
                st.json(ready_sources[:5])

    if authority_order:
        st.caption("现成TTM权威顺序：" + " > ".join(authority_order))
    if authority_pick:
        st.caption(
            f"现成TTM当前选源：{authority_pick.get('source', '')} | 数值={authority_pick.get('yield_pct', '')} | 日期={authority_pick.get('as_of', '')}"
        )

    dividend_recon = dy_result.get("dividend_per_share_reconciliation", {})
    recon_rows = dividend_recon.get("source_rows", []) if isinstance(dividend_recon, dict) else []
    recon_order = dividend_recon.get("authority_order", []) if isinstance(dividend_recon, dict) else []
    recon_pick = dividend_recon.get("authority_pick", {}) if isinstance(dividend_recon, dict) else {}
    recon_mode = str(dividend_recon.get("selection_mode", "")) if isinstance(dividend_recon, dict) else ""
    recon_consensus_sources = dividend_recon.get("consensus_sources", []) if isinstance(dividend_recon, dict) else []
    recon_has_diff = bool(dividend_recon.get("has_diff", False)) if isinstance(dividend_recon, dict) else False

    if recon_rows:
        with st.expander("查看分红对盘明细", expanded=False):
            st.caption("这里展示各来源分红事件汇总，用于判断每股分红合计是否一致。")
            try:
                def _recon_idx(src: str) -> int:
                    try:
                        return recon_order.index(src)
                    except Exception:
                        return 999

                sorted_recon = sorted(recon_rows, key=lambda item: (_recon_idx(str(item.get("source", ""))), str(item.get("source", ""))))
                show_recon = [
                    {
                        "来源": str(item.get("source", "")),
                        "每10股分红合计": item.get("per10_sum", None),
                        "每股分红合计": item.get("per_share_sum", None),
                        "事件数": item.get("event_count", 0),
                        "最新除权日": str(item.get("latest_ex_date", "")),
                    }
                    for item in sorted_recon
                ]
                st.dataframe(pd.DataFrame(show_recon), width="stretch", height=220)
            except Exception:
                st.json(recon_rows)

            if recon_order:
                st.caption("分红权威顺序：" + " > ".join(recon_order))
            if recon_mode == "consensus" and recon_consensus_sources:
                st.caption("分红选源模式：多源共识优先 | 共识来源=" + ", ".join([str(x) for x in recon_consensus_sources]))
            elif recon_mode:
                st.caption("分红选源模式：按权威顺序降级。")
            if recon_pick:
                st.caption(
                    f"分红当前选源：{recon_pick.get('source', '')} | 每股合计={recon_pick.get('per_share_sum', '')} | 事件数={recon_pick.get('event_count', 0)}"
                )
            if recon_has_diff:
                st.warning("分红来源间的每股合计存在差异，当前结果已按权威顺序选源。")

    with st.expander("查看完整股息率原始结果", expanded=False):
        st.json(dy_result)
    st.markdown("</div>", unsafe_allow_html=True)


def run() -> None:
    st.set_page_config(page_title="投研整合工作台", layout="wide", initial_sidebar_state="collapsed")
    _inject_styles()

    # 兼容热更新后的旧会话对象：若结构不完整，清理旧结果避免属性报错
    _old_result = st.session_state.get("ai_input_result")
    if _old_result is not None:
        if not hasattr(_old_result, "json_path") or not hasattr(_old_result, "md_path") or not hasattr(_old_result, "payload"):
            st.session_state.pop("ai_input_result", None)

    with st.sidebar:
        st.header("归档保留设置")
        archive_dir = st.text_input(
            "归档目录",
            value=os.environ.get("QUANT_ARCHIVE_DIR", "quant_archive"),
            help="相对路径默认在项目根目录下",
        ).strip()
        jygs_keep_days = st.number_input(
            "九阳公社保留天数",
            min_value=1,
            max_value=60,
            value=int(os.environ.get("JYGS_ARCHIVE_KEEP_DAYS", "5") or 5),
            step=1,
        )
        hot_keep_days = st.number_input(
            "要闻保留天数",
            min_value=1,
            max_value=90,
            value=int(os.environ.get("HOTNEWS_ARCHIVE_KEEP_DAYS", "10") or 10),
            step=1,
        )
        os.environ["QUANT_ARCHIVE_DIR"] = archive_dir or "quant_archive"
        os.environ["JYGS_ARCHIVE_KEEP_DAYS"] = str(int(jygs_keep_days))
        os.environ["HOTNEWS_ARCHIVE_KEEP_DAYS"] = str(int(hot_keep_days))
        st.caption("当前会话生效；可自行改为固定环境变量。")

    st.markdown(
        """
<div class="hero">
  <div class="eyebrow">Neo Quant Console</div>
  <h2>&#25237;&#30740;&#26234;&#33021;&#24037;&#20316;&#21488;</h2>
  <p>&#30740;&#25253;&#12289;&#20010;&#32929;&#19978;&#19979;&#25991;&#12289;&#34892;&#19994;&#23545;&#26631;&#19982;&#23548;&#20986;&#32467;&#26524;&#38598;&#20013;&#21040;&#21516;&#19968;&#20010;&#30028;&#38754;&#37324;&#65292;&#35753;&#20449;&#24687;&#23637;&#31034;&#26356;&#20687;&#19968;&#20010;&#19987;&#19994;&#30340;&#25237;&#30740;&#25511;&#21046;&#21488;&#12290;</p>
  <div class="hero-badges">
    <span>&#30740;&#25253;&#21512;&#24182;</span>
    <span>&#34892;&#19994;&#23545;&#26631;</span>
    <span>&#26234;&#33021;&#23548;&#20986;</span>
    <span>&#32929;&#24687;&#29575;&#27979;&#31639;</span>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )

    default_out_dir = str(PROJECT_ROOT)
    tool_options = ["信息合并导出", "股息率工具"]
    tool_choice_key = "tool_choice_top"
    tool_choice_memory_key = "tool_choice_top_memory"
    if tool_choice_memory_key not in st.session_state:
        st.session_state[tool_choice_memory_key] = tool_options[0]
    if st.session_state.get(tool_choice_key) not in tool_options:
        st.session_state[tool_choice_key] = st.session_state[tool_choice_memory_key]

    tool_choice = st.radio(
        "工具选择",
        options=tool_options,
        horizontal=True,
        key=tool_choice_key,
    )
    st.session_state[tool_choice_memory_key] = tool_choice

    btn_reports = False
    btn_stock = False
    btn_all = False
    dy_btn = False
    symbol = ""
    mode = "deep"
    target_user = ""
    target_date = date.today()
    window_days = 1
    allow_fallback = False
    disable_weekend_shift = False
    overwrite_latest = True
    out_dir = default_out_dir
    out_prefix = ""
    dy_symbol = ""
    dy_future_price_input = ""
    dy_date = date.today()
    dy_ttm_days = 365
    dy_strict = False

    if tool_choice == "信息合并导出":
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("信息合并导出")

        if "stock_list" not in st.session_state:
            st.session_state["stock_list"] = []

        symbol = _symbol_searchbox(
            label="股票代码或名称",
            key="fetch_symbol",
            placeholder="输入首字母/代码/中文名，点击候选或按钮加入列表",
        )

        fetch_auto_selected = bool(st.session_state.pop("fetch_symbol__just_selected", False))
        _add_c1, _add_c2 = st.columns([1.2, 4.8])
        add_stock_clicked = _add_c1.button(
            "加入列表",
            key="fetch_add_stock",
            use_container_width=True,
            disabled=not bool(symbol.strip()),
        )
        _add_c2.caption("输入时不会自动重置；点击候选或“加入列表”后才会真正加入。")

        if (add_stock_clicked or fetch_auto_selected) and symbol.strip():
            parsed = _resolve_symbol_for_ui(symbol, "加入股票")
            if parsed:
                code = str(parsed.get("code", "")).strip()
                name = str(parsed.get("name", "")).strip()
                existing_codes = {s["code"] for s in st.session_state["stock_list"]}
                if code not in existing_codes:
                    st.session_state["stock_list"].append({"code": code, "name": name, "display": f"{code} {name}"})
                st.session_state["fetch_symbol__clear_pending"] = True
                st.session_state.pop("fetch_symbol__selected", None)
                st.session_state.pop("fetch_symbol__just_selected", None)
                st.rerun()

        if st.session_state["stock_list"]:
            st.caption(f"📋 待分析股票（{len(st.session_state['stock_list'])}只）：")
            _remove_code: str | None = None
            for _i, _s in enumerate(st.session_state["stock_list"]):
                _sc1, _sc2 = st.columns([5, 1])
                _sc1.markdown(
                    f'<span style="color:#5ce6ff;font-weight:600">{_s["code"]}</span>'
                    f'&ensp;<span style="color:#f5fbff;font-size:1.05em;font-weight:500">{_s["name"]}</span>',
                    unsafe_allow_html=True,
                )
                if _sc2.button("✕", key=f"remove_stock_{_s['code']}"):
                    _remove_code = _s["code"]
            if _remove_code is not None:
                st.session_state["stock_list"] = [
                    s for s in st.session_state["stock_list"] if s["code"] != _remove_code
                ]
                st.rerun()
            if st.button("清空列表", key="fetch_clear_stocks"):
                st.session_state["stock_list"] = []
                st.rerun()
        else:
            st.caption("💡 可直接输入后单股运行；也可先加入列表再批量导出。")
        mode = st.radio("东财模式", options=["quick", "deep"], index=1, horizontal=True, key="fetch_mode")
        target_user = st.text_input(
            "九阳用户(名称/UID/主页URL)",
            value="",
            placeholder="盘前纪要 或 4df747... 或 https://www.jiuyangongshe.com/u/...",
            key="fetch_target_user",
        )
        target_date = _date_selector("请求日期", default=date.today(), key_prefix="fetch_target_date")
        window_days = st.number_input("研报篇数", min_value=1, max_value=30, value=1, step=1, key="fetch_window_days",
                                       help="提取最近 N 篇研报（按日期倒序），不受周末/节假日影响")
        allow_fallback = st.checkbox("无符合条件时取最新一篇", value=False, key="fetch_allow_fallback")
        disable_weekend_shift = st.checkbox("关闭周末自动回退周五", value=False, key="fetch_disable_weekend")
        overwrite_latest = st.checkbox("覆盖写 latest 文件(推荐)", value=True, key="fetch_overwrite_latest")
        out_dir = st.text_input("输出目录", value=default_out_dir, placeholder=r"例如 G:\lianghua\投研系统", key="fetch_out_dir")
        out_prefix = st.text_input("输出前缀", value="", placeholder="默认 ai_input_bundle", key="fetch_out_prefix")
        st.caption("批量导出时优先使用列表中的股票；列表为空时会直接使用当前输入值。")
        st.caption("① 每天只需一次 | ② 换股时用 | ③ 首次使用或完整导出")
        _btn_c1, _btn_c2, _btn_c3 = st.columns(3)
        btn_reports = _btn_c1.button("① 研报+要闻", key="fetch_btn_reports", help="仅抓取研报与市场要闻（不需要股票代码）")
        btn_stock = _btn_c2.button("② 个股数据", key="fetch_btn_stock", help="仅抓取个股接口数据，研报复用今日缓存")
        btn_all = _btn_c3.button("③ 全部下载", type="primary", key="fetch_btn_all", help="研报+要闻+个股一次性全部下载")
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("股息率工具")
        dy_symbol = _symbol_searchbox(
            label="股票代码或名称",
            key="dy_symbol",
            placeholder="输入首字母/代码/中文名，如 ynby / 000538 / 云南白药",
            clear_on_submit=False,
        )
        dy_future_price_input = st.text_input(
            "输入价格(可选，留空=按日期模式)",
            value="",
            placeholder="例如 56.80",
            key="dy_future_price_input",
        )
        dy_date = _date_selector(
            "日期(无价格时必填)",
            default=date.today(),
            key_prefix="dy_date",
            disabled=bool(dy_future_price_input.strip()),
        )
        dy_ttm_days = st.number_input(
            "TTM窗口天数",
            min_value=90,
            max_value=720,
            value=365,
            step=5,
            help="用于统计近 N 天内的分红事件并重建 TTM 股息率。",
            key="dy_ttm_days",
        )
        dy_strict = st.checkbox(
            "严格匹配除权日（仅无价格模式）",
            value=False,
            disabled=bool(dy_future_price_input.strip()),
            key="dy_strict",
        )
        st.caption("规则：输入价格时按最近一次分红事件 + 输入价格计算；未输入价格时按查询日期匹配除权日，并同步统计 TTM。")
        dy_btn = st.button("计算股息率", width="stretch", key="dy_btn")
        st.markdown("</div>", unsafe_allow_html=True)

    _selected_scope = "reports" if btn_reports else "stock" if btn_stock else "all" if btn_all else None
    if _selected_scope:
        if _selected_scope == "reports":
            # 研报+要闻不需要股票，执行一次即可
            _run_export(
                symbol="",
                mode=mode,
                target_user=target_user,
                target_date=target_date,
                window_days=int(window_days),
                allow_fallback=allow_fallback,
                disable_weekend_shift=disable_weekend_shift,
                out_prefix=out_prefix,
                out_dir=out_dir,
                overwrite_latest=overwrite_latest,
                scope="reports",
            )
        else:
            # 优先使用列表中的股票，如果列表为空则用搜索框当前值
            if st.session_state.get("stock_list"):
                _stocks_to_process = list(st.session_state["stock_list"])
            elif symbol.strip():
                _stocks_to_process = [{"code": symbol.strip(), "name": ""}]
            else:
                st.error('请输入股票代码或通过"加入"按钮添加股票到列表')
                _stocks_to_process = []

            if _stocks_to_process:
                # 多股票：用逗号拼接传给 runner，由 export 脚本统一处理合并
                _codes = [s.get("code", "") for s in _stocks_to_process if s.get("code", "").strip()]
                if len(_codes) > 1:
                    _symbols_csv = ",".join(_codes)
                    _run_export(
                        symbol="",
                        symbols=_symbols_csv,
                        mode=mode,
                        target_user=target_user,
                        target_date=target_date,
                        window_days=int(window_days),
                        allow_fallback=allow_fallback,
                        disable_weekend_shift=disable_weekend_shift,
                        out_prefix=out_prefix,
                        out_dir=out_dir,
                        overwrite_latest=overwrite_latest,
                        scope=_selected_scope,
                    )
                else:
                    # 单只股票走原有逻辑
                    _run_export(
                        symbol=_codes[0] if _codes else "",
                        mode=mode,
                        target_user=target_user,
                        target_date=target_date,
                        window_days=int(window_days),
                        allow_fallback=allow_fallback,
                        disable_weekend_shift=disable_weekend_shift,
                        out_prefix=out_prefix,
                        out_dir=out_dir,
                        overwrite_latest=overwrite_latest,
                        scope=_selected_scope,
                    )

    if dy_btn:
        _run_dividend_calc(
            symbol=dy_symbol,
            query_date=dy_date,
            future_price_text=dy_future_price_input,
            strict_date=dy_strict,
            ttm_days=int(dy_ttm_days),
        )

    # ── 结果展示区：按当前工具类型分别显示，避免切换工具时结果"串台" ──
    if tool_choice == "信息合并导出":
        result = st.session_state.get("ai_input_result")
        if result:
            assert isinstance(result, AiInputExportResult)
            json_paths = getattr(result, "json_paths", None) or [result.json_path]
            md_paths = getattr(result, "md_paths", None) or [result.md_path]
            preview_path = result.json_path
            if len(json_paths) > 1:
                path_map = {p.name: p for p in json_paths}
                selected_name = st.selectbox("预览文件", options=list(path_map.keys()), key="ai_input_preview_json")
                preview_path = path_map.get(selected_name, result.json_path)
            payload = result.payload
            if preview_path != result.json_path:
                try:
                    payload = json.loads(preview_path.read_text(encoding="utf-8"))
                except Exception:
                    payload = result.payload
            meta = payload.get("meta", {})
            counts = _safe_counts(payload)
            instruction = payload.get("llm_instruction", {}) if isinstance(payload, dict) else {}

            st.success("AI 输入文件已生成")
            if not bool(instruction.get("attached")):
                err_text = str(instruction.get("error", "")).strip()
                st.warning(f"当前导出未附带提示词内容。{err_text}" if err_text else "当前导出未附带提示词内容。")
            latest_hint = "（当前为 latest 覆盖文件）" if "latest" in preview_path.name.lower() else ""
            path_lines: list[str] = []
            for p in json_paths:
                path_lines.append(f"JSON: {p}")
                md_candidate = next((m for m in md_paths if m.stem == p.stem), None)
                if md_candidate:
                    path_lines.append(f"Markdown: {md_candidate}")
            st.markdown(
                f"""
<div class="path-box">
{'<br/>'.join(path_lines)} {latest_hint}
</div>
                """,
                unsafe_allow_html=True,
            )

            _render_kpi_cards(counts)
            st.caption(
                f"请求日: {meta.get('request_date', '')} | "
                f"研报目标日: {meta.get('report_target_date', meta.get('target_date', ''))}"
            )
            _render_downloads(result)

            t1, t2, t3 = st.tabs(["研报原文", "个股上下文", "接口诊断"])
            with t1:
                _render_reports(payload)
            with t2:
                _render_stock_context(payload)
            with t3:
                _render_diagnostics(payload)
        else:
            st.info("在上方填写参数后，点击导出按钮生成 AI 输入包。")
    else:
        # 股息率工具：只显示股息率结果
        if st.session_state.get("dividend_yield_result"):
            st.divider()
            _render_dividend_result()


if __name__ == "__main__":
    run()
