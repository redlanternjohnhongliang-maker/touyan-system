from __future__ import annotations

import os
import streamlit as st

from src.config.settings import load_settings
from src.services.report_ingest import ingest_today_reports
from src.services.single_stock_report import build_single_stock_markdown
from src.services.update_pipeline import manual_update_stock_data
from src.storage.db import init_db
from src.storage.repository import (
    add_watchlist_symbol,
    get_latest_stock_snapshot,
    list_recent_reports,
    list_watchlist,
    remove_watchlist_symbol,
    save_analysis_log,
)


def run() -> None:
    st.set_page_config(page_title="AI投研助理 V1", layout="wide")
    st.title("AI投研助理 V1（首版骨架）")

    settings = load_settings()
    init_db(settings.db_path)

    if not settings.gemini_api_key:
        st.warning("未检测到 GEMINI_API_KEY。当前可运行数据骨架，AI生成部分后续接入。")

    with st.sidebar:
        st.header("股票池管理")
        new_symbol = st.text_input("添加股票代码（例：600519）")
        new_name = st.text_input("股票名称（可选）")
        if st.button("添加到股票池") and new_symbol.strip():
            add_watchlist_symbol(settings.db_path, new_symbol.strip(), new_name.strip())
            st.success(f"已添加：{new_symbol.strip()}")

        remove_symbol = st.text_input("删除股票代码")
        if st.button("从股票池删除") and remove_symbol.strip():
            remove_watchlist_symbol(settings.db_path, remove_symbol.strip())
            st.success(f"已删除：{remove_symbol.strip()}")

        st.divider()
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
        st.caption("当前会话生效；你可在代码或环境变量中继续调整。")

        st.divider()
        st.header("研报抓取")
        if st.button("抓取今日盘前纪要"):
            count = ingest_today_reports(settings.db_path, target_user="盘前纪要")
            st.info(f"已抓取并入库：{count} 条")

    watchlist = list_watchlist(settings.db_path)
    symbols = [item["symbol"] for item in watchlist]

    if not symbols:
        st.info("股票池为空，请先在左侧添加股票代码。")
        return

    selected_symbol = st.selectbox("点单分析：选择一只股票", symbols)
    update_mode = st.radio(
        "更新模式",
        options=["quick", "deep"],
        index=0,
        format_func=lambda x: "快速（推荐）" if x == "quick" else "深度（较慢）",
        horizontal=True,
    )
    col1, col2 = st.columns([1, 1])

    with col1:
        if st.button("手动更新该股数据"):
            bundle = manual_update_stock_data(settings.db_path, selected_symbol, mode=update_mode)
            error_count = len(bundle.get("_errors", []))
            if error_count == 0:
                st.success("更新完成（全部接口成功）")
            else:
                st.warning(f"更新完成（{error_count} 个接口异常，已降级）")

            diagnostics = bundle.get("_diagnostics", [])
            if diagnostics:
                st.caption("本次接口诊断")
                for item in diagnostics:
                    endpoint = item.get("endpoint", "unknown")
                    duration_ms = item.get("duration_ms", 0)
                    rows = item.get("rows", 0)
                    if item.get("ok"):
                        st.text(f"✅ {endpoint} | rows={rows} | {duration_ms}ms")
                    else:
                        st.text(f"❌ {endpoint} | rows=0 | {duration_ms}ms | {item.get('error', '')}")

    snapshot = get_latest_stock_snapshot(settings.db_path, selected_symbol)

    with col2:
        if st.button("生成单股分析报告"):
            bundle = snapshot or {"symbol": selected_symbol}
            # 从 DB 获取近 5 天盘前纪要研报，传入情绪引擎
            db_reports = list_recent_reports(settings.db_path, days=5)
            markdown, evidence = build_single_stock_markdown(
                selected_symbol, bundle, db_reports=db_reports
            )
            save_analysis_log(settings.db_path, selected_symbol, markdown, evidence)
            st.markdown(markdown)

    if snapshot:
        st.caption(f"最近数据抓取时间：{snapshot.get('_fetched_at', '未知')}")
        if snapshot.get("_errors"):
            with st.expander("上次抓取异常详情"):
                for msg in snapshot.get("_errors", []):
                    st.text(msg)


if __name__ == "__main__":
    run()
