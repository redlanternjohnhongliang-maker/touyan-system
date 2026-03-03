"""研报情绪引擎 — 今日 + 近 5 日研报情绪演化分析。

核心逻辑：
1. 从 research_report（卖方研报）和 DB 中的盘前纪要研报中提取情绪信号。
2. 按日期分桶，计算每日情绪倾向（看多 / 看空 / 中性）。
3. 输出"主线契合度"评分（0-100）和情绪演化时间线。

设计原则：
- 无研报数据时明确输出"证据不足"，不猜测。
- 纯规则 / 关键词驱动（后续可替换为 LLM 微调打分）。
"""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

# ── 关键词字典 ──────────────────────────────────────────────
_BULLISH_KEYWORDS: list[str] = [
    "买入", "增持", "推荐", "强烈推荐", "首次覆盖", "上调目标价",
    "超预期", "业绩高增", "景气上行", "拐点", "放量突破",
    "龙头", "高成长", "加速", "受益", "利好", "强势",
    "底部反转", "趋势向上", "盈利改善", "估值修复",
]
_BEARISH_KEYWORDS: list[str] = [
    "卖出", "减持", "回避", "下调目标价", "下调评级",
    "低于预期", "业绩下滑", "景气下行", "风险", "利空",
    "减值", "商誉", "暴雷", "退市", "ST", "亏损",
    "高估", "压力", "破位", "缩量下跌",
]
_NEUTRAL_KEYWORDS: list[str] = [
    "持有", "中性", "观望", "维持评级", "符合预期",
]


def _classify_text(text: str) -> tuple[str, float]:
    """对一段文本做情绪分类。

    返回 (sentiment, confidence):
        sentiment: "bullish" | "bearish" | "neutral"
        confidence: 0.0 ~ 1.0
    """
    if not text:
        return "neutral", 0.0

    text_lower = text.lower()
    bull_hits = sum(1 for kw in _BULLISH_KEYWORDS if kw in text_lower)
    bear_hits = sum(1 for kw in _BEARISH_KEYWORDS if kw in text_lower)
    neut_hits = sum(1 for kw in _NEUTRAL_KEYWORDS if kw in text_lower)

    total = bull_hits + bear_hits + neut_hits
    if total == 0:
        return "neutral", 0.0

    if bull_hits > bear_hits and bull_hits > neut_hits:
        return "bullish", min(bull_hits / total, 1.0)
    if bear_hits > bull_hits and bear_hits > neut_hits:
        return "bearish", min(bear_hits / total, 1.0)
    return "neutral", min(neut_hits / total, 1.0)


def _extract_date_from_record(record: dict[str, Any]) -> str | None:
    """尝试从记录中提取日期字符串 (YYYY-MM-DD)。"""
    for key in ("日期", "报告日期", "report_date", "publish_date", "发布日期", "公告日期", "date"):
        raw = str(record.get(key, "")).strip()
        if not raw or raw == "None":
            continue
        # 尝试 YYYY-MM-DD
        match = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
        if match:
            return match.group(1)
        # 尝试 YYYYMMDD
        match = re.search(r"(\d{8})", raw)
        if match:
            d = match.group(1)
            return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return None


def _build_text_for_record(record: dict[str, Any]) -> str:
    """将研报记录拼接为可分析文本。"""
    parts: list[str] = []
    for key in ("报告名称", "标题", "title", "研报标题", "内容摘要", "content", "研报内容",
                "东财评级", "评级", "rating"):
        val = str(record.get(key, "")).strip()
        if val and val != "None":
            parts.append(val)
    return " ".join(parts)


# ── 公开接口 ──────────────────────────────────────────────

def analyze_sentiment(
    research_reports: list[dict[str, Any]],
    db_reports: list[dict[str, Any]] | None = None,
    window_days: int = 5,
) -> dict[str, Any]:
    """分析研报情绪。

    参数:
        research_reports: 东财卖方研报列表（来自 bundle["research_report"]）
        db_reports:       本地 DB 中近期盘前纪要研报（可选）
        window_days:      回溯天数（默认 5 天）

    返回 dict:
        score          : 0~100 主线契合度评分
        sentiment      : "bullish" | "bearish" | "neutral" | "insufficient"
        daily_timeline : [{date, bullish, bearish, neutral, dominant}]
        evidence_count : 有效研报条目数
        detail         : 人类可读的简要描述
    """
    today = date.today()
    window_start = today - timedelta(days=window_days)

    # 合并两个来源
    all_records: list[dict[str, Any]] = list(research_reports or [])
    if db_reports:
        all_records.extend(db_reports)

    if not all_records:
        return {
            "score": 0,
            "sentiment": "insufficient",
            "daily_timeline": [],
            "evidence_count": 0,
            "detail": "无研报数据，无法评估主线契合度。",
        }

    # 按日期分桶
    daily_buckets: dict[str, list[tuple[str, float]]] = defaultdict(list)
    undated_sentiments: list[tuple[str, float]] = []

    for rec in all_records:
        text = _build_text_for_record(rec)
        sentiment, confidence = _classify_text(text)
        rec_date = _extract_date_from_record(rec)
        if rec_date:
            daily_buckets[rec_date].append((sentiment, confidence))
        else:
            undated_sentiments.append((sentiment, confidence))

    # 将无日期的归入今天
    today_str = today.strftime("%Y-%m-%d")
    if undated_sentiments:
        daily_buckets[today_str].extend(undated_sentiments)

    # ── 构建全量时间线（用于总体评分）──
    all_timeline: list[dict[str, Any]] = []
    sorted_dates = sorted(daily_buckets.keys())
    for d in sorted_dates:
        items = daily_buckets[d]
        counts = {"bullish": 0, "bearish": 0, "neutral": 0}
        for s, _ in items:
            counts[s] = counts.get(s, 0) + 1
        dominant = max(counts, key=counts.get)  # type: ignore
        all_timeline.append({
            "date": d,
            "bullish": counts["bullish"],
            "bearish": counts["bearish"],
            "neutral": counts["neutral"],
            "dominant": dominant,
        })

    # ── 窗口内时间线（用于趋势展示）──
    window_start_str = window_start.strftime("%Y-%m-%d")
    window_timeline = [d for d in all_timeline if d["date"] >= window_start_str]
    # 如果窗口内无数据，退回使用最近 N 天有数据的时间线
    if not window_timeline and all_timeline:
        window_timeline = all_timeline[-min(window_days, len(all_timeline)):]

    # ── 总体评分基于全量数据 ──
    total_bull = sum(d["bullish"] for d in all_timeline)
    total_bear = sum(d["bearish"] for d in all_timeline)
    total_neut = sum(d["neutral"] for d in all_timeline)
    total_all = total_bull + total_bear + total_neut

    if total_all == 0:
        score = 50
        overall_sentiment = "neutral"
    else:
        # 偏多比例映射到 0~100
        bull_ratio = total_bull / total_all
        bear_ratio = total_bear / total_all
        # score: bull_ratio=1 → 100, bear_ratio=1 → 0, 均匀 → 50
        score = int(round(bull_ratio * 100 - bear_ratio * 100 + 50))
        score = max(0, min(100, score))

        if bull_ratio > 0.5:
            overall_sentiment = "bullish"
        elif bear_ratio > 0.5:
            overall_sentiment = "bearish"
        else:
            overall_sentiment = "neutral"

    # 趋势判断（最近两天对比，基于窗口时间线）
    trend_note = ""
    if len(window_timeline) >= 2:
        prev = window_timeline[-2]
        curr = window_timeline[-1]
        if curr["bullish"] > prev["bullish"] and curr["bearish"] <= prev["bearish"]:
            trend_note = "情绪升温（看多增加）"
        elif curr["bearish"] > prev["bearish"] and curr["bullish"] <= prev["bullish"]:
            trend_note = "情绪降温（看空增加）"
        else:
            trend_note = "情绪稳定"

    # 判断数据新鲜度
    most_recent = sorted_dates[-1] if sorted_dates else ""
    data_age_note = ""
    if most_recent and most_recent < window_start_str:
        # 数据较旧，提示用户
        data_age_note = f"（最新研报日期 {most_recent}，距今较远）"

    SENTIMENT_CN = {"bullish": "偏多", "bearish": "偏空", "neutral": "中性"}
    detail_parts = [
        f"研报情绪：{SENTIMENT_CN.get(overall_sentiment, '中性')}",
        f"（看多{total_bull}篇 / 看空{total_bear}篇 / 中性{total_neut}篇）",
    ]
    if data_age_note:
        detail_parts.append(data_age_note)
    if trend_note:
        detail_parts.append(f"，趋势：{trend_note}")

    return {
        "score": score,
        "sentiment": overall_sentiment,
        "daily_timeline": window_timeline,
        "evidence_count": total_all,
        "detail": "".join(detail_parts),
        "trend": trend_note,
    }
