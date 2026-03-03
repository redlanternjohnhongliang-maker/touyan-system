from __future__ import annotations

from typing import Any

from src.services.truth_guard import evaluate_evidence
from src.services.sentiment_engine import analyze_sentiment
from src.services.factor_scoring import compute_all_factors, compute_composite_score


# ── 评分等级配色标签 ────────────────────────────────
_LEVEL_EMOJI = {"强": "🟢", "中": "🟡", "弱": "🔴", "不足": "⬜"}
_SENTIMENT_EMOJI = {"bullish": "📈", "bearish": "📉", "neutral": "➖", "insufficient": "❓"}
_SENTIMENT_CN = {"bullish": "偏多", "bearish": "偏空", "neutral": "中性", "insufficient": "数据不足"}


def build_single_stock_markdown(
    symbol: str,
    bundle: dict[str, Any],
    db_reports: list[dict[str, Any]] | None = None,
) -> tuple[str, list[dict[str, str]]]:
    ok, evidence = evaluate_evidence(bundle)

    # ── 情绪引擎 ──
    sentiment_result = analyze_sentiment(
        research_reports=bundle.get("research_report", []),
        db_reports=db_reports,
    )

    # ── 多因子评分 ──
    factors = compute_all_factors(bundle)
    # 将情绪引擎结果作为因子纳入综合评分
    sentiment_as_factor = {
        "name": "主线契合度",
        "score": sentiment_result["score"],
        "level": "不足" if sentiment_result["sentiment"] == "insufficient" else
                 ("强" if sentiment_result["score"] >= 70 else ("中" if sentiment_result["score"] >= 40 else "弱")),
        "detail": sentiment_result["detail"],
        "evidence": [],
    }
    all_factors_for_composite = [sentiment_as_factor] + factors
    composite = compute_composite_score(all_factors_for_composite)

    # ── 报告生成 ──
    header = f"# 单股分析报告 — {symbol}\n"
    status = "✅ 证据充足，可给出审慎结论" if ok else "⚠️ 证据不足，仅供参考"

    body: list[str] = [
        "## 结论状态",
        status,
        "",
        f"## 综合评分：{composite} / 100",
        "",
    ]

    # ── 主线契合度 ──
    se = sentiment_result
    body.extend([
        "## 1. 主线契合度（研报情绪）",
        f"- 评分：**{se['score']}** {_SENTIMENT_EMOJI.get(se['sentiment'], '')} {_SENTIMENT_CN.get(se['sentiment'], '')}",
        f"- {se['detail']}",
    ])
    if se.get("trend"):
        body.append(f"- 趋势变化：{se['trend']}")
    if se["daily_timeline"]:
        body.append("")
        body.append("| 日期 | 看多 | 看空 | 中性 | 主导 |")
        body.append("|------|------|------|------|------|")
        for day in se["daily_timeline"][-5:]:
            dominant_cn = {"bullish": "多", "bearish": "空", "neutral": "中"}.get(day["dominant"], "中")
            body.append(f"| {day['date']} | {day['bullish']} | {day['bearish']} | {day['neutral']} | {dominant_cn} |")
    body.append("")

    # ── 各因子维度 ──
    factor_names_ordered = [
        "基本面与盈利质量",
        "公告与事件风险",
        "卖方预期",
        "防守价值",
        "筹码结构",
        "高管行为验证",
        "行情确认",
    ]
    section_num = 2
    for fname in factor_names_ordered:
        factor = next((f for f in factors if f["name"] == fname), None)
        if not factor:
            continue
        emoji = _LEVEL_EMOJI.get(factor["level"], "⬜")
        body.append(f"## {section_num}. {factor['name']}")
        body.append(f"- 评分：**{factor['score']}** {emoji} {factor['level']}")
        body.append(f"- {factor['detail']}")
        if factor["evidence"]:
            body.append("- 证据：")
            for e in factor["evidence"][:5]:
                body.append(f"  - {e}")
        body.append("")
        section_num += 1

    # ── 数据覆盖 ──
    body.extend([
        "## 数据覆盖",
        f"- 主营构成：{len(bundle.get('zygc', []))} 条",
        f"- 个股新闻：{len(bundle.get('news', []))} 条",
        f"- 业绩报表：{len(bundle.get('yjbb', []))} 条",
        f"- 个股研报：{len(bundle.get('research_report', []))} 条",
        f"- 公告：{len(bundle.get('notice', []))} 条",
        f"- 财务指标：{len(bundle.get('financial_indicator', []))} 条",
        f"- 股东户数：{len(bundle.get('gdhs', []))} 条",
        f"- 历史行情：{len(bundle.get('hist', []))} 条",
        f"- 增减持：{len(bundle.get('ggcg', []))} 条",
        "",
    ])

    # ── 证据清单 ──
    body.append("## 证据清单")
    if evidence:
        body.extend([f"- {item['type']}（来源：{item['source']}）" for item in evidence])
    else:
        body.append("- 无可用证据")

    # ── 抓取异常 ──
    errors = bundle.get("_errors", [])
    if errors:
        body.extend(["", "## 抓取异常", *[f"- {message}" for message in errors[:10]]])

    return "\n".join([header] + body), evidence
