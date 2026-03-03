from __future__ import annotations

from typing import Any


def evaluate_evidence(bundle: dict[str, Any]) -> tuple[bool, list[dict[str, str]]]:
    """
    返回：
    - 是否满足最小证据要求（至少 2 类核心证据）
    - 证据列表（用于报告可追溯展示）
    """
    evidence: list[dict[str, str]] = []

    if bundle.get("yjbb"):
        evidence.append({"type": "业绩报表", "source": "stock_yjbb_em"})
    if bundle.get("notice"):
        evidence.append({"type": "公告", "source": "stock_notice_report"})
    if bundle.get("research_report"):
        evidence.append({"type": "个股研报", "source": "stock_research_report_em"})
    if bundle.get("financial_indicator"):
        evidence.append({"type": "财务指标", "source": "stock_financial_analysis_indicator_em"})
    if bundle.get("hist"):
        evidence.append({"type": "历史行情", "source": "stock_zh_a_hist"})
    if bundle.get("gdhs"):
        evidence.append({"type": "股东户数", "source": "stock_zh_a_gdhs"})
    if bundle.get("ggcg"):
        evidence.append({"type": "增减持", "source": "stock_ggcg_em"})
    if bundle.get("news"):
        evidence.append({"type": "个股新闻", "source": "stock_news_em"})

    is_valid = len(evidence) >= 2
    return is_valid, evidence
