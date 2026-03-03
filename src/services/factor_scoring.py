"""多因子评分框架 — 7 大维度量化打分。

每个因子输出标准化结构:
    {
        "name": "维度名称",
        "score": 0~100,
        "level": "强" | "中" | "弱" | "不足",
        "detail": "人类可读说明",
        "evidence": ["证据1", "证据2"],
    }

评分原则:
- 数据缺失时 score=0, level="不足"，不做推测。
- 每个因子独立计算，最终由报告层汇总。
"""
from __future__ import annotations

import math
from typing import Any


# ═══════════════ 工具函数 ═══════════════

def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _level(score: int) -> str:
    if score >= 70:
        return "强"
    if score >= 40:
        return "中"
    if score > 0:
        return "弱"
    return "不足"


def _no_data(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "score": 0,
        "level": "不足",
        "detail": f"{name}：数据不足，无法评分。",
        "evidence": [],
    }


# ═══════════════ 1. 基本面与盈利质量 ═══════════════

def score_fundamentals(bundle: dict[str, Any]) -> dict[str, Any]:
    """基于业绩报表 + 财务指标评估盈利质量。"""
    name = "基本面与盈利质量"
    yjbb = bundle.get("yjbb", [])
    fi = bundle.get("financial_indicator", [])

    if not yjbb and not fi:
        return _no_data(name)

    evidence: list[str] = []
    scores: list[float] = []

    # ── 业绩报表维度 ──
    if yjbb:
        latest = yjbb[0]
        # 营收同比
        rev_yoy = _safe_float(latest.get("营业收入-同比增长", latest.get("revenue_yoy", None)))
        # 净利润同比
        profit_yoy = _safe_float(latest.get("净利润-同比增长", latest.get("net_profit_yoy", None)))

        if rev_yoy != 0.0:
            rev_score = max(0, min(100, 50 + rev_yoy * 1.0))
            scores.append(rev_score)
            evidence.append(f"营收同比 {rev_yoy:+.1f}%")
        if profit_yoy != 0.0:
            profit_score = max(0, min(100, 50 + profit_yoy * 1.0))
            scores.append(profit_score)
            evidence.append(f"净利润同比 {profit_yoy:+.1f}%")

    # ── 财务指标维度（英文字段名，来自 akshare stock_financial_analysis_indicator_em）──
    if fi:
        latest_fi = fi[0]

        # 营收同比增长率 (%)
        rev_yoy_fi = _safe_float(latest_fi.get("TOTALOPERATEREVETZ", None))
        if rev_yoy_fi != 0.0 and not yjbb:
            rev_score = max(0, min(100, 50 + rev_yoy_fi * 1.0))
            scores.append(rev_score)
            evidence.append(f"营收同比 {rev_yoy_fi:+.1f}%")

        # 归母净利润同比增长率 (%)
        profit_yoy_fi = _safe_float(latest_fi.get("PARENTNETPROFITTZ", None))
        if profit_yoy_fi != 0.0 and not yjbb:
            profit_score = max(0, min(100, 50 + profit_yoy_fi * 1.0))
            scores.append(profit_score)
            evidence.append(f"净利润同比 {profit_yoy_fi:+.1f}%")

        # ROE (加权)
        roe = _safe_float(latest_fi.get("ROEJQ",
              latest_fi.get("净资产收益率(%)",
              latest_fi.get("加权净资产收益率(%)", None))))
        if roe != 0.0:
            roe_score = max(0, min(100, roe * 4))
            scores.append(roe_score)
            evidence.append(f"ROE {roe:.1f}%")

        # 毛利率
        gpm = _safe_float(latest_fi.get("XSMLL",
              latest_fi.get("销售毛利率(%)", None)))
        if gpm != 0.0:
            gpm_score = max(0, min(100, gpm * 1.2))
            scores.append(gpm_score)
            evidence.append(f"毛利率 {gpm:.1f}%")

        # 资产负债率 (低更好)
        debt_ratio = _safe_float(latest_fi.get("ZCFZL", None))
        if debt_ratio != 0.0:
            # 负债率映射: 10% → 85, 40% → 60, 70% → 30
            debt_score = max(0, min(100, 100 - debt_ratio * 1.0))
            scores.append(debt_score)
            evidence.append(f"资产负债率 {debt_ratio:.1f}%")

    if not scores:
        return _no_data(name)

    avg_score = int(round(sum(scores) / len(scores)))
    return {
        "name": name,
        "score": avg_score,
        "level": _level(avg_score),
        "detail": f"{name}：综合评分 {avg_score}，{'; '.join(evidence)}。",
        "evidence": evidence,
    }


# ═══════════════ 2. 公告与事件风险 ═══════════════

_RISK_KEYWORDS = [
    "处罚", "立案", "违规", "ST", "退市", "诉讼", "仲裁",
    "减值", "暴雷", "担保", "质押", "冻结", "被问询",
]
_POSITIVE_EVENT_KEYWORDS = [
    "回购", "分红", "增持", "股权激励", "并购", "重组",
    "中标", "新签订单", "战略合作",
]


def score_event_risk(bundle: dict[str, Any]) -> dict[str, Any]:
    name = "公告与事件风险"
    notices = bundle.get("notice", [])
    news = bundle.get("news", [])

    all_texts: list[str] = []
    for item in notices:
        all_texts.append(str(item.get("公告标题", item.get("title", ""))))
    for item in news:
        all_texts.append(str(item.get("新闻标题", item.get("新闻内容", item.get("title", "")))))

    if not all_texts:
        return _no_data(name)

    evidence: list[str] = []
    risk_count = 0
    pos_count = 0

    for text in all_texts:
        for kw in _RISK_KEYWORDS:
            if kw in text:
                risk_count += 1
                if len(evidence) < 5:
                    evidence.append(f"⚠ {text[:60]}")
                break
        for kw in _POSITIVE_EVENT_KEYWORDS:
            if kw in text:
                pos_count += 1
                if len(evidence) < 5:
                    evidence.append(f"✅ {text[:60]}")
                break

    # 风险越多分越低 (反向); 正面事件加分
    total = len(all_texts)
    risk_ratio = risk_count / total if total else 0
    pos_ratio = pos_count / total if total else 0
    # 基线 60, 风险-60*ratio, 正面+30*ratio
    score = int(round(60 - risk_ratio * 60 + pos_ratio * 30))
    score = max(0, min(100, score))

    if not evidence:
        evidence.append(f"近期 {total} 条公告/新闻中未检测到显著风险或利好事件")

    return {
        "name": name,
        "score": score,
        "level": _level(score),
        "detail": f"{name}：评分 {score}（风险事件 {risk_count} 条, 正面事件 {pos_count} 条, 总 {total} 条）。",
        "evidence": evidence,
    }


# ═══════════════ 3. 防守价值 ═══════════════

def score_defense_value(bundle: dict[str, Any]) -> dict[str, Any]:
    """基于行情数据评估估值水平和回撤保护。"""
    name = "防守价值"
    hist = bundle.get("hist", [])
    fi = bundle.get("financial_indicator", [])

    if not hist:
        return _no_data(name)

    evidence: list[str] = []
    scores: list[float] = []

    # 近 60 日最大回撤
    closes = [_safe_float(r.get("收盘", r.get("close", 0))) for r in hist]
    closes = [c for c in closes if c > 0]

    if len(closes) >= 20:
        window = closes[-60:] if len(closes) >= 60 else closes
        peak = window[0]
        max_dd = 0.0
        for c in window:
            if c > peak:
                peak = c
            dd = (peak - c) / peak
            if dd > max_dd:
                max_dd = dd
        dd_pct = max_dd * 100
        # 回撤小 → 防守好: dd < 5% → 85, dd 10% → 65, dd 20% → 45, dd 30%+ → 25
        dd_score = max(0, min(100, 95 - dd_pct * 2.5))
        scores.append(dd_score)
        evidence.append(f"近期最大回撤 {dd_pct:.1f}%")

    # 当前价格距 120 日均线位置
    if len(closes) >= 120:
        ma120 = sum(closes[-120:]) / 120
        curr = closes[-1]
        dev = (curr - ma120) / ma120 * 100
        # 在均线下方 → 更具防守性（逆向）: dev=-10% → 70, dev=0 → 55, dev=+20% → 35
        ma_score = max(0, min(100, 55 - dev * 1.0))
        scores.append(ma_score)
        evidence.append(f"偏离 120 日均线 {dev:+.1f}%")

    if not scores:
        return _no_data(name)

    avg = int(round(sum(scores) / len(scores)))
    return {
        "name": name,
        "score": avg,
        "level": _level(avg),
        "detail": f"{name}：评分 {avg}，{'; '.join(evidence)}。",
        "evidence": evidence,
    }


# ═══════════════ 4. 筹码结构 ═══════════════

def score_chips_structure(bundle: dict[str, Any]) -> dict[str, Any]:
    name = "筹码结构"
    gdhs = bundle.get("gdhs", [])

    if not gdhs:
        return _no_data(name)

    evidence: list[str] = []

    # 取最近 2~3 期股东户数看变化
    counts: list[float] = []
    for rec in gdhs[:4]:
        val = _safe_float(rec.get("股东户数", rec.get("gdhs", 0)))
        if val > 0:
            counts.append(val)

    if len(counts) < 2:
        if counts:
            evidence.append(f"最新股东户数 {counts[0]:.0f}")
            return {
                "name": name,
                "score": 50,
                "level": "中",
                "detail": f"{name}：仅有 1 期数据，无法比较变化趋势。最新户数 {counts[0]:.0f}。",
                "evidence": evidence,
            }
        return _no_data(name)

    # 最新 vs 上一期
    latest, prev = counts[0], counts[1]
    change_pct = (latest - prev) / prev * 100

    evidence.append(f"最新股东户数 {latest:.0f}（前期 {prev:.0f}）")
    evidence.append(f"变化 {change_pct:+.1f}%")

    # 户数减少 → 筹码集中 → 利好: change=-10% → 80, 0% → 55, +10% → 30
    score = int(round(55 - change_pct * 2.5))
    score = max(0, min(100, score))

    return {
        "name": name,
        "score": score,
        "level": _level(score),
        "detail": f"{name}：评分 {score}，股东户数 {change_pct:+.1f}%（{'集中' if change_pct < 0 else '分散'}趋势）。",
        "evidence": evidence,
    }


# ═══════════════ 5. 高管行为验证 ═══════════════

def score_executive_behavior(bundle: dict[str, Any]) -> dict[str, Any]:
    name = "高管行为验证"
    ggcg = bundle.get("ggcg", [])

    if not ggcg:
        return _no_data(name)

    evidence: list[str] = []
    net_buy_count = 0
    net_sell_count = 0

    for rec in ggcg:
        change_type = str(rec.get("变动方向", rec.get("增减持", "")))
        if "增持" in change_type:
            net_buy_count += 1
        elif "减持" in change_type:
            net_sell_count += 1

    total = net_buy_count + net_sell_count
    if total == 0:
        evidence.append(f"有 {len(ggcg)} 条增减持记录但无法解析方向")
        return {
            "name": name,
            "score": 50,
            "level": "中",
            "detail": f"{name}：记录解析受限。",
            "evidence": evidence,
        }

    evidence.append(f"增持 {net_buy_count} 笔, 减持 {net_sell_count} 笔")

    # 净增持 → 看好: 全增持 → 85, 全减持 → 15
    buy_ratio = net_buy_count / total
    score = int(round(15 + buy_ratio * 70))
    score = max(0, min(100, score))

    return {
        "name": name,
        "score": score,
        "level": _level(score),
        "detail": f"{name}：评分 {score}，增持 {net_buy_count} 笔 / 减持 {net_sell_count} 笔。",
        "evidence": evidence,
    }


# ═══════════════ 6. 行情确认（趋势/波动/回撤）═══════════════

def score_price_action(bundle: dict[str, Any]) -> dict[str, Any]:
    name = "行情确认"
    hist = bundle.get("hist", [])

    if not hist or len(hist) < 10:
        return _no_data(name)

    closes = [_safe_float(r.get("收盘", r.get("close", 0))) for r in hist]
    closes = [c for c in closes if c > 0]

    if len(closes) < 10:
        return _no_data(name)

    evidence: list[str] = []
    scores: list[float] = []

    # 20 日涨幅
    if len(closes) >= 20:
        ret_20 = (closes[-1] - closes[-20]) / closes[-20] * 100
        # 涨幅映射: -15% → 20, 0% → 50, +15% → 80
        ret_score = max(0, min(100, 50 + ret_20 * 2.0))
        scores.append(ret_score)
        evidence.append(f"20 日涨幅 {ret_20:+.1f}%")

    # 5 日涨幅（短期动量）
    if len(closes) >= 5:
        ret_5 = (closes[-1] - closes[-5]) / closes[-5] * 100
        ret5_score = max(0, min(100, 50 + ret_5 * 3.0))
        scores.append(ret5_score)
        evidence.append(f"5 日涨幅 {ret_5:+.1f}%")

    # 波动率（20 日标准差 / 均值）
    if len(closes) >= 20:
        window = closes[-20:]
        mean = sum(window) / len(window)
        if mean > 0:
            var = sum((c - mean) ** 2 for c in window) / len(window)
            vol = math.sqrt(var) / mean * 100
            # 低波动 → 稳定: vol < 2% → 80, vol 5% → 50, vol > 10% → 20
            vol_score = max(0, min(100, 100 - vol * 8))
            scores.append(vol_score)
            evidence.append(f"20 日波动率 {vol:.1f}%")

    if not scores:
        return _no_data(name)

    avg = int(round(sum(scores) / len(scores)))
    return {
        "name": name,
        "score": avg,
        "level": _level(avg),
        "detail": f"{name}：评分 {avg}，{'; '.join(evidence)}。",
        "evidence": evidence,
    }


# ═══════════════ 7. 卖方预期（研报覆盖度）═══════════════

def score_sell_side(bundle: dict[str, Any]) -> dict[str, Any]:
    name = "卖方预期"
    reports = bundle.get("research_report", [])

    if not reports:
        return _no_data(name)

    evidence: list[str] = []
    count = len(reports)
    evidence.append(f"近期研报 {count} 篇")

    # 覆盖度分数: 1篇 → 30, 10篇 → 60, 50+ → 90
    coverage_score = min(100, 20 + count * 1.5)

    # 提取评级关键词
    upgrade = 0
    downgrade = 0
    for r in reports:
        title = str(r.get("报告名称", r.get("标题", r.get("title", ""))))
        rating = str(r.get("东财评级", r.get("评级", "")))
        combined = title + " " + rating
        for kw in ["买入", "增持", "推荐", "强烈推荐", "首次覆盖"]:
            if kw in combined:
                upgrade += 1
                break
        for kw in ["减持", "卖出", "回避", "下调"]:
            if kw in combined:
                downgrade += 1
                break

    if upgrade or downgrade:
        evidence.append(f"正面评级 {upgrade} 篇, 负面评级 {downgrade} 篇")
        # 调整分数
        if upgrade + downgrade > 0:
            pos_ratio = upgrade / (upgrade + downgrade)
            coverage_score = coverage_score * 0.6 + (pos_ratio * 100) * 0.4

    score = int(round(coverage_score))
    score = max(0, min(100, score))

    return {
        "name": name,
        "score": score,
        "level": _level(score),
        "detail": f"{name}：评分 {score}，近期 {count} 篇研报覆盖。",
        "evidence": evidence,
    }


# ═══════════════ 汇总接口 ═══════════════

def compute_all_factors(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    """计算全部 7 个因子评分，返回列表。"""
    return [
        score_fundamentals(bundle),
        score_event_risk(bundle),
        score_sell_side(bundle),
        score_defense_value(bundle),
        score_chips_structure(bundle),
        score_executive_behavior(bundle),
        score_price_action(bundle),
    ]


def compute_composite_score(factors: list[dict[str, Any]]) -> int:
    """加权平均综合评分。数据不足的维度不参与计算。"""
    weights = {
        "主线契合度": 15,
        "基本面与盈利质量": 20,
        "公告与事件风险": 15,
        "卖方预期": 15,
        "防守价值": 15,
        "筹码结构": 10,
        "高管行为验证": 10,
        "行情确认": 15,
    }
    total_weight = 0
    weighted_sum = 0.0
    for f in factors:
        if f["level"] == "不足":
            continue
        w = weights.get(f["name"], 10)
        weighted_sum += f["score"] * w
        total_weight += w

    if total_weight == 0:
        return 0
    return int(round(weighted_sum / total_weight))
