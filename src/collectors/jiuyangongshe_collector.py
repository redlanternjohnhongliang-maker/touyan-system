"""九阳公社（韭研公社） 盘前纪要采集器。

使用 DrissionPage + Edge 无头浏览器绕过长亭 WAF 验证，
从「盘前纪要」用户主页抓取每日研报列表，再逐篇提取正文。

采集策略：
1. 访问用户主页 -> 通过 URL 分页(/u/{uid}/page/{N})定位目标日期文章
2. 从 Nuxt/Vue 组件数据中提取文章 ID + 标题 + 日期（比 DOM 解析更可靠）
3. 逐篇访问文章 -> 提取 .detail-container 正文
4. 取 ≤ 目标日期的最新 N 篇文章，返回结构化 dict 列表
"""
from __future__ import annotations

import json
import re
import time
import logging
from datetime import date
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup
from bs4.element import Tag

logger = logging.getLogger(__name__)

# -- 常量 --
BASE_URL = "https://www.jiuyangongshe.com"
USER_ID = "4df747be1bf143a998171ef03559b517"
USER_PAGE_URL = f"{BASE_URL}/u/{USER_ID}"
EDGE_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROFILE_DIR = str((PROJECT_ROOT / ".cache" / "edge_jygs").resolve())
USER_ID_PRESET = {
    "盘前纪要": USER_ID,
}


def _make_page():
    """创建无头 Edge 浏览器页面。"""
    from DrissionPage import ChromiumPage, ChromiumOptions

    co = ChromiumOptions()
    co.set_browser_path(EDGE_PATH)
    co.headless()
    co.set_argument("--no-sandbox")
    co.set_argument("--disable-gpu")
    co.set_argument("--disable-blink-features=AutomationControlled")
    co.set_user_data_path(PROFILE_DIR)
    co.auto_port()
    return ChromiumPage(co)


def _parse_user_page(html: str) -> list[dict[str, str]]:
    """从用户主页 HTML 中提取文章列表（去重）。

    策略：
    1. 对每个 /a/xxx 链接，向上遍历 DOM 祖先节点，在最近的小容器里提取日期
    2. 如果 DOM 方式全部失败，回退到全文正则按顺序映射
    """
    html = html or ""
    soup = BeautifulSoup(html, "lxml")

    seen_ids: set[str] = set()
    articles_raw: list[dict[str, str]] = []

    for a_tag in soup.select("a[href]"):
        href = a_tag.get("href", "")
        match = re.search(r"/a/([a-z0-9]+)", href)
        if not match:
            continue
        aid = match.group(1)
        if aid in seen_ids:
            continue
        seen_ids.add(aid)

        title = a_tag.get_text(strip=True) or ""

        # ---- 策略1: 向上查找最近的小容器（<800字符），提取 YYYY-MM-DD ----
        date_str = ""
        node = a_tag
        for _ in range(5):
            parent = node.parent
            if parent is None or parent.name in ("html", "body", "[document]"):
                break
            parent_text = parent.get_text(" ", strip=True)
            if len(parent_text) > 800:
                break  # 容器太大，可能是整个列表
            m = re.search(r"(\d{4}-\d{2}-\d{2})", parent_text)
            if m:
                date_str = m.group(1)
                break
            node = parent

        if not title:
            title = f"\u76d8\u524d\u7eaa\u8981_{aid}"

        articles_raw.append({
            "id": aid,
            "url": f"{BASE_URL}/a/{aid}",
            "date_str": date_str,
            "title": title,
        })

    # ---- 策略2 (兜底): DOM 方式全部没提取到日期时，用全文正则按顺序映射 ----
    if articles_raw and not any(a.get("date_str") for a in articles_raw):
        text = soup.get_text(" ", strip=True)
        timestamps = re.findall(r"(\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2}", text)
        for i, article in enumerate(articles_raw):
            if i < len(timestamps):
                article["date_str"] = timestamps[i]

    logger.info(
        "用户主页解析到 %d 篇（含日期 %d 篇）",
        len(articles_raw),
        sum(1 for a in articles_raw if a.get("date_str")),
    )
    return articles_raw


def _parse_article_page(html: str) -> dict[str, str]:
    """从文章详情页 HTML 中提取标题、日期、正文。"""
    html = html or ""
    soup = BeautifulSoup(html, "lxml")

    title = ""
    title_tag = soup.find("title")
    if title_tag:
        title = title_tag.get_text(strip=True).replace("-\u97ed\u7814\u516c\u793e", "").strip()

    date_str = ""
    date_el = soup.select_one(".date.fs14, [class*=date]")
    if date_el:
        raw = date_el.get_text(strip=True)
        m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
        if m:
            date_str = m.group(1)

    content = ""
    content_html = ""
    detail = soup.select_one(".detail-container")
    if detail:
        for comment_el in detail.select("[class*=comment]"):
            comment_el.decompose()
        content_html = str(detail)
        # 保留段落/列表层级，避免正文被压成一行
        blocks = detail.find_all(
            ["h1", "h2", "h3", "h4", "h5", "p", "li", "blockquote", "pre", "code", "div"],
            recursive=True,
        )
        lines: list[str] = []
        for block in blocks:
            if not isinstance(block, Tag):
                continue
            if block.find(["h1", "h2", "h3", "h4", "h5", "p", "li", "blockquote", "pre", "code", "div"], recursive=False):
                continue
            text = block.get_text(" ", strip=True)
            if text:
                lines.append(text)
        if lines:
            content = "\n\n".join(lines)
        else:
            content = detail.get_text("\n", strip=True)
    else:
        content = soup.get_text("\n", strip=True)
        content_html = str(soup)

    return {"title": title, "date_str": date_str, "content": _clean_report_content(content), "content_html": content_html}


# ── 研报尾缀/页脚垃圾清理 ──

# 固定声明文本（精确匹配或包含匹配）
_BOILERPLATE_LINES: list[str] = [
    "本文内容基于互联网信息整理，不构成投资建议",
    "仅用于研究学习，提高市场认知能力",
    "作者利益披露",
    "不作为证券推荐或投资建议",
    "截至发文时，作者不持有相关标的",
    "声明：文章观点来自网友",
    "不代表韭研公社观点及立场",
    "站内所有文章均不构成投资建议",
    "请投资者注意风险，独立审慎决策",
]

# 页脚交互元素关键词（出现即截断后续所有内容）
_FOOTER_CUTOFF_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"^工分$"),
    re.compile(r"^转发$"),
    re.compile(r"^收藏$"),
    re.compile(r"^投诉$"),
    re.compile(r"^复制链接$"),
    re.compile(r"^分享到微信$"),
    re.compile(r"^打赏作者$"),
    re.compile(r"^无用$"),
    re.compile(r"^有用\s*\d*$"),
    re.compile(r"^\d+个人打赏$"),
    re.compile(r"^评论（\d+）$"),
    re.compile(r"^只看楼主$"),
    re.compile(r"^热度排序$"),
    re.compile(r"^最新发布$"),
    re.compile(r"^最新互动$"),
    re.compile(r"^上一页"),
    re.compile(r"^前往\s*页$"),
    re.compile(r"^确定要分配的奖金$"),
    re.compile(r"^取消\s+确认$"),
    re.compile(r"^确认$"),
    re.compile(r"^\d+$"),  # 纯数字行（分页器）
]


def _clean_report_content(content: str) -> str:
    """清理九阳公社研报正文尾缀：移除声明、页脚交互元素、分页器等垃圾信息。"""
    if not content:
        return content
    lines = content.split("\n")
    cleaned: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            cleaned.append(line)
            continue
        # 检查是否命中固定声明
        if any(bp in stripped for bp in _BOILERPLATE_LINES):
            continue
        # 检查是否命中页脚截断模式 — 一旦匹配，丢弃此行及后续所有内容
        hit_cutoff = False
        for pat in _FOOTER_CUTOFF_PATTERNS:
            if pat.search(stripped):
                hit_cutoff = True
                break
        if hit_cutoff:
            break
        cleaned.append(line)
    # 去除尾部空行
    while cleaned and not cleaned[-1].strip():
        cleaned.pop()
    return "\n".join(cleaned)


def _resolve_user_page_url(target_user: str) -> tuple[str, str]:
    raw = str(target_user or "").strip()
    if not raw:
        return USER_PAGE_URL, USER_ID

    # 支持直接传用户主页 URL
    m_url = re.search(r"/u/([a-zA-Z0-9]+)", raw)
    if raw.startswith("http") and m_url:
        uid = m_url.group(1)
        return f"{BASE_URL}/u/{uid}", uid

    # 支持直接传 UID
    if re.fullmatch(r"[a-zA-Z0-9]{16,64}", raw):
        return f"{BASE_URL}/u/{raw}", raw

    # 预置用户别名
    uid = USER_ID_PRESET.get(raw, USER_ID)
    return f"{BASE_URL}/u/{uid}", uid


def _wait_until_html_contains(page: Any, keywords: list[str], timeout_sec: float = 8.0, poll_sec: float = 0.35) -> str:
    end_ts = time.time() + max(0.5, timeout_sec)
    last_html = ""
    while time.time() < end_ts:
        try:
            html = page.html or ""
        except Exception:
            html = ""
        if html:
            last_html = html
            for kw in keywords:
                if kw and kw in html:
                    return html
        time.sleep(max(0.1, poll_sec))
    return last_html


# ============================================================
# 从 Nuxt/Vue 组件数据中提取文章列表（核心分页方案）
# ============================================================

_JS_EXTRACT_VUE_DATA = """
try {
    const nuxt = window.$nuxt;
    if (!nuxt) return JSON.stringify({error: 'no $nuxt'});
    function find(comp, depth) {
        if (depth > 10) return null;
        if (comp.$data && Array.isArray(comp.$data.list) && comp.$data.paginate) return comp;
        if (comp.$children) {
            for (let child of comp.$children) {
                const found = find(child, depth + 1);
                if (found) return found;
            }
        }
        return null;
    }
    let comp = null;
    for (let child of nuxt.$children) {
        comp = find(child, 0);
        if (comp) break;
    }
    if (!comp) return JSON.stringify({error: 'vue component not found'});

    const list = comp.$data.list || [];
    return JSON.stringify({
        paginate: comp.$data.paginate,
        articles: list.map(a => ({
            id: a.article_id || '',
            title: a.title || '',
            date: a.create_time || '',
        })),
    });
} catch(e) { return JSON.stringify({error: e.message}); }
"""


def _extract_vue_articles(page: Any) -> tuple[list[dict[str, str]], dict]:
    """从页面 Nuxt/Vue 组件中提取文章列表和分页信息。

    Returns:
        (articles, paginate) — articles 是 [{id, title, date}, ...],
        paginate 是 {page, pageSize, total} 或空 dict。
    """
    try:
        raw = page.run_js(_JS_EXTRACT_VUE_DATA, as_expr=False)
        if not raw:
            return [], {}
        data = json.loads(raw)
        if "error" in data:
            logger.debug("Vue 提取失败: %s", data["error"])
            return [], {}
        articles = data.get("articles", [])
        paginate = data.get("paginate", {})
        logger.debug("Vue 提取到 %d 篇文章, paginate=%s", len(articles), paginate)
        return articles, paginate
    except Exception as e:
        logger.debug("Vue 提取异常: %s", e)
        return [], {}


def _user_page_url_for(uid: str, page_num: int = 1) -> str:
    """生成用户主页 URL，支持分页。"""
    if page_num <= 1:
        return f"{BASE_URL}/u/{uid}"
    return f"{BASE_URL}/u/{uid}/page/{page_num}"


def _estimate_page_for_date(target_date: date, paginate: dict) -> int:
    """估算目标日期对应的页码。

    盘前纪要大约每交易日发一篇，每页 15 篇 ≈ 3 周。
    """
    days_ago = max(0, (date.today() - target_date).days)
    page_size = paginate.get("pageSize", 15) or 15
    total_pages = max(1, -(-paginate.get("total", 15) // page_size))  # ceil

    # 大约: 每5个日历天 ≈ 3.5个交易日 ≈ 3.5篇文章
    estimated_articles_ago = int(days_ago * 0.7)
    estimated_page = max(1, estimated_articles_ago // page_size + 1)

    return min(estimated_page, total_pages)


def _load_page_articles(
    page: Any,
    uid: str,
    page_num: int,
) -> tuple[list[dict[str, str]], dict]:
    """加载指定页码的文章列表。

    先尝试 Vue 提取，失败则回退到 HTML 解析。
    Returns: (articles, paginate)
    """
    url = _user_page_url_for(uid, page_num)
    logger.info("加载用户主页第 %d 页: %s", page_num, url)
    page.get(url)
    html = _wait_until_html_contains(
        page,
        keywords=["盘前纪要", "detail", "article", "book-title"],
        timeout_sec=10.0,
    )

    # 优先从 Vue 数据提取
    vue_articles, paginate = _extract_vue_articles(page)
    if vue_articles:
        # 转换为统一格式
        articles = []
        for a in vue_articles:
            date_str = ""
            if a.get("date"):
                m = re.search(r"(\d{4}-\d{2}-\d{2})", a["date"])
                if m:
                    date_str = m.group(1)
            articles.append({
                "id": a["id"],
                "url": f"{BASE_URL}/a/{a['id']}",
                "date_str": date_str,
                "title": a.get("title", ""),
            })
        logger.info("第 %d 页 Vue 提取到 %d 篇文章", page_num, len(articles))
        return articles, paginate

    # 回退: 从 HTML 解析
    fallback_articles = _parse_user_page(html or "")
    logger.info("第 %d 页 HTML 解析到 %d 篇文章（Vue 提取失败，使用回退）", page_num, len(fallback_articles))
    return fallback_articles, {}


def _collect_articles_for_date(
    page: Any,
    uid: str,
    target_date: date,
    num_articles: int,
) -> list[dict[str, str]]:
    """收集 ≤ target_date 的文章，确保至少 num_articles 篇（如果存在的话）。

    策略：
    1. 加载第1页拿分页信息
    2. 如果第1页已有足够匹配文章 → 直接返回
    3. 否则定位目标日期所在页 → 向更旧方向（更大页码）持续加载直到凑够
    """
    target_iso = target_date.isoformat()

    # ---- 第1步：加载第1页，获取分页信息 ----
    articles_p1, paginate = _load_page_articles(page, uid, 1)

    # 快速检查：第1页就够用
    matched_p1 = [a for a in articles_p1 if a.get("date_str") and a["date_str"] <= target_iso]
    if len(matched_p1) >= num_articles:
        logger.info("第1页已包含 %d 篇 ≤ %s 的文章（需 %d），直接使用",
                     len(matched_p1), target_iso, num_articles)
        return articles_p1

    # ---- 第2步：需要翻页 ----
    if not paginate:
        logger.warning("无分页信息，仅使用第1页文章")
        return articles_p1

    page_size = paginate.get("pageSize", 15) or 15
    total_pages = max(1, -(-paginate.get("total", 15) // page_size))
    est_page = _estimate_page_for_date(target_date, paginate)
    logger.info("目标日期 %s 估算在第 %d/%d 页", target_iso, est_page, total_pages)

    # ---- 第3步：二分搜索定位目标日期所在页 ----
    all_articles: list[dict[str, str]] = list(articles_p1)  # 第1页文章先放进去
    loaded_pages: set[int] = {1}
    target_found_page: int | None = None

    # 搜索队列：从估算页开始，前后容错
    search_queue = [est_page]
    for delta in [-1, 1, -2, 2]:
        p = est_page + delta
        if 1 <= p <= total_pages and p not in loaded_pages and p not in search_queue:
            search_queue.append(p)

    for pg in search_queue:
        if pg in loaded_pages:
            continue
        loaded_pages.add(pg)

        pg_articles, _ = _load_page_articles(page, uid, pg)
        if not pg_articles:
            continue
        all_articles.extend(pg_articles)

        page_dates = [a["date_str"] for a in pg_articles if a.get("date_str")]
        if not page_dates:
            continue

        min_date = min(page_dates)
        max_date = max(page_dates)
        logger.info("第 %d 页日期范围: %s ~ %s (目标 %s)", pg, min_date, max_date, target_iso)

        if min_date <= target_iso <= max_date:
            target_found_page = pg
            logger.info("第 %d 页包含目标日期", pg)
            break
        elif target_iso < min_date:
            # 全部太新 → 需要更大页码（更旧）
            logger.info("第 %d 页全部太新，需要更后的页", pg)
            next_pg = pg + max(1, (total_pages - pg) // 3)
            if next_pg not in loaded_pages and next_pg <= total_pages:
                search_queue.append(next_pg)
        else:
            # 全部太旧 → 需要更小页码（更新）
            logger.info("第 %d 页全部太旧，需要更前的页", pg)
            next_pg = max(1, pg - max(1, pg // 3))
            if next_pg not in loaded_pages:
                search_queue.append(next_pg)

    # ---- 第4步：从定位页向旧方向扩展，直到凑够 num_articles 篇 ----
    if target_found_page is not None:
        # 统计当前已有多少匹配文章
        matched_count = sum(
            1 for a in all_articles
            if a.get("date_str") and a["date_str"] <= target_iso
        )
        logger.info("当前已收集 %d 篇 ≤ %s 的文章（需 %d）", matched_count, target_iso, num_articles)

        # 向更旧方向（更大页码）继续加载，直到凑够
        # 同时也加载更新方向的相邻页（确保边界文章不遗漏）
        expand_pg = target_found_page + 1
        # 先加载更新方向相邻页（如还没加载）
        newer_pg = target_found_page - 1
        if newer_pg >= 1 and newer_pg not in loaded_pages:
            loaded_pages.add(newer_pg)
            adj_art, _ = _load_page_articles(page, uid, newer_pg)
            all_articles.extend(adj_art)

        # 向旧方向扩展，最多再加载 5 页
        max_expand = 5
        expanded = 0
        while matched_count < num_articles and expand_pg <= total_pages and expanded < max_expand:
            if expand_pg in loaded_pages:
                expand_pg += 1
                continue
            loaded_pages.add(expand_pg)
            older_art, _ = _load_page_articles(page, uid, expand_pg)
            if not older_art:
                break
            all_articles.extend(older_art)
            new_matched = sum(
                1 for a in older_art
                if a.get("date_str") and a["date_str"] <= target_iso
            )
            matched_count += new_matched
            logger.info("扩展加载第 %d 页，新增 %d 篇匹配，累计 %d/%d",
                        expand_pg, new_matched, matched_count, num_articles)
            expand_pg += 1
            expanded += 1

    # 如果完全没找到目标页，至少返回已收集的文章
    matched_total = sum(1 for a in all_articles if a.get("date_str") and a["date_str"] <= target_iso)
    logger.info("最终收集 %d 篇候选（其中 %d 篇 ≤ %s）", len(all_articles), matched_total, target_iso)
    return all_articles


def fetch_daily_reports_for_user(
    target_user: str = "\u76d8\u524d\u7eaa\u8981",
    target_date: date | None = None,
    window_days: int = 1,
    strict_date: bool = False,
) -> list[dict[str, Any]]:
    """抓取盘前纪要用户的最近 N 篇文章。

    参数语义（兼容旧参数名 ``window_days``）：
        window_days : 需要提取的 **文章篇数**，而非日历天数。
        target_date : 只取 ≤ 该日期的文章。
        strict_date : 若无符合条件文章，True 返回空列表，False 取最新一篇。
    """
    if target_date is None:
        target_date = date.today()

    num_articles = max(1, window_days)  # 兼容旧参数名，语义＝篇数

    user_page_url, resolved_uid = _resolve_user_page_url(target_user)
    page = None
    try:
        page = _make_page()

        # ---- 使用 URL 分页方案收集文章 ----
        articles = _collect_articles_for_date(page, resolved_uid, target_date, num_articles)
        logger.info("收集到 %d 篇候选文章", len(articles))

        if not articles:
            logger.warning("用户主页未获取到文章列表")
            return []

        # ---- 按日期筛选（≤ target_date）并排序 ----
        dated_articles: list[dict[str, str]] = []
        undated_articles: list[dict[str, str]] = []
        too_new_articles: list[dict[str, str]] = []
        for art in articles:
            d = art.get("date_str", "")
            if d:
                if d <= target_date.isoformat():
                    dated_articles.append(art)
                else:
                    too_new_articles.append(art)
            else:
                undated_articles.append(art)

        # 按日期降序（最新优先）+ 去重
        seen_ids: set[str] = set()
        deduped: list[dict[str, str]] = []
        dated_articles.sort(key=lambda a: a["date_str"], reverse=True)
        for a in dated_articles:
            aid = a.get("id", "")
            if aid and aid not in seen_ids:
                seen_ids.add(aid)
                deduped.append(a)
        dated_articles = deduped

        logger.info(
            "文章分类: 符合日期 %d / 太新跳过 %d / 无日期待确认 %d",
            len(dated_articles), len(too_new_articles), len(undated_articles),
        )

        # 取最近 num_articles 篇（多取一些候选，因为逐篇验证时可能会跳过部分）
        # 多取 2 倍 + 无日期补充，确保最终能凑够
        candidate_count = num_articles * 2 + len(undated_articles)
        target_articles = dated_articles[:candidate_count]

        # 若有日期文章不足，补充无日期文章（后续逐篇确认日期后再筛选）
        if len(target_articles) < candidate_count and undated_articles:
            extra = undated_articles[: candidate_count - len(target_articles)]
            target_articles.extend(extra)

        logger.info(
            "需要 %d 篇，候选 %d 篇（有日期 %d / 待确认 %d）",
            num_articles,
            len(target_articles),
            sum(1 for a in target_articles if a.get("date_str")),
            sum(1 for a in target_articles if not a.get("date_str")),
        )

        if not target_articles:
            if strict_date:
                logger.info("严格日期模式，无文章可选，返回空列表")
                return []
            logger.info("无符合条件文章，取列表中最新一篇")
            target_articles = articles[:1]

        # ---- 逐篇抓取正文（以详情页日期为最终判断） ----
        results: list[dict[str, Any]] = []
        for art in target_articles:
            if len(results) >= num_articles:
                break  # 已够篇数
            try:
                logger.info("抓取文章: %s", art["url"])
                page.get(art["url"])
                html = _wait_until_html_contains(page, keywords=["detail-container", "article", "盘前纪要"], timeout_sec=6.0)

                parsed = _parse_article_page(html or page.html or "")

                # 以文章页提取的日期为准（更可靠）
                art_date = parsed["date_str"] or art.get("date_str", "")

                # 始终校验：若文章实际日期晚于 target_date 则跳过
                if art_date and art_date > target_date.isoformat():
                    logger.info("文章 %s 实际日期 %s 晚于目标日期 %s，跳过", art["url"], art_date, target_date.isoformat())
                    continue

                results.append({
                    "title": parsed["title"] or art.get("title", "\u76d8\u524d\u7eaa\u8981"),
                    "content": parsed["content"],
                    "content_html": parsed.get("content_html", ""),
                    "report_date": art_date or target_date.isoformat(),
                    "source_url": art["url"],
                    "target_user": target_user or "盘前纪要",
                    "target_uid": resolved_uid,
                    "user_page_url": user_page_url,
                })
            except Exception as e:
                logger.warning("文章抓取失败 %s: %s", art["url"], e)
                continue

        logger.info("成功抓取 %d 篇盘前纪要", len(results))
        return results

    except Exception as e:
        logger.error("九阳公社采集失败: %s", e)
        return []
    finally:
        if page is not None:
            try:
                page.quit()
            except Exception:
                pass
