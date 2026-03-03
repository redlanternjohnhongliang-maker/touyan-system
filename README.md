# 投研整合工作台

> **定位**：本地运行的 AI 投研助手系统，自动抓取九阳公社（韭研公社）研报 + 东方财富多接口个股上下文，输出可直接喂给 AI 的原始输入包，并集成独立股息率多源交叉验证工具。

---

## 目录

1. [功能总览](#1-功能总览)
2. [项目结构](#2-项目结构)
3. [环境准备](#3-环境准备)
4. [快速启动](#4-快速启动)
5. [功能详解](#5-功能详解)
   - 5.1 [信息合并导出](#51-信息合并导出)
   - 5.2 [股息率多源计算](#52-股息率多源计算)
   - 5.3 [V1 骨架版（分析报告）](#53-v1-骨架版分析报告)
6. [命令行工具](#6-命令行工具)
7. [数据源与接口](#7-数据源与接口)
8. [技术架构](#8-技术架构)
20. [已实现功能清单](#9-已实现功能清单)
21. [变更记录](#10-变更记录)
22. [部署指南 (Streamlit Community Cloud)](#11-部署指南-streamlit-community-cloud)

---

## 1. 功能总览

| 功能模块 | 说明 |
|---|---|
| **信息合并导出** | 抓取九阳公社研报 + 东财个股数据，输出 JSON + Markdown 给 AI |
| **股息率工具** | 多源交叉验证（东财/同花顺/巨潮/新浪），共识优先选源，4 季度滚动窗口 |
| **V1 分析骨架** | 研报情绪引擎 + 8 维度多因子评分（基本面、事件、卖方、防守、筹码、高管、行情…） |
| **接口健康自检** | 一键诊断 9 个东财端点的连通性和数据命中率 |

---

## 2. 项目结构

```
投研系统/
├── README.md                   ← 本文档
├── PROJECT_MEMORY.md           ← 项目记忆（长期维护）
├── requirements.txt            ← Python 依赖
├── .gitignore
│
├── src/                        ← 核心源码
│   ├── app.py                  ← V1 骨架版入口
│   ├── app_ai_input.py         ← 投研工作台入口（推荐）
│   ├── collectors/
│   │   ├── eastmoney_adapter.py    ← 东财接口适配器（akshare）
│   │   └── jiuyangongshe_collector.py  ← 九阳公社 DrissionPage 采集
│   ├── config/
│   │   └── settings.py         ← 配置加载（环境变量）
│   ├── services/
│   │   ├── ai_input_runner.py      ← 信息合并导出调度
│   │   ├── archive_retention.py    ← 归档保留策略
│   │   ├── dividend_yield_service.py   ← ★ 股息率核心服务
│   │   ├── factor_scoring.py       ← 多因子评分
│   │   ├── report_ingest.py        ← 研报入库
│   │   ├── sentiment_engine.py     ← 研报情绪分析
│   │   ├── single_stock_report.py  ← 单股分析报告
│   │   ├── symbol_resolver.py      ← 股票名称/代码解析
│   │   ├── truth_guard.py          ← 真实性门槛
│   │   └── update_pipeline.py      ← 数据更新管道
│   ├── storage/
│   │   ├── db.py               ← SQLite 数据库
│   │   └── repository.py       ← 数据仓库层
│   └── ui/
│       ├── ai_input_streamlit.py   ← ★ 投研工作台页面
│       └── streamlit_app.py        ← V1 骨架版页面
│
├── tools/                      ← 工具/调试/校准脚本
│   ├── calc_dividend_yield.py          ← 股息率命令行
│   ├── calibrate_dividend_yield_all.py ← 全市场股息率校准
│   ├── backtest_dividend_sources_5y.py ← 5 年数据源覆盖回测
│   ├── export_merged_raw_content.py    ← 命令行导出合并内容
│   ├── check_interfaces.py            ← 东财接口健康自检
│   └── ...                             ← 其他调试/探索脚本
│
├── data/                       ← 数据目录
│   ├── app.db                  ← SQLite 数据库
│   ├── *.csv                   ← 股票历史行情缓存
│   └── hs300.csv               ← 沪深300成分列表
│
└── quant_archive/              ← 归档目录
    ├── jiuyangongshe_reports/  ← 九阳研报归档
    └── market_hot_news/        ← 热点新闻归档
```

---

## 3. 环境准备

### 3.1 系统要求
- Windows 10/11
- Python 3.9+（推荐使用 Anaconda `quant` 环境）
- Microsoft Edge 浏览器（九阳公社采集需要）

### 3.2 安装依赖

```powershell
# 方式1：使用 Anaconda 环境
conda activate quant
pip install -r requirements.txt

# 方式2：使用 venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

额外依赖（九阳公社采集）：
```powershell
pip install DrissionPage
```

### 3.3 环境变量

```powershell
# Gemini API 密钥（可选，用于 AI 生成）
$env:GEMINI_API_KEY="your_key_here"

# 数据库路径（可选，默认 data/app.db）
$env:APP_DB_PATH="data/app.db"
```

---

## 4. 快速启动

### 推荐方式：投研工作台（Streamlit）

```powershell
cd 投研系统
streamlit run src/ui/ai_input_streamlit.py
```

或通过入口脚本自动拉起：
```powershell
python src/app_ai_input.py
```

### V1 骨架版
```powershell
streamlit run src/ui/streamlit_app.py
```

---

## 5. 功能详解

### 5.1 信息合并导出

**入口**：工作台左侧面板

**流程**：
1. 输入股票代码/名称/首字母（如 `600120`、`国电电力`、`gddl`）
2. 选择模式（`quick` / `deep`）
3. 设置九阳用户（默认"盘前纪要"）
4. 点击"生成并可视化"

> 输入股票时，页面会自动显示“联想候选”下拉栏，点击候选即可自动填入，无需完整手打。

**输出内容**：
| 字段 | 说明 |
|---|---|
| `reports_today` | 九阳研报原文（text + html） |
| `stock_context.zygc` | 主营构成（近12个月） |
| `stock_context.financial` | 主要财务指标 |
| `stock_context.hist` | 近35日股价行情 |
| `stock_context.news` | 近30日相关新闻 |
| `stock_context.research` | 近12月个股研报 |
| `stock_context.notice` | 近期公告 |
| `stock_context.gdhs` | 股东户数变化 |
| `stock_context.ggcg` | 高管增减持 |
| `diagnostics` | 接口健康诊断 |

**更新模式**：
- `quick`：仅拉单票直连接口，秒级响应
- `deep`：包含全量扫描接口（业绩报表、股东户数等），可能几十秒

### 5.2 股息率多源计算

**入口**：工作台右侧面板

#### 核心特性

1. **多源交叉验证**（4 个分红明细数据源）：
   - 同花顺分红明细（`ths_bonus`）
   - 巨潮资讯分红（`cninfo`）
   - 东方财富分红明细（`eastmoney_fhps_detail`）
   - 新浪历史分红（`sina_history_dividend`）

2. **共识优先选源**：
   - 统计各数据源计算的每股分红金额
   - ≥2 个数据源结果一致 → 采用共识值
   - 无共识时回退到权威排序（同花顺 > 巨潮 > 东财 > 新浪）

3. **多轮验证**（默认 2 轮）：
   - 对比两轮之间每个数据源结果是否一致
   - 结果中标注 `round_validation`

4. **4 季度滚动窗口**：
   - 统计最近 4 个季度的分红事件（非固定 365 天）
   - 正确处理年中+年末两次分红的股票

5. **5 个现成股息率备用源**：
   - 东财行情页、同花顺基础页、新浪财经页、雪球页面、百度股市通

6. **输入模式**：
   - **日期模式**：输入日期，使用该日收盘价计算
   - **价格模式**：直接输入假设价格计算

#### 计算公式

```
股息率(%) = 每股分红 (最近4季度累计) / 价格 × 100
每股分红 = 每10股分红 / 10
```

#### UI 展示

- 现成股息率多源对比表（5 个来源 + 状态）
- 每股分红交叉验证表（数据源、每10股合计、每股合计、事件数、最新除权日）
- 共识/权威选择模式标注
- 季度窗口覆盖范围

### 5.3 V1 骨架版（分析报告）

**入口**：`streamlit run src/ui/streamlit_app.py`

**8 维度多因子评分**：
| 维度 | 数据来源 | 权重 |
|---|---|---|
| 主线契合度 | 研报情绪分析 | 15% |
| 基本面 | ROE、毛利率、营收/利润增速、负债率 | 加权 |
| 事件风险 | 新闻关键词 | 加权 |
| 卖方预期 | 研报评级 | 加权 |
| 防守价值 | 最大回撤、MA120偏离 | 加权 |
| 筹码结构 | 股东户数变化 | 加权 |
| 高管行为 | 增减持记录 | 加权 |
| 行情确认 | 20/5日涨幅、波动率 | 加权 |

---

## 6. 命令行工具

### 接口健康自检
```powershell
python tools/check_interfaces.py
```

### 命令行导出合并内容
```powershell
python tools/export_merged_raw_content.py --symbol 600120 --mode deep
# 允许回退旧文
python tools/export_merged_raw_content.py --symbol 600120 --mode deep --allow-fallback
```

### 股息率命令行计算
```powershell
python tools/calc_dividend_yield.py --symbol 600278 --date 2026-02-28 --future-price 8.50
```

### 全市场股息率校准
```powershell
python tools/calibrate_dividend_yield_all.py --count 200
```

### 5 年数据源覆盖回测
```powershell
python tools/backtest_dividend_sources_5y.py --symbol 600795
```

---

## 7. 数据源与接口

### 东方财富（通过 akshare）

| 接口 | 用途 | 模式 |
|---|---|---|
| `stock_zygc_em` | 主营构成 | quick+deep |
| `stock_news_em` | 个股新闻 | quick+deep |
| `stock_yjbb_em` | 业绩报表 | deep |
| `stock_research_report_em` | 个股研报 | quick+deep |
| `stock_notice_report` | 公告 | deep |
| `stock_financial_analysis_indicator_em` | 财务指标 | quick+deep |
| `stock_zh_a_gdhs` | 股东户数 | deep |
| `stock_zh_a_hist` | 历史行情 | quick+deep |
| `stock_ggcg_em` | 高管增减持 | deep |
| `stock_fhps_detail_em` | 东财分红明细 | 股息率 |
| `stock_fhps_detail_ths` | 同花顺分红明细 | 股息率 |
| `stock_dividend_cninfo` | 巨潮分红 | 股息率 |
| `stock_history_dividend_detail` | 新浪历史分红 | 股息率 |

### 网页抓取（现成股息率）

| 来源 | URL 模式 | 方式 |
|---|---|---|
| 东财行情页 | `quote.eastmoney.com` | requests + 正则 |
| 同花顺基础页 | `basic.10jqka.com.cn` | requests + BeautifulSoup |
| 新浪财经 | `finance.sina.com.cn` | requests + 正则 |
| 雪球 | `xueqiu.com` | requests + JSON |
| 百度股市通 | `gushitong.baidu.com` | requests + JSON |

### 九阳公社

| 方式 | 说明 |
|---|---|
| DrissionPage + Edge 无头 | 绕过长亭 WAF，从用户主页抓取研报列表与正文 |

---

## 8. 技术架构

```
┌─────────────────────────────────────────────────┐
│                  Streamlit UI                     │
│      ai_input_streamlit.py / streamlit_app.py    │
├─────────────────────────────────────────────────┤
│                  Services 层                      │
│  ai_input_runner │ dividend_yield_service        │
│  factor_scoring  │ sentiment_engine              │
│  single_stock_report │ symbol_resolver           │
├─────────────────────────────────────────────────┤
│                Collectors 层                      │
│  eastmoney_adapter │ jiuyangongshe_collector     │
├─────────────────────────────────────────────────┤
│                Storage 层                         │
│      SQLite (data/app.db) │ 文件归档              │
├─────────────────────────────────────────────────┤
│              外部数据源                            │
│  akshare │ 东财/同花顺/新浪/巨潮/雪球/百度 网页    │
│  九阳公社 │ Gemini API (可选)                     │
└─────────────────────────────────────────────────┘
```

**关键设计决策**：
- **路径计算**：全部基于 `Path(__file__).resolve().parents[N]`，不依赖绝对路径
- **共识优先**：股息率选源采用多数一致原则，而非固定权威排序
- **4 季度窗口**：分红统计使用自然季度（如 2026Q1/2025Q4/2025Q3/2025Q2），非固定天数
- **多轮验证**：分红明细默认抓取 2 轮，对比一致性
- **降级策略**：任何单一数据源失败均不阻断整体流程

---

## 9. 已实现功能清单

### 核心功能
- [x] 九阳公社研报自动抓取（DrissionPage + Edge 无头）
- [x] 东方财富 9 端点个股数据聚合（akshare）
- [x] 信息合并导出（JSON + Markdown）
- [x] 投研工作台 Streamlit UI（双栏布局、渐变背景、指标卡片）
- [x] 股票名称/代码双向自动解析
- [x] 周末自动回退到周五研报日
- [x] 最新文件覆盖写（latest 文件名）
- [x] 免确认即时交互（已移除 st.form 延迟）

### 股息率系统
- [x] 5 个现成股息率网页抓取源（东财/同花顺/新浪/雪球/百度）
- [x] 4 个分红明细数据源（东财/同花顺/巨潮/新浪）
- [x] 多源共识优先选源算法
- [x] 多轮验证（默认 2 轮）
- [x] 4 季度滚动窗口
- [x] 价格模式 / 日期模式双入口
- [x] 每股分红交叉验证表 UI
- [x] 全市场校准工具
- [x] 5 年数据源覆盖回测

### V1 分析骨架
- [x] 研报情绪引擎（看多/看空/中性，5 日趋势）
- [x] 8 维度多因子评分框架
- [x] 真实性门槛（证据不足不下结论）

### 基础设施
- [x] SQLite 本地存储
- [x] 接口健康自动诊断
- [x] quick/deep 双模式（秒级 vs 完整）
- [x] 公告抓取增强（akshare + 东财公告中心回退）
- [x] 东财热门要闻 Top10
- [x] 九阳用户三种输入方式（名称/UID/URL）
- [x] 中文编码修复（mojibake 兼容）
- [x] 自定义年月日选择器（替代 st.date_input，避免 locale 乱码）

### 待实现
- [ ] Gemini AI 文本生成接入（严格证据绑定）
- [ ] 语义级研报情绪分析
- [ ] 定时自动更新

---

## 10. 变更记录

### 2026-02-27
- 建立项目，创建可运行首版骨架
- 接入九阳公社启发式抓取 + 东财多接口真实调用
- 接口稳定性全面修复（deep 模式 7 OK + 2 WARN + 0 ERR）
- 研报情绪引擎 + 8 维度多因子评分框架

### 2026-02-28
- 新增信息合并导出脚本与 Streamlit 工作台
- 联调验证（600120 deep + 600278 deep）
- 公告抓取增强（东财公告中心回退）
- 九阳用户参数三种输入方式

### 2026-03-01（股息率系统）
- 新增 5 个现成股息率网页抓取源
- 新增 4 个分红明细数据源
- 实现多源共识优先选源（替代固定权威排序）
- 实现多轮验证（默认 2 轮）
- 4 季度滚动窗口（替代固定 365 天）
- 全市场校准工具 + 5 年回测
- 每股分红交叉验证表 UI
- 中文 locale 乱码修复（自定义年月日选择器）
- 移除 st.form 延迟（免确认即时交互）
- 项目重组至 `投研系统/` 独立子目录，路径全部改为相对计算

---

## 11. 部署指南 (Streamlit Community Cloud)

本项目支持一键部署到 Streamlit Community Cloud。请注意：项目原本基于 Windows + Edge 浏览器进行无头抓取，而在 Streamlit Cloud 环境中（Linux）需通过 Chromium 替代。

### 11.1 准备云端依赖
若要让 `DrissionPage` 在极简云端 Linux 环境有效工作，必须在项目**根目录**创建 `packages.txt` 系统依赖文件，内容如下：
```text
chromium
chromium-driver
```

*(注意：你可能需要略微修改 `src/collectors/jiuyangongshe_collector.py` 中 `DrissionPage` 的浏览器路径配置 `ChromeOptions().set_browser_path()` 以适配 Linux 环境的 Chromium)*

### 11.2 上传项目到 GitHub
1. 登录 GitHub，点击右上角 `+` 选择 **New repository**。
2. 填写 Repository name（如 `touyan-system`），保持 Public/Private，**不要**勾选初始化 README/gitignore（保持空仓库）。
3. 在本地 `投研系统` 文件夹内，按住 Shift 右键选择“在此处打开 PowerShell 窗口”，依次输入：
   ```powershell
   git init
   git add .
   git commit -m "Initial commit for Streamlit Deployment"
   git branch -M main
   # 将下面的 URL 替换为你刚在 GitHub 创建的仓库地址
   git remote add origin https://github.com/你的用户名/仓库名.git
   git push -u origin main
   ```

### 11.3 在 Streamlit Community Cloud 部署
1. 访问 [Streamlit Community Cloud](https://share.streamlit.io/)，点击 **Continue with GitHub** 登录并授权。
2. 点击右上角 **New app** -> **Deploy a public app from GitHub** (或 Use existing repo)。
3. 填写部署信息：
   - **Repository**: 选择你刚推送的仓库（例如 `你的用户名/touyan-system`）
   - **Branch**: `main`
   - **Main file path**: 填写你的首页启动脚本路径，如 `src/ui/ai_input_streamlit.py`
4. **【重要】设置环境变量**：点击下方的 **Advanced settings**，在 Secrets 框里粘贴你的环境变量，例如：
   ```toml
   GEMINI_API_KEY = "你的_API_KEY"
   ```
5. 点击 **Deploy!** 
6. 等待 2~5 分钟构建依赖（它会自动读取 `requirements.txt` 和 `packages.txt`），完成后即可通过生成的公网 URL 访问你的投研工作台。
