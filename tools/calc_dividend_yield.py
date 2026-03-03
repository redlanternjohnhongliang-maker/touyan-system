from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.dividend_yield_service import calculate_dividend_yield


def main() -> None:
    parser = argparse.ArgumentParser(
        description="多源确认分红并计算股息率（独立功能）"
    )
    parser.add_argument("--symbol", required=True, help="股票代码，如 600278")
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="查询日期，格式 YYYY-MM-DD，默认今天",
    )
    parser.add_argument(
        "--future-price",
        type=float,
        default=None,
        help="可选：给定未来价格，输出该价格下的预测股息率",
    )
    parser.add_argument(
        "--strict-date",
        action="store_true",
        help="严格匹配除权日=查询日，不做附近日期匹配",
    )
    parser.add_argument(
        "--use-latest-event",
        action="store_true",
        help="忽略日期匹配，直接使用查询日之前最近一次分红事件",
    )
    parser.add_argument(
        "--nearby-days",
        type=int,
        default=10,
        help="非严格模式下，允许在查询日前后匹配事件的最大天数",
    )
    parser.add_argument(
        "--ttm-days",
        type=int,
        default=365,
        help="TTM 股息率统计窗口天数，默认 365",
    )
    parser.add_argument(
        "--out",
        default="",
        help="可选：输出 JSON 文件路径",
    )
    args = parser.parse_args()

    result = calculate_dividend_yield(
        symbol=args.symbol,
        query_date=args.date,
        future_price=args.future_price,
        use_latest_event=args.use_latest_event,
        strict_date=args.strict_date,
        nearby_days=max(0, int(args.nearby_days)),
        ttm_days=max(1, int(args.ttm_days)),
    )

    text = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    print(text)
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
        print(str(out_path.resolve()))


if __name__ == "__main__":
    main()
