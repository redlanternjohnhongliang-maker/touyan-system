from __future__ import annotations

import json
import os
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _archive_base_dir() -> Path:
    env_dir = os.getenv("QUANT_ARCHIVE_DIR", "").strip()
    if env_dir:
        return Path(env_dir)
    return _project_root() / "quant_archive"


def _env_keep_days(var_name: str, default_days: int) -> int:
    try:
        return max(1, int(os.getenv(var_name, str(default_days)).strip() or default_days))
    except Exception:
        return max(1, int(default_days))


def _safe_part(text: str) -> str:
    val = (text or "").strip()
    if not val:
        return "unknown"
    val = re.sub(r"[\\/:*?\"<>|]+", "_", val)
    val = re.sub(r"\s+", "_", val)
    return val[:80] or "unknown"


def _try_parse_date_from_name(name: str) -> date | None:
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", str(name or ""))
    if not m:
        return None
    try:
        return date.fromisoformat(m.group(1))
    except Exception:
        return None


def _cleanup_expired_json(folder: Path, keep_days: int, today: date) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    cutoff = today - timedelta(days=max(1, keep_days) - 1)
    for fp in folder.glob("*.json"):
        file_date = _try_parse_date_from_name(fp.name)
        if file_date and file_date < cutoff:
            try:
                fp.unlink(missing_ok=True)
            except Exception:
                pass


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def archive_jiuyangongshe_reports(
    reports: list[dict[str, Any]],
    target_user: str,
    target_day: date | None = None,
    keep_days: int | None = None,
) -> Path:
    day = target_day or date.today()
    resolved_keep_days = int(keep_days) if keep_days is not None else _env_keep_days("JYGS_ARCHIVE_KEEP_DAYS", 5)
    folder = _archive_base_dir() / "jiuyangongshe_reports"
    file_name = f"{day.isoformat()}_{_safe_part(target_user)}.json"
    payload = {
        "meta": {
            "source": "jiuyangongshe",
            "target_user": target_user,
            "target_day": day.isoformat(),
            "keep_days": int(max(1, resolved_keep_days)),
        },
        "count": len(reports or []),
        "items": reports or [],
    }
    out_path = folder / file_name
    _write_json(out_path, payload)
    _cleanup_expired_json(folder=folder, keep_days=resolved_keep_days, today=day)
    return out_path


def archive_market_hot_news(
    news_rows: list[dict[str, Any]],
    target_day: date | None = None,
    keep_days: int | None = None,
) -> Path:
    day = target_day or date.today()
    resolved_keep_days = int(keep_days) if keep_days is not None else _env_keep_days("HOTNEWS_ARCHIVE_KEEP_DAYS", 10)
    folder = _archive_base_dir() / "market_hot_news"
    file_name = f"{day.isoformat()}_merged.json"
    payload = {
        "meta": {
            "source": "eastmoney+tonghuashun_merged",
            "target_day": day.isoformat(),
            "keep_days": int(max(1, resolved_keep_days)),
        },
        "count": len(news_rows or []),
        "items": news_rows or [],
    }
    out_path = folder / file_name
    _write_json(out_path, payload)
    _cleanup_expired_json(folder=folder, keep_days=resolved_keep_days, today=day)
    return out_path
