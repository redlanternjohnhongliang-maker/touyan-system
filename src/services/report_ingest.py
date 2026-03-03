from __future__ import annotations

from datetime import date

from src.collectors.jiuyangongshe_collector import fetch_daily_reports_for_user
from src.services.archive_retention import archive_jiuyangongshe_reports
from src.storage.repository import save_report


def ingest_today_reports(db_path: str, target_user: str) -> int:
    today = date.today()
    reports = fetch_daily_reports_for_user(target_user=target_user, target_date=today)
    try:
        archive_jiuyangongshe_reports(reports=reports, target_user=target_user, target_day=today)
    except Exception:
        pass
    for item in reports:
        save_report(
            db_path=db_path,
            source_site="jiuyangongshe",
            source_user=target_user,
            title=item.get("title", ""),
            content=item.get("content", ""),
            report_date=item.get("report_date", str(today)),
            source_url=item.get("source_url", ""),
        )
    return len(reports)
