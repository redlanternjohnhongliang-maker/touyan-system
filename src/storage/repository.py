from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Iterable

from src.storage.db import get_connection


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def add_watchlist_symbol(db_path: str, symbol: str, name: str = "") -> None:
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO watchlist(symbol, name, created_at)
            VALUES (?, ?, ?)
            """,
            (symbol.strip(), name.strip(), _now_iso()),
        )
        conn.commit()


def remove_watchlist_symbol(db_path: str, symbol: str) -> None:
    with get_connection(db_path) as conn:
        conn.execute("DELETE FROM watchlist WHERE symbol = ?", (symbol.strip(),))
        conn.commit()


def list_watchlist(db_path: str) -> list[dict[str, Any]]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT symbol, name, created_at FROM watchlist ORDER BY created_at DESC"
        ).fetchall()
    return [dict(row) for row in rows]


def save_report(
    db_path: str,
    source_site: str,
    source_user: str,
    title: str,
    content: str,
    report_date: str,
    source_url: str,
) -> None:
    content_hash = hashlib.sha256((title + "\n" + content).encode("utf-8")).hexdigest()
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO reports(
                source_site, source_user, title, content, report_date, source_url, content_hash, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_site,
                source_user,
                title,
                content,
                report_date,
                source_url,
                content_hash,
                _now_iso(),
            ),
        )
        conn.commit()


def list_recent_reports(db_path: str, days: int = 5) -> list[dict[str, Any]]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT source_site, source_user, title, content, report_date, source_url, fetched_at
            FROM reports
            ORDER BY report_date DESC, fetched_at DESC
            LIMIT ?
            """,
            (max(days * 10, 20),),
        ).fetchall()
    return [dict(row) for row in rows]


def save_stock_snapshot(db_path: str, symbol: str, payload: dict[str, Any]) -> None:
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO stock_snapshots(symbol, payload_json, fetched_at)
            VALUES (?, ?, ?)
            """,
            (symbol.strip(), json.dumps(payload, ensure_ascii=False), _now_iso()),
        )
        conn.commit()


def get_latest_stock_snapshot(db_path: str, symbol: str) -> dict[str, Any] | None:
    with get_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT payload_json, fetched_at
            FROM stock_snapshots
            WHERE symbol = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol.strip(),),
        ).fetchone()
    if not row:
        return None
    payload = json.loads(row["payload_json"])
    payload["_fetched_at"] = row["fetched_at"]
    return payload


def save_analysis_log(
    db_path: str, symbol: str, report_markdown: str, evidence: Iterable[dict[str, Any]]
) -> None:
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO analysis_logs(symbol, report_markdown, evidence_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                symbol.strip(),
                report_markdown,
                json.dumps(list(evidence), ensure_ascii=False),
                _now_iso(),
            ),
        )
        conn.commit()
