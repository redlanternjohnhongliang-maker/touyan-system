from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS watchlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL UNIQUE,
    name TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_site TEXT NOT NULL,
    source_user TEXT NOT NULL,
    title TEXT,
    content TEXT,
    report_date TEXT NOT NULL,
    source_url TEXT,
    content_hash TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    UNIQUE(source_site, source_user, content_hash)
);

CREATE TABLE IF NOT EXISTS stock_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analysis_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    report_markdown TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_watchlist_created_at
ON watchlist(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_reports_report_date_fetched_at
ON reports(report_date DESC, fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_stock_snapshots_symbol_id
ON stock_snapshots(symbol, id DESC);

CREATE INDEX IF NOT EXISTS idx_analysis_logs_symbol_created_at
ON analysis_logs(symbol, created_at DESC);
"""


def get_connection(db_path: str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-20000;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: str) -> None:
    with get_connection(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
