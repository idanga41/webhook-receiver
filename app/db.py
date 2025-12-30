import os
import sqlite3
from typing import Any, Dict, List, Optional

DEFAULT_DB_PATH = os.getenv("DB_PATH", "./webhooks.db")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS webhooks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  received_at TEXT NOT NULL,
  source_ip TEXT,
  headers_json TEXT,
  body_json TEXT,
  raw_body TEXT
);
"""

def get_conn(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    # If a directory is provided in DB_PATH, ensure it exists.
    dir_name = os.path.dirname(db_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)

    with get_conn(db_path) as conn:
        conn.execute(SCHEMA_SQL)
        conn.commit()

def insert_webhook(
    received_at: str,
    source_ip: Optional[str],
    headers_json: str,
    body_json: Optional[str],
    raw_body: str,
    db_path: str = DEFAULT_DB_PATH,
) -> int:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO webhooks (received_at, source_ip, headers_json, body_json, raw_body)
            VALUES (?, ?, ?, ?, ?)
            """,
            (received_at, source_ip, headers_json, body_json, raw_body),
        )
        conn.commit()
        return int(cur.lastrowid)

def list_webhooks(limit: int = 50, db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, received_at, source_ip, body_json, raw_body
            FROM webhooks
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

def get_webhook(webhook_id: int, db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    with get_conn(db_path) as conn:
        row = conn.execute(
            """
            SELECT id, received_at, source_ip, headers_json, body_json, raw_body
            FROM webhooks
            WHERE id = ?
            """,
            (webhook_id,),
        ).fetchone()
        return dict(row) if row else None

def delete_all(db_path: str = DEFAULT_DB_PATH) -> int:
    with get_conn(db_path) as conn:
        cur = conn.execute("DELETE FROM webhooks")
        conn.commit()
        return int(cur.rowcount)