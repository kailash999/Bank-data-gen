from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

import psycopg2
from psycopg2.extras import execute_values

from utils.config_utils import load_config

import os

@contextmanager
def get_conn(config_path: str = "config/db.json") -> Iterator[Any]:
    cfg = load_config(config_path)
    if cfg["password"] == "${DB_PASSWORD}":
        cfg["password"] = os.getenv("DB_PASSWORD")
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["dbname"],
         
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def log_run(
    conn: Any,
    script_name: str,
    status: str,
    rows_processed: int,
    rows_written: int,
    errors: int,
    notes: str,
) -> None:
    now = datetime.utcnow()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO run_log (script_name, status, rows_processed, rows_written, errors, started_at, finished_at, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (script_name, status, rows_processed, rows_written, errors, now, now, notes),
        )


__all__ = ["get_conn", "execute_values", "log_run"]
