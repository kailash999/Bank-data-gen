from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception as exc:  # pragma: no cover
    raise RuntimeError("psycopg2 is required for DB scripts") from exc

from utils.io_utils import load_json


@contextmanager
def get_conn(config_path: str = "config/db.json") -> Iterator[Any]:
    cfg = load_json(config_path)
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["database"],
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def log_run(conn: Any, script_name: str, status: str, details: str = "") -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO run_log (script_name, run_at, status, details)
            VALUES (%s, %s, %s, %s)
            """,
            (script_name, datetime.now(timezone.utc), status, details),
        )


__all__ = ["get_conn", "execute_values", "log_run"]
