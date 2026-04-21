from __future__ import annotations
import os

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import psycopg2
from psycopg2 import sql

from utils.config_utils import load_config
from utils.logging_utils import configure_logger

logger = configure_logger("00_create_db_and_tables")


def ensure_database_exists(cfg: dict) -> None:
    if cfg["password"] == "${DB_PASSWORD}":
        cfg["password"] = os.getenv("DB_PASSWORD")
    print(f"Using database config: host={cfg['password']}, port={cfg['port']}, user={cfg['user']}, dbname={cfg['dbname']}")
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        dbname="postgres",
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (cfg["dbname"],))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {}") .format(sql.Identifier(cfg["dbname"])))
                logger.info("Created database %s", cfg["dbname"])
            else:
                logger.info("Database %s already exists", cfg["dbname"])
    finally:
        conn.close()


def apply_schema(cfg: dict, schema_path: Path) -> None:
    schema_sql = schema_path.read_text(encoding="utf-8")
    with psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["dbname"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
    logger.info("Applied schema from %s", schema_path)


def main() -> None:
    cfg = load_config("config/db.json")
    schema_path = ROOT_DIR / "scripts" / "db" / "create_tables.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file missing: {schema_path}")

    ensure_database_exists(cfg)
    apply_schema(cfg, schema_path)


if __name__ == "__main__":
    main()
