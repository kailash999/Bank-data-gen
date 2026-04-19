from __future__ import annotations

from typing import Any

try:
    import psycopg2
except Exception as exc:  # pragma: no cover
    raise RuntimeError("psycopg2 is required for DB scripts") from exc

from utils.io_utils import load_json
from utils.logging_utils import configure_logger

logger = configure_logger("00_create_db_and_tables")


def create_database_if_missing(cfg: dict[str, Any]) -> None:
    admin_database = cfg.get("admin_database", "postgres")
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        user=cfg["user"],
        password=cfg["password"],
        dbname=admin_database,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (cfg["database"],))
            exists = cur.fetchone() is not None
            if exists:
                logger.info("Database already exists: %s", cfg["database"])
                return

            cur.execute(f'CREATE DATABASE "{cfg["database"]}"')
            logger.info("Created database: %s", cfg["database"])
    finally:
        conn.close()


def create_tables(cfg: dict[str, Any]) -> None:
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["database"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS run_log (
                    id BIGSERIAL PRIMARY KEY,
                    script_name TEXT NOT NULL,
                    run_at TIMESTAMPTZ NOT NULL,
                    status TEXT NOT NULL,
                    details TEXT NOT NULL DEFAULT ''
                )
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agents (
                    agent_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    gender TEXT NOT NULL,
                    city TEXT NOT NULL,
                    income_monthly_inr NUMERIC(14, 2) NOT NULL,
                    account_type TEXT NOT NULL,
                    account_age_days INTEGER NOT NULL,
                    user_type TEXT NOT NULL,
                    risk_tier TEXT NOT NULL,
                    is_mule BOOLEAN NOT NULL,
                    is_hawala_node BOOLEAN NOT NULL,
                    tx_amount_range TEXT NOT NULL,
                    tx_frequency_per_day INTEGER NOT NULL,
                    preferred_channels TEXT NOT NULL,
                    behavior_description TEXT NOT NULL,
                    account_number TEXT NOT NULL,
                    ifsc_code TEXT NOT NULL,
                    ip_range TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    account_created_at TIMESTAMPTZ NOT NULL,
                    kyc_tier TEXT NOT NULL,
                    credit_history_years INTEGER NOT NULL,
                    segment TEXT NOT NULL
                )
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS beneficiary_links (
                    link_id TEXT PRIMARY KEY,
                    sender_agent_id TEXT NOT NULL REFERENCES agents(agent_id),
                    receiver_agent_id TEXT NOT NULL REFERENCES agents(agent_id),
                    link_type TEXT NOT NULL,
                    established_date DATE NOT NULL,
                    is_active BOOLEAN NOT NULL,
                    CONSTRAINT no_self_link CHECK (sender_agent_id <> receiver_agent_id)
                )
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_beneficiary_links_sender
                ON beneficiary_links (sender_agent_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_beneficiary_links_receiver
                ON beneficiary_links (receiver_agent_id)
                """
            )

        conn.commit()
        logger.info("Ensured tables and indexes exist")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    cfg = load_json("config/db.json")
    create_database_if_missing(cfg)
    create_tables(cfg)


if __name__ == "__main__":
    main()
