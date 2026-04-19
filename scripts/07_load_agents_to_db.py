from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.config_utils import load_config
from utils.db import execute_values, get_conn, log_run
from utils.io_utils import read_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("07_load_agents_to_db")

INSERT_COLUMNS = [
    "agent_id",
    "name",
    "age",
    "gender",
    "city",
    "state",
    "pin_code",
    "income_monthly_inr",
    "account_number",
    "account_type",
    "account_age_days",
    "account_created_at",
    "ifsc_code",
    "kyc_tier",
    "registered_mobile",
    "registered_email",
    "device_id",
    "device_type",
    "ip_range",
    "credit_history_years",
    "user_type",
    "risk_tier",
    "is_mule",
    "is_hawala_node",
    "is_structuring",
    "is_high_velocity",
    "is_dormant_reactivated",
    "is_round_tripper",
    "tx_amount_min_inr",
    "tx_amount_max_inr",
    "tx_freq_min_per_day",
    "tx_freq_max_per_day",
    "preferred_channels",
    "behavior_description",
]


def chunks(items, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def to_row(agent: dict) -> tuple:
    tx_amt = agent.get("tx_amount_range", {})
    tx_freq = agent.get("tx_frequency_per_day", {})
    return (
        agent.get("agent_id"),
        agent.get("name"),
        agent.get("age"),
        agent.get("gender"),
        agent.get("city"),
        agent.get("state"),
        agent.get("pin_code"),
        agent.get("income_monthly_inr"),
        agent.get("account_number"),
        agent.get("account_type"),
        agent.get("account_age_days"),
        agent.get("account_created_at"),
        agent.get("ifsc_code"),
        agent.get("kyc_tier"),
        agent.get("registered_mobile"),
        agent.get("registered_email"),
        agent.get("device_id"),
        agent.get("device_type"),
        agent.get("ip_range"),
        agent.get("credit_history_years"),
        agent.get("user_type"),
        agent.get("risk_tier"),
        agent.get("is_mule"),
        agent.get("is_hawala_node"),
        agent.get("is_structuring", False),
        agent.get("is_high_velocity", False),
        agent.get("is_dormant_reactivated", False),
        agent.get("is_round_tripper", False),
        tx_amt.get("min_inr"),
        tx_amt.get("max_inr"),
        tx_freq.get("min"),
        tx_freq.get("max"),
        agent.get("preferred_channels"),
        agent.get("behavior_description"),
    )


def main() -> None:
    records = list(read_jsonl("data/temp/enriched_agents.jsonl"))
    expected_total = sum(int(v) for v in load_config("config/agent_segments.json").values())

    processed = len(records)
    written = 0

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            for chunk in chunks(records, 500):
                values = [to_row(agent) for agent in chunk]
                sql = f"INSERT INTO agents ({', '.join(INSERT_COLUMNS)}) VALUES %s ON CONFLICT (agent_id) DO NOTHING"
                execute_values(cur, sql, values)
                written += len(values)

            cur.execute("SELECT COUNT(*) FROM agents")
            count = cur.fetchone()[0]
            if count != expected_total:
                log_run(conn, "07_load_agents_to_db.py", "failed", processed, written, 1, f"count mismatch:{count}")
                raise RuntimeError(f"agents table count mismatch: expected={expected_total} actual={count}")

        log_run(conn, "07_load_agents_to_db.py", "success", processed, written, 0, "agents loaded")

    logger.info("Loaded agents processed=%s written=%s", processed, written)


if __name__ == "__main__":
    main()
