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


def _clip(value, max_len: int):
    if value is None:
        return None
    text = str(value)
    return text[:max_len]


def _to_int(value):
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_row(agent: dict) -> tuple:
    tx_amt = agent.get("tx_amount_range", {})
    tx_freq = agent.get("tx_frequency_per_day", {})
    return (
        _clip(agent.get("agent_id"), 12),
        _clip(agent.get("name"), 100),
        _to_int(agent.get("age")),
        _clip(agent.get("gender"), 10),
        _clip(agent.get("city"), 50),
        _clip(agent.get("state"), 50),
        _clip(agent.get("pin_code"), 6),
        _to_int(agent.get("income_monthly_inr")),
        _clip(agent.get("account_number"), 12),
        _clip(agent.get("account_type"), 20),
        _to_int(agent.get("account_age_days")),
        agent.get("account_created_at"),
        _clip(agent.get("ifsc_code"), 11),
        _clip(agent.get("kyc_tier"), 10),
        _clip(agent.get("registered_mobile"), 10),
        _clip(agent.get("registered_email"), 100),
        _clip(agent.get("device_id"), 20),
        _clip(agent.get("device_type"), 20),
        _clip(agent.get("ip_range"), 20),
        _to_float(agent.get("credit_history_years")),
        _clip(agent.get("user_type"), 30),
        _clip(agent.get("risk_tier"), 10),
        agent.get("is_mule"),
        agent.get("is_hawala_node"),
        agent.get("is_structuring", False),
        agent.get("is_high_velocity", False),
        agent.get("is_dormant_reactivated", False),
        agent.get("is_round_tripper", False),
        _to_int(tx_amt.get("min_inr")),
        _to_int(tx_amt.get("max_inr")),
        _to_float(tx_freq.get("min")),
        _to_float(tx_freq.get("max")),
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
