from __future__ import annotations

from utils.db import execute_values, get_conn, log_run
from utils.io_utils import read_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("07_load_agents_to_db")


REQUIRED = [
    "agent_id",
    "name",
    "age",
    "gender",
    "city",
    "income_monthly_inr",
    "account_type",
    "account_age_days",
    "user_type",
    "risk_tier",
    "is_mule",
    "is_hawala_node",
    "tx_amount_range",
    "tx_frequency_per_day",
    "preferred_channels",
    "behavior_description",
    "account_number",
    "ifsc_code",
    "ip_range",
    "device_id",
    "account_created_at",
    "kyc_tier",
    "credit_history_years",
    "segment",
]


def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def main() -> None:
    rows = list(read_jsonl("data/temp/enriched_agents.jsonl"))

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            for chunk in chunks(rows, 500):
                values = [tuple(r.get(c) for c in REQUIRED) for r in chunk]
                sql = f"""
                    INSERT INTO agents ({', '.join(REQUIRED)})
                    VALUES %s
                    ON CONFLICT (agent_id) DO NOTHING
                """
                execute_values(cur, sql, values)

            cur.execute("SELECT COUNT(*) FROM agents")
            count = cur.fetchone()[0]
            if count != 40_000:
                log_run(conn, "07_load_agents_to_db", "failed", f"count_mismatch:{count}")
                raise RuntimeError(f"agents count mismatch: {count}")

            cur.execute(
                "SELECT COUNT(*) FROM agents WHERE "
                + " OR ".join(f"{c} IS NULL" for c in REQUIRED)
            )
            null_count = cur.fetchone()[0]
            if null_count > 0:
                log_run(conn, "07_load_agents_to_db", "failed", f"null_required:{null_count}")
                raise RuntimeError(f"required column nulls found: {null_count}")

        log_run(conn, "07_load_agents_to_db", "success", f"loaded:{len(rows)}")

    logger.info("Loaded agents successfully")


if __name__ == "__main__":
    main()
