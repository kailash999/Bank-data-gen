from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.db import get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase3.common import write_csv_json

logger = configure_logger("phase3_02_export_agents")

SQL_AGENTS = """
SELECT
  agent_id                AS customer_id,
  name,
  age,
  gender,
  city,
  state,
  pin_code,
  income_monthly_inr,
  account_number,
  account_type,
  account_age_days,
  account_created_at,
  ifsc_code,
  kyc_tier,
  registered_mobile,
  registered_email,
  device_id,
  device_type,
  ip_range,
  credit_history_years,
  user_type,
  risk_tier,
  is_mule,
  is_hawala_node,
  is_structuring,
  is_high_velocity,
  is_dormant_reactivated,
  is_round_tripper,
  tx_amount_min_inr,
  tx_amount_max_inr,
  tx_freq_min_per_day,
  tx_freq_max_per_day,
  preferred_channels,
  behavior_description
FROM agents
ORDER BY agent_id ASC
"""

SQL_LINKS = """
SELECT
  link_id,
  sender_agent_id         AS sender_customer_id,
  receiver_agent_id       AS receiver_customer_id,
  link_type,
  established_date,
  is_active
FROM beneficiary_links
ORDER BY link_id ASC
"""


def _fetch_dicts(cur, sql: str) -> list[dict]:
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def main() -> None:
    out_dir = Path("data/exports/phase3")
    processed = written = errors = 0

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            agent_rows = _fetch_dicts(cur, SQL_AGENTS)
            link_rows = _fetch_dicts(cur, SQL_LINKS)

            write_csv_json(agent_rows, out_dir / "agents_export.csv", out_dir / "agents_export.json")
            write_csv_json(link_rows, out_dir / "beneficiary_links_export.csv", out_dir / "beneficiary_links_export.json")

            processed = written = len(agent_rows) + len(link_rows)
            log_run(conn, "phase3/02_export_agents.py", "success", processed, written, errors, "agents + links export complete")

    logger.info("Exported agents=%s links=%s to %s", len(agent_rows), len(link_rows), out_dir)


if __name__ == "__main__":
    main()
