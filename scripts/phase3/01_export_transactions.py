from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.db import get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase3.common import write_csv_json

logger = configure_logger("phase3_01_export_transactions")

SQL = """
SELECT
  t.tx_id,
  t.session_id,
  t.sender_agent_id       AS sender_customer_id,
  t.receiver_agent_id     AS receiver_customer_id,
  t.amount_inr,
  t.channel,
  t.mcc_code,
  t.payment_type,
  t.sender_city,
  t.receiver_city,
  t.receiver_country,
  t.narration,
  t.timestamp,
  s.device_id,
  s.device_type,
  s.device_change,
  s.ip_address,
  s.ip_geo_city,
  s.ip_geo_country,
  s.ip_risk_score,
  s.ip_change,
  s.login_failed_count,
  s.is_ato_session,
  s.login_at,
  s.logout_at
FROM transactions t
JOIN sessions s ON s.session_id = t.session_id
ORDER BY t.timestamp ASC
"""


def main() -> None:
    out_dir = Path("data/exports/phase3")
    processed = written = errors = 0

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            processed = written = len(rows)

            write_csv_json(rows, out_dir / "transactions_export.csv", out_dir / "transactions_export.json")
            log_run(conn, "phase3/01_export_transactions.py", "success", processed, written, errors, "transactions export complete")

    logger.info("Exported transactions rows=%s to %s", written, out_dir)


if __name__ == "__main__":
    main()
