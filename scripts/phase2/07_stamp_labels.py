from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


import json
from datetime import timedelta
from pathlib import Path

from utils.db import execute_values, get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase2.common import ensure_phase2_columns

logger = configure_logger("phase2_07_stamp_labels")
REG_PATH = Path("data/temp/mule_inflow_registry.json")


def main() -> None:
    inflow = json.loads(REG_PATH.read_text(encoding="utf-8")) if REG_PATH.exists() else {}
    processed = written = errors = 0

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            ensure_phase2_columns(cur)
            cur.execute(
                """
                SELECT t.tx_id, t.sender_agent_id, t.receiver_agent_id, t.amount_inr, t.timestamp,
                       t.ato_signal_score, t.count_1h,
                       a.is_mule, a.is_hawala_node, a.is_structuring,
                       a.is_round_tripper, a.is_dormant_reactivated, a.risk_tier
                FROM transactions t
                JOIN agents a ON a.agent_id = t.sender_agent_id
                ORDER BY t.timestamp
                """
            )
            rows = cur.fetchall()

            labels = []
            for tx_id, sender, receiver, amount, ts, ato_score, count_1h, is_mule, is_hawala, is_struct, is_rt, is_dormant, risk in rows:
                processed += 1
                reason, suspicious, conf = "NONE", False, 0.0
                score = int(ato_score or 0)

                post_bulk = False
                if is_mule and sender in inflow:
                    try:
                        its = __import__("datetime").datetime.fromisoformat(inflow[sender]["inflow_timestamp"])
                        post_bulk = (ts - its).total_seconds() <= 14400
                    except Exception:
                        post_bulk = False

                velocity_ratio_gt_3 = False
                cur.execute("SELECT AVG(count_1h) FROM transactions WHERE sender_agent_id=%s AND timestamp < %s", (sender, ts))
                base = float(cur.fetchone()[0] or 0)
                velocity_ratio_gt_3 = (float(count_1h or 0) / (base + 1)) > 3.0

                if score >= 10:
                    reason, suspicious, conf = "ATO_HIGH", True, 0.93
                elif score >= 6:
                    reason, suspicious, conf = "ATO_MEDIUM", True, 0.75
                elif is_mule and post_bulk:
                    reason, suspicious, conf = "MULE_DISPERSE", True, 0.90
                elif is_hawala:
                    reason, suspicious, conf = "HAWALA_MATCHED", True, 0.95
                elif is_struct and amount < 50000:
                    reason, suspicious, conf = "STRUCTURING", True, 0.80
                elif velocity_ratio_gt_3:
                    reason, suspicious, conf = "VELOCITY_SPIKE", True, 0.70
                elif is_rt:
                    reason, suspicious, conf = "ROUND_TRIP", True, 0.75
                elif is_dormant:
                    reason, suspicious, conf = "DORMANT_BURST", True, 0.70

                labels.append((tx_id, suspicious, reason, conf, score, risk))

            execute_values(
                cur,
                """
                INSERT INTO ground_truth_labels (
                  tx_id, is_suspicious, suspicion_reason, suspicion_confidence, ato_signal_score, agent_risk_tier
                ) VALUES %s ON CONFLICT (tx_id) DO NOTHING
                """,
                labels,
            )
            written = len(labels)

            cur.execute("SELECT COUNT(*) FILTER (WHERE is_suspicious), COUNT(*) FROM ground_truth_labels")
            suspicious_count, total = cur.fetchone()
            rate = (suspicious_count / total) if total else 0.0
            cur.execute("SELECT suspicion_reason, COUNT(*), AVG(suspicion_confidence) FROM ground_truth_labels GROUP BY suspicion_reason")
            breakdown = cur.fetchall()

            note = f"rate={rate:.4f} breakdown={breakdown}"
            if rate < 0.01 or rate > 0.02:
                logger.warning("Suspicious rate out of target range: %.4f", rate)
            log_run(conn, "phase2/07_stamp_labels.py", "success", processed, written, errors, note)

    logger.info("Stamped labels=%s", written)


if __name__ == "__main__":
    main()
