from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from statistics import mean

from utils.config_utils import load_config
from utils.db import get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase2.common import ensure_phase2_columns

logger = configure_logger("phase2_06_compute_ato_scores")


def main() -> None:
    weights = load_config("config/ato_signals.json")["signals"]
    processed = written = errors = 0

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            ensure_phase2_columns(cur)
            cur.execute(
                """
                SELECT s.session_id, s.agent_id, s.login_failed_count, s.device_change, s.ip_change, s.ip_risk_score,
                       a.tx_amount_max_inr
                FROM sessions s
                JOIN agents a ON a.agent_id = s.agent_id
                """
            )
            sessions = cur.fetchall()

            for sid, aid, fail_count, device_change, ip_change, ip_risk, tx_max in sessions:
                processed += 1
                cur.execute("SELECT change_type FROM profile_change_events WHERE session_id=%s", (sid,))
                changes = {r[0] for r in cur.fetchall()}

                cur.execute("SELECT seconds_to_first_tx FROM payee_addition_events WHERE session_id=%s", (sid,))
                payees = [r[0] for r in cur.fetchall()]

                cur.execute("SELECT amount_inr, timestamp FROM transactions WHERE session_id=%s ORDER BY timestamp", (sid,))
                txs = cur.fetchall()

                signals = {
                    "login_failed": (fail_count or 0) >= 2,
                    "device_change": bool(device_change),
                    "ip_change": bool(ip_change),
                    "ip_risk_high": float(ip_risk or 0) > 0.5,
                    "mobile_change": "mobile_number" in changes,
                    "email_change": "email" in changes,
                    "password_change": "password" in changes,
                    "payee_added": bool(payees),
                    "quick_payee_tx": any((x or 999999) < 300 for x in payees),
                    "high_tx_count": len(txs) > 3,
                    "rapid_transfers": False,
                    "high_amount": False,
                }

                if len(txs) >= 2:
                    gaps = [(txs[i + 1][1] - txs[i][1]).total_seconds() for i in range(len(txs) - 1)]
                    signals["rapid_transfers"] = mean(gaps) < 120

                session_total = sum(float(r[0]) for r in txs)
                cur.execute(
                    """
                    SELECT s2.session_id
                    FROM sessions s2
                    WHERE s2.agent_id=%s AND s2.session_id<>%s AND s2.login_at < (SELECT login_at FROM sessions WHERE session_id=%s)
                    """,
                    (aid, sid, sid),
                )
                prior_sids = [r[0] for r in cur.fetchall()]
                baseline = []
                for psid in prior_sids[:30]:
                    cur.execute("SELECT COALESCE(SUM(amount_inr),0) FROM transactions WHERE session_id=%s", (psid,))
                    baseline.append(float(cur.fetchone()[0]))
                avg_base = mean(baseline) if baseline else float(tx_max or 1)
                amt_vs_baseline = session_total / (avg_base + 1)
                signals["high_amount"] = amt_vs_baseline > 3.0

                score = 0
                score_map = {
                    "login_failed": weights["login_failed_count_gte_2"],
                    "device_change": weights["device_change"],
                    "ip_change": weights["ip_change"],
                    "ip_risk_high": weights["ip_risk_score_gt_0_5"],
                    "mobile_change": weights["mobile_change"],
                    "email_change": weights["email_change"],
                    "password_change": weights["password_change"],
                    "payee_added": weights["payee_added"],
                    "quick_payee_tx": weights["time_since_payee_add_lt_300"],
                    "high_tx_count": weights["tx_count_session_gt_3"],
                    "rapid_transfers": weights["tx_gap_avg_s_lt_120"],
                    "high_amount": weights["amt_vs_baseline_gt_3"],
                }
                for k, fired in signals.items():
                    if fired:
                        score += int(score_map[k])

                cur.execute("UPDATE sessions SET ato_signal_score=%s WHERE session_id=%s", (score, sid))
                cur.execute("UPDATE transactions SET ato_signal_score=%s, amt_vs_baseline=%s WHERE session_id=%s", (score, round(amt_vs_baseline, 4), sid))
                written += 1

            cur.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE ato_signal_score >= 10),
                  COUNT(*) FILTER (WHERE ato_signal_score >= 6 AND ato_signal_score < 10),
                  COUNT(*) FILTER (WHERE ato_signal_score < 6),
                  MIN(ato_signal_score), MAX(ato_signal_score), AVG(ato_signal_score)
                FROM sessions
                """
            )
            dist = cur.fetchone()
            log_run(conn, "phase2/06_compute_ato_scores.py", "success", processed, written, errors, f"dist={dist}")

    logger.info("Scored sessions=%s", written)


if __name__ == "__main__":
    main()
