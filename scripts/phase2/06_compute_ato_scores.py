from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from utils.config_utils import load_config
from utils.db import execute_values, get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase2.common import ensure_phase2_columns

logger = configure_logger("phase2_06_compute_ato_scores")


def main() -> None:
    weights = load_config("config/ato_signals.json")["signals"]
    processed = written = errors = 0

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            ensure_phase2_columns(cur)
            read_cur = conn.cursor(name="phase2_06_score_rows")
            read_cur.itersize = 5000
            read_cur.execute(
                """
                WITH tx_by_session AS (
                    SELECT session_id, COUNT(*) AS tx_count, COALESCE(SUM(amount_inr), 0) AS session_total
                    FROM transactions
                    GROUP BY session_id
                ),
                tx_gaps AS (
                    SELECT session_id, AVG(gap_s) AS avg_gap_s
                    FROM (
                        SELECT
                            session_id,
                            EXTRACT(EPOCH FROM timestamp - LAG(timestamp) OVER (PARTITION BY session_id ORDER BY timestamp)) AS gap_s
                        FROM transactions
                    ) g
                    WHERE gap_s IS NOT NULL
                    GROUP BY session_id
                ),
                prior_baseline AS (
                    SELECT
                        s.session_id,
                        AVG(COALESCE(prior_tx.session_total, 0)) AS avg_prior_30_total
                    FROM sessions s
                    LEFT JOIN LATERAL (
                        SELECT s2.session_id
                        FROM sessions s2
                        WHERE s2.agent_id = s.agent_id
                          AND s2.login_at < s.login_at
                        ORDER BY s2.login_at DESC
                        LIMIT 30
                    ) prior_s ON TRUE
                    LEFT JOIN tx_by_session prior_tx ON prior_tx.session_id = prior_s.session_id
                    GROUP BY s.session_id
                ),
                session_enriched AS (
                    SELECT
                        s.session_id,
                        s.login_failed_count,
                        s.device_change,
                        s.ip_change,
                        s.ip_risk_score,
                        a.tx_amount_max_inr,
                        COALESCE(txs.tx_count, 0) AS tx_count,
                        COALESCE(txs.session_total, 0) AS session_total,
                        gaps.avg_gap_s,
                        pb.avg_prior_30_total
                    FROM sessions s
                    JOIN agents a ON a.agent_id = s.agent_id
                    LEFT JOIN tx_by_session txs ON txs.session_id = s.session_id
                    LEFT JOIN tx_gaps gaps ON gaps.session_id = s.session_id
                    LEFT JOIN prior_baseline pb ON pb.session_id = s.session_id
                ),
                profile_flags AS (
                    SELECT
                        session_id,
                        BOOL_OR(change_type = 'mobile_number') AS mobile_change,
                        BOOL_OR(change_type = 'email') AS email_change,
                        BOOL_OR(change_type = 'password') AS password_change
                    FROM profile_change_events
                    GROUP BY session_id
                ),
                payee_flags AS (
                    SELECT
                        session_id,
                        COUNT(*) AS payee_count,
                        MIN(seconds_to_first_tx) AS min_seconds_to_first_tx
                    FROM payee_addition_events
                    GROUP BY session_id
                )
                SELECT
                    se.session_id,
                    se.login_failed_count,
                    se.device_change,
                    se.ip_change,
                    se.ip_risk_score,
                    se.tx_amount_max_inr,
                    se.tx_count,
                    se.session_total,
                    se.avg_gap_s,
                    se.avg_prior_30_total,
                    COALESCE(pf.mobile_change, FALSE) AS mobile_change,
                    COALESCE(pf.email_change, FALSE) AS email_change,
                    COALESCE(pf.password_change, FALSE) AS password_change,
                    COALESCE(pay.payee_count, 0) AS payee_count,
                    pay.min_seconds_to_first_tx
                FROM session_enriched se
                LEFT JOIN profile_flags pf ON pf.session_id = se.session_id
                LEFT JOIN payee_flags pay ON pay.session_id = se.session_id
                """
            )

            session_updates = []
            tx_updates = []

            while True:
                sessions = read_cur.fetchmany(5000)
                if not sessions:
                    break

                for (
                    sid,
                    fail_count,
                    device_change,
                    ip_change,
                    ip_risk,
                    tx_max,
                    tx_count,
                    session_total,
                    avg_gap_s,
                    avg_prior_30_total,
                    mobile_change,
                    email_change,
                    password_change,
                    payee_count,
                    min_seconds_to_first_tx,
                ) in sessions:
                    processed += 1

                    avg_base = float(avg_prior_30_total) if avg_prior_30_total is not None else float(tx_max or 1)
                    amt_vs_baseline = float(session_total or 0) / (avg_base + 1)

                    signals = {
                        "login_failed": (fail_count or 0) >= 2,
                        "device_change": bool(device_change),
                        "ip_change": bool(ip_change),
                        "ip_risk_high": float(ip_risk or 0) > 0.5,
                        "mobile_change": bool(mobile_change),
                        "email_change": bool(email_change),
                        "password_change": bool(password_change),
                        "payee_added": int(payee_count or 0) > 0,
                        "quick_payee_tx": (min_seconds_to_first_tx or 999999) < 300,
                        "high_tx_count": int(tx_count or 0) > 3,
                        "rapid_transfers": avg_gap_s is not None and float(avg_gap_s) < 120,
                        "high_amount": amt_vs_baseline > 3.0,
                    }

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

                    session_updates.append((score, sid))
                    tx_updates.append((score, round(amt_vs_baseline, 4), sid))
                    written += 1

                if session_updates:
                    execute_values(
                        cur,
                        "UPDATE sessions AS s SET ato_signal_score = d.score FROM (VALUES %s) AS d(score, session_id) WHERE s.session_id = d.session_id",
                        session_updates,
                        page_size=5000,
                    )
                    session_updates = []

                if tx_updates:
                    execute_values(
                        cur,
                        "UPDATE transactions AS t SET ato_signal_score = d.score, amt_vs_baseline = d.amt_vs_baseline FROM (VALUES %s) AS d(score, amt_vs_baseline, session_id) WHERE t.session_id = d.session_id",
                        tx_updates,
                        page_size=5000,
                    )
                    tx_updates = []

            read_cur.close()

            if session_updates:
                execute_values(
                    cur,
                    "UPDATE sessions AS s SET ato_signal_score = d.score FROM (VALUES %s) AS d(score, session_id) WHERE s.session_id = d.session_id",
                    session_updates,
                    page_size=5000,
                )

            if tx_updates:
                execute_values(
                    cur,
                    "UPDATE transactions AS t SET ato_signal_score = d.score, amt_vs_baseline = d.amt_vs_baseline FROM (VALUES %s) AS d(score, amt_vs_baseline, session_id) WHERE t.session_id = d.session_id",
                    tx_updates,
                    page_size=5000,
                )

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
