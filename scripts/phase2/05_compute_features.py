from __future__ import annotations

from collections import defaultdict, deque

from utils.db import execute_values, get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase2.common import ensure_phase2_columns

logger = configure_logger("phase2_05_compute_features")


def main() -> None:
    processed = written = errors = 0
    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            ensure_phase2_columns(cur)

            cur.execute(
                """
                UPDATE transactions
                SET amt_log = LN(1 + amount_inr),
                    is_international = CASE WHEN receiver_country <> 'IN' THEN TRUE ELSE FALSE END,
                    mcc_enc = CASE mcc_code
                      WHEN '5311' THEN 0
                      WHEN '5411' THEN 1
                      WHEN '6011' THEN 2
                      WHEN '4111' THEN 3
                      WHEN '5912' THEN 4
                      WHEN '7011' THEN 5
                      WHEN '5812' THEN 6
                      ELSE 7 END,
                    payment_type_enc = CASE payment_type
                      WHEN 'card' THEN 0
                      WHEN 'wire' THEN 1
                      WHEN 'ach' THEN 2
                      WHEN 'crypto' THEN 3
                      ELSE 0 END
                """
            )

            cur.execute(
                """
                WITH ranked AS (
                  SELECT tx_id,
                    COUNT(*) OVER (PARTITION BY sender_agent_id ORDER BY timestamp RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS count_1h,
                    SUM(amount_inr) OVER (PARTITION BY sender_agent_id ORDER BY timestamp RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW) AS sum_24h
                  FROM transactions
                )
                UPDATE transactions t
                SET count_1h=r.count_1h,
                    sum_24h=r.sum_24h
                FROM ranked r
                WHERE t.tx_id=r.tx_id
                """
            )

            cur.execute("SELECT tx_id, sender_agent_id, receiver_agent_id, timestamp FROM transactions ORDER BY sender_agent_id, timestamp")
            rows = cur.fetchall()
            processed = len(rows)

            updates = []
            state: dict[str, deque] = defaultdict(deque)
            for tx_id, sender, recv, ts in rows:
                q = state[sender]
                while q and q[0][0] < ts - __import__("datetime").timedelta(hours=24):
                    q.popleft()
                q.append((ts, recv))
                uniq = len({x[1] for x in q})
                updates.append((uniq, tx_id))

            if updates:
                execute_values(cur, "UPDATE transactions AS t SET uniq_payees_24h = data.uniq FROM (VALUES %s) AS data(uniq, tx_id) WHERE t.tx_id=data.tx_id", updates, page_size=5000)
                written += len(updates)

            cur.execute("UPDATE transactions SET avg_tx_24h = sum_24h / (uniq_payees_24h + 1), velocity_score = count_1h * (amt_log / 10.0)")

            cur.execute(
                """
                SELECT COUNT(*) FROM transactions
                WHERE amt_log IS NULL OR count_1h IS NULL OR sum_24h IS NULL OR uniq_payees_24h IS NULL OR velocity_score IS NULL
                """
            )
            nulls = cur.fetchone()[0]
            note = f"feature_nulls={nulls}"
            log_run(conn, "phase2/05_compute_features.py", "success" if nulls == 0 else "failed", processed, written, 0 if nulls == 0 else 1, note)
            if nulls:
                raise RuntimeError(f"Feature null check failed: {nulls}")

    logger.info("Features computed for %s tx rows", processed)


if __name__ == "__main__":
    main()
