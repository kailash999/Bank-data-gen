from __future__ import annotations

import json
import random
from datetime import timedelta
from pathlib import Path

from utils.db import execute_values, get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase2.common import ensure_phase2_columns

logger = configure_logger("phase2_02c_generate_transactions")
REG_PATH = Path("data/temp/mule_inflow_registry.json")


def _channel(amount: float, user_type: str) -> tuple[str, str]:
    if user_type == "hawala_node":
        return "NEFT", "wire"
    if user_type in {"structuring_user", "merchant"} and random.random() < 0.02:
        return "crypto", "crypto"
    if amount < 10000:
        return "UPI", "card"
    if amount < 50000:
        return "IMPS", "wire"
    if amount < 200000:
        return "NEFT", "wire"
    return "RTGS", "wire"


def main() -> None:
    reg = json.loads(REG_PATH.read_text(encoding="utf-8")) if REG_PATH.exists() else {}
    processed = written = errors = 0

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            ensure_phase2_columns(cur)
            cur.execute("SELECT agent_id, user_type, city, tx_amount_min_inr, tx_amount_max_inr, tx_freq_min_per_day, tx_freq_max_per_day FROM agents")
            agents = {r[0]: r for r in cur.fetchall()}

            cur.execute("SELECT sender_agent_id, receiver_agent_id, link_type FROM beneficiary_links")
            links = {}
            link_type = {}
            for s, r, lt in cur.fetchall():
                links.setdefault(s, []).append(r)
                link_type[(s, r)] = lt

            cur.execute(
                """
                SELECT session_id, agent_id, login_at, is_ato_session, status
                FROM sessions
                WHERE status <> 'complete'
                ORDER BY login_at ASC
                """
            )
            sessions = cur.fetchall()

            cur.execute("SELECT COALESCE(MAX(CAST(SUBSTRING(tx_id FROM 4) AS INTEGER)), 0) FROM transactions WHERE tx_id LIKE 'TX_%'")
            tx_counter = int(cur.fetchone()[0]) + 1

            staged = []
            for session_id, agent_id, login_at, is_ato, status in sessions:
                row = agents.get(agent_id)
                if not row:
                    continue
                _, user_type, city, min_amt, max_amt, min_freq, max_freq = row
                receivers_pool = links.get(agent_id, [])
                processed += 1

                is_dispersal = False
                if is_ato:
                    tx_count = random.randint(4, 8)
                elif user_type == "mule_account":
                    inflow = reg.get(agent_id)
                    if inflow:
                        inflow_ts = __import__("datetime").datetime.fromisoformat(inflow["inflow_timestamp"])
                        hours = max(0, (login_at - inflow_ts).total_seconds() / 3600)
                        if hours <= 4:
                            tx_count = random.randint(5, 15)
                            is_dispersal = True
                        else:
                            tx_count = random.randint(1, 3)
                    else:
                        tx_count = random.randint(1, 3)
                elif user_type == "hawala_node":
                    tx_count = random.randint(1, 3)
                elif user_type == "structuring_user":
                    tx_count = random.randint(4, 8)
                elif user_type == "merchant":
                    tx_count = random.randint(max(1, int((min_freq or 3) / 3)), max(1, int((max_freq or 9) / 3)))
                else:
                    tx_count = random.randint(max(1, int(min_freq or 1)), max(1, int(max_freq or 3)))

                if is_ato:
                    amounts = [random.randint(2000, 9900) for _ in range(tx_count)]
                elif is_dispersal and reg.get(agent_id):
                    total = float(reg[agent_id]["inflow_amount"]) * 0.9
                    amounts = [min(49000, max(1000, total / tx_count + random.randint(-2500, 2500))) for _ in range(tx_count)]
                elif user_type == "hawala_node":
                    amounts = [random.randint(50000, 2000000) for _ in range(tx_count)]
                elif user_type == "structuring_user":
                    amounts = [random.randint(35000, 49999) for _ in range(tx_count)]
                else:
                    amounts = [random.randint(int(min_amt or 100), int(max_amt or 50000)) for _ in range(tx_count)]

                if is_ato:
                    all_ids = list(agents.keys())
                    exclude = set(receivers_pool + [agent_id])
                    cands = [a for a in all_ids if a not in exclude]
                    if not cands:
                        continue
                    receivers = random.choices(cands, k=tx_count)
                elif is_dispersal:
                    downstream = [r for r in receivers_pool if link_type.get((agent_id, r)) == "mule_downstream"]
                    if not downstream:
                        downstream = receivers_pool
                    if not downstream:
                        continue
                    receivers = random.choices(downstream, k=tx_count)
                elif user_type == "hawala_node":
                    peers = [r for r in receivers_pool if agents.get(r) and agents[r][1] == "hawala_node" and agents[r][2] != city]
                    if not peers:
                        peers = receivers_pool
                    if not peers:
                        continue
                    receivers = random.choices(peers, k=tx_count)
                else:
                    if not receivers_pool:
                        continue
                    receivers = random.choices(receivers_pool, k=tx_count)

                current_time = login_at + timedelta(seconds=30)
                for idx in range(tx_count):
                    rid = receivers[idx]
                    channel, ptype = _channel(float(amounts[idx]), user_type)
                    narr = random.choice(["Transfer", "Payment", "Settlement"]) if user_type == "mule_account" else "Personal"
                    if user_type == "hawala_node":
                        narr = str(random.randint(1000, 9999))
                    if user_type == "merchant":
                        narr = random.choice([f"Invoice #{random.randint(10000,99999)}", "Goods payment", "Service charges"])

                    staged.append(
                        (
                            f"TX_{tx_counter:08d}",
                            session_id,
                            agent_id,
                            rid,
                            float(amounts[idx]),
                            channel,
                            random.choice(["5311", "5411", "6011", "4111", "5912", "7011", "5812"]),
                            ptype,
                            city,
                            agents.get(rid, [None, None, city])[2],
                            "IN",
                            narr,
                            current_time,
                        )
                    )
                    tx_counter += 1
                    current_time += timedelta(seconds=random.randint(5, 300 if is_ato else 1800))

                cur.execute("UPDATE sessions SET logout_at=%s, status='complete' WHERE session_id=%s", (current_time + timedelta(seconds=30), session_id))

                if len(staged) >= 5000:
                    execute_values(cur, """
                        INSERT INTO transactions (
                          tx_id, session_id, sender_agent_id, receiver_agent_id, amount_inr,
                          channel, mcc_code, payment_type, sender_city, receiver_city,
                          receiver_country, narration, timestamp
                        ) VALUES %s ON CONFLICT (tx_id) DO NOTHING
                    """, staged)
                    written += len(staged)
                    staged = []

            if staged:
                execute_values(cur, """
                    INSERT INTO transactions (
                      tx_id, session_id, sender_agent_id, receiver_agent_id, amount_inr,
                      channel, mcc_code, payment_type, sender_city, receiver_city,
                      receiver_country, narration, timestamp
                    ) VALUES %s ON CONFLICT (tx_id) DO NOTHING
                """, staged)
                written += len(staged)

            log_run(conn, "phase2/02c_generate_transactions.py", "success", processed, written, errors, "transactions generated")

    logger.info("Generated transactions=%s from sessions=%s", written, processed)


if __name__ == "__main__":
    main()
