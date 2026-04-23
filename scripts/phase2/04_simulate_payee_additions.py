from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


import random
from datetime import timedelta

from utils.db import execute_values, get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase2.common import ensure_phase2_columns

logger = configure_logger("phase2_04_simulate_payee_additions")


def main() -> None:
    processed = written = errors = 0
    events = []
    seq = 1

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            ensure_phase2_columns(cur)
            cur.execute("SELECT sender_agent_id, receiver_agent_id FROM beneficiary_links")
            links = {}
            for s, r in cur.fetchall():
                links.setdefault(s, set()).add(r)

            cur.execute("SELECT agent_id, city, user_type FROM agents")
            agents = {a: (city, ut) for a, city, ut in cur.fetchall()}
            all_ids = list(agents.keys())

            cur.execute(
                """
                SELECT s.session_id, s.agent_id, s.login_at, s.is_ato_session, tx.first_tx
                FROM sessions s
                LEFT JOIN (
                    SELECT session_id, MIN(timestamp) AS first_tx
                    FROM transactions
                    GROUP BY session_id
                ) tx ON tx.session_id = s.session_id
                WHERE s.status='complete'
                """
            )
            sessions = cur.fetchall()

            for session_id, agent_id, login_at, is_ato, first_tx in sessions:
                processed += 1
                if not first_tx:
                    continue

                city, user_type = agents.get(agent_id, (None, None))
                existing = links.get(agent_id, set())

                if is_ato:
                    cands = [x for x in all_ids if x != agent_id and x not in existing and agents.get(x, (None, None))[0] != city]
                    if not cands:
                        cands = [x for x in all_ids if x != agent_id]
                    payee = random.choice(cands)
                    gap = random.randint(40, 180)
                    added = first_tx - timedelta(seconds=gap)
                    events.append((f"EVT_PA_{seq:08d}", agent_id, session_id, payee, added, gap)); seq += 1
                    continue

                if user_type == "mule_account":
                    downs = [x for x in existing]
                    if not downs:
                        continue
                    n = random.randint(2, 5)
                    for _ in range(n):
                        payee = random.choice(downs)
                        added = login_at + timedelta(seconds=random.randint(30, max(31, int((first_tx - login_at).total_seconds()) - 30)))
                        sec = abs(int((first_tx - added).total_seconds()))
                        events.append((f"EVT_PA_{seq:08d}", agent_id, session_id, payee, added, sec)); seq += 1
                    continue

                if user_type == "structuring_user":
                    n = random.randint(1, 3)
                    cands = [x for x in all_ids if x != agent_id and x not in existing]
                    if not cands:
                        continue
                    for _ in range(n):
                        payee = random.choice(cands)
                        added = login_at + timedelta(seconds=random.randint(60, 300))
                        sec = abs(int((first_tx - added).total_seconds()))
                        events.append((f"EVT_PA_{seq:08d}", agent_id, session_id, payee, added, sec)); seq += 1
                    continue

                if random.random() < 0.02:
                    cands = [x for x in all_ids if x != agent_id and x not in existing]
                    if cands:
                        payee = random.choice(cands)
                        sec = random.randint(3600, 86400)
                        added = first_tx - timedelta(seconds=min(sec, int((first_tx - login_at).total_seconds()) - 1 if first_tx > login_at else 1))
                        events.append((f"EVT_PA_{seq:08d}", agent_id, session_id, payee, added, sec)); seq += 1

            if events:
                execute_values(
                    cur,
                    """
                    INSERT INTO payee_addition_events (
                      event_id, agent_id, session_id, new_payee_agent_id, added_at, seconds_to_first_tx
                    ) VALUES %s ON CONFLICT (event_id) DO NOTHING
                    """,
                    events,
                )
                written = len(events)

            log_run(conn, "phase2/04_simulate_payee_additions.py", "success", processed, written, errors, "payee additions simulated")

    logger.info("Inserted payee events=%s", written)


if __name__ == "__main__":
    main()
