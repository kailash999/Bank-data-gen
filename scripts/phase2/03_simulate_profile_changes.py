from __future__ import annotations

import random
from datetime import timedelta

from utils.db import execute_values, get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase2.common import ensure_phase2_columns

logger = configure_logger("phase2_03_simulate_profile_changes")


def mask_mobile(m: str | None) -> str:
    s = (m or "0000000000")[-10:]
    return "X" * 7 + s[-3:]


def mask_email(e: str | None) -> str:
    e = e or "unknown@example.com"
    user, _, dom = e.partition("@")
    return (user[:1] + "***@" + dom) if dom else "u***@example.com"


def main() -> None:
    processed = written = errors = 0
    events = []
    seq = 1

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            ensure_phase2_columns(cur)
            cur.execute(
                """
                SELECT s.session_id, s.agent_id, s.login_at, s.attack_type,
                       a.registered_mobile, a.registered_email, s.is_ato_session
                FROM sessions s
                JOIN agents a ON a.agent_id = s.agent_id
                WHERE s.status='complete'
                """
            )
            sessions = cur.fetchall()

            for session_id, agent_id, login_at, attack_type, mobile, email, is_ato in sessions:
                processed += 1
                cur.execute("SELECT MIN(timestamp) FROM transactions WHERE session_id=%s", (session_id,))
                first_tx = cur.fetchone()[0]
                window_start = login_at + timedelta(seconds=30)
                window_end = (first_tx - timedelta(seconds=30)) if first_tx else (login_at + timedelta(seconds=120))
                if window_end <= window_start:
                    window_end = login_at + timedelta(seconds=60)

                if is_ato:
                    ts_pw = min(window_end - timedelta(seconds=5), window_start + timedelta(seconds=random.randint(10, 30)))
                    events.append((f"EVT_PC_{seq:08d}", agent_id, session_id, "password", None, None, ts_pw)); seq += 1

                    if attack_type in {"sim_swap", "credential_stuffing"}:
                        ts_m = min(window_end - timedelta(seconds=3), window_start + timedelta(seconds=random.randint(30, 90)))
                        events.append((f"EVT_PC_{seq:08d}", agent_id, session_id, "mobile_number", mask_mobile(mobile), mask_mobile("9" + str(random.randint(10**8, 10**9 -1))), ts_m)); seq += 1

                    if attack_type == "sim_swap":
                        ts_e = min(window_end - timedelta(seconds=1), ts_m + timedelta(seconds=random.randint(10, 30)))
                        events.append((f"EVT_PC_{seq:08d}", agent_id, session_id, "email", mask_email(email), mask_email(f"user{random.randint(100,999)}@mail.com"), ts_e)); seq += 1
                else:
                    if random.random() < 0.005:
                        ts = window_start + timedelta(seconds=random.randint(15, 90))
                        events.append((f"EVT_PC_{seq:08d}", agent_id, session_id, "password", None, None, ts)); seq += 1

            if events:
                execute_values(
                    cur,
                    """
                    INSERT INTO profile_change_events (
                      event_id, agent_id, session_id, change_type, old_value_masked, new_value_masked, changed_at
                    ) VALUES %s ON CONFLICT (event_id) DO NOTHING
                    """,
                    events,
                )
                written = len(events)

            cur.execute("UPDATE agents SET profile_changed=TRUE WHERE user_type='ato_victim'")
            log_run(conn, "phase2/03_simulate_profile_changes.py", "success", processed, written, errors, "profile changes simulated")

    logger.info("Inserted profile change events=%s", written)


if __name__ == "__main__":
    main()
