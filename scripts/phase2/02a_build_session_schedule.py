from __future__ import annotations

import random
from datetime import timedelta

from utils.config_utils import load_config
from utils.db import execute_values, get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase2.common import BASE_DATE, ensure_phase2_columns, random_ip_from_cidr, weighted_choice

logger = configure_logger("phase2_02a_build_session_schedule")


def _session_count(user_type: str) -> int:
    if user_type in {"mule_account", "hawala_node", "structuring_user"}:
        return 1
    if user_type in {"ato_victim", "salary_user", "household"}:
        return random.randint(1, 2)
    if user_type == "merchant":
        return random.randint(2, 3)
    if user_type == "domestic_informal":
        return random.randint(0, 1)
    return 1


def main() -> None:
    cfg = load_config("config/simulation.json")
    slots = cfg["time_slots"]

    processed = written = errors = 0
    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            ensure_phase2_columns(cur)
            cur.execute("SELECT COUNT(*) FROM agents")
            if cur.fetchone()[0] < 40000:
                raise RuntimeError("agents table is not ready")

            cur.execute(
                """
                SELECT agent_id, user_type, device_id, device_type, ip_range, city
                FROM agents
                """
            )
            agents = cur.fetchall()

            sessions = []
            for agent_id, user_type, device_id, device_type, ip_range, city in agents:
                per_day_total = 0
                for day in (1, 2, 3):
                    count = _session_count(user_type)
                    per_day_total += count
                    for sn in range(count):
                        weights = [(k, float(v["weight"])) for k, v in slots.items()]
                        if user_type == "merchant":
                            weights = [("morning", 0.35), ("afternoon", 0.45), ("evening", 0.20)]
                        slot = weighted_choice(weights)
                        st_h, st_m = map(int, slots[slot]["start"].split(":"))
                        en_h, en_m = map(int, slots[slot]["end"].split(":"))
                        day_start = BASE_DATE + timedelta(days=day - 1, hours=st_h, minutes=st_m)
                        day_end = BASE_DATE + timedelta(days=day - 1, hours=en_h, minutes=en_m)
                        login_at = day_start + timedelta(seconds=random.randint(0, int((day_end - day_start).total_seconds())))
                        login_at += timedelta(minutes=random.randint(-15, 15))
                        min_bound = BASE_DATE + timedelta(days=day - 1, hours=6)
                        max_bound = BASE_DATE + timedelta(days=day - 1, hours=23, minutes=30)
                        if login_at < min_bound:
                            login_at = min_bound
                        if login_at > max_bound:
                            login_at = max_bound

                        session_id = f"SESS_{agent_id}_D{day}_{sn}"
                        sessions.append(
                            (
                                session_id,
                                agent_id,
                                day,
                                login_at,
                                None,
                                device_id,
                                device_type or "mobile",
                                random_ip_from_cidr(ip_range),
                                city,
                                "IN",
                                round(random.uniform(0.01, 0.15), 3),
                                False,
                                random.random() < 0.02,
                                True,
                                0,
                                False,
                                "scheduled",
                            )
                        )
                if user_type == "domestic_informal" and per_day_total == 0:
                    day = 1
                    session_id = f"SESS_{agent_id}_D{day}_0"
                    sessions.append((session_id, agent_id, day, BASE_DATE + timedelta(hours=8), None, device_id, device_type or "mobile", random_ip_from_cidr(ip_range), city, "IN", 0.05, False, False, True, 0, False, "scheduled"))
                processed += 1

            execute_values(
                cur,
                """
                INSERT INTO sessions (
                  session_id, agent_id, day_number, login_at, logout_at, device_id, device_type, ip_address,
                  ip_geo_city, ip_geo_country, ip_risk_score, device_change, ip_change,
                  login_success, login_failed_count, is_ato_session, status
                ) VALUES %s ON CONFLICT (session_id) DO NOTHING
                """,
                sessions,
                page_size=1000,
            )
            written = len(sessions)
            log_run(conn, "phase2/02a_build_session_schedule.py", "success", processed, written, errors, "session schedule created")

    logger.info("Inserted/attempted %s sessions", written)


if __name__ == "__main__":
    main()
