from __future__ import annotations

import random

from utils.db import get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase2.common import ensure_phase2_columns, random_ip_from_cidr, weighted_choice

logger = configure_logger("phase2_02b_inject_ato_sessions")
HIGH_RISK = ["185.220.0.0/16", "198.96.0.0/16", "104.244.0.0/16", "45.142.0.0/16"]


def main() -> None:
    processed = written = errors = 0
    used_ips: set[str] = set()

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            ensure_phase2_columns(cur)
            cur.execute("SELECT agent_id, city FROM agents WHERE user_type='ato_victim'")
            victims = cur.fetchall()

            for agent_id, city in victims:
                cur.execute("SELECT COUNT(*) FROM sessions WHERE agent_id=%s AND is_ato_session=TRUE", (agent_id,))
                if cur.fetchone()[0] > 0:
                    continue

                attack_day = weighted_choice([(1, 0.5), (2, 0.35), (3, 0.15)])
                cur.execute(
                    """
                    SELECT session_id, login_at FROM sessions
                    WHERE agent_id=%s AND day_number=%s
                    ORDER BY login_at DESC LIMIT 1
                    """,
                    (agent_id, attack_day),
                )
                pick = cur.fetchone()
                if not pick:
                    cur.execute("SELECT session_id, login_at FROM sessions WHERE agent_id=%s ORDER BY login_at DESC LIMIT 1", (agent_id,))
                    pick = cur.fetchone()
                if not pick:
                    errors += 1
                    continue

                session_id, _ = pick
                attack_type = weighted_choice([
                    ("credential_stuffing", 0.40),
                    ("sim_swap", 0.35),
                    ("phishing", 0.25),
                ])

                ip = None
                for _ in range(10):
                    ip = random_ip_from_cidr(random.choice(HIGH_RISK), default="185.220.0.1")
                    if ip not in used_ips:
                        break
                if ip:
                    used_ips.add(ip)

                if attack_type == "credential_stuffing":
                    failed = random.randint(2, 4)
                elif attack_type == "sim_swap":
                    failed = 0
                else:
                    failed = random.randint(0, 1)

                other_cities = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Kolkata"]
                if city in other_cities:
                    other_cities.remove(city)
                geo_city = random.choice(other_cities or [city])
                geo_country = weighted_choice([("IN", 0.80), ("SG", 0.08), ("AE", 0.07), ("US", 0.05)])

                cur.execute(
                    """
                    UPDATE sessions
                    SET device_id=%s,
                        device_type=%s,
                        ip_address=%s,
                        ip_geo_city=%s,
                        ip_geo_country=%s,
                        device_change=TRUE,
                        ip_change=TRUE,
                        ip_risk_score=%s,
                        is_ato_session=TRUE,
                        attack_type=%s,
                        login_failed_count=%s,
                        login_success=TRUE,
                        status='ato_injected'
                    WHERE session_id=%s
                    """,
                    (
                        f"DEV_ATO_{random.randint(0,16**8-1):08X}",
                        random.choice(["mobile", "desktop"]),
                        ip,
                        geo_city,
                        geo_country,
                        round(random.uniform(0.75, 0.99), 3),
                        attack_type,
                        failed,
                        session_id,
                    ),
                )
                processed += 1
                written += 1

            log_run(conn, "phase2/02b_inject_ato_sessions.py", "success", processed, written, errors, "ato sessions injected")

    logger.info("Injected %s ATO sessions", written)


if __name__ == "__main__":
    main()
