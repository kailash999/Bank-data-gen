from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


import json
import random
from datetime import timedelta
from pathlib import Path

from utils.db import execute_values, get_conn, log_run
from utils.logging_utils import configure_logger
from scripts.phase2.common import day_bounds, ensure_phase2_columns, random_ip_from_cidr, random_ts_in_window

logger = configure_logger("phase2_01_seed_mule_inflows")
REG_PATH = Path("data/temp/mule_inflow_registry.json")


def main() -> None:
    if REG_PATH.exists():
        try:
            payload = json.loads(REG_PATH.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and payload:
                logger.info("Registry already exists (%s entries), skipping regeneration", len(payload))
                return
        except Exception:
            pass

    processed = written = errors = 0
    registry: dict[str, dict] = {}

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            ensure_phase2_columns(cur)
            cur.execute(
                """
                SELECT bl.sender_agent_id AS controller_id,
                       bl.receiver_agent_id AS mule_id,
                       a.tx_amount_max_inr,
                       a.city AS mule_city,
                       c.city AS controller_city,
                       c.device_id AS controller_device,
                       c.ip_range AS controller_ip_range
                FROM beneficiary_links bl
                JOIN agents a ON a.agent_id = bl.receiver_agent_id
                JOIN agents c ON c.agent_id = bl.sender_agent_id
                WHERE bl.link_type = 'mule_control'
                """
            )
            rows = cur.fetchall()
            if not rows:
                raise RuntimeError("No mule_control links found")

            sessions, txs = [], []
            start, end = day_bounds(1)
            window_end = start.replace(hour=9, minute=0, second=0)

            for controller_id, mule_id, tx_max, mule_city, ctrl_city, ctrl_device, ctrl_ip in rows:
                processed += 1
                tx_max = float(tx_max or 10000)
                amount = random.uniform(tx_max * 5, tx_max * 20)
                amount = int(max(50_000, min(500_0000, amount)))

                inflow_ts = random_ts_in_window(start, window_end) + timedelta(seconds=random.randint(0, 3600))
                session_id = f"SESS_CTRL_{controller_id}_D1"
                tx_id = f"TX_BULK_{mule_id}"

                sessions.append(
                    (
                        session_id,
                        controller_id,
                        1,
                        inflow_ts - timedelta(seconds=60),
                        inflow_ts + timedelta(seconds=120),
                        ctrl_device or "DEV_DEFAULT",
                        "desktop",
                        random_ip_from_cidr(ctrl_ip),
                        ctrl_city,
                        "IN",
                        round(random.uniform(0.01, 0.10), 3),
                        False,
                        False,
                        True,
                        0,
                        False,
                        "complete",
                    )
                )

                txs.append(
                    (
                        tx_id,
                        session_id,
                        controller_id,
                        mule_id,
                        amount,
                        "RTGS" if amount > 200000 else "NEFT",
                        "6011",
                        "wire",
                        ctrl_city,
                        mule_city,
                        "IN",
                        random.choice(["Business settlement", "Project payment", "Contract disbursement"]),
                        inflow_ts,
                    )
                )
                registry[mule_id] = {
                    "controller_id": controller_id,
                    "inflow_amount": amount,
                    "inflow_timestamp": inflow_ts.isoformat(),
                    "tx_id": tx_id,
                }

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
            )
            execute_values(
                cur,
                """
                INSERT INTO transactions (
                  tx_id, session_id, sender_agent_id, receiver_agent_id, amount_inr,
                  channel, mcc_code, payment_type, sender_city, receiver_city,
                  receiver_country, narration, timestamp
                ) VALUES %s ON CONFLICT (tx_id) DO NOTHING
                """,
                txs,
            )

            written = len(txs)
            log_run(conn, "phase2/01_seed_mule_inflows.py", "success", processed, written, errors, "mule inflows seeded")

    REG_PATH.parent.mkdir(parents=True, exist_ok=True)
    REG_PATH.write_text(json.dumps(registry, indent=2), encoding="utf-8")
    logger.info("Seeded inflows for %s mule pairs", len(registry))


if __name__ == "__main__":
    main()
