from __future__ import annotations

from utils.db import execute_values, get_conn, log_run
from utils.io_utils import read_jsonl, write_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("08_load_links_to_db")

COLUMNS = [
    "link_id",
    "sender_agent_id",
    "receiver_agent_id",
    "link_type",
    "established_date",
    "is_active",
]


def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def main() -> None:
    links = list(read_jsonl("data/temp/beneficiary_links.jsonl"))
    fk_errors = []

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT agent_id FROM agents")
            valid_agents = {r[0] for r in cur.fetchall()}

            inserted = 0
            for chunk in chunks(links, 1000):
                valid_chunk = []
                for l in chunk:
                    s = l["sender_agent_id"]
                    r = l["receiver_agent_id"]
                    if s not in valid_agents or r not in valid_agents:
                        fk_errors.append({"link_id": l["link_id"], "sender": s, "receiver": r})
                        continue
                    valid_chunk.append(tuple(l.get(c) for c in COLUMNS))

                if valid_chunk:
                    sql = f"""
                        INSERT INTO beneficiary_links ({', '.join(COLUMNS)})
                        VALUES %s
                        ON CONFLICT (link_id) DO NOTHING
                    """
                    execute_values(cur, sql, valid_chunk)
                    inserted += len(valid_chunk)

            cur.execute("SELECT COUNT(*) FROM beneficiary_links")
            total = cur.fetchone()[0]

            cur.execute(
                """
                SELECT a.agent_id
                FROM agents a
                LEFT JOIN beneficiary_links b1 ON b1.sender_agent_id = a.agent_id
                LEFT JOIN beneficiary_links b2 ON b2.receiver_agent_id = a.agent_id
                WHERE b1.link_id IS NULL AND b2.link_id IS NULL
                LIMIT 1
                """
            )
            missing = cur.fetchone()
            if missing:
                log_run(conn, "08_load_links_to_db", "failed", "isolated_agent_detected")
                raise RuntimeError(f"Agent with zero links found: {missing[0]}")

        if fk_errors:
            write_jsonl("data/temp/errors/link_fk_errors.jsonl", fk_errors)
        if fk_errors:
            log_run(conn, "08_load_links_to_db", "failed", f"fk_violations:{len(fk_errors)}")
            raise RuntimeError(f"FK violations detected: {len(fk_errors)}")

        log_run(conn, "08_load_links_to_db", "success", f"inserted:{inserted}, total:{total}")

    logger.info("Loaded links successfully. total=%s", total)


if __name__ == "__main__":
    main()
