from __future__ import annotations

from utils.db import execute_values, get_conn, log_run
from utils.io_utils import read_jsonl, write_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("08_load_links_to_db")

COLUMNS = ["link_id", "sender_agent_id", "receiver_agent_id", "link_type", "established_date", "is_active"]


def chunks(items, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def main() -> None:
    links = list(read_jsonl("data/temp/beneficiary_links.jsonl"))
    processed = len(links)
    inserted = 0
    fk_errors = []

    with get_conn("config/db.json") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT agent_id FROM agents")
            valid_agents = {row[0] for row in cur.fetchall()}

            for chunk in chunks(links, 1000):
                valid_rows = []
                for link in chunk:
                    sender = link["sender_agent_id"]
                    receiver = link["receiver_agent_id"]
                    if sender not in valid_agents or receiver not in valid_agents:
                        fk_errors.append({"link_id": link["link_id"], "sender": sender, "receiver": receiver})
                        continue
                    valid_rows.append(tuple(link.get(c) for c in COLUMNS))

                if valid_rows:
                    sql = f"INSERT INTO beneficiary_links ({', '.join(COLUMNS)}) VALUES %s ON CONFLICT (link_id) DO NOTHING"
                    execute_values(cur, sql, valid_rows)
                    inserted += len(valid_rows)

            cur.execute("SELECT COUNT(*) FROM beneficiary_links")
            total_links = cur.fetchone()[0]

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
            isolated = cur.fetchone()
            if isolated:
                log_run(conn, "08_load_links_to_db.py", "failed", processed, inserted, len(fk_errors) + 1, "isolated agent")
                raise RuntimeError(f"Agent has no links: {isolated[0]}")

        if fk_errors:
            write_jsonl("data/temp/errors/link_fk_errors.jsonl", fk_errors)
            log_run(
                conn,
                "08_load_links_to_db.py",
                "failed",
                processed,
                inserted,
                len(fk_errors),
                "fk violations encountered",
            )
            raise RuntimeError(f"FK violations: {len(fk_errors)}")

        log_run(conn, "08_load_links_to_db.py", "success", processed, inserted, 0, f"total_links={total_links}")

    logger.info("Loaded beneficiary links processed=%s inserted=%s", processed, inserted)


if __name__ == "__main__":
    main()
