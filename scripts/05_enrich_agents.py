from __future__ import annotations

import ipaddress
import random
from datetime import datetime, timedelta

from utils.io_utils import load_json, read_jsonl, write_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("05_enrich_agents")


def random_ip(cidr: str) -> str:
    network = ipaddress.ip_network(cidr, strict=False)
    hosts = list(network.hosts())
    return str(random.choice(hosts)) if hosts else str(network.network_address)


def pick_ifsc(city: str, bank_pool: list[dict]) -> str:
    eligible = [b for b in bank_pool if city in b.get("cities", [])]
    if not eligible:
        eligible = bank_pool
    bank = random.choice(eligible)
    return f"{bank['ifsc_prefix']}{random.randint(0, 9999):04d}"


def unique_12_digit(seen: set[str], retries: int = 10) -> str:
    for _ in range(retries):
        acc = "".join(random.choices("0123456789", k=12))
        if acc not in seen:
            return acc
    raise RuntimeError("account_number collision after 10 retries")


def unique_device(seen: set[str]) -> str:
    while True:
        dev = "DEV_" + "".join(random.choices("0123456789ABCDEF", k=8))
        if dev not in seen:
            return dev


def main() -> None:
    agents = list(read_jsonl("data/temp/valid_agents.jsonl"))
    if len(agents) != 40_000:
        raise RuntimeError(f"Input record count != 40,000 (got {len(agents)})")

    bank_pool = load_json("config/bank_pool.json")
    city_pool = load_json("config/city_pool.json")
    city_map = {c["city"]: c for c in city_pool}

    seen_accounts: set[str] = set()
    seen_devices: set[str] = set()
    seen_mobile: set[str] = set()
    enriched = []

    for idx, agent in enumerate(agents, start=1):
        city = agent.get("city")
        if city not in city_map:
            logger.warning("City %s not found, falling back to Mumbai", city)
            city = "Mumbai"
        city_cfg = city_map[city]

        agent_id = f"AGT_{idx:05d}"
        account_number = unique_12_digit(seen_accounts)
        device_id = unique_device(seen_devices)

        seen_accounts.add(account_number)
        seen_devices.add(device_id)

        mobile = str(agent.get("mobile_number", ""))
        if mobile:
            if mobile in seen_mobile:
                raise RuntimeError(f"Duplicate mobile_number detected: {mobile}")
            seen_mobile.add(mobile)

        account_age_days = int(agent["account_age_days"])
        created_at = datetime.utcnow() - timedelta(days=account_age_days)
        created_at = created_at.replace(
            hour=random.randint(0, 23), minute=random.randint(0, 59), second=0, microsecond=0
        )

        income = float(agent["income_monthly_inr"])
        if income >= 50000:
            kyc_tier = "FULL"
        elif income >= 15000:
            kyc_tier = "PARTIAL"
        else:
            kyc_tier = "MINIMAL"

        agent.update(
            {
                "agent_id": agent_id,
                "account_number": account_number,
                "ifsc_code": pick_ifsc(city, bank_pool),
                "ip_range": random_ip(city_cfg["isp_cidr"]),
                "device_id": device_id,
                "account_created_at": created_at.isoformat() + "Z",
                "kyc_tier": kyc_tier,
                "credit_history_years": round(account_age_days / 365, 1),
                "city": city,
            }
        )
        enriched.append(agent)

    if len({a["agent_id"] for a in enriched}) != len(enriched):
        raise RuntimeError("agent_id uniqueness check failed")
    if len({a["account_number"] for a in enriched}) != len(enriched):
        raise RuntimeError("account_number uniqueness check failed")
    if len({a["device_id"] for a in enriched}) != len(enriched):
        raise RuntimeError("device_id uniqueness check failed")

    write_jsonl("data/temp/enriched_agents.jsonl", enriched)
    logger.info("Enriched %s agents", len(enriched))


if __name__ == "__main__":
    main()
