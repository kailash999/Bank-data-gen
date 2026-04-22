from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import ipaddress
import random
from datetime import datetime, timedelta, timezone

from utils.config_utils import load_config
from utils.io_utils import read_jsonl, write_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("05_enrich_agents")


def random_ip_from_cidr(cidr: str) -> str:
    network = ipaddress.ip_network(cidr, strict=False)
    host_space = max(1, network.num_addresses - 2)
    offset = random.randint(1, host_space)
    return str(network.network_address + offset)


def select_bank_ifsc(city: str, banks: list[dict]) -> str:
    eligible = [b for b in banks if "all" in b["cities"] or city in b["cities"]]
    bank = random.choice(eligible)
    return f"{bank['ifsc_prefix']}{random.randint(0, 9999):04d}"


def unique_digits(existing: set[str], size: int, retries: int = 10) -> str:
    for _ in range(retries):
        value = "".join(random.choices("0123456789", k=size))
        if value not in existing:
            return value
    raise RuntimeError(f"Unable to generate unique numeric string of size={size}")


def unique_device(existing: set[str]) -> str:
    while True:
        value = "DEV_" + "".join(random.choices("0123456789ABCDEF", k=8))
        if value not in existing:
            return value


def main() -> None:
    valid_agents = list(read_jsonl("data/temp/valid_agents.jsonl"))
    target_total = sum(int(v) for v in load_config("config/agent_segments.json").values())
    if len(valid_agents) != target_total:
        raise RuntimeError(f"Input count mismatch: expected={target_total}, got={len(valid_agents)}")

    city_cfg = load_config("config/city_pool.json")
    bank_cfg = load_config("config/bank_pool.json")

    cities = city_cfg["cities"]
    city_map = {c["name"]: c for c in cities}
    default_city = cities[0]["name"]

    account_numbers: set[str] = set()
    device_ids: set[str] = set()
    mobiles: set[str] = set()
    enriched = []

    for idx, agent in enumerate(valid_agents, start=1):
        city_name = agent.get("city")
        if city_name not in city_map:
            logger.warning("Unknown city '%s'; falling back to %s", city_name, default_city)
            city_name = default_city
        city_info = city_map[city_name]

        account_number = unique_digits(account_numbers, 12)
        device_id = unique_device(device_ids)

        account_numbers.add(account_number)
        device_ids.add(device_id)

        registered_mobile = str(agent.get("registered_mobile") or agent.get("mobile_number") or "")
        if registered_mobile:
            if registered_mobile in mobiles:
                raise RuntimeError(f"Duplicate registered_mobile: {registered_mobile}")
            mobiles.add(registered_mobile)

        account_age_days = int(agent["account_age_days"])
        created_at = datetime.now(timezone.utc) - timedelta(days=account_age_days)
        created_at = created_at.replace(
            hour=random.randint(0, 23), minute=random.randint(0, 59), second=random.randint(0, 59), microsecond=0
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
                "agent_id": f"AGT_{idx:05d}",
                "city": city_name,
                "state": city_info["state"],
                "account_number": account_number,
                "ifsc_code": select_bank_ifsc(city_name, bank_cfg["banks"]),
                "ip_range": random_ip_from_cidr(city_info["isp_cidr"]),
                "device_id": device_id,
                "account_created_at": created_at.isoformat(),
                "kyc_tier": kyc_tier,
                "credit_history_years": round(account_age_days / 365, 1),
                "registered_mobile": registered_mobile,
                "registered_email": agent.get("registered_email") or agent.get("email") or "",
            }
        )
        enriched.append(agent)

    if len({a["agent_id"] for a in enriched}) != len(enriched):
        raise RuntimeError("agent_id uniqueness check failed")
    if len({a["account_number"] for a in enriched}) != len(enriched):
        raise RuntimeError("account_number uniqueness check failed")
    if len({a["device_id"] for a in enriched}) != len(enriched):
        raise RuntimeError("device_id uniqueness check failed")
    if len([a for a in enriched if a.get("registered_mobile")]) != len(mobiles):
        raise RuntimeError("registered_mobile uniqueness check failed")

    write_jsonl("data/temp/enriched_agents.jsonl", enriched)
    logger.info("Enriched %s agents", len(enriched))


if __name__ == "__main__":
    main()
