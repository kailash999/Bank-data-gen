from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from collections import Counter

from utils.config_utils import load_config
from utils.io_utils import read_jsonl, write_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("04_validate_agents")

REQUIRED_FIELDS = [
    "name",
    "age",
    "gender",
    "city",
    "income_monthly_inr",
    "account_type",
    "account_age_days",
    "user_type",
    "risk_tier",
    "is_mule",
    "is_hawala_node",
    "tx_amount_range",
    "tx_frequency_per_day",
    "preferred_channels",
    "behavior_description",
]


def _to_float(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "").replace("₹", "").replace("INR", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _to_int(value) -> int | None:
    parsed = _to_float(value)
    if parsed is None:
        return None
    try:
        return int(parsed)
    except (TypeError, ValueError):
        return None


def validate_agent(agent: dict, valid_cities: set[str]) -> str | None:
    for field in REQUIRED_FIELDS:
        if agent.get(field) is None:
            return f"missing_field:{field}"

    tx_range = agent.get("tx_amount_range", {})
    min_inr = _to_float(tx_range.get("min_inr", 0))
    max_inr = _to_float(tx_range.get("max_inr", 0))
    if min_inr is None or max_inr is None:
        return "invalid_amount_type"
    if min_inr >= max_inr:
        return "invalid_amount_range"
    if min_inr <= 0:
        return "invalid_amount_min"

    tx_freq = agent.get("tx_frequency_per_day", {})
    min_freq = _to_float(tx_freq.get("min", 0))
    max_freq = _to_float(tx_freq.get("max", 0))
    if min_freq is None or max_freq is None:
        return "invalid_frequency_type"
    if min_freq > max_freq:
        return "invalid_frequency_range"
    if min_freq < 0:
        return "invalid_frequency_min"

    risk = str(agent.get("risk_tier", "")).upper()
    if agent.get("is_mule") and risk != "HIGH":
        return "mule_risk_mismatch"
    if agent.get("is_hawala_node") and risk != "HIGH":
        return "hawala_risk_mismatch"
    if agent.get("is_structuring") and risk not in {"MEDIUM", "HIGH"}:
        return "structuring_risk_mismatch"

    if agent.get("city") not in valid_cities:
        return "invalid_city"

    income = _to_float(agent.get("income_monthly_inr", 0))
    if income is None or income <= 0:
        return "invalid_income"

    account_age = _to_int(agent.get("account_age_days", 0))
    if account_age is None or account_age <= 0:
        return "invalid_account_age"

    channels = agent.get("preferred_channels")
    if not isinstance(channels, list) or not channels:
        return "empty_channels"

    return None


def main() -> None:
    segment_targets = load_config("config/agent_segments.json")
    city_cfg = load_config("config/city_pool.json")
    valid_cities = {c["name"] for c in city_cfg["cities"]}

    valid = []
    rejected = []
    per_segment = Counter()

    for agent in read_jsonl("data/temp/raw_agents.jsonl"):
        reason = validate_agent(agent, valid_cities)
        if reason:
            rejected.append({**agent, "rejection_reason": reason})
            continue
        valid.append(agent)
        per_segment[agent.get("segment", "UNKNOWN")] += 1

    write_jsonl("data/temp/valid_agents.jsonl", valid)
    write_jsonl("data/temp/rejected_agents.jsonl", rejected)

    total = len(valid) + len(rejected)
    rejection_rate = (len(rejected) / total) if total else 1.0
    if rejection_rate > 0.2:
        raise RuntimeError(f"Rejection rate {rejection_rate:.2%} exceeds 20%")

    for segment, target in segment_targets.items():
        actual = per_segment.get(segment, 0)
        if actual == 0:
            raise RuntimeError(f"Segment completely missing after validation: {segment}")
        if actual < int(target):
            logger.warning("Segment shortfall: %s missing %s", segment, int(target) - actual)

    logger.info("Validation finished valid=%s rejected=%s", len(valid), len(rejected))


if __name__ == "__main__":
    main()
