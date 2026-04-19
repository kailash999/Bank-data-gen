from __future__ import annotations

from collections import Counter

from utils.io_utils import load_json, read_jsonl, write_jsonl
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


def validate(agent: dict, valid_cities: set[str]) -> str | None:
    for field in REQUIRED_FIELDS:
        if field not in agent or agent[field] is None:
            return f"missing_field:{field}"

    txr = agent.get("tx_amount_range", {})
    if txr.get("min_inr", 0) >= txr.get("max_inr", 0):
        return "invalid_amount_range_order"
    if txr.get("min_inr", 0) <= 0:
        return "invalid_amount_range_min"

    freq = agent.get("tx_frequency_per_day", {})
    if freq.get("min", 0) > freq.get("max", 0):
        return "invalid_frequency_order"
    if freq.get("min", 0) < 0:
        return "invalid_frequency_min"

    risk = str(agent.get("risk_tier", "")).upper()
    if agent.get("is_mule") is True and risk != "HIGH":
        return "mule_risk_not_high"
    if agent.get("is_hawala_node") is True and risk != "HIGH":
        return "hawala_risk_not_high"
    if agent.get("is_structuring") is True and risk not in {"MEDIUM", "HIGH"}:
        return "structuring_risk_invalid"

    if agent.get("city") not in valid_cities:
        return "invalid_city"
    if float(agent.get("income_monthly_inr", 0)) <= 0:
        return "invalid_income"
    if int(agent.get("account_age_days", 0)) <= 0:
        return "invalid_account_age"

    channels = agent.get("preferred_channels")
    if not isinstance(channels, list) or len(channels) == 0:
        return "empty_channels"

    return None


def main() -> None:
    segments_cfg = load_json("config/agent_segments.json")
    city_pool = load_json("config/city_pool.json")
    valid_cities = {c["city"] for c in city_pool}

    valid_rows = []
    rejected_rows = []
    segment_counts = Counter()

    for agent in read_jsonl("data/temp/raw_agents.jsonl"):
        reason = validate(agent, valid_cities)
        if reason is None:
            valid_rows.append(agent)
            segment_counts[agent.get("segment", "UNKNOWN")] += 1
        else:
            agent["rejection_reason"] = reason
            rejected_rows.append(agent)

    write_jsonl("data/temp/valid_agents.jsonl", valid_rows)
    write_jsonl("data/temp/rejected_agents.jsonl", rejected_rows)

    total = len(valid_rows) + len(rejected_rows)
    rej_rate = (len(rejected_rows) / total) if total else 1.0
    if rej_rate > 0.2:
        raise RuntimeError(f"Rejection rate too high: {rej_rate:.2%}")

    for segment, target in segments_cfg.items():
        actual = segment_counts.get(segment, 0)
        if actual == 0:
            raise RuntimeError(f"Segment missing from valid set: {segment}")
        if actual < int(target):
            logger.warning("Segment shortfall for %s: missing=%s", segment, int(target) - actual)

    logger.info("Validation complete. valid=%s rejected=%s", len(valid_rows), len(rejected_rows))


if __name__ == "__main__":
    main()
