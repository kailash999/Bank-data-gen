from __future__ import annotations

import ipaddress
import random
from datetime import datetime, timedelta, timezone
from typing import Iterable

BASE_DATE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def day_bounds(day_number: int) -> tuple[datetime, datetime]:
    start = BASE_DATE + timedelta(days=day_number - 1, hours=6)
    end = BASE_DATE + timedelta(days=day_number - 1, hours=23, minutes=30)
    return start, end


def random_ts_in_window(start: datetime, end: datetime) -> datetime:
    if end <= start:
        return start
    delta_s = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta_s))


def random_ip_from_cidr(cidr: str | None, default: str = "10.0.0.1") -> str:
    if not cidr:
        return default
    try:
        network = ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        return default
    host_space = max(1, network.num_addresses - 2)
    return str(network.network_address + random.randint(1, host_space))


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def weighted_choice(pairs: Iterable[tuple[object, float]]):
    items = list(pairs)
    values = [i[0] for i in items]
    weights = [i[1] for i in items]
    return random.choices(values, weights=weights, k=1)[0]


def ensure_phase2_columns(cur) -> None:
    cur.execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS day_number INTEGER")
    cur.execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS status VARCHAR(20)")
    cur.execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS attack_type VARCHAR(30)")
    cur.execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS ato_signal_score INTEGER")

    cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS mcc_enc INTEGER")
    cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS payment_type_enc INTEGER")
    cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS ato_signal_score INTEGER")
    cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS amt_vs_baseline NUMERIC(14,4)")

    cur.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS profile_changed BOOLEAN DEFAULT FALSE")
