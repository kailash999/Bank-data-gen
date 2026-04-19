from __future__ import annotations

import random
from collections import defaultdict, deque
from datetime import datetime, timedelta

from utils.io_utils import read_jsonl, write_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("06_build_links")


def rand_date_within_2y() -> str:
    return (datetime.utcnow() - timedelta(days=random.randint(0, 730))).date().isoformat()


def add_link(links: list[dict], seen_pairs: set[tuple[str, str]], sender: str, receiver: str, link_type: str) -> bool:
    if sender == receiver:
        return False
    pair = (sender, receiver)
    if pair in seen_pairs:
        return False
    seen_pairs.add(pair)
    links.append(
        {
            "sender_agent_id": sender,
            "receiver_agent_id": receiver,
            "link_type": link_type,
            "established_date": rand_date_within_2y(),
            "is_active": True,
        }
    )
    return True


def build_connected_hawala(hawala: list[dict], links: list[dict], seen_pairs: set[tuple[str, str]]):
    # ring for guaranteed connectedness + extra random peers across cities
    ids = [a["agent_id"] for a in hawala]
    for i, src in enumerate(ids):
        dst = ids[(i + 1) % len(ids)]
        add_link(links, seen_pairs, src, dst, "hawala_peer")
        add_link(links, seen_pairs, dst, src, "hawala_peer")

    for a in hawala:
        peers = [h for h in hawala if h["city"] != a["city"] and h["agent_id"] != a["agent_id"]]
        random.shuffle(peers)
        for peer in peers[: random.randint(2, 8)]:
            add_link(links, seen_pairs, a["agent_id"], peer["agent_id"], "hawala_peer")
            add_link(links, seen_pairs, peer["agent_id"], a["agent_id"], "hawala_peer")


def is_connected(ids: list[str], edges: list[tuple[str, str]]) -> bool:
    graph = defaultdict(set)
    for a, b in edges:
        graph[a].add(b)
        graph[b].add(a)
    start = ids[0]
    q = deque([start])
    seen = {start}
    while q:
        cur = q.popleft()
        for nxt in graph[cur]:
            if nxt not in seen:
                seen.add(nxt)
                q.append(nxt)
    return set(ids) <= seen


def main() -> None:
    agents = list(read_jsonl("data/temp/enriched_agents.jsonl"))
    by_type = defaultdict(list)
    for a in agents:
        by_type[a["segment"]].append(a)

    merchants = by_type.get("merchant", [])
    households = by_type.get("household", [])
    domestic = by_type.get("domestic_informal", [])
    mules = by_type.get("mule_account", [])
    hawala = by_type.get("hawala_node", [])

    links: list[dict] = []
    seen_pairs: set[tuple[str, str]] = set()

    non_merchants = [a for a in agents if a["segment"] != "merchant"]
    regular_agents = [a for a in agents if a["segment"] in {"household", "merchant", "salary_user"}]

    for a in agents:
        aid = a["agent_id"]
        seg = a["segment"]

        if seg == "salary_user" and merchants:
            add_link(links, seen_pairs, aid, random.choice(merchants)["agent_id"], "salary")
            fam_pool = households + domestic
            for r in random.sample(fam_pool, k=min(len(fam_pool), random.randint(2, 4))):
                add_link(links, seen_pairs, aid, r["agent_id"], "family")
            for r in random.sample(merchants, k=min(len(merchants), random.randint(1, 2))):
                add_link(links, seen_pairs, aid, r["agent_id"], "utility")

        elif seg == "merchant":
            for c in random.sample(non_merchants, k=min(len(non_merchants), random.randint(10, 50))):
                add_link(links, seen_pairs, c["agent_id"], aid, "customer")
            for s in random.sample(merchants, k=min(max(0, len(merchants) - 1), random.randint(2, 5))):
                add_link(links, seen_pairs, aid, s["agent_id"], "supplier")

        elif seg == "household":
            pool = households + domestic + merchants
            sample = random.sample(pool, k=min(len(pool), random.randint(3, 8)))
            for r in sample:
                add_link(links, seen_pairs, aid, r["agent_id"], random.choice(["family", "service", "merchant"]))

        elif seg == "mule_account":
            controllers = [x for x in agents if x["segment"] != "mule_account" and x["agent_id"] != aid]
            c_sel = random.sample(controllers, k=min(len(controllers), random.randint(1, 2)))
            controller_ids = {c["agent_id"] for c in c_sel}
            for c in c_sel:
                add_link(links, seen_pairs, c["agent_id"], aid, "mule_control")

            recipients = [x for x in agents if x["agent_id"] != aid and x["agent_id"] not in controller_ids]
            random.shuffle(recipients)
            picked = []
            seen_cities = set()
            for r in recipients:
                if len(picked) >= random.randint(5, 15):
                    break
                if r["city"] not in seen_cities or len(seen_cities) >= 4:
                    picked.append(r)
                    seen_cities.add(r["city"])
            for r in picked:
                add_link(links, seen_pairs, aid, r["agent_id"], "mule_downstream")

        elif seg == "structuring_user":
            pool = [x for x in agents if x["agent_id"] != aid]
            for r in random.sample(pool, k=min(len(pool), random.randint(10, 30))):
                add_link(links, seen_pairs, aid, r["agent_id"], "structuring")

        elif seg == "domestic_informal":
            for r in random.sample(households, k=min(len(households), random.randint(1, 2))):
                add_link(links, seen_pairs, aid, r["agent_id"], "employer")
            for r in random.sample(households, k=min(len(households), random.randint(1, 2))):
                add_link(links, seen_pairs, aid, r["agent_id"], "family")

        elif seg == "ato_victim":
            for r in random.sample(regular_agents, k=min(len(regular_agents), random.randint(3, 8))):
                add_link(links, seen_pairs, aid, r["agent_id"], random.choice(["family", "merchant", "utility"]))

    if hawala:
        build_connected_hawala(hawala, links, seen_pairs)

    # Ensure no isolated agents
    linked = set()
    for l in links:
        linked.add(l["sender_agent_id"])
        linked.add(l["receiver_agent_id"])
    all_ids = {a["agent_id"] for a in agents}
    isolated = all_ids - linked
    if isolated:
        fallback = random.choice(agents)["agent_id"]
        for aid in isolated:
            if aid == fallback:
                continue
            add_link(links, seen_pairs, aid, fallback, "forced")

    # Connectivity checks
    mule_ids = {m["agent_id"] for m in mules}
    for mid in mule_ids:
        upstream = any(l["receiver_agent_id"] == mid and l["link_type"] == "mule_control" for l in links)
        downstream = any(l["sender_agent_id"] == mid and l["link_type"] == "mule_downstream" for l in links)
        if not (upstream and downstream):
            raise RuntimeError(f"Mule {mid} missing upstream/downstream links")

    if hawala:
        hawala_ids = [h["agent_id"] for h in hawala]
        hawala_edges = [
            (l["sender_agent_id"], l["receiver_agent_id"])
            for l in links
            if l["sender_agent_id"] in hawala_ids and l["receiver_agent_id"] in hawala_ids
        ]
        if not is_connected(hawala_ids, hawala_edges):
            raise RuntimeError("Hawala network is not fully connected")

    for idx, l in enumerate(links, start=1):
        l["link_id"] = f"LNK_{idx:06d}"

    write_jsonl("data/temp/beneficiary_links.jsonl", links)
    logger.info("Built %s links", len(links))


if __name__ == "__main__":
    main()
