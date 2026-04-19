from __future__ import annotations

import json
import random
from pathlib import Path

from utils.io_utils import chunked, dump_json, load_json
from utils.logging_utils import configure_logger

logger = configure_logger("01_build_batches")


def normalize_city_weights(city_pool: list[dict]) -> list[dict]:
    total = sum(float(c.get("weight", 0.0)) for c in city_pool)
    if total <= 0:
        raise ValueError("City weights sum to 0; cannot sample cities")
    if abs(total - 1.0) > 1e-6:
        logger.warning("City weights sum is %s, normalizing to 1.0", total)
    for c in city_pool:
        c["_norm_weight"] = float(c.get("weight", 0.0)) / total
    return city_pool


def main() -> None:
    segments_cfg = load_json("config/agent_segments.json")
    city_pool = normalize_city_weights(load_json("config/city_pool.json"))
    openai_cfg = load_json("config/openai.json")

    output_dir = Path(openai_cfg.get("batch_output_dir", "data/temp/batches"))
    output_dir.mkdir(parents=True, exist_ok=True)

    slots: list[str] = []
    for segment, count in segments_cfg.items():
        slots.extend([segment] * int(count))

    if len(slots) != 40_000:
        logger.warning("Expected 40,000 slots, got %s", len(slots))

    random.shuffle(slots)

    city_names = [c["city"] for c in city_pool]
    city_weights = [c["_norm_weight"] for c in city_pool]

    manifest: dict[str, dict] = {}
    batch_rows: list[dict] = []

    for idx, segment in enumerate(slots, start=1):
        prompt_path = Path("prompts") / f"{segment}.txt"
        if not prompt_path.exists():
            raise FileNotFoundError(f"Prompt template missing for segment={segment}: {prompt_path}")
        template = prompt_path.read_text(encoding="utf-8")

        city = random.choices(city_names, weights=city_weights, k=1)[0]
        custom_id = f"AGT_{idx:05d}"
        income_range = "INR 10,000-100,000"
        user_prompt = template.format(CITY=city, INCOME_RANGE=income_range, SEGMENT=segment)

        batch_rows.append(
            {
                "custom_id": custom_id,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": openai_cfg.get("model", "gpt-4o"),
                    "max_tokens": int(openai_cfg.get("max_tokens", 500)),
                    "messages": [
                        {
                            "role": "system",
                            "content": "Return a single valid JSON object only.",
                        },
                        {"role": "user", "content": user_prompt},
                    ],
                },
            }
        )
        manifest[custom_id] = {"segment": segment, "city": city}

    batch_size = int(openai_cfg.get("batch_size", 1000))
    for n, rows in enumerate(chunked(batch_rows, batch_size), start=1):
        path = output_dir / f"batch_{n:03d}.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    dump_json(output_dir / "batch_manifest.json", manifest, indent=2)

    unique_custom_ids = len(manifest)
    if unique_custom_ids != len(batch_rows):
        raise RuntimeError("Duplicate custom_id generated")

    logger.info(
        "Wrote %s batch files with %s requests and %s manifest entries",
        len(list(output_dir.glob("batch_*.jsonl"))),
        len(batch_rows),
        len(manifest),
    )


if __name__ == "__main__":
    main()
