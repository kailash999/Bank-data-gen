from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import json
import random
from pathlib import Path

from utils.config_utils import load_config
from utils.io_utils import chunked, dump_json
from utils.logging_utils import configure_logger

logger = configure_logger("01_build_batches")


def main() -> None:
    segments_cfg = load_config("config/agent_segments.json")
    city_cfg = load_config("config/city_pool.json")
    openai_cfg = load_config("config/openai.json")

    cities = city_cfg["cities"]
    output_dir = Path(openai_cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    slots: list[str] = []
    for segment, count in segments_cfg.items():
        slots.extend([segment] * int(count))
    expected_total = sum(int(v) for v in segments_cfg.values())

    random.shuffle(slots)

    total_weight = sum(float(c["weight"]) for c in cities)
    if abs(total_weight - 1.0) > 1e-9:
        logger.warning("City weights sum to %s; normalizing", total_weight)
    city_names = [c["name"] for c in cities]
    city_weights = [float(c["weight"]) / total_weight for c in cities]

    manifest: dict[str, dict] = {}
    requests = []

    for idx, segment in enumerate(slots, start=1):
        prompt_path = ROOT_DIR / "prompts" / f"{segment}.txt"
        if not prompt_path.exists():
            raise FileNotFoundError(f"Prompt template file missing for segment '{segment}' at {prompt_path}")

        city = random.choices(city_names, weights=city_weights, k=1)[0]
        prompt = prompt_path.read_text(encoding="utf-8").format(CITY=city, SEGMENT=segment, INCOME_RANGE="")

        custom_id = f"AGT_{idx:05d}"
        requests.append(
            {
                "custom_id": custom_id,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": openai_cfg["model"],
                    "max_tokens": int(openai_cfg["max_tokens"]),
                    "messages": [
                        {"role": "system", "content": "Return one valid JSON object only."},
                        {"role": "user", "content": prompt},
                    ],
                },
            }
        )
        manifest[custom_id] = {"segment": segment, "city": city}

    batch_size = int(openai_cfg["batch_size"])
    for batch_num, rows in enumerate(chunked(requests, batch_size), start=1):
        with (output_dir / f"batch_{batch_num:03d}.jsonl").open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    dump_json(output_dir / "batch_manifest.json", manifest)

    if len(manifest) != expected_total:
        raise RuntimeError(f"Manifest size mismatch: expected={expected_total} actual={len(manifest)}")

    logger.info("Generated %s requests in %s", len(requests), output_dir)


if __name__ == "__main__":
    main()
