from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import json
from pathlib import Path

from utils.config_utils import load_config
from utils.io_utils import load_json, read_jsonl, write_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("03_parse_responses")


def _extract_json(content: str) -> dict:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        cleaned = content.strip()
        if cleaned.startswith("```") and cleaned.endswith("```"):
            cleaned = cleaned.strip("`").replace("json\n", "", 1).strip()
        return json.loads(cleaned)


def main() -> None:
    openai_cfg = load_config("config/openai.json")
    manifest_path = Path(openai_cfg["output_dir"]) / "batch_manifest.json"
    manifest = load_json(manifest_path)

    results_dir = Path("data/temp/results")
    parsed = []
    errors = []
    retry_ids = set()
    latest_rows: dict[str, dict] = {}
    duplicate_ids = set()

    for result_file in sorted(results_dir.glob("result_*.jsonl")):
        for row in read_jsonl(result_file):
            custom_id = row.get("custom_id")
            if not custom_id:
                errors.append({"error": "missing_custom_id", "file": str(result_file)})
                continue
            if custom_id in latest_rows:
                duplicate_ids.add(custom_id)
            latest_rows[custom_id] = row

    for custom_id in sorted(duplicate_ids):
        errors.append({"custom_id": custom_id, "error": "duplicate_custom_id_overwritten"})

    for custom_id, row in latest_rows.items():
        try:
            content = row["response"]["body"]["choices"][0]["message"]["content"]
        except Exception:
            errors.append({"custom_id": custom_id, "error": "missing_response_content"})
            retry_ids.add(custom_id)
            continue
        if not content:
            errors.append({"custom_id": custom_id, "error": "empty_content"})
            retry_ids.add(custom_id)
            continue

        try:
            agent = _extract_json(content)
        except Exception as exc:
            errors.append({"custom_id": custom_id, "error": f"parse_error:{exc}"})
            retry_ids.add(custom_id)
            continue

        m = manifest.get(custom_id, {})
        agent["agent_ref"] = custom_id
        agent["segment"] = m.get("segment", agent.get("segment"))
        if not agent.get("city"):
            agent["city"] = m.get("city")
        parsed.append(agent)

    seen = set(latest_rows.keys())
    missing_ids = sorted(set(manifest.keys()) - seen)
    for cid in missing_ids:
        errors.append({"custom_id": cid, "error": "missing_result"})
    retry_ids.update(missing_ids)

    write_jsonl("data/temp/raw_agents.jsonl", parsed)
    write_jsonl("data/temp/errors/parse_errors.jsonl", errors)
    write_jsonl(
        "data/temp/retry_queue.jsonl",
        ({"custom_id": cid, "source": "03_parse_responses"} for cid in sorted(retry_ids)),
    )

    logger.info(
        "raw=%s parse_errors=%s missing_results=%s total=%s retry_queue=%s",
        len(parsed),
        len(errors),
        len(missing_ids),
        len(parsed) + len(errors),
        len(retry_ids),
    )


if __name__ == "__main__":
    main()
