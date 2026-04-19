from __future__ import annotations

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
    seen = set()

    for result_file in sorted(results_dir.glob("result_*.jsonl")):
        for row in read_jsonl(result_file):
            custom_id = row.get("custom_id")
            if not custom_id:
                errors.append({"error": "missing_custom_id", "file": str(result_file)})
                continue
            if custom_id in seen:
                errors.append({"custom_id": custom_id, "error": "duplicate_custom_id"})
                continue
            seen.add(custom_id)

            try:
                content = row["response"]["body"]["choices"][0]["message"]["content"]
            except Exception:
                errors.append({"custom_id": custom_id, "error": "missing_response_content"})
                continue
            if not content:
                errors.append({"custom_id": custom_id, "error": "empty_content"})
                continue

            try:
                agent = _extract_json(content)
            except Exception as exc:
                errors.append({"custom_id": custom_id, "error": f"parse_error:{exc}"})
                continue

            m = manifest.get(custom_id, {})
            agent["agent_ref"] = custom_id
            agent["segment"] = m.get("segment", agent.get("segment"))
            if not agent.get("city"):
                agent["city"] = m.get("city")
            parsed.append(agent)

    write_jsonl("data/temp/raw_agents.jsonl", parsed)
    write_jsonl("data/temp/errors/parse_errors.jsonl", errors)
    logger.info("raw=%s parse_errors=%s total=%s", len(parsed), len(errors), len(parsed) + len(errors))


if __name__ == "__main__":
    main()
