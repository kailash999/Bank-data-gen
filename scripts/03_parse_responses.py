from __future__ import annotations

import json
from pathlib import Path

from utils.io_utils import load_json, read_jsonl, write_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("03_parse_responses")


def parse_content(content: str) -> dict:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        cleaned = content.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`")
            cleaned = cleaned.replace("json\n", "", 1).strip()
        return json.loads(cleaned)


def main() -> None:
    manifest = load_json("data/temp/batches/batch_manifest.json")
    result_files = sorted(Path("data/temp/results").glob("result_*.jsonl"))

    seen_ids: set[str] = set()
    parsed_rows = []
    errors = []

    for rf in result_files:
        for line in read_jsonl(rf):
            custom_id = line.get("custom_id")
            if not custom_id:
                errors.append({"file": str(rf), "error": "missing_custom_id", "raw": line})
                continue
            if custom_id in seen_ids:
                errors.append({"custom_id": custom_id, "error": "duplicate_custom_id"})
                continue

            seen_ids.add(custom_id)
            body = line.get("response", {}).get("body", {})
            choices = body.get("choices", [])
            if not choices:
                errors.append({"custom_id": custom_id, "error": "empty_choices"})
                continue
            content = choices[0].get("message", {}).get("content", "")
            if not content:
                errors.append({"custom_id": custom_id, "error": "empty_content"})
                continue

            try:
                obj = parse_content(content)
            except Exception as exc:
                errors.append({"custom_id": custom_id, "error": f"parse_error:{exc}"})
                continue

            m = manifest.get(custom_id, {})
            obj["agent_ref"] = custom_id
            obj["segment"] = m.get("segment", obj.get("segment"))
            obj["city"] = obj.get("city") or m.get("city")
            parsed_rows.append(obj)

    write_jsonl("data/temp/raw_agents.jsonl", parsed_rows)
    write_jsonl("data/temp/errors/parse_errors.jsonl", errors)

    total = len(parsed_rows) + len(errors)
    logger.info("Parsed=%s Errors=%s Total=%s", len(parsed_rows), len(errors), total)


if __name__ == "__main__":
    main()
