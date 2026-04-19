from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from utils.io_utils import dump_json, load_json, read_jsonl, write_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("02_submit_batches")


try:
    from openai import OpenAI
except Exception as exc:  # pragma: no cover
    raise RuntimeError("openai package is required for batch submission") from exc


def backoff_sleep(attempt: int) -> None:
    time.sleep(min(120, 2**attempt))


def upload_with_retry(client: OpenAI, path: Path, max_attempts: int = 5):
    for attempt in range(max_attempts):
        try:
            with path.open("rb") as f:
                return client.files.create(file=f, purpose="batch")
        except Exception:
            if attempt == max_attempts - 1:
                raise
            backoff_sleep(attempt + 1)


def download_with_retry(client: OpenAI, file_id: str, target: Path, max_attempts: int = 3) -> None:
    for attempt in range(max_attempts):
        try:
            content = client.files.content(file_id)
            target.write_bytes(content.read())
            return
        except Exception:
            if attempt == max_attempts - 1:
                raise
            backoff_sleep(attempt + 1)


def collect_batch_custom_ids(batch_file: Path) -> list[str]:
    return [row["custom_id"] for row in read_jsonl(batch_file)]


def main() -> None:
    cfg = load_json("config/openai.json")
    batches_dir = Path(cfg.get("batch_output_dir", "data/temp/batches"))
    results_dir = Path(cfg.get("results_output_dir", "data/temp/results"))
    results_dir.mkdir(parents=True, exist_ok=True)

    status_path = batches_dir / "batch_status.json"
    retry_path = Path("data/temp/retry_queue.jsonl")

    client = OpenAI(api_key=cfg.get("api_key"))

    status: dict[str, Any] = load_json(status_path) if status_path.exists() else {}

    batch_files = sorted(batches_dir.glob("batch_*.jsonl"))
    for path in batch_files:
        key = path.stem
        if key in status:
            continue
        uploaded = upload_with_retry(client, path)
        batch = client.batches.create(
            input_file_id=uploaded.id,
            endpoint="/v1/chat/completions",
            completion_window=cfg.get("completion_window", "24h"),
        )
        status[key] = {
            "file_id": uploaded.id,
            "batch_id": batch.id,
            "status": "pending",
            "output_file_id": None,
        }
        dump_json(status_path, status)
        logger.info("Submitted %s => %s", key, batch.id)

    poll_seconds = int(cfg.get("poll_seconds", 1800))

    while True:
        pending = [k for k, v in status.items() if v["status"] not in {"complete", "failed"}]
        if not pending:
            break

        for key in pending:
            batch_id = status[key]["batch_id"]
            batch = client.batches.retrieve(batch_id)
            b_status = batch.status
            status[key]["status"] = b_status

            if b_status == "completed":
                output_file_id = batch.output_file_id
                if output_file_id:
                    target = results_dir / f"result_{key.split('_')[-1]}.jsonl"
                    download_with_retry(client, output_file_id, target)
                    status[key]["status"] = "complete"
                    status[key]["output_file_id"] = output_file_id
                    logger.info("Downloaded result for %s", key)
            elif b_status == "failed":
                req_ids = collect_batch_custom_ids(batches_dir / f"{key}.jsonl")
                write_jsonl(retry_path, ({"custom_id": i, "source_batch": key} for i in req_ids), append=True)
                status[key]["status"] = "failed"
                logger.error("Batch %s failed; queued %s custom_ids", key, len(req_ids))

            dump_json(status_path, status)

        remaining = [k for k, v in status.items() if v["status"] not in {"complete", "failed"}]
        if remaining:
            logger.info("Pending batches remaining=%s, sleeping %ss", len(remaining), poll_seconds)
            time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
