from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import time
from pathlib import Path

from openai import OpenAI

from utils.config_utils import load_config
from utils.io_utils import dump_json, load_json, read_jsonl, write_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("02_submit_batches")


def _retry_sleep(attempt: int) -> None:
    time.sleep(min(300, 2**attempt))


def _upload_file(client: OpenAI, batch_file: Path):
    for attempt in range(1, 6):
        try:
            with batch_file.open("rb") as f:
                return client.files.create(file=f, purpose="batch")
        except Exception:
            if attempt == 5:
                raise
            _retry_sleep(attempt)


def _download_output(client: OpenAI, file_id: str, output_path: Path) -> None:
    for attempt in range(1, 4):
        try:
            data = client.files.content(file_id)
            output_path.write_bytes(data.read())
            return
        except Exception:
            if attempt == 3:
                raise
            _retry_sleep(attempt)
import os


def main() -> None:
    cfg = load_config("config/openai.json")
    output_dir = Path(cfg["output_dir"])
    results_dir = Path("data/temp/results")
    results_dir.mkdir(parents=True, exist_ok=True)

    status_path = output_dir / "batch_status.json"
    retry_path = Path("data/temp/retry_queue.jsonl")
    batch_files = sorted(output_dir.glob("batch_*.jsonl"))
    print(f"Found {cfg["api_key"]} api_key.")
    if cfg["api_key"] == "${OPENAI_API_KEY}":
        cfg["api_key"] = os.getenv("OPENAI_API_KEY")
    client = OpenAI(api_key=cfg["api_key"])
    status = load_json(status_path) if status_path.exists() else {}

    for batch_file in batch_files:
        name = batch_file.stem
        if name in status:
            continue
        file_obj = _upload_file(client, batch_file)
        batch_obj = client.batches.create(
            input_file_id=file_obj.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        status[name] = {
            "file_id": file_obj.id,
            "batch_id": batch_obj.id,
            "status": batch_obj.status,
            "output_file_id": None,
        }
        dump_json(status_path, status)

    poll_seconds = int(cfg["poll_interval_minutes"]) * 60
    while True:
        pending = [k for k, v in status.items() if v["status"] not in {"complete", "failed"}]
        if not pending:
            break

        for name in pending:
            batch_obj = client.batches.retrieve(status[name]["batch_id"])
            st = batch_obj.status

            if st == "completed":
                out_file_id = batch_obj.output_file_id
                if out_file_id:
                    result_num = name.split("_")[-1]
                    _download_output(client, out_file_id, results_dir / f"result_{result_num}.jsonl")
                    status[name]["output_file_id"] = out_file_id
                    status[name]["status"] = "complete"
            elif st == "failed":
                failed_ids = [row["custom_id"] for row in read_jsonl(output_dir / f"{name}.jsonl")]
                write_jsonl(retry_path, ({"custom_id": cid, "source_batch": name} for cid in failed_ids), append=True)
                status[name]["status"] = "failed"
            else:
                status[name]["status"] = st

            dump_json(status_path, status)

        if any(v["status"] not in {"complete", "failed"} for v in status.values()):
            time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
