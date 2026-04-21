from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import time
from pathlib import Path

from openai import OpenAI, PermissionDeniedError

from utils.config_utils import load_config
from utils.io_utils import dump_json, load_json, read_jsonl, write_jsonl
from utils.logging_utils import configure_logger

logger = configure_logger("02_submit_batches")
TERMINAL_STATUSES = {"complete", "failed", "permission_denied"}


def _retry_sleep(attempt: int) -> None:
    time.sleep(min(300, 2**attempt))


def _upload_file(client: OpenAI, batch_file: Path):
    for attempt in range(1, 6):
        try:
            with batch_file.open("rb") as f:
                return client.files.create(file=f, purpose="batch")
        except PermissionDeniedError:
            raise
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
        except PermissionDeniedError:
            raise
        except Exception:
            if attempt == 3:
                raise
            _retry_sleep(attempt)
import os


def _is_primary_batch_file(path: Path) -> bool:
    stem = path.stem
    if not stem.startswith("batch_"):
        return False
    suffix = stem.split("_", 1)[1]
    return suffix.isdigit()


def _materialize_retry_batches(output_dir: Path, retry_path: Path, batch_size: int) -> None:
    if not retry_path.exists():
        return

    retry_ids = {row["custom_id"] for row in read_jsonl(retry_path) if row.get("custom_id")}
    if not retry_ids:
        return

    source_requests = {}
    for batch_file in sorted(output_dir.glob("batch_*.jsonl")):
        if not _is_primary_batch_file(batch_file):
            continue
        for row in read_jsonl(batch_file):
            custom_id = row.get("custom_id")
            if custom_id in retry_ids:
                source_requests[custom_id] = row

    matched_ids = sorted(source_requests.keys())
    missing_ids = sorted(retry_ids - set(matched_ids))
    if missing_ids:
        logger.warning("Retry queue has %s IDs not found in primary batch files", len(missing_ids))

    retry_rows = [source_requests[cid] for cid in matched_ids]
    if not retry_rows:
        logger.warning("Retry queue is present but no retryable requests were found")
        return

    retry_files = sorted(output_dir.glob("batch_retry_*.jsonl"))
    start_num = len(retry_files) + 1
    for idx in range(0, len(retry_rows), batch_size):
        chunk = retry_rows[idx : idx + batch_size]
        retry_num = start_num + (idx // batch_size)
        retry_file = output_dir / f"batch_retry_{retry_num:03d}.jsonl"
        write_jsonl(retry_file, chunk)

    logger.info("Materialized %s retry requests into %s retry batch file(s)", len(retry_rows), (len(retry_rows) - 1) // batch_size + 1)


def _is_primary_batch_file(path: Path) -> bool:
    stem = path.stem
    if not stem.startswith("batch_"):
        return False
    suffix = stem.split("_", 1)[1]
    return suffix.isdigit()


def _materialize_retry_batches(output_dir: Path, retry_path: Path, batch_size: int) -> None:
    if not retry_path.exists():
        return

    retry_ids = {row["custom_id"] for row in read_jsonl(retry_path) if row.get("custom_id")}
    if not retry_ids:
        return

    source_requests = {}
    for batch_file in sorted(output_dir.glob("batch_*.jsonl")):
        if not _is_primary_batch_file(batch_file):
            continue
        for row in read_jsonl(batch_file):
            custom_id = row.get("custom_id")
            if custom_id in retry_ids:
                source_requests[custom_id] = row

    matched_ids = sorted(source_requests.keys())
    missing_ids = sorted(retry_ids - set(matched_ids))
    if missing_ids:
        logger.warning("Retry queue has %s IDs not found in primary batch files", len(missing_ids))

    retry_rows = [source_requests[cid] for cid in matched_ids]
    if not retry_rows:
        logger.warning("Retry queue is present but no retryable requests were found")
        return

    retry_files = sorted(output_dir.glob("batch_retry_*.jsonl"))
    start_num = len(retry_files) + 1
    for idx in range(0, len(retry_rows), batch_size):
        chunk = retry_rows[idx : idx + batch_size]
        retry_num = start_num + (idx // batch_size)
        retry_file = output_dir / f"batch_retry_{retry_num:03d}.jsonl"
        write_jsonl(retry_file, chunk)

    logger.info("Materialized %s retry requests into %s retry batch file(s)", len(retry_rows), (len(retry_rows) - 1) // batch_size + 1)
    write_jsonl(retry_path, [])
    logger.info("Cleared retry queue after materialization: %s", retry_path)


def _is_primary_batch_file(path: Path) -> bool:
    stem = path.stem
    if not stem.startswith("batch_"):
        return False
    suffix = stem.split("_", 1)[1]
    return suffix.isdigit()


def _materialize_retry_batches(output_dir: Path, retry_path: Path, batch_size: int) -> None:
    if not retry_path.exists():
        return

    retry_ids = {row["custom_id"] for row in read_jsonl(retry_path) if row.get("custom_id")}
    if not retry_ids:
        return

    source_requests = {}
    for batch_file in sorted(output_dir.glob("batch_*.jsonl")):
        if not _is_primary_batch_file(batch_file):
            continue
        for row in read_jsonl(batch_file):
            custom_id = row.get("custom_id")
            if custom_id in retry_ids:
                source_requests[custom_id] = row

    matched_ids = sorted(source_requests.keys())
    missing_ids = sorted(retry_ids - set(matched_ids))
    if missing_ids:
        logger.warning("Retry queue has %s IDs not found in primary batch files", len(missing_ids))

    retry_rows = [source_requests[cid] for cid in matched_ids]
    if not retry_rows:
        logger.warning("Retry queue is present but no retryable requests were found")
        return

    retry_files = sorted(output_dir.glob("batch_retry_*.jsonl"))
    start_num = len(retry_files) + 1
    for idx in range(0, len(retry_rows), batch_size):
        chunk = retry_rows[idx : idx + batch_size]
        retry_num = start_num + (idx // batch_size)
        retry_file = output_dir / f"batch_retry_{retry_num:03d}.jsonl"
        write_jsonl(retry_file, chunk)

    logger.info("Materialized %s retry requests into %s retry batch file(s)", len(retry_rows), (len(retry_rows) - 1) // batch_size + 1)
    write_jsonl(retry_path, [])
    logger.info("Cleared retry queue after materialization: %s", retry_path)


def main() -> None:
    cfg = load_config("config/openai.json")
    output_dir = Path(cfg["output_dir"])
    results_dir = Path("data/temp/results")
    results_dir.mkdir(parents=True, exist_ok=True)

    status_path = output_dir / "batch_status.json"
    retry_path = Path("data/temp/retry_queue.jsonl")
    batch_size = int(cfg["batch_size"])
    _materialize_retry_batches(output_dir, retry_path, batch_size)
    batch_files = sorted(output_dir.glob("batch_*.jsonl"))
    print(f"Found {cfg["api_key"]} api_key.")
    if cfg["api_key"] == "${OPENAI_API_KEY}":
        cfg["api_key"] = os.getenv("OPENAI_API_KEY")
    client = OpenAI(api_key=cfg["api_key"])
    status = load_json(status_path) if status_path.exists() else {}
    logger.info("Loaded %s batch status entries from %s", len(status), status_path)

    for batch_file in batch_files:
        name = batch_file.stem
        if name in status:
            logger.info("Skipping already tracked batch file %s (status=%s, batch_id=%s)", name, status[name].get("status"), status[name].get("batch_id"))
            continue
        file_obj = _upload_file(client, batch_file)
        try:
            batch_obj = client.batches.create(
                input_file_id=file_obj.id,
                endpoint="/v1/chat/completions",
                completion_window="24h",
            )
        except PermissionDeniedError as exc:
            raise RuntimeError(
                "OpenAI rejected batch creation with 403 PermissionDenied. "
                "Verify that the API key/project has Batch API access and can use the configured model."
            ) from exc
        status[name] = {
            "file_id": file_obj.id,
            "batch_id": batch_obj.id,
            "status": batch_obj.status,
            "output_file_id": None,
        }
        logger.info("Submitted batch file %s => batch_id=%s status=%s", name, batch_obj.id, batch_obj.status)
        dump_json(status_path, status)

    poll_seconds = int(cfg["poll_interval_minutes"]) * 60
    while True:
        pending = [k for k, v in status.items() if v["status"] not in TERMINAL_STATUSES]
        if not pending:
            break
        logger.info("Polling %s pending batch(es): %s", len(pending), ", ".join(pending[:10]))

        for name in pending:
            old_status = status[name].get("status")
            try:
                batch_obj = client.batches.retrieve(status[name]["batch_id"])
            except PermissionDeniedError as exc:
                logger.error(
                    "Permission denied while retrieving batch %s (%s). Marking as permission_denied. %s",
                    name,
                    status[name]["batch_id"],
                    exc,
                )
                status[name]["status"] = "permission_denied"
                status[name]["error"] = str(exc)
                dump_json(status_path, status)
                continue

            st = batch_obj.status
            status[name]["status"] = st

            if st == "completed":
                out_file_id = batch_obj.output_file_id
                if out_file_id:
                    result_num = name
                    try:
                        _download_output(client, out_file_id, results_dir / f"result_{result_num}.jsonl")
                    except PermissionDeniedError as exc:
                        logger.error(
                            "Permission denied while downloading output file %s for %s. Marking as permission_denied. %s",
                            out_file_id,
                            name,
                            exc,
                        )
                        status[name]["status"] = "permission_denied"
                        status[name]["error"] = str(exc)
                    else:
                        status[name]["output_file_id"] = out_file_id
                        status[name]["status"] = "complete"
            elif st == "failed":
                failed_ids = [row["custom_id"] for row in read_jsonl(output_dir / f"{name}.jsonl")]
                write_jsonl(retry_path, ({"custom_id": cid, "source_batch": name} for cid in failed_ids), append=True)
                status[name]["status"] = "failed"

            if status[name]["status"] != old_status:
                logger.info(
                    "Batch %s (batch_id=%s) status changed: %s -> %s",
                    name,
                    status[name].get("batch_id"),
                    old_status,
                    status[name]["status"],
                )

            dump_json(status_path, status)

        if any(v["status"] not in TERMINAL_STATUSES for v in status.values()):
            time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
