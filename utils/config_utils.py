from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from utils.io_utils import load_json


def _resolve_env(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("ENV:"):
        env_name = value.split(":", 1)[1]
        resolved = os.getenv(env_name)
        if resolved is None:
            raise RuntimeError(f"Missing required environment variable: {env_name}")
        return resolved
    if isinstance(value, dict):
        return {k: _resolve_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env(v) for v in value]
    return value


def load_config(path: str | Path) -> dict[str, Any]:
    data = load_json(path)
    if not isinstance(data, dict):
        raise RuntimeError(f"Config at {path} must be a JSON object")
    return _resolve_env(data)
