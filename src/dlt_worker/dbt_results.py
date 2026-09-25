"""Reading a dbt build's outcome from target/run_results.json.

dbt-oss v2 is a binary, so its results arrive as a file rather than Python
objects. Each entry carries status, execution_time and message directly;
the node's type and name are not keys and have to be read out of its
unique_id — <type>.<package>.<name>, where a test adds a .<hash> suffix.
"""

from __future__ import annotations

import json
import os
from typing import Any

# Bound on one node's message in the run report (keeps the payload small).
MAX_MESSAGE_CHARS = 500

MODEL_RESOURCE_TYPES = {"model", "seed", "snapshot"}
TEST_RESOURCE_TYPES = {"test", "unit_test"}
FAILED_STATUSES = {"error", "fail"}


def _node_name(unique_id: str) -> str:
    parts = unique_id.split(".")
    if len(parts) < 3:
        return unique_id
    if parts[0] == "test" and len(parts) >= 4:
        return parts[2]
    return ".".join(parts[2:])


def read_run_results(project_dir: str) -> list[dict[str, Any]]:
    """Per-node results of the last dbt command run in project_dir.

    Empty when dbt wrote no results (it failed before building anything):
    the caller reports that failure from dbt's output instead.
    """
    path = os.path.join(project_dir, "target", "run_results.json")
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, ValueError):
        return []

    nodes = []
    for r in data.get("results") or []:
        unique_id = str(r.get("unique_id") or "")
        message = str(r.get("message") or "")
        nodes.append(
            {
                "resource_type": unique_id.split(".", 1)[0],
                "name": _node_name(unique_id),
                "status": str(r.get("status") or ""),
                "execution_time": round(float(r.get("execution_time") or 0.0), 3),
                "message": message[:MAX_MESSAGE_CHARS],
            }
        )
    return nodes


def count_nodes(nodes: list[dict[str, Any]]) -> tuple[int, int, int, int]:
    """Count (models_total, models_failed, tests_total, tests_failed)."""
    models_total = models_failed = tests_total = tests_failed = 0
    for n in nodes:
        failed = n["status"] in FAILED_STATUSES
        if n["resource_type"] in MODEL_RESOURCE_TYPES:
            models_total += 1
            models_failed += failed
        elif n["resource_type"] in TEST_RESOURCE_TYPES:
            tests_total += 1
            tests_failed += failed
    return models_total, models_failed, tests_total, tests_failed
