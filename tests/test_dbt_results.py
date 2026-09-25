"""Tests for dbt_results: reading dbt-oss v2's run_results.json."""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

from dlt_worker.dbt_results import MAX_MESSAGE_CHARS, count_nodes, read_run_results

FIXTURES = Path(__file__).parent / "fixtures"


def _project_with(tmp_path: Path, fixture: str) -> str:
    (tmp_path / "target").mkdir()
    shutil.copy(FIXTURES / fixture, tmp_path / "target" / "run_results.json")
    return str(tmp_path)


def test_full_build_counts_models_and_tests(tmp_path: Path) -> None:
    nodes = read_run_results(_project_with(tmp_path, "run_results_v2_build.json"))
    assert count_nodes(nodes) == (8, 0, 5, 0)


def test_test_nodes_are_named_without_their_hash(tmp_path: Path) -> None:
    """v2 test ids are test.<package>.<name>.<hash>: the last segment is a
    hash, so taking it would name every test by a hex string."""
    nodes = read_run_results(_project_with(tmp_path, "run_results_v2_build.json"))
    tests = {n["name"] for n in nodes if n["resource_type"] == "test"}
    assert "not_null_fct_trips_pickup_at" in tests
    assert not any(re.fullmatch(r"[0-9a-f]{10}", name) for name in tests)


def test_model_nodes_keep_their_name(tmp_path: Path) -> None:
    nodes = read_run_results(_project_with(tmp_path, "run_results_v2_build.json"))
    models = {n["name"] for n in nodes if n["resource_type"] == "model"}
    assert "stg_trips" in models
    assert "fct_trips" in models


def test_error_node_is_counted_failed_and_keeps_its_message(tmp_path: Path) -> None:
    nodes = read_run_results(_project_with(tmp_path, "run_results_v2_error.json"))
    assert count_nodes(nodes) == (2, 1, 0, 0)
    (failed,) = [n for n in nodes if n["status"] == "error"]
    assert "Binder Error" in failed["message"]


def test_message_is_truncated(tmp_path: Path) -> None:
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "run_results.json").write_text(
        json.dumps(
            {
                "results": [
                    {
                        "unique_id": "model.p.m",
                        "status": "error",
                        "execution_time": 1.23456,
                        "message": "x" * (MAX_MESSAGE_CHARS + 50),
                    },
                ]
            }
        )
    )
    (node,) = read_run_results(str(tmp_path))
    assert len(node["message"]) == MAX_MESSAGE_CHARS
    assert node["execution_time"] == 1.235


def test_versioned_model_name_keeps_its_version(tmp_path: Path) -> None:
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "run_results.json").write_text(
        json.dumps(
            {
                "results": [
                    {
                        "unique_id": "model.p.orders.v2",
                        "status": "success",
                        "execution_time": 0.1,
                        "message": None,
                    },
                ]
            }
        )
    )
    (node,) = read_run_results(str(tmp_path))
    assert node["name"] == "orders.v2"
    assert node["message"] == ""


def test_missing_or_corrupt_file_is_empty(tmp_path: Path) -> None:
    assert read_run_results(str(tmp_path)) == []
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "run_results.json").write_text("{not json")
    assert read_run_results(str(tmp_path)) == []


def test_count_nodes() -> None:
    nodes = [
        {"resource_type": "model", "status": "success"},
        {"resource_type": "model", "status": "error"},
        {"resource_type": "seed", "status": "success"},
        {"resource_type": "test", "status": "pass"},
        {"resource_type": "test", "status": "fail"},
        {"resource_type": "unit_test", "status": "pass"},
    ]
    assert count_nodes(nodes) == (3, 1, 3, 1)
