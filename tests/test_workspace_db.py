"""Tests for workspace_db: local-first run recording."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

from dlt_worker import config, workspace_db
from dlt_worker.api_client import PipelineRunReport, TransformationRunReport
from dlt_worker.workspace_db import WorkspaceRecorder, _timestamp


class FakeConnection:
    """Collects executed statements; usable as a context manager."""

    def __init__(self, rowcount: int = 1) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.rowcount = rowcount

    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, query: str, params: tuple[Any, ...]) -> MagicMock:
        self.executed.append((query, params))
        cur = MagicMock()
        cur.rowcount = self.rowcount
        return cur


def _pipeline_report(status: str = "success") -> PipelineRunReport:
    return PipelineRunReport(
        pipeline_id="p1",
        status=status,
        started_at="2026-01-01T00:00:00+00:00",
        completed_at="2026-01-01T00:01:00+00:00",
        rows_loaded=42,
        error_message="" if status == "success" else "boom",
    )


def _transformation_report() -> TransformationRunReport:
    return TransformationRunReport(
        transformation_id="t1",
        status="success",
        started_at="2026-01-01T00:00:00+00:00",
        completed_at="2026-01-01T00:05:00+00:00",
        commit_sha="abc123",
        models_total=3,
        tests_total=2,
        model_results='[{"name": "m1"}]',
    )


# --- from_env ---


def test_from_env_off_by_default() -> None:
    config.WORKSPACE_DB_URL = ""
    assert workspace_db.from_env() is None


def test_from_env_returns_recorder_when_set() -> None:
    config.WORKSPACE_DB_URL = "postgres://localhost/workspace"
    try:
        recorder = workspace_db.from_env()
        assert recorder is not None
        assert recorder._dsn == "postgres://localhost/workspace"
    finally:
        config.WORKSPACE_DB_URL = ""


# --- _timestamp ---


def test_timestamp_empty_is_none() -> None:
    assert _timestamp("") is None


def test_timestamp_parses_zulu_suffix() -> None:
    parsed = _timestamp("2026-01-01T00:00:00Z")
    assert parsed == datetime(2026, 1, 1, tzinfo=timezone.utc)


# --- write protocol ---


class TestWrites:
    def setup_method(self) -> None:
        self.conn = FakeConnection()
        self.recorder = WorkspaceRecorder("postgres://test/workspace")

    def _executed(self) -> tuple[str, tuple[Any, ...]]:
        assert len(self.conn.executed) == 1
        return self.conn.executed[0]

    def test_pipeline_run_start_inserts_running_row(self) -> None:
        started = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with patch("dlt_worker.workspace_db.psycopg.connect", return_value=self.conn):
            self.recorder.record_pipeline_run_start("run-1", "p1", started)

        query, params = self._executed()
        assert "INSERT INTO pipeline_runs" in query
        assert "'running'" in query
        assert "ON CONFLICT (id) DO UPDATE" in query  # Run-now crash retry
        assert params == ("run-1", "p1", started)

    def test_pipeline_run_end_upserts_full_row(self) -> None:
        report = _pipeline_report("failed")
        with patch("dlt_worker.workspace_db.psycopg.connect", return_value=self.conn):
            self.recorder.record_pipeline_run_end("run-1", report)

        query, params = self._executed()
        # Upsert: a complete row even if the start write was lost.
        assert "INSERT INTO pipeline_runs" in query
        assert "ON CONFLICT (id) DO UPDATE" in query
        assert params[0] == "run-1"
        assert params[1] == "p1"
        assert params[2] == "failed"
        assert params[3] == datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert params[5] == 42
        assert params[6] == "boom"

    def test_transformation_run_end_writes_all_columns(self) -> None:
        report = _transformation_report()
        with patch("dlt_worker.workspace_db.psycopg.connect", return_value=self.conn):
            self.recorder.record_transformation_run_end("run-2", report)

        query, params = self._executed()
        assert "INSERT INTO transformation_runs" in query
        assert "::jsonb" in query  # model_results is a JSON string
        assert params[0] == "run-2"
        assert params[5] == "abc123"
        assert params[10] == '[{"name": "m1"}]'

    def test_db_error_is_swallowed(self) -> None:
        """A workspace DB failure must never propagate into the run loop."""
        with patch(
            "dlt_worker.workspace_db.psycopg.connect",
            side_effect=RuntimeError("connection refused"),
        ):
            self.recorder.record_pipeline_run_start(
                "run-1", "p1", datetime.now(timezone.utc)
            )
            self.recorder.record_pipeline_run_end("run-1", _pipeline_report())
            self.recorder.finalize_stale_runs()

    def test_finalize_stale_runs_sweeps_both_tables(self) -> None:
        with patch("dlt_worker.workspace_db.psycopg.connect", return_value=self.conn):
            self.recorder.finalize_stale_runs()

        assert len(self.conn.executed) == 2
        queries = [q for q, _ in self.conn.executed]
        assert "UPDATE pipeline_runs" in queries[0]
        assert "UPDATE transformation_runs" in queries[1]
        for query in queries:
            assert "status = 'failed'" in query
            assert "worker restarted mid-run" in query
            assert "interval '2 hours'" in query
