"""Tests for subprocess-per-run isolation of pipelines and dbt runs."""

from __future__ import annotations

import multiprocessing
import subprocess
import sys
from typing import Any
from unittest.mock import patch

import pytest

from dlt_worker import config
from dlt_worker.api_client import (
    PipelineConfig,
    PipelineRunReport,
    TransformationConfig,
    TransformationRunReport,
)
from dlt_worker.run_isolation import (
    _child_main,
    _transformation_child_main,
    run_pipeline_isolated,
    run_transformation_isolated,
)


def _make_config(**overrides: Any) -> PipelineConfig:
    defaults: dict[str, Any] = {
        "id": "p1",
        "name": "test-pipeline",
        "source_type": "sql_database",
        "source_config": {"tables": ["orders"]},
        "source_credentials": {"connection_string": "postgresql://u:p@host/db"},
        "dataset_name": "raw",
        "schedule": None,
        "write_disposition": "append",
        "enabled": True,
    }
    defaults.update(overrides)
    return PipelineConfig(**defaults)


def _success_report(cfg: PipelineConfig) -> PipelineRunReport:
    return PipelineRunReport(
        pipeline_id=cfg.id,
        status="success",
        started_at="2026-01-01T00:00:00+00:00",
        completed_at="2026-01-01T00:01:00+00:00",
        rows_loaded=42,
    )


def test_disabled_runs_in_process(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "PIPELINE_SUBPROCESS", False)
    cfg = _make_config()
    report = _success_report(cfg)
    with patch("dlt_worker.pipeline_runner.run_pipeline", return_value=report) as m:
        result = run_pipeline_isolated(cfg)
    m.assert_called_once_with(cfg)
    assert result is report


def test_child_main_sends_report_over_pipe(monkeypatch: pytest.MonkeyPatch) -> None:
    """The child entry re-applies config + the iceberg patch and pipes the
    report back (exercised in-process so the collaborators can be mocked)."""
    cfg = _make_config()
    report = _success_report(cfg)
    recv_conn, send_conn = multiprocessing.Pipe(duplex=False)

    with (
        patch("dlt_worker.run_isolation.config") as mock_config,
        patch("dlt_worker.run_isolation.iceberg_stream") as mock_stream,
        patch("dlt_worker.run_isolation.logging"),
        patch("dlt_worker.pipeline_runner.run_pipeline", return_value=report) as m_run,
    ):
        mock_config.ICEBERG_LOAD_CHUNK_ROWS = 200_000
        mock_config.ICEBERG_LOAD_COMMIT_EVERY = 20
        mock_config.ICEBERG_CREDENTIAL_REFRESH_SECONDS = 900
        _child_main(send_conn, cfg, {})

    mock_config.load.assert_called_once_with()
    mock_stream.apply.assert_called_once_with(200_000, 20, 900)
    m_run.assert_called_once_with(cfg)
    assert recv_conn.recv() == report
    recv_conn.close()


def test_child_death_becomes_failed_report(monkeypatch: pytest.MonkeyPatch) -> None:
    """A child that exits without sending a report (here: config.load()
    failing on missing env, standing in for an OOM kill) must surface as a
    failed run, not an exception or a hang."""
    monkeypatch.setattr(config, "PIPELINE_SUBPROCESS", True)
    # Guarantee the spawned child's config.load() exits: _require treats an
    # empty value as missing.
    monkeypatch.setenv("CUSTOMER_SLUG", "")

    cfg = _make_config()
    report = run_pipeline_isolated(cfg)

    assert report.status == "failed"
    assert report.pipeline_id == cfg.id
    assert "died without reporting" in report.error_message
    assert "exit code 1" in report.error_message
    assert report.started_at and report.completed_at


class _HungProc:
    """Stands in for a run child wedged in a network read: alive, never
    sends, ignores nothing — until terminated."""

    exitcode = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.alive = True
        self.terminated = False
        self.killed = False

    def start(self) -> None:
        pass

    def is_alive(self) -> bool:
        return self.alive

    def terminate(self) -> None:
        self.terminated = True
        self.alive = False

    def kill(self) -> None:
        self.killed = True
        self.alive = False

    def join(self, timeout: float | None = None) -> None:
        pass


def test_hung_child_is_killed_at_the_wall_clock_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """B4: a child that never reports must not block the poll loop forever —
    the deadline expires, the child is terminated, and a failed run report
    comes back."""
    monkeypatch.setattr(config, "PIPELINE_SUBPROCESS", True)
    monkeypatch.setattr(config, "PIPELINE_RUN_TIMEOUT_SECONDS", 1)

    real_recv, real_send = multiprocessing.Pipe(duplex=False)
    proc = _HungProc()

    class _KeepAliveSend:
        """close() is a no-op so the pipe never reaches EOF, like a live
        child holding its end."""

        def close(self) -> None:
            pass

    fake_ctx = type(
        "FakeCtx",
        (),
        {
            "Pipe": staticmethod(lambda duplex: (real_recv, _KeepAliveSend())),
            "Process": staticmethod(lambda **kwargs: proc),
        },
    )

    cfg = _make_config()
    with patch(
        "dlt_worker.run_isolation.multiprocessing.get_context",
        return_value=fake_ctx,
    ):
        report = run_pipeline_isolated(cfg)

    real_send.close()

    assert report.status == "failed"
    assert "wall-clock limit" in report.error_message
    assert proc.terminated is True


# --- transformations ---


def _make_transformation(**overrides: Any) -> TransformationConfig:
    defaults: dict[str, Any] = {
        "id": "t1",
        "name": "test-transformation",
        "repo_url": "https://git.example.com/dbt.git",
        "repo_ref": "main",
        "git_credentials": {"username": "u", "token": "secret"},
        "schedule": None,
        "trigger_after_pipeline_id": "",
        "dbt_selector": "",
        "enabled": True,
    }
    defaults.update(overrides)
    return TransformationConfig(**defaults)


def _transformation_report(cfg: TransformationConfig) -> TransformationRunReport:
    return TransformationRunReport(
        transformation_id=cfg.id,
        status="success",
        started_at="2026-01-01T00:00:00+00:00",
        completed_at="2026-01-01T00:01:00+00:00",
        models_total=8,
    )


def test_transformation_disabled_runs_in_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "TRANSFORMATION_SUBPROCESS", False)
    cfg = _make_transformation()
    report = _transformation_report(cfg)
    with patch(
        "dlt_worker.transformation_runner.run_transformation", return_value=report
    ) as m:
        result = run_transformation_isolated(cfg)
    m.assert_called_once_with(cfg)
    assert result is report


def test_transformation_child_main_sends_report_over_pipe() -> None:
    """The dbt child re-applies config/telemetry and pipes the report back.

    No iceberg patch: dbt reaches the warehouse through DuckDB, not
    PyIceberg."""
    cfg = _make_transformation()
    report = _transformation_report(cfg)
    recv_conn, send_conn = multiprocessing.Pipe(duplex=False)

    with (
        patch("dlt_worker.run_isolation.config") as mock_config,
        patch("dlt_worker.run_isolation.iceberg_stream") as mock_stream,
        patch("dlt_worker.run_isolation.logging"),
        patch(
            "dlt_worker.transformation_runner.run_transformation", return_value=report
        ) as m_run,
    ):
        _transformation_child_main(send_conn, cfg, {})

    mock_config.load.assert_called_once_with()
    mock_stream.apply.assert_not_called()
    m_run.assert_called_once_with(cfg)
    assert recv_conn.recv() == report
    recv_conn.close()


def test_transformation_child_death_becomes_failed_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dbt child killed mid-build (here: config.load() exiting on missing
    env, standing in for an OOM kill) must surface as a failed run — the
    2026-08-10 failure mode, where an in-process build instead left 800 MB
    resident in a worker that kept running."""
    monkeypatch.setattr(config, "TRANSFORMATION_SUBPROCESS", True)
    monkeypatch.setenv("CUSTOMER_SLUG", "")

    cfg = _make_transformation(pending_run_id="run-123")
    report = run_transformation_isolated(cfg)

    assert report.status == "failed"
    assert report.transformation_id == cfg.id
    assert report.run_id == "run-123"
    assert "died without reporting" in report.error_message
    assert "transformation run subprocess" in report.error_message


def test_hung_transformation_is_killed_at_the_wall_clock_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dbt build with no timeout of its own (a model querying the
    warehouse) must not wedge the poll loop forever."""
    monkeypatch.setattr(config, "TRANSFORMATION_SUBPROCESS", True)
    monkeypatch.setattr(config, "TRANSFORMATION_RUN_TIMEOUT_SECONDS", 1)

    real_recv, real_send = multiprocessing.Pipe(duplex=False)
    proc = _HungProc()

    class _KeepAliveSend:
        def close(self) -> None:
            pass

    fake_ctx = type(
        "FakeCtx",
        (),
        {
            "Pipe": staticmethod(lambda duplex: (real_recv, _KeepAliveSend())),
            "Process": staticmethod(lambda **kwargs: proc),
        },
    )

    cfg = _make_transformation()
    with patch(
        "dlt_worker.run_isolation.multiprocessing.get_context",
        return_value=fake_ctx,
    ):
        report = run_transformation_isolated(cfg)

    real_send.close()

    assert report.status == "failed"
    assert "wall-clock limit" in report.error_message
    assert proc.terminated is True


def test_poll_loop_imports_neither_dlt_nor_dbt() -> None:
    """The parent must not import the run engines.

    dlt costs ~115 MB resident and dbt another ~60 MB, and the poll loop
    uses neither — only children do. A stray module-level import in the
    parent's chain silently gives that memory back to nobody, so it is
    asserted rather than commented."""
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import dlt_worker.main; "
            "print(sorted(m for m in ('dlt', 'dbt') if m in sys.modules))",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "[]", (
        f"parent imported run engines: {proc.stdout.strip()}"
    )
