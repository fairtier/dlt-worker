"""Tests for subprocess-per-run pipeline isolation."""

from __future__ import annotations

import multiprocessing
from typing import Any
from unittest.mock import patch

import pytest

from dlt_worker import config
from dlt_worker.api_client import PipelineConfig, PipelineRunReport
from dlt_worker.run_isolation import _child_main, run_pipeline_isolated


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
    with patch("dlt_worker.run_isolation.run_pipeline", return_value=report) as m:
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
        patch("dlt_worker.run_isolation.run_pipeline", return_value=report) as m_run,
    ):
        mock_config.ICEBERG_LOAD_CHUNK_ROWS = 200_000
        mock_config.ICEBERG_LOAD_COMMIT_EVERY = 20
        _child_main(send_conn, cfg)

    mock_config.load.assert_called_once_with()
    mock_stream.apply.assert_called_once_with(200_000, 20)
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
