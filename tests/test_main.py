"""Tests for main module: scheduling logic and retry."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

from dlt_worker import config, main
from dlt_worker.main import _should_run
from dlt_worker.platform_client import PipelineConfig, PipelineRunReport


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


# --- _should_run tests ---


def test_should_run_first_time() -> None:
    cfg = _make_config(enabled=True, schedule="*/5 * * * *", last_run_at=None)
    now = datetime.now(timezone.utc)
    assert _should_run(cfg, now) is True


def test_should_run_before_cron_tick() -> None:
    # Use a fixed time at minute :01 so last_run at :00 has next tick at :05.
    now = datetime(2025, 6, 1, 12, 1, 0, tzinfo=timezone.utc)
    cfg = _make_config(
        enabled=True,
        schedule="*/5 * * * *",
        last_run_at=datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert _should_run(cfg, now) is False


def test_should_run_at_cron_tick() -> None:
    now = datetime.now(timezone.utc)
    cfg = _make_config(
        enabled=True,
        schedule="*/5 * * * *",
        last_run_at=now - timedelta(minutes=6),
    )
    assert _should_run(cfg, now) is True


def test_should_run_disabled() -> None:
    cfg = _make_config(enabled=False, schedule="*/5 * * * *", last_run_at=None)
    now = datetime.now(timezone.utc)
    assert _should_run(cfg, now) is False


def test_should_run_no_schedule() -> None:
    cfg = _make_config(schedule=None, enabled=True)
    now = datetime.now(timezone.utc)
    assert _should_run(cfg, now) is False


def test_should_run_empty_schedule() -> None:
    cfg = _make_config(schedule="", enabled=True)
    now = datetime.now(timezone.utc)
    assert _should_run(cfg, now) is False


def test_should_run_trigger_now() -> None:
    cfg = _make_config(trigger_now=True, enabled=True, schedule=None)
    now = datetime.now(timezone.utc)
    assert _should_run(cfg, now) is True


def test_should_run_trigger_now_disabled() -> None:
    cfg = _make_config(trigger_now=True, enabled=False)
    now = datetime.now(timezone.utc)
    assert _should_run(cfg, now) is False


# --- _run_with_retry tests ---


def _success_report(cfg: PipelineConfig) -> PipelineRunReport:
    return PipelineRunReport(
        pipeline_id=cfg.id,
        status="success",
        started_at="2026-01-01T00:00:00+00:00",
        completed_at="2026-01-01T00:01:00+00:00",
        rows_loaded=42,
    )


def _failure_report(cfg: PipelineConfig) -> PipelineRunReport:
    return PipelineRunReport(
        pipeline_id=cfg.id,
        status="failed",
        started_at="2026-01-01T00:00:00+00:00",
        completed_at="2026-01-01T00:01:00+00:00",
        error_message="connection reset",
    )


class TestRunWithRetry:
    """Tests for _run_with_retry."""

    def setup_method(self) -> None:
        config.PIPELINE_MAX_RETRIES = 2
        config.PIPELINE_RETRY_BASE_DELAY = 30
        main._shutdown = False

    @patch("dlt_worker.main.run_pipeline")
    def test_success_first_attempt(self, mock_run: MagicMock) -> None:
        """No retry when first attempt succeeds."""
        cfg = _make_config()
        mock_run.return_value = _success_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        main._run_with_retry(cfg, "", client)

        mock_run.assert_called_once()
        client.report_pipeline_run.assert_called_once()
        assert client.report_pipeline_run.call_args[0][0].status == "success"

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline")
    def test_fail_then_succeed(
        self, mock_run: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """Retry after failure, only success is reported."""
        cfg = _make_config()
        mock_run.side_effect = [_failure_report(cfg), _success_report(cfg)]
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        main._run_with_retry(cfg, "", client)

        assert mock_run.call_count == 2
        # Only the success report is sent
        client.report_pipeline_run.assert_called_once()
        assert client.report_pipeline_run.call_args[0][0].status == "success"

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline")
    def test_all_attempts_fail(
        self, mock_run: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """Final failure is reported after all retries exhausted."""
        cfg = _make_config()
        mock_run.return_value = _failure_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        main._run_with_retry(cfg, "", client)

        # 1 initial + 2 retries = 3 attempts
        assert mock_run.call_count == 3
        client.report_pipeline_run.assert_called_once()
        assert client.report_pipeline_run.call_args[0][0].status == "failed"

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline")
    def test_shutdown_during_retry_wait(
        self,
        mock_run: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        """Shutdown during backoff reports last failure immediately."""
        cfg = _make_config()
        mock_run.return_value = _failure_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        # Simulate shutdown after a few sleep(1) calls
        call_count = 0

        def trigger_shutdown(seconds: float) -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                main._shutdown = True

        mock_sleep.side_effect = trigger_shutdown

        main._run_with_retry(cfg, "", client)

        # Only 1 attempt — shutdown happens during backoff wait
        assert mock_run.call_count == 1
        client.report_pipeline_run.assert_called_once()
        assert client.report_pipeline_run.call_args[0][0].status == "failed"

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline")
    def test_exponential_backoff_delays(
        self,
        mock_run: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        """Backoff delays follow base_delay * 2^attempt pattern."""
        cfg = _make_config()
        config.PIPELINE_RETRY_BASE_DELAY = 10
        mock_run.return_value = _failure_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        main._run_with_retry(cfg, "", client)

        # 3 attempts, 2 backoff waits: 10*2^0=10s, 10*2^1=20s → 30 sleep(1) calls
        assert mock_sleep.call_count == 30

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline")
    def test_run_id_propagated(
        self, mock_run: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """run_id is set on the report before sending."""
        cfg = _make_config()
        mock_run.return_value = _success_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        main._run_with_retry(cfg, "run-123", client)

        report = client.report_pipeline_run.call_args[0][0]
        assert report.run_id == "run-123"
