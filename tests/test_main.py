"""Tests for main module: scheduling logic and retry."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dlt_worker import config, main
from dlt_worker.main import _should_run
from dlt_worker.scheduler_state import SchedulerState
from dlt_worker.api_client import (
    PipelineConfig,
    PipelineRunReport,
    TransformationConfig,
    TransformationRunReport,
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


def test_should_run_invalid_cron_returns_false() -> None:
    """B2: an invalid cron string disables this one config instead of
    raising into the tick loop and abandoning every other pipeline."""
    cfg = _make_config(
        schedule="not a cron",
        last_run_at=datetime(2025, 6, 1, tzinfo=timezone.utc),
    )
    now = datetime.now(timezone.utc)
    assert _should_run(cfg, now) is False


def test_should_run_invalid_cron_never_run_before() -> None:
    """Even a never-run config must not fire on an invalid schedule."""
    cfg = _make_config(schedule="* * *", last_run_at=None)
    now = datetime.now(timezone.utc)
    assert _should_run(cfg, now) is False


def test_should_run_backs_off_after_failure() -> None:
    """B3: a failing scheduled config waits for the next cron slot after
    the last failed attempt instead of re-firing every tick."""
    now = datetime.now(timezone.utc)
    cfg = _make_config(schedule="*/5 * * * *", last_run_at=now - timedelta(minutes=30))
    main._last_failure_at.clear()
    try:
        main._last_failure_at["p1"] = now - timedelta(minutes=1)
        assert _should_run(cfg, now) is False

        main._last_failure_at["p1"] = now - timedelta(minutes=6)
        assert _should_run(cfg, now) is True
    finally:
        main._last_failure_at.clear()


def test_should_run_trigger_now_bypasses_failure_backoff() -> None:
    """An explicit Run-now must fire even during failure backoff."""
    now = datetime.now(timezone.utc)
    cfg = _make_config(trigger_now=True, schedule="*/5 * * * *")
    main._last_failure_at["p1"] = now
    try:
        assert _should_run(cfg, now) is True
    finally:
        main._last_failure_at.clear()


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
        main._recorder = None

    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_success_first_attempt(self, mock_run: MagicMock) -> None:
        """No retry when first attempt succeeds."""
        cfg = _make_config()
        mock_run.return_value = _success_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        main._run_with_retry(cfg, "", "local-1", client)

        mock_run.assert_called_once()
        client.report_pipeline_run.assert_called_once()
        assert client.report_pipeline_run.call_args[0][0].status == "success"

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_fail_then_succeed(
        self, mock_run: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """Retry after failure, only success is reported."""
        cfg = _make_config()
        mock_run.side_effect = [_failure_report(cfg), _success_report(cfg)]
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        main._run_with_retry(cfg, "", "local-1", client)

        assert mock_run.call_count == 2
        # Only the success report is sent
        client.report_pipeline_run.assert_called_once()
        assert client.report_pipeline_run.call_args[0][0].status == "success"

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_all_attempts_fail(
        self, mock_run: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """Final failure is reported after all retries exhausted."""
        cfg = _make_config()
        mock_run.return_value = _failure_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        main._run_with_retry(cfg, "", "local-1", client)

        # 1 initial + 2 retries = 3 attempts
        assert mock_run.call_count == 3
        client.report_pipeline_run.assert_called_once()
        assert client.report_pipeline_run.call_args[0][0].status == "failed"

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline_isolated")
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

        main._run_with_retry(cfg, "", "local-1", client)

        # Only 1 attempt — shutdown happens during backoff wait
        assert mock_run.call_count == 1
        client.report_pipeline_run.assert_called_once()
        assert client.report_pipeline_run.call_args[0][0].status == "failed"

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline_isolated")
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

        main._run_with_retry(cfg, "", "local-1", client)

        # 3 attempts, 2 backoff waits: 10*2^0=10s, 10*2^1=20s → 30 sleep(1) calls
        assert mock_sleep.call_count == 30

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_run_id_propagated(
        self, mock_run: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """run_id is set on the report before sending."""
        cfg = _make_config()
        mock_run.return_value = _success_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        main._run_with_retry(cfg, "run-123", "run-123", client)

        report = client.report_pipeline_run.call_args[0][0]
        assert report.run_id == "run-123"


# --- files mode (_run_due_pipelines_files) ---


def _write_pipeline_yaml(
    pipelines_dir: Path, filename: str, pipeline_id: str, **overrides: Any
) -> None:
    fields: dict[str, Any] = {
        "id": pipeline_id,
        "name": filename.removesuffix(".yaml"),
        "source_type": "sql_database",
        "source_config": {"tables": ["orders"]},
        "dataset_name": "raw",
        "schedule": "*/5 * * * *",
        "enabled": True,
    }
    fields.update(overrides)
    lines = []
    for key, value in fields.items():
        if value is None:
            continue  # rendered files omit empty fields (yaml omitempty)
        if isinstance(value, dict):
            lines.append(f"{key}:")
            for k, v in value.items():
                lines.append(f"  {k}: {v}")
        elif isinstance(value, bool):
            lines.append(f"{key}: {str(value).lower()}")
        else:
            lines.append(f"{key}: {value!r}")
    (pipelines_dir / "pipelines" / filename).write_text("\n".join(lines) + "\n")


class TestRunDuePipelinesFiles:
    """Files mode: definitions/schedules from the checkout, triggers and
    credentials from the poll, last_run_at worker-owned."""

    @pytest.fixture(autouse=True)
    def _files_mode(self, tmp_path: Path) -> Any:
        self.checkout = tmp_path / "checkout"
        (self.checkout / "pipelines").mkdir(parents=True)
        self.state_dir = tmp_path / "state"
        self.state_dir.mkdir()
        config.PIPELINES_DIR = str(self.checkout)
        config.DLT_STATE_DIR = str(self.state_dir)
        config.AGE_KEY_FILE = ""
        main._shutdown = False
        main._creds_cache.clear()
        main._last_failure_at.clear()
        yield
        config.PIPELINES_DIR = ""
        config.AGE_KEY_FILE = ""
        main._creds_cache.clear()
        main._last_failure_at.clear()

    def _client(self, polled: list[PipelineConfig] | None) -> MagicMock:
        client = MagicMock()
        client.try_get_pipeline_configs.return_value = polled
        client.report_pipeline_run.return_value = True
        return client

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_dispatcher_uses_files_mode(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        """With PIPELINES_DIR set, definitions come from files — the
        legacy full-truth accessor is never consulted."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        client = self._client([])

        main._run_due_pipelines(client)

        client.try_get_pipeline_configs.assert_called_once()
        client.get_pipeline_configs.assert_not_called()

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_central_down_scheduled_run_fires_with_cached_creds(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        """The Phase 2 acceptance property: a due run fires during a
        central outage using in-memory-cached credentials."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1")
        cfg = _make_config(id="p1")
        mock_run.return_value = _success_report(cfg)

        # First tick: central up, credentials get cached, run fires.
        api_cfg = _make_config(id="p1", source_credentials={"key": "s3cr3t"})
        client = self._client([api_cfg])
        succeeded = main._run_due_pipelines(client)
        assert succeeded == {"p1"}

        # Second tick: central down, schedule due again after 6 minutes.
        state = SchedulerState.load(str(self.state_dir))
        state.record("p1", datetime.now(timezone.utc) - timedelta(minutes=6))
        down = self._client(None)
        succeeded = main._run_due_pipelines(down)

        assert succeeded == {"p1"}
        ran_cfg = mock_run.call_args[0][0]
        assert ran_cfg.source_credentials == {"key": "s3cr3t"}

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_central_down_no_cached_creds_skips_gracefully(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1")
        client = self._client(None)

        succeeded = main._run_due_pipelines(client)

        assert succeeded == set()
        mock_run.assert_not_called()
        # Not recorded — the run retries as soon as credentials appear.
        assert SchedulerState.load(str(self.state_dir)).get("p1") is None

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_trigger_now_joined_by_id(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        cfg = _make_config(id="p1")
        mock_run.return_value = _success_report(cfg)
        api_cfg = _make_config(id="p1", trigger_now=True, pending_run_id="run-9")
        client = self._client([api_cfg])

        succeeded = main._run_due_pipelines(client)

        assert succeeded == {"p1"}
        # The pending run id flows into the "running" report.
        running = client.report_pipeline_run.call_args_list[0][0][0]
        assert running.status == "running"
        assert running.run_id == "run-9"

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_polled_definitions_ignored(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        """File truth wins: a file-disabled pipeline does not run even if
        the poll says enabled + triggered (worker-parity semantics)."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", enabled=False)
        api_cfg = _make_config(id="p1", enabled=True, trigger_now=True)
        client = self._client([api_cfg])

        succeeded = main._run_due_pipelines(client)

        assert succeeded == set()
        mock_run.assert_not_called()

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_migration_seeds_api_last_run_at_once(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        """A pipeline unknown to scheduler.json adopts central's
        last_run_at instead of re-firing as "never ran"."""
        # The next cron fire must stay in the future for the duration of the
        # test no matter when it runs: the fixed */5 schedule used before
        # made this fail whenever the suite ran in the minute right after a
        # 5-minute boundary (the pipeline really was due then). An hourly
        # schedule anchored ~30 minutes away from "now" cannot come due.
        now = datetime.now(timezone.utc)
        schedule = f"{(now.minute + 30) % 60} * * * *"
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=schedule)
        recent = now - timedelta(minutes=1)
        api_cfg = _make_config(id="p1", last_run_at=recent)
        client = self._client([api_cfg])

        succeeded = main._run_due_pipelines(client)

        # Ran 1 minute ago, next fire ~30 minutes out — not due, now seeded.
        assert succeeded == set()
        mock_run.assert_not_called()
        assert SchedulerState.load(str(self.state_dir)).get("p1") == recent

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_local_state_wins_over_polled_last_run_at(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        """Once seeded, central's last_run_at is ignored (worker owns it)."""
        # Anchored-away hourly schedule for the same reason as in
        # test_migration_seeds_api_last_run_at_once: a fixed */5 schedule is
        # genuinely due right after every 5-minute wall-clock boundary.
        now = datetime.now(timezone.utc)
        schedule = f"{(now.minute + 30) % 60} * * * *"
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=schedule)
        state = SchedulerState.load(str(self.state_dir))
        state.record("p1", now - timedelta(minutes=1))
        stale = now - timedelta(hours=2)
        client = self._client([_make_config(id="p1", last_run_at=stale)])

        succeeded = main._run_due_pipelines(client)

        assert succeeded == set()
        mock_run.assert_not_called()

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_success_recorded_failure_not(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        config.PIPELINE_MAX_RETRIES = 0
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        _write_pipeline_yaml(self.checkout, "sales.yaml", "p2", schedule=None)
        ok = _make_config(id="p1")
        bad = _make_config(id="p2")
        mock_run.side_effect = lambda cfg: (
            _success_report(ok) if cfg.id == "p1" else _failure_report(bad)
        )
        client = self._client(
            [
                _make_config(id="p1", trigger_now=True),
                _make_config(id="p2", trigger_now=True),
            ]
        )

        succeeded = main._run_due_pipelines(client)

        assert succeeded == {"p1"}
        state = SchedulerState.load(str(self.state_dir))
        assert state.get("p1") is not None
        assert state.get("p2") is None  # failure re-fires, like legacy mode
        mock_snapshot.assert_called_once()  # only for the success

    # --- age credential files (Phase 3) ---

    def _age_key(self, creds_by_stem: dict[str, dict[str, Any]]) -> None:
        """Enable file credentials: keypair + encrypted files per stem."""
        import json

        import pyrage

        identity = pyrage.x25519.Identity.generate()
        key_file = self.checkout.parent / "key.txt"
        key_file.write_text(f"{identity}\n")
        config.AGE_KEY_FILE = str(key_file)
        for stem, creds in creds_by_stem.items():
            ciphertext = pyrage.encrypt(
                json.dumps(creds).encode(), [identity.to_public()]
            )
            (self.checkout / "pipelines" / f"{stem}.credentials.age").write_bytes(
                ciphertext
            )

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_fresh_process_central_down_runs_from_credential_file(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        """The Phase 3 acceptance property: a fresh worker process (empty
        credential cache) fires a due run during a central outage using
        credentials decrypted from the checkout."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1")
        self._age_key({"orders": {"password": "fr0m-f1le"}})
        cfg = _make_config(id="p1")
        mock_run.return_value = _success_report(cfg)
        assert main._creds_cache == {}  # fresh process, nothing cached
        client = self._client(None)  # central down

        succeeded = main._run_due_pipelines(client)

        assert succeeded == {"p1"}
        ran_cfg = mock_run.call_args[0][0]
        assert ran_cfg.source_credentials == {"password": "fr0m-f1le"}

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_file_credentials_win_over_polled(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        """Git truth wins: polled/cached credentials are not consulted for
        a pipeline whose credential file decrypted."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        self._age_key({"orders": {"password": "fr0m-f1le"}})
        cfg = _make_config(id="p1")
        mock_run.return_value = _success_report(cfg)
        api_cfg = _make_config(
            id="p1", trigger_now=True, source_credentials={"password": "stale"}
        )
        client = self._client([api_cfg])

        succeeded = main._run_due_pipelines(client)

        assert succeeded == {"p1"}
        ran_cfg = mock_run.call_args[0][0]
        assert ran_cfg.source_credentials == {"password": "fr0m-f1le"}

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_no_credential_file_keeps_cache_fallback(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        """A pipeline without a credential file keeps the 0.1.0 behavior:
        cached credentials during an outage, skip when nothing is known."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1")
        _write_pipeline_yaml(self.checkout, "sales.yaml", "p2")
        self._age_key({"orders": {"password": "fr0m-f1le"}})  # p1 only
        ok1, ok2 = _make_config(id="p1"), _make_config(id="p2")
        mock_run.side_effect = lambda cfg: (
            _success_report(ok1) if cfg.id == "p1" else _success_report(ok2)
        )
        client = self._client(None)  # central down, empty cache

        succeeded = main._run_due_pipelines(client)

        # p1 runs from its file; p2 has no file, no cache → skipped.
        assert succeeded == {"p1"}

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_prune_on_file_removal(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1")
        state = SchedulerState.load(str(self.state_dir))
        state.record("p1", datetime.now(timezone.utc))
        state.record("gone", datetime.now(timezone.utc))
        client = self._client([])

        main._run_due_pipelines(client)

        reloaded = SchedulerState.load(str(self.state_dir))
        assert reloaded.get("p1") is not None
        assert reloaded.get("gone") is None

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_prune_suppressed_on_bad_file(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        """A transiently broken file must not lose its last_run_at."""
        (self.checkout / "pipelines" / "broken.yaml").write_text("id: [unclosed\n")
        state = SchedulerState.load(str(self.state_dir))
        kept = datetime.now(timezone.utc)
        state.record("p-broken", kept)
        client = self._client([])

        main._run_due_pipelines(client)

        assert SchedulerState.load(str(self.state_dir)).get("p-broken") == kept

    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_legacy_mode_untouched(self, mock_run: MagicMock) -> None:
        """PIPELINES_DIR unset = 0.0.11 behavior: poll is full truth and
        scheduler.json is never created."""
        config.PIPELINES_DIR = ""
        cfg = _make_config(id="p1", trigger_now=True)
        mock_run.return_value = _success_report(cfg)
        client = MagicMock()
        client.get_pipeline_configs.return_value = [cfg]
        client.report_pipeline_run.return_value = True

        succeeded = main._run_due_pipelines(client)

        assert succeeded == {"p1"}
        client.get_pipeline_configs.assert_called_once()
        client.try_get_pipeline_configs.assert_not_called()
        assert not (self.state_dir / "scheduler.json").exists()

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_one_crashing_pipeline_does_not_stop_the_tick(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        """B2b: an exception escaping one pipeline's processing is isolated
        to that pipeline — later pipelines in the same tick still run."""
        _write_pipeline_yaml(self.checkout, "a.yaml", "p1", trigger_now=None)
        _write_pipeline_yaml(self.checkout, "b.yaml", "p2", trigger_now=None)
        ok = _make_config(id="p2")

        def run(cfg: PipelineConfig) -> PipelineRunReport:
            if cfg.id == "p1":
                raise RuntimeError("boom")
            return _success_report(ok)

        mock_run.side_effect = run
        client = self._client([_make_config(id="p1"), _make_config(id="p2")])

        succeeded = main._run_due_pipelines(client)

        assert succeeded == {"p2"}

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_failing_pipeline_does_not_refire_next_tick(
        self, mock_run: MagicMock, mock_snapshot: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """B3 end-to-end: after a failed run, the next tick within the same
        cron window skips the pipeline; it becomes due again a slot later."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1")
        cfg = _make_config(id="p1")
        mock_run.return_value = _failure_report(cfg)
        client = self._client([_make_config(id="p1")])

        main._run_due_pipelines(client)  # never-run → fires, fails
        assert mock_run.call_count > 0
        calls_after_first_tick = mock_run.call_count

        main._run_due_pipelines(client)  # immediately after → backed off
        assert mock_run.call_count == calls_after_first_tick

        # A cron slot later the pipeline is due again.
        main._last_failure_at["p1"] = datetime.now(timezone.utc) - timedelta(minutes=6)
        main._run_due_pipelines(client)
        assert mock_run.call_count > calls_after_first_tick

    @patch("dlt_worker.main.trigger_snapshot")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_absent_from_successful_poll_skips_instead_of_empty_creds(
        self, mock_run: MagicMock, mock_snapshot: MagicMock
    ) -> None:
        """B6: central answered but omitted the pipeline (deleted centrally,
        mirror lag on delete); with no credential file and no cache the run
        must be skipped, not fired with empty credentials."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1")
        client = self._client([])  # successful poll, pipeline absent

        succeeded = main._run_due_pipelines(client)

        assert succeeded == set()
        mock_run.assert_not_called()
        # Not recorded — the run retries once credentials appear.
        assert SchedulerState.load(str(self.state_dir)).get("p1") is None


# --- transformation scheduling ---


def _make_tconfig(**overrides: Any) -> TransformationConfig:
    defaults: dict[str, Any] = {
        "id": "t1",
        "name": "nightly",
        "repo_url": "",
        "repo_ref": "main",
        "git_credentials": {},
        "schedule": None,
        "trigger_after_pipeline_id": "",
        "dbt_selector": "",
        "enabled": True,
    }
    defaults.update(overrides)
    return TransformationConfig(**defaults)


def _transformation_report(status: str = "success") -> TransformationRunReport:
    return TransformationRunReport(
        transformation_id="t1",
        status=status,
        started_at="2025-06-01T12:00:00Z",
        completed_at="2025-06-01T12:05:00Z",
    )


class TestRunDueTransformations:
    """Tests for main._run_due_transformations."""

    @patch("dlt_worker.main.run_transformation")
    def test_chained_after_pipeline_success(self, mock_run: MagicMock) -> None:
        cfg = _make_tconfig(trigger_after_pipeline_id="p1")
        client = MagicMock()
        client.get_transformation_configs.return_value = [cfg]
        client.report_transformation_run.return_value = True
        mock_run.return_value = _transformation_report()

        main._run_due_transformations(client, succeeded_pipelines={"p1"})

        mock_run.assert_called_once_with(cfg)
        client.report_transformation_run.assert_called_once()

    @patch("dlt_worker.main.run_transformation")
    def test_not_chained_without_pipeline_success(self, mock_run: MagicMock) -> None:
        cfg = _make_tconfig(trigger_after_pipeline_id="p1")
        client = MagicMock()
        client.get_transformation_configs.return_value = [cfg]

        main._run_due_transformations(client, succeeded_pipelines=set())

        mock_run.assert_not_called()

    @patch("dlt_worker.main.run_transformation")
    def test_trigger_now_and_chained_runs_once(self, mock_run: MagicMock) -> None:
        cfg = _make_tconfig(trigger_after_pipeline_id="p1", trigger_now=True)
        client = MagicMock()
        client.get_transformation_configs.return_value = [cfg]
        client.report_transformation_run.return_value = True
        mock_run.return_value = _transformation_report()

        main._run_due_transformations(client, succeeded_pipelines={"p1"})

        mock_run.assert_called_once()

    @patch("dlt_worker.main.run_transformation")
    def test_disabled_never_runs(self, mock_run: MagicMock) -> None:
        cfg = _make_tconfig(
            enabled=False, trigger_after_pipeline_id="p1", trigger_now=True
        )
        client = MagicMock()
        client.get_transformation_configs.return_value = [cfg]

        main._run_due_transformations(client, succeeded_pipelines={"p1"})

        mock_run.assert_not_called()

    @patch("dlt_worker.main.run_transformation")
    def test_fetch_error_is_swallowed(self, mock_run: MagicMock) -> None:
        client = MagicMock()
        client.get_transformation_configs.side_effect = RuntimeError("boom")

        # Must not raise — pipeline processing already happened.
        main._run_due_transformations(client, succeeded_pipelines=set())

        mock_run.assert_not_called()

    @patch("dlt_worker.main.run_transformation")
    def test_one_crashing_transformation_does_not_stop_the_tick(
        self, mock_run: MagicMock
    ) -> None:
        """B2b: an exception escaping one transformation's processing must
        not abandon the remaining transformations in the tick."""
        bad = _make_tconfig(id="t1", trigger_now=True)
        good = _make_tconfig(id="t2", trigger_now=True)
        client = MagicMock()
        client.get_transformation_configs.return_value = [bad, good]
        client.report_transformation_run.return_value = True

        def run(cfg: TransformationConfig) -> TransformationRunReport:
            if cfg.id == "t1":
                raise RuntimeError("boom")
            return _transformation_report()

        mock_run.side_effect = run

        main._run_due_transformations(client, succeeded_pipelines=set())

        assert mock_run.call_count == 2

    def test_should_run_works_for_transformations(self) -> None:
        now = datetime.now(timezone.utc)
        cfg = _make_tconfig(
            schedule="*/5 * * * *", last_run_at=now - timedelta(minutes=6)
        )
        assert _should_run(cfg, now) is True


# --- local-first run recording (WORKSPACE_DB_URL) ---


class TestLocalFirstRecording:
    """With a recorder active, every run lands in the workspace database
    before and after execution, and central reporting degrades to
    bounded-retry best-effort — the local row is the record."""

    def setup_method(self) -> None:
        config.PIPELINE_MAX_RETRIES = 0
        main._shutdown = False
        self.recorder = MagicMock()
        main._recorder = self.recorder

    def teardown_method(self) -> None:
        main._recorder = None

    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_pipeline_recorded_start_and_end(self, mock_run: MagicMock) -> None:
        cfg = _make_config(trigger_now=True)
        mock_run.return_value = _success_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        now = datetime.now(timezone.utc)
        main._execute_pipeline(cfg, now, client)

        rec = self.recorder
        rec.record_pipeline_run_start.assert_called_once()
        run_id, pipeline_id, started_at = rec.record_pipeline_run_start.call_args[0]
        assert pipeline_id == cfg.id
        assert started_at == now
        rec.record_pipeline_run_end.assert_called_once()
        end_run_id, end_report = rec.record_pipeline_run_end.call_args[0]
        assert end_run_id == run_id  # one identity from start to end
        assert end_report.status == "success"

    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_pending_run_id_reused_as_local_id(self, mock_run: MagicMock) -> None:
        """A central Run-now id becomes the local row id too — the same
        run has one identity in both stores."""
        cfg = _make_config(trigger_now=True, pending_run_id="run-42")
        mock_run.return_value = _success_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        main._execute_pipeline(cfg, datetime.now(timezone.utc), client)

        assert self.recorder.record_pipeline_run_start.call_args[0][0] == "run-42"

    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_scheduled_run_gets_generated_uuid(self, mock_run: MagicMock) -> None:
        cfg = _make_config(trigger_now=True)
        mock_run.return_value = _success_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = True

        main._execute_pipeline(cfg, datetime.now(timezone.utc), client)

        run_id = self.recorder.record_pipeline_run_start.call_args[0][0]
        uuid.UUID(run_id)  # raises if not a valid worker-generated UUID

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_central_report_retried_best_effort(
        self, mock_run: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """A central report failure is retried a bounded number of times
        and never fails the run — the local row is already written."""
        cfg = _make_config()
        mock_run.return_value = _success_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = False

        report = main._run_with_retry(cfg, "", "local-1", client)

        assert report is not None and report.status == "success"
        assert client.report_pipeline_run.call_count == main._CENTRAL_REPORT_ATTEMPTS
        self.recorder.record_pipeline_run_end.assert_called_once()

    @patch("dlt_worker.main.time.sleep")
    @patch("dlt_worker.main.run_pipeline_isolated")
    def test_central_report_single_shot_without_recorder(
        self, mock_run: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """WORKSPACE_DB_URL unset = today's behavior: one attempt."""
        main._recorder = None
        cfg = _make_config()
        mock_run.return_value = _success_report(cfg)
        client = MagicMock()
        client.report_pipeline_run.return_value = False

        main._run_with_retry(cfg, "", "local-1", client)

        client.report_pipeline_run.assert_called_once()

    @patch("dlt_worker.main.run_transformation")
    def test_transformation_recorded_start_and_end(self, mock_run: MagicMock) -> None:
        cfg = _make_tconfig(trigger_now=True, pending_run_id="run-7")
        client = MagicMock()
        client.get_transformation_configs.return_value = [cfg]
        client.report_transformation_run.return_value = True
        mock_run.return_value = _transformation_report()

        main._run_due_transformations(client, succeeded_pipelines=set())

        rec = self.recorder
        rec.record_transformation_run_start.assert_called_once()
        assert rec.record_transformation_run_start.call_args[0][0] == "run-7"
        rec.record_transformation_run_end.assert_called_once()
        run_id, report = rec.record_transformation_run_end.call_args[0]
        assert run_id == "run-7"
        assert report.status == "success"


class TestConfigureLogging:
    """_configure_logging must win over dlt's root-logger setup.

    Importing dlt installs a root StreamHandler and sets the root level to
    WARNING. A plain logging.basicConfig(level=INFO) is then a no-op (a handler
    already exists), so the worker's INFO logs (startup, "Running pipeline",
    outcomes) were silently dropped and only ERRORs reached Loki. This guards
    the force=True fix.
    """

    def _reset_root(self) -> tuple[int, list[Any]]:
        root = logging.getLogger()
        saved = (root.level, root.handlers[:])
        return saved

    def _restore_root(self, saved: tuple[int, list[Any]]) -> None:
        root = logging.getLogger()
        root.setLevel(saved[0])
        root.handlers[:] = saved[1]

    def test_info_flows_despite_preexisting_warning_root(self) -> None:
        saved = self._reset_root()
        try:
            # Simulate what `import dlt` leaves behind: a root handler + WARNING.
            root = logging.getLogger()
            root.handlers[:] = [logging.StreamHandler()]
            root.setLevel(logging.WARNING)

            # A plain basicConfig would be a no-op here (handler present) —
            # root stays WARNING and INFO is disabled. Confirm that baseline...
            logging.basicConfig(level=logging.INFO)
            assert root.level == logging.WARNING
            assert not logging.getLogger("dlt_worker.main").isEnabledFor(logging.INFO)

            # ...then _configure_logging (force=True) must re-own it at INFO.
            main._configure_logging()
            assert root.level == logging.INFO
            assert logging.getLogger("dlt_worker.main").isEnabledFor(logging.INFO)
        finally:
            self._restore_root(saved)
