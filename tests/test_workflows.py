"""Tests for running linear workflows (main._run_due_workflows and friends).

A workflow step runs its pipeline or transformation through exactly the
code path of a standalone run; these tests pin the workflow-level contract
on top: due evaluation, step order, stop-on-failure, the report sequence,
and that nothing runs twice in one tick.
"""

from __future__ import annotations

import copy
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dlt_worker import config, main
from dlt_worker.api_client import (
    PipelineConfig,
    PipelineRunReport,
    PipelineTrigger,
    ReportResult,
    TransformationConfig,
    TransformationRunReport,
    WorkflowConfig,
    WorkflowRunReport,
    WorkflowStepConfig,
)
from dlt_worker.scheduler_state import SchedulerState
from tests.test_main import _Clock, _make_tconfig, _write_pipeline_yaml


def _wf(**overrides: Any) -> WorkflowConfig:
    defaults: dict[str, Any] = {
        "id": "w1",
        "name": "nightly",
        "schedule": None,
        "enabled": True,
        "steps": [
            WorkflowStepConfig(0, "pipeline", "p1", "orders", True),
            WorkflowStepConfig(1, "transformation", "t1", "marts", True),
        ],
    }
    defaults.update(overrides)
    return WorkflowConfig(**defaults)


def _p_report(cfg: PipelineConfig, status: str = "success") -> PipelineRunReport:
    return PipelineRunReport(
        pipeline_id=cfg.id,
        status=status,
        started_at=datetime.now(UTC).isoformat(),
        completed_at=datetime.now(UTC).isoformat(),
        rows_loaded=1,
        error_message="" if status == "success" else "source exploded",
    )


def _t_report(
    cfg: TransformationConfig, status: str = "success"
) -> TransformationRunReport:
    return TransformationRunReport(
        transformation_id=cfg.id,
        status=status,
        started_at=datetime.now(UTC).isoformat(),
        completed_at=datetime.now(UTC).isoformat(),
        error_message="" if status == "success" else "model failed",
    )


# --- due evaluation ---


class TestWorkflowDue:
    @pytest.fixture(autouse=True)
    def _clean(self) -> Iterator[None]:
        main._workflow_started_at.clear()
        yield
        main._workflow_started_at.clear()

    def test_trigger_now_is_manual(self) -> None:
        assert main._workflow_due(_wf(trigger_now=True), datetime.now(UTC)) == (
            "manual"
        )

    def test_trigger_now_outranks_disabled(self) -> None:
        """Disabling stops the schedule, not a run a person asked for."""
        wf = _wf(enabled=False, trigger_now=True, schedule="* * * * *")
        assert main._workflow_due(wf, datetime.now(UTC)) == "manual"

    def test_disabled_is_not_due(self) -> None:
        wf = _wf(enabled=False, schedule="* * * * *")
        assert main._workflow_due(wf, datetime.now(UTC)) is None

    def test_no_schedule_is_manual_only(self) -> None:
        assert main._workflow_due(_wf(schedule=None), datetime.now(UTC)) is None

    def test_never_run_is_due(self) -> None:
        wf = _wf(schedule="0 3 * * *")
        assert main._workflow_due(wf, datetime.now(UTC)) == "schedule"

    def test_before_next_slot_is_not_due(self) -> None:
        now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
        wf = _wf(schedule="0 3 * * *", last_run_at=now - timedelta(hours=1))
        assert main._workflow_due(wf, now) is None

    def test_at_next_slot_is_due(self) -> None:
        now = datetime(2026, 1, 2, 3, 0, tzinfo=UTC)
        wf = _wf(schedule="0 3 * * *", last_run_at=now - timedelta(days=1))
        assert main._workflow_due(wf, now) == "schedule"

    def test_failed_last_run_waits_for_next_slot(self) -> None:
        """last_run_at counts failures too: no retry every tick."""
        now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
        wf = _wf(
            schedule="0 3 * * *",
            last_run_at=now - timedelta(hours=1),
            last_run_status="failed",
        )
        assert main._workflow_due(wf, now) is None

    def test_invalid_cron_is_not_due(self) -> None:
        assert main._workflow_due(_wf(schedule="not cron"), datetime.now(UTC)) is None

    def test_local_start_covers_a_lost_start_report(self) -> None:
        """If the start report never landed the server still has the old
        baseline; this process's own start time holds the schedule."""
        now = datetime(2026, 1, 2, 3, 30, tzinfo=UTC)
        wf = _wf(schedule="0 3 * * *", last_run_at=now - timedelta(days=2))
        assert main._workflow_due(wf, now) == "schedule"
        main._workflow_started_at["w1"] = now - timedelta(minutes=29)
        assert main._workflow_due(wf, now) is None


# --- execution ---


class _Recorder:
    """A client whose workflow reports are snapshotted as sent — the worker
    mutates one report object across transitions."""

    def __init__(self, workflows: list[WorkflowConfig]) -> None:
        self.client = MagicMock()
        self.client.get_workflow_configs.return_value = workflows
        self.client.report_pipeline_run.return_value = True
        self.client.report_transformation_run.return_value = True
        self.sent: list[WorkflowRunReport] = []
        self.accept: bool | ReportResult = True

        def report(r: WorkflowRunReport) -> bool | ReportResult:
            self.sent.append(copy.deepcopy(r))
            return self.accept

        self.client.report_workflow_run.side_effect = report

    def statuses(self) -> list[tuple[str, list[str]]]:
        return [(r.status, [s.status for s in r.step_runs]) for r in self.sent]


class TestRunWorkflows:
    @pytest.fixture(autouse=True)
    def _files_mode(self, tmp_path: Path) -> Iterator[None]:
        self.checkout = tmp_path / "checkout"
        (self.checkout / "pipelines").mkdir(parents=True)
        self.state_dir = tmp_path / "state"
        self.state_dir.mkdir()
        config.PIPELINES_DIR = str(self.checkout)
        config.DLT_STATE_DIR = str(self.state_dir)
        config.AGE_KEY_FILE = ""
        config.PIPELINE_MAX_RETRIES = 0
        main._shutdown = False
        main._recorder = None
        main._creds_cache.clear()
        main._last_failure_at.clear()
        main._workflow_started_at.clear()
        main._unsent_workflow_reports.clear()

        self.p_run = patch("dlt_worker.main.run_pipeline_isolated").start()
        self.p_run.side_effect = lambda cfg: _p_report(cfg)
        self.t_run = patch("dlt_worker.main.run_transformation_isolated").start()
        self.t_run.side_effect = lambda cfg: _t_report(cfg)
        patch("dlt_worker.main.trigger_snapshot").start()
        patch("dlt_worker.main.time.sleep").start()
        yield
        patch.stopall()
        config.PIPELINES_DIR = ""
        main._shutdown = False
        main._creds_cache.clear()
        main._last_failure_at.clear()
        main._workflow_started_at.clear()
        main._unsent_workflow_reports.clear()

    def _tick(
        self,
        rec: _Recorder,
        transformations: list[TransformationConfig] | None = None,
        triggers: list[PipelineTrigger] | None = None,
    ) -> main._Tick:
        """One full tick: pipelines → workflows → transformations."""
        rec.client.try_get_pipeline_triggers.return_value = (
            triggers if triggers is not None else [PipelineTrigger(id="p1")]
        )
        rec.client.get_transformation_configs.return_value = (
            transformations if transformations is not None else [_make_tconfig()]
        )
        tick = main._Tick()
        with patch("dlt_worker.main._Tick", return_value=tick):
            main._poll_and_run(rec.client)
        return tick

    # --- happy path / report sequence ---

    def test_happy_path_runs_steps_in_order_and_reports_each_transition(
        self,
    ) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        order: list[str] = []
        self.p_run.side_effect = lambda cfg: order.append(cfg.id) or _p_report(cfg)
        self.t_run.side_effect = lambda cfg: order.append(cfg.id) or _t_report(cfg)
        # Steps arrive sorted by position; the transformation is first here.
        wf = _wf(
            trigger_now=True,
            pending_run_id="11111111-1111-1111-1111-111111111111",
            steps=[
                WorkflowStepConfig(0, "transformation", "t1", "marts", True),
                WorkflowStepConfig(1, "pipeline", "p1", "orders", True),
            ],
        )
        rec = _Recorder([wf])

        self._tick(rec)

        assert order == ["t1", "p1"]
        assert rec.statuses() == [
            ("running", ["pending", "pending"]),  # start: clears trigger_now
            ("running", ["running", "pending"]),
            ("running", ["success", "pending"]),
            ("running", ["success", "running"]),
            ("running", ["success", "success"]),
            ("success", ["success", "success"]),
        ]
        # One identity for the whole run: the pending id, every time.
        assert {r.run_id for r in rec.sent} == {wf.pending_run_id}
        assert {r.trigger for r in rec.sent} == {"manual"}
        assert {r.started_at for r in rec.sent} == {rec.sent[0].started_at}
        final = rec.sent[-1]
        assert final.completed_at
        assert all(s.started_at and s.completed_at for s in final.step_runs)
        # Each step names the run it produced — the id that run was
        # reported under.
        t_report = rec.client.report_transformation_run.call_args[0][0]
        p_report = rec.client.report_pipeline_run.call_args[0][0]
        assert final.step_runs[0].transformation_run_id == t_report.run_id
        assert final.step_runs[1].pipeline_run_id == p_report.run_id
        assert final.step_runs[0].name == "marts"

    def test_scheduled_run_gets_one_generated_uuid(self) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        rec = _Recorder([_wf(schedule="0 3 * * *")])  # never run → due

        self._tick(rec)

        run_ids = {r.run_id for r in rec.sent}
        assert len(run_ids) == 1
        uuid.UUID(run_ids.pop())
        assert {r.trigger for r in rec.sent} == {"schedule"}
        assert rec.sent[-1].status == "success"

    def test_not_due_reports_nothing(self) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        rec = _Recorder([_wf(schedule=None)])

        self._tick(rec)

        assert rec.sent == []
        self.p_run.assert_not_called()

    # --- failure handling ---

    def test_failed_step_skips_the_rest(self) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        self.p_run.side_effect = lambda cfg: _p_report(cfg, "failed")
        wf = _wf(
            trigger_now=True,
            steps=[
                WorkflowStepConfig(0, "pipeline", "p1", "orders", True),
                WorkflowStepConfig(1, "transformation", "t1", "marts", True),
            ],
        )
        rec = _Recorder([wf])

        self._tick(rec)

        self.t_run.assert_not_called()
        final = rec.sent[-1]
        assert final.status == "failed"
        assert [s.status for s in final.step_runs] == ["failed", "skipped"]
        assert final.completed_at
        assert "step 1" in final.error_message
        assert "orders" in final.error_message
        assert "source exploded" in final.error_message
        assert final.step_runs[0].error_message == "source exploded"
        # The failed run is still a run: it has an id and was reported.
        assert final.step_runs[0].pipeline_run_id
        rec.client.report_pipeline_run.assert_called_once()

    def test_disabled_pipeline_file_fails_without_running(self) -> None:
        _write_pipeline_yaml(
            self.checkout, "orders.yaml", "p1", schedule=None, enabled=False
        )
        rec = _Recorder([_wf(trigger_now=True)])  # ref_enabled True

        self._tick(rec)

        self.p_run.assert_not_called()
        self.t_run.assert_not_called()
        final = rec.sent[-1]
        assert [s.status for s in final.step_runs] == ["failed", "skipped"]
        assert final.step_runs[0].error_message == (
            "pipeline is disabled (in its definition file)"
        )
        assert final.step_runs[0].pipeline_run_id == ""

    def test_disabled_pipeline_run_by_its_run_now_still_fails_the_step(
        self,
    ) -> None:
        """A Run-now runs a disabled pipeline standalone; a workflow step
        must not take that run as its own — a disabled target fails."""
        _write_pipeline_yaml(
            self.checkout, "orders.yaml", "p1", schedule=None, enabled=False
        )
        rec = _Recorder([_wf(trigger_now=True)])

        self._tick(rec, triggers=[PipelineTrigger(id="p1", trigger_now=True)])

        self.p_run.assert_called_once()  # the Run-now, not the step
        step = rec.sent[-1].step_runs[0]
        assert step.status == "failed"
        assert step.error_message == "pipeline is disabled (in its definition file)"

    def test_pipeline_file_enabled_wins_over_ref_enabled(self) -> None:
        """Worker contract: for a pipeline the definition file's `enabled`
        wins over the database-derived ref_enabled, as for every other
        pipeline field in files mode (the database may lag the checkout)."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        wf = _wf(
            trigger_now=True,
            steps=[
                WorkflowStepConfig(0, "pipeline", "p1", "orders", False),
                WorkflowStepConfig(1, "transformation", "t1", "marts", True),
            ],
        )
        rec = _Recorder([wf])

        self._tick(rec)

        self.p_run.assert_called_once()
        assert [s.status for s in rec.sent[-1].step_runs] == ["success", "success"]

    def test_disabled_transformation_fails_without_running(self) -> None:
        wf = _wf(
            trigger_now=True,
            steps=[WorkflowStepConfig(0, "transformation", "t1", "marts", False)],
        )
        rec = _Recorder([wf])

        self._tick(rec, transformations=[])

        self.t_run.assert_not_called()
        assert rec.sent[-1].step_runs[0].error_message == "transformation is disabled"

    def test_missing_pipeline_definition_fails_the_step(self) -> None:
        """No file in the checkout (mirror lag, deleted, broken YAML)."""
        rec = _Recorder([_wf(trigger_now=True)])

        self._tick(rec)

        final = rec.sent[-1]
        assert final.status == "failed"
        assert [s.status for s in final.step_runs] == ["failed", "skipped"]
        assert "pipeline not found" in final.step_runs[0].error_message
        self.t_run.assert_not_called()

    def test_missing_transformation_fails_the_step(self) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        rec = _Recorder([_wf(trigger_now=True)])

        self._tick(rec, transformations=[])

        final = rec.sent[-1]
        assert [s.status for s in final.step_runs] == ["success", "failed"]
        assert "transformation not found" in final.step_runs[1].error_message

    def test_pipeline_not_in_poll_response_fails_the_step(self) -> None:
        """No credential file and the poll answered without the pipeline:
        the standalone path skips it; a step fails with that reason."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        rec = _Recorder([_wf(trigger_now=True)])

        self._tick(rec, triggers=[])

        self.p_run.assert_not_called()
        msg = rec.sent[-1].step_runs[0].error_message
        assert "poll response does not include it" in msg

    def test_crashing_step_fails_with_type_only(self) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        self.p_run.side_effect = RuntimeError("password=hunter2")
        rec = _Recorder([_wf(trigger_now=True)])

        self._tick(rec)

        step = rec.sent[-1].step_runs[0]
        assert step.status == "failed"
        assert step.error_message == "internal error (RuntimeError)"
        assert "hunter2" not in rec.sent[-1].error_message

    def test_one_broken_workflow_does_not_stop_the_next(self) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        bad = _wf(id="w-bad", trigger_now=True)
        good = _wf(id="w-good", trigger_now=True)
        rec = _Recorder([bad, good])

        with patch(
            "dlt_worker.main._workflow_due",
            side_effect=[RuntimeError("boom"), "manual"],
        ):
            self._tick(rec)

        assert {r.workflow_id for r in rec.sent} == {"w-good"}
        assert rec.sent[-1].status == "success"

    def test_shutdown_between_steps_fails_and_skips(self) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)

        def run_then_shutdown(cfg: PipelineConfig) -> PipelineRunReport:
            main._shutdown = True
            return _p_report(cfg)

        self.p_run.side_effect = run_then_shutdown
        rec = _Recorder([_wf(trigger_now=True)])
        tick = main._Tick()
        rec.client.try_get_pipeline_triggers.return_value = [PipelineTrigger(id="p1")]
        main._run_due_pipelines(rec.client, tick)
        main._run_due_workflows(rec.client, tick, [_make_tconfig()])

        final = rec.sent[-1]
        assert final.status == "failed"
        assert [s.status for s in final.step_runs] == ["success", "skipped"]
        assert "shut down" in final.error_message

    # --- API compatibility / report failures ---

    def test_api_without_workflows_leaves_the_tick_alone(self) -> None:
        """get_workflow_configs answers [] for an API that predates
        workflows; pipelines and transformations run as before."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1")
        rec = _Recorder([])

        self._tick(rec, transformations=[_make_tconfig(trigger_now=True)])

        self.p_run.assert_called_once()
        self.t_run.assert_called_once()
        rec.client.report_workflow_run.assert_not_called()

    def test_workflow_fetch_crash_is_contained(self) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1")
        rec = _Recorder([])
        rec.client.get_workflow_configs.side_effect = RuntimeError("boom")

        self._tick(rec, transformations=[_make_tconfig(trigger_now=True)])

        self.t_run.assert_called_once()

    def test_report_failures_do_not_stop_the_workflow(self) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        rec = _Recorder([_wf(trigger_now=True, pending_run_id="pend-1")])
        rec.accept = False

        self._tick(rec)

        self.p_run.assert_called_once()
        self.t_run.assert_called_once()
        assert rec.sent[-1].status == "success"
        # The final outcome waits for the next tick instead of being lost.
        assert "w1" in main._unsent_workflow_reports

    def test_undelivered_outcome_is_flushed_and_holds_the_workflow(self) -> None:
        """Until the final report lands the server may still show the run
        pending — re-running it would run one Run-now twice."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        wf = _wf(trigger_now=True, pending_run_id="pend-1")
        rec = _Recorder([wf])
        rec.accept = False
        self._tick(rec)
        assert self.p_run.call_count == 1

        # Next tick: still refused → held back, nothing re-runs.
        rec.sent.clear()
        self._tick(rec)
        assert self.p_run.call_count == 1
        assert [r.status for r in rec.sent] == ["success"]  # the flush only

        # API back: the flush lands and the workflow is no longer held.
        rec.accept = True
        rec.sent.clear()
        rec.client.get_workflow_configs.return_value = [_wf(schedule=None)]
        self._tick(rec)
        assert [r.run_id for r in rec.sent] == ["pend-1"]
        assert main._unsent_workflow_reports == {}

    def test_undeliverable_outcome_is_dropped_eventually(self) -> None:
        report = WorkflowRunReport(
            run_id="r",
            workflow_id="w1",
            status="success",
            trigger="schedule",
            started_at="2026-01-01T00:00:00+00:00",
        )
        main._unsent_workflow_reports["w1"] = (
            report,
            main._WORKFLOW_REPORT_MAX_ATTEMPTS - 1,
        )
        client = MagicMock()
        client.report_workflow_run.return_value = False

        main._flush_workflow_reports(client)

        assert main._unsent_workflow_reports == {}

    def test_rejected_final_report_is_not_retained(self, caplog: Any) -> None:
        """InvalidArgument / NotFound: the same report fails the same way, so
        it is dropped at once — not retried for ten ticks while holding its
        workflow back."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        rec = _Recorder([_wf(trigger_now=True, pending_run_id="pend-1")])
        rec.accept = ReportResult.REJECTED

        with caplog.at_level("ERROR", logger="dlt_worker.main"):
            self._tick(rec)

        assert rec.sent[-1].status == "success"
        assert main._unsent_workflow_reports == {}
        assert any(
            "pend-1" in r.getMessage() and "not retrying" in r.getMessage()
            for r in caplog.records
        )

        # Next tick: nothing to flush, and the workflow is not held back.
        rec.accept = True
        rec.sent.clear()
        rec.client.get_workflow_configs.return_value = [_wf(trigger_now=True)]
        self._tick(rec)
        assert [r.status for r in rec.sent][-1] == "success"
        assert self.p_run.call_count == 2

    def test_rejected_flush_is_dropped_on_first_refusal(self) -> None:
        report = WorkflowRunReport(
            run_id="r",
            workflow_id="w1",
            status="success",
            trigger="schedule",
            started_at="2026-01-01T00:00:00+00:00",
        )
        main._unsent_workflow_reports["w1"] = (report, 1)
        client = MagicMock()
        client.report_workflow_run.return_value = ReportResult.REJECTED

        main._flush_workflow_reports(client)

        assert main._unsent_workflow_reports == {}
        client.report_workflow_run.assert_called_once()

    def test_transient_flush_failure_is_kept(self) -> None:
        report = WorkflowRunReport(
            run_id="r",
            workflow_id="w1",
            status="success",
            trigger="schedule",
            started_at="2026-01-01T00:00:00+00:00",
        )
        main._unsent_workflow_reports["w1"] = (report, 1)
        client = MagicMock()
        client.report_workflow_run.return_value = ReportResult.TRANSIENT

        main._flush_workflow_reports(client)

        assert main._unsent_workflow_reports == {"w1": (report, 2)}

    # --- dedupe with standalone runs ---

    def test_pipeline_that_ran_by_its_own_cron_is_not_run_again(self) -> None:
        """Same tick: the pipeline's own schedule fired first; the step
        takes that run instead of loading the same data twice."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1")  # */5, never run
        rec = _Recorder([_wf(trigger_now=True)])

        self._tick(rec)

        self.p_run.assert_called_once()
        own_run = rec.client.report_pipeline_run.call_args[0][0]
        final = rec.sent[-1]
        assert final.status == "success"
        assert final.step_runs[0].pipeline_run_id == own_run.run_id

    def test_failed_own_run_fails_the_step_without_rerun(self) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1")
        self.p_run.side_effect = lambda cfg: _p_report(cfg, "failed")
        rec = _Recorder([_wf(trigger_now=True)])

        self._tick(rec)

        self.p_run.assert_called_once()
        final = rec.sent[-1]
        assert [s.status for s in final.step_runs] == ["failed", "skipped"]
        assert "earlier in this tick" in final.step_runs[0].error_message

    def test_transformation_run_by_workflow_is_not_run_again_this_tick(
        self,
    ) -> None:
        """Due by its own schedule AND chained after p1 AND a step: once."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        t1 = _make_tconfig(schedule="* * * * *", trigger_after_pipeline_id="p1")
        rec = _Recorder([_wf(trigger_now=True)])

        self._tick(rec, transformations=[t1])

        self.t_run.assert_called_once()

    def test_step_consumes_the_transformations_pending_trigger(self) -> None:
        """A transformation with a Run-now queued that a workflow runs:
        the step's run IS that run (reported under its pending id)."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        t1 = _make_tconfig(trigger_now=True, pending_run_id="t-pend")
        rec = _Recorder([_wf(trigger_now=True)])

        self._tick(rec, transformations=[t1])

        self.t_run.assert_called_once()
        assert rec.sent[-1].step_runs[1].transformation_run_id == "t-pend"

    def test_workflow_pipeline_success_chains_its_transformations(self) -> None:
        """trigger_after_pipeline still fires for a pipeline a workflow ran
        (when the chained transformation is not itself a step)."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        chained = _make_tconfig(id="t9", trigger_after_pipeline_id="p1")
        wf = _wf(
            trigger_now=True,
            steps=[WorkflowStepConfig(0, "pipeline", "p1", "orders", True)],
        )
        rec = _Recorder([wf])

        tick = self._tick(rec, transformations=[chained])

        assert tick.succeeded == {"p1"}
        self.t_run.assert_called_once()
        assert self.t_run.call_args[0][0].id == "t9"

    def test_workflow_pipeline_success_moves_its_own_cron_baseline(self) -> None:
        """A workflow-run pipeline records last_run_at in scheduler.json,
        so its own schedule does not re-fire right after."""
        now = datetime.now(UTC)
        hourly = f"{(now.minute + 30) % 60} * * * *"
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=hourly)
        state = SchedulerState.load(str(self.state_dir))
        state.record("p1", now - timedelta(hours=2))  # due by its own cron
        state = SchedulerState.load(str(self.state_dir))

        # Workflow first (its own cron is evaluated in the pipelines phase,
        # which already ran it this tick in the full flow — so run the
        # workflow phase directly against a tick whose phase was skipped).
        rec = _Recorder([_wf(trigger_now=True, steps=[_wf().steps[0]])])
        tick = main._Tick(
            pipelines={"p1": _load("p1")},
            polled=[PipelineTrigger(id="p1")],
            state=state,
        )
        main._creds_cache["p1"] = {}
        main._run_due_workflows(rec.client, tick, [])
        assert self.p_run.call_count == 1

        recorded = SchedulerState.load(str(self.state_dir)).get("p1")
        assert recorded is not None and recorded >= now

        # Next tick, its own cron is not due any more.
        rec.client.get_workflow_configs.return_value = []
        self._tick(rec, transformations=[])
        assert self.p_run.call_count == 1

    def test_workflow_step_failure_backs_off_its_own_cron(self) -> None:
        """Failure bookkeeping is shared too: a pipeline that just failed
        as a step waits for its next own slot instead of firing next tick."""
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule="0 0 * * *")
        state = SchedulerState.load(str(self.state_dir))
        state.record("p1", datetime.now(UTC) - timedelta(days=2))
        self.p_run.side_effect = lambda cfg: _p_report(cfg, "failed")
        rec = _Recorder([_wf(trigger_now=True, steps=[_wf().steps[0]])])
        tick = main._Tick(
            pipelines={"p1": _load("p1")},
            polled=[PipelineTrigger(id="p1")],
            state=SchedulerState.load(str(self.state_dir)),
        )
        main._creds_cache["p1"] = {}
        main._run_due_workflows(rec.client, tick, [])
        assert "p1" in main._last_failure_at

        rec.client.get_workflow_configs.return_value = []
        self._tick(rec, transformations=[])
        assert self.p_run.call_count == 1

    def test_step_runs_carry_workflow_trigger_in_telemetry(self) -> None:
        _write_pipeline_yaml(self.checkout, "orders.yaml", "p1", schedule=None)
        rec = _Recorder([_wf(trigger_now=True)])

        with patch("dlt_worker.main.telemetry.record_pipeline_run") as rec_metric:
            self._tick(rec)

        assert rec_metric.call_args[0][3] == "workflow"


def _load(pipeline_id: str) -> PipelineConfig:
    cfg = main._reload_definition(pipeline_id)
    assert cfg is not None
    return cfg


# --- time: run starts vs. the tick's schedule baseline ---


class TestWorkflowTimes:
    """A workflow runs after every due pipeline of the tick, so its steps
    start long after the tick did. Each run records when it actually began
    (durations, the stuck-run sweep), while a pipeline's own cron baseline
    stays the tick time whichever way it was run — a standalone run and a
    workflow step move it alike, so the schedule does not drift."""

    TICK = datetime(2026, 1, 1, 10, 0, 5, tzinfo=UTC)

    @pytest.fixture(autouse=True)
    def _files_mode(self, tmp_path: Path) -> Iterator[None]:
        self.checkout = tmp_path / "checkout"
        (self.checkout / "pipelines").mkdir(parents=True)
        self.state_dir = tmp_path / "state"
        self.state_dir.mkdir()
        config.PIPELINES_DIR = str(self.checkout)
        config.DLT_STATE_DIR = str(self.state_dir)
        config.AGE_KEY_FILE = ""
        config.PIPELINE_MAX_RETRIES = 0
        main._shutdown = False
        main._creds_cache.clear()
        main._last_failure_at.clear()
        main._workflow_started_at.clear()
        main._unsent_workflow_reports.clear()
        self.recorder = MagicMock()
        main._recorder = self.recorder
        self.clock = _Clock(self.TICK)

        # p0: hourly, due this tick, takes 20 minutes. p1: every 15 minutes,
        # last run a moment before the tick, so only the workflow runs it.
        _write_pipeline_yaml(self.checkout, "a_long.yaml", "p0", schedule="0 * * * *")
        _write_pipeline_yaml(
            self.checkout, "b_step.yaml", "p1", schedule="*/15 * * * *"
        )
        state = SchedulerState.load(str(self.state_dir))
        state.record("p0", datetime(2026, 1, 1, 9, 0, 5, tzinfo=UTC))
        state.record("p1", datetime(2026, 1, 1, 10, 0, 1, tzinfo=UTC))

        durations = {"p0": 20, "p1": 5}
        self.p_status = "success"

        def run_pipeline(cfg: PipelineConfig) -> PipelineRunReport:
            report = self.clock.run_for(cfg, durations[cfg.id])
            if cfg.id == "p1":
                report.status = self.p_status
            return report

        def run_transformation(cfg: TransformationConfig) -> TransformationRunReport:
            # The child's own clock, deliberately unlike the run's start.
            report = TransformationRunReport(
                transformation_id=cfg.id,
                status="success",
                started_at="2020-01-01T00:00:00+00:00",
                completed_at="",
            )
            self.clock.at += timedelta(minutes=10)
            report.completed_at = self.clock.at.isoformat()
            return report

        patch("dlt_worker.main._utcnow", self.clock).start()
        patch("dlt_worker.main.run_pipeline_isolated", side_effect=run_pipeline).start()
        patch(
            "dlt_worker.main.run_transformation_isolated",
            side_effect=run_transformation,
        ).start()
        patch("dlt_worker.main.trigger_snapshot").start()
        patch("dlt_worker.main.time.sleep").start()
        yield
        patch.stopall()
        main._recorder = None
        config.PIPELINES_DIR = ""
        main._creds_cache.clear()
        main._last_failure_at.clear()
        main._workflow_started_at.clear()
        main._unsent_workflow_reports.clear()

    def _tick(self, rec: _Recorder) -> None:
        rec.client.try_get_pipeline_triggers.return_value = [
            PipelineTrigger(id="p0"),
            PipelineTrigger(id="p1"),
        ]
        rec.client.get_transformation_configs.return_value = [_make_tconfig()]
        main._poll_and_run(rec.client)

    def _workflow(self) -> WorkflowConfig:
        return _wf(
            trigger_now=True,
            steps=[
                WorkflowStepConfig(0, "pipeline", "p1", "b_step", True),
                WorkflowStepConfig(1, "transformation", "t1", "nightly", True),
            ],
        )

    def _at(self, minutes: int) -> datetime:
        return self.TICK + timedelta(minutes=minutes)

    def test_step_keeps_the_pipelines_cron_baseline_on_the_tick(self) -> None:
        rec = _Recorder([self._workflow()])

        self._tick(rec)

        state = SchedulerState.load(str(self.state_dir))
        assert state.get("p0") == self.TICK
        # Not 10:20:05, when the step got its turn: that would move a
        # */15 schedule's next slot from 10:15 to 10:30.
        assert state.get("p1") == self.TICK

    def test_failed_step_backs_off_from_the_tick(self) -> None:
        self.p_status = "failed"
        rec = _Recorder([self._workflow()])

        self._tick(rec)

        assert rec.sent[-1].status == "failed"
        assert main._last_failure_at["p1"] == self.TICK

    def test_step_runs_record_their_own_start(self) -> None:
        rec = _Recorder([self._workflow()])

        self._tick(rec)

        # Pipeline step: local start row, and the run's own report.
        starts = {
            c[0][1]: c[0][2]
            for c in self.recorder.record_pipeline_run_start.call_args_list
        }
        assert starts == {"p0": self.TICK, "p1": self._at(20)}
        p1_report = next(
            c[0][0]
            for c in rec.client.report_pipeline_run.call_args_list
            if c[0][0].pipeline_id == "p1"
        )
        assert p1_report.started_at == self._at(20).isoformat()

        # Transformation step: the run's start, not the child's clock.
        [t_start] = self.recorder.record_transformation_run_start.call_args_list
        assert t_start[0][2] == self._at(25)
        t_report = rec.client.report_transformation_run.call_args[0][0]
        assert t_report.started_at == self._at(25).isoformat()

    def test_workflow_and_step_timestamps_follow_the_clock(self) -> None:
        rec = _Recorder([self._workflow()])

        self._tick(rec)

        final = rec.sent[-1]
        assert final.status == "success"
        assert final.started_at == self._at(20).isoformat()
        assert final.completed_at == self._at(35).isoformat()
        assert [(s.started_at, s.completed_at) for s in final.step_runs] == [
            (self._at(20).isoformat(), self._at(25).isoformat()),
            (self._at(25).isoformat(), self._at(35).isoformat()),
        ]
        assert main._workflow_started_at["w1"] == self._at(20)
