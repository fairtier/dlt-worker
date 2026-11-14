"""Main poll loop with graceful shutdown.

Polls the FairTier API for pipeline and transformation configurations,
evaluates cron schedules, and runs work that is due. Handles
SIGTERM/SIGINT for graceful shutdown (important in Kubernetes).
"""

from __future__ import annotations

import logging
import signal
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field, fields, replace
from datetime import UTC, datetime
from typing import Any

from croniter import croniter

from dlt_worker import config, iceberg_stream, telemetry, workspace_db
from dlt_worker.api_client import (
    APIClient,
    PipelineConfig,
    PipelineRunReport,
    PipelineTrigger,
    TransformationConfig,
    TransformationRunReport,
    WorkflowConfig,
    WorkflowRunReport,
    WorkflowStepConfig,
    WorkflowStepRunReport,
)
from dlt_worker.health import start_health_server
from dlt_worker.pipeline_files import load_pipeline_configs
from dlt_worker.run_isolation import (
    run_pipeline_isolated,
    run_source_test_isolated,
    run_transformation_isolated,
)
from dlt_worker.scheduler_state import SchedulerState
from dlt_worker.snapshot import trigger_snapshot

logger = logging.getLogger(__name__)

# Shutdown flag
_shutdown = False

# Local-first run recording (WORKSPACE_DB_URL) — None means off.
_recorder: workspace_db.WorkspaceRecorder | None = None

# Central run-report retries when local-first recording is active: the
# local row is the record, central is only the Console cache — retry a
# few times, then log-and-continue. Never applies without the recorder
# (the central row is the only record then, but retrying can't help a
# run that already finished — keep the legacy single attempt).
_CENTRAL_REPORT_ATTEMPTS = 3
_CENTRAL_REPORT_RETRY_DELAY = 5

# Seconds between local stale-run sweeps (finalizing orphaned rows the
# central stuck-run sweep cannot see).
_STALE_SWEEP_INTERVAL = 3600

# Files mode: last-known source credentials per pipeline id, refreshed on
# every successful poll. In-memory ONLY — credentials must never touch
# disk unencrypted and must never be logged. A pipeline briefly absent
# from one poll response keeps its last-known credentials for the next
# central outage; entries are evicted only once the pipeline is gone from
# both a successful poll and an error-free file set (deleted for real —
# its credentials must not linger for the process lifetime). Fallback
# only: credentials decrypted from the checkout's .credentials.age files
# take precedence.
_creds_cache: dict[str, dict[str, Any]] = {}

# Failure-aware backoff: start time of the last *failed* run per config id
# (pipelines and transformations). last_run_at only advances on success, so
# a deterministically failing scheduled config would otherwise re-fire
# every poll tick — thousands of attempts a day hammering the source and
# delaying every other pipeline. With this, the next attempt waits for the
# next cron slot after the last failure, same cadence as successes.
# Cleared on success. In-memory only: a restart forgets failures, costing
# at most one immediate re-fire.
_last_failure_at: dict[str, datetime] = {}

# Workflows: start time of the last run this process began, per workflow id.
# The server's last_run_at is the real cron baseline (it covers failures and
# restarts); this only covers the window in which the start report has not
# landed, so a flapping API cannot make a scheduled workflow fire every tick.
_workflow_started_at: dict[str, datetime] = {}

# Workflows: final reports the API did not take, per workflow id, with the
# number of delivery attempts so far. Flushed before every GetWorkflowConfigs
# — until one lands, the server still shows the run as running (or a manual
# trigger as pending), so its workflow is held back rather than re-run.
_unsent_workflow_reports: dict[str, tuple[WorkflowRunReport, int]] = {}
_WORKFLOW_REPORT_MAX_ATTEMPTS = 10


@dataclass
class _RunOutcome:
    """What one pipeline or transformation run in this tick ended as.

    Kept per tick so a workflow step whose target already ran in the same
    tick takes that run's outcome instead of running it a second time.
    """

    run_id: str
    status: str
    error_message: str = ""
    started_at: str = ""
    completed_at: str = ""


@dataclass
class _Tick:
    """State shared by the phases of one poll tick.

    Pipelines run first, then workflows, then transformations; each phase
    leaves behind what the later ones need to look targets up and to avoid
    running anything twice in one tick.
    """

    now: datetime = field(default_factory=lambda: _utcnow())
    # Files mode definitions of this tick, by pipeline id.
    pipelines: dict[str, PipelineConfig] = field(default_factory=dict)
    polled: list[PipelineTrigger] | None = None
    state: SchedulerState | None = None
    succeeded: set[str] = field(default_factory=set)
    pipeline_runs: dict[str, _RunOutcome] = field(default_factory=dict)
    transformation_runs: dict[str, _RunOutcome] = field(default_factory=dict)


def _configure_logging() -> None:
    """Configure root logging at INFO, taking over from dlt.

    force=True is essential: importing dlt (transitively, at module load)
    installs a root StreamHandler and sets the root level to WARNING. Without
    force, basicConfig is a no-op when a handler already exists, so the root
    level stays WARNING and every INFO log the worker emits (startup, "Running
    pipeline: X", run outcomes) is silently dropped — only ERRORs (e.g. a
    pipeline crash) get through, which is why file-drop failures were invisible
    in central Loki. force=True re-owns the root config at INFO.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )


def _handle_signal(signum: int, _frame: object) -> None:
    global _shutdown
    logger.info("Received signal %d, shutting down gracefully...", signum)
    _shutdown = True


def _utcnow() -> datetime:
    """The wall clock — one seam, so tests can move time."""
    return datetime.now(UTC)


def _should_run(cfg: PipelineConfig | TransformationConfig, now: datetime) -> bool:
    """Determine if a pipeline or transformation should run based on its
    cron schedule or trigger flag."""
    # A Run-now on a PIPELINE outranks `enabled`: disabling stops the
    # schedule, not a run a person just asked for. The server already agrees
    # — GetEnabledPipelines serves a disabled pipeline that has a pending run
    # (`enabled = true OR pr.id IS NOT NULL`) — so checking enabled first left
    # that run pending forever, with the Console showing a trigger that never
    # fired. In files mode this is not the poll overruling the file: only
    # trigger_now/pending_run_id are taken from the poll, and every definition
    # field (`enabled` included, when no trigger is present) still comes from
    # the checkout.
    #
    # Transformations are deliberately stricter and stay so: disabled means it
    # never runs — by schedule, by chain, or by trigger — which is why
    # GetEnabledTransformations filters on `enabled` alone and a disabled one
    # never reaches the worker at all.
    if cfg.trigger_now and isinstance(cfg, PipelineConfig):
        return True

    if not cfg.enabled:
        return False

    if cfg.trigger_now:
        return True

    if not cfg.schedule:
        return False

    # An invalid cron string must disable this one config, not raise into
    # the tick loop and abandon every other pipeline (files mode validates
    # at load, but central-delivered configs reach here unvalidated).
    if not croniter.is_valid(cfg.schedule):
        logger.warning(
            "Config %s has an invalid cron schedule %r — skipping",
            cfg.name,
            cfg.schedule,
        )
        return False

    # The cron baseline is the last success — or the last failure when
    # that is newer, so a failing config retries at its schedule's cadence
    # instead of every tick (trigger_now above bypasses this).
    baseline = cfg.last_run_at
    last_failure = _last_failure_at.get(cfg.id)
    if last_failure is not None and (baseline is None or last_failure > baseline):
        baseline = last_failure

    if baseline is None:
        return True  # never run before

    # is_valid can't catch date-arithmetic failures (croniter errors
    # subclass ValueError) — treat those as not-due too.
    try:
        next_run = croniter(cfg.schedule, baseline).get_next(datetime)
    except ValueError:
        logger.warning(
            "Config %s: cron schedule %r failed to evaluate — skipping",
            cfg.name,
            cfg.schedule,
        )
        return False
    return now >= next_run


def run() -> None:
    """Main loop: poll for work, run pipelines, report results."""
    global _recorder

    config.load()

    _configure_logging()

    telemetry.setup("worker", config.CUSTOMER_SLUG)

    _recorder = workspace_db.from_env()
    if _recorder:
        logger.info("Local-first run recording enabled (workspace database)")
        _recorder.finalize_stale_runs()

    # Only the process that actually runs dlt needs the patch, and with
    # subprocess isolation that is the child — run_isolation applies it
    # there. Skipping it here is what keeps dlt (~115 MB of imports, since
    # apply() reaches into dlt.destinations) out of the poll loop for good.
    if config.ICEBERG_LOAD_CHUNK_ROWS > 0 and not config.PIPELINE_SUBPROCESS:
        iceberg_stream.apply(
            config.ICEBERG_LOAD_CHUNK_ROWS,
            config.ICEBERG_LOAD_COMMIT_EVERY,
            config.ICEBERG_CREDENTIAL_REFRESH_SECONDS,
        )

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    client = APIClient(
        base_url=config.FAIRTIER_API_URL,
        customer_slug=config.CUSTOMER_SLUG,
        oidc_token_url=config.OIDC_TOKEN_URL,
        oidc_client_id=config.OIDC_CLIENT_ID,
        oidc_client_secret=config.OIDC_CLIENT_SECRET,
    )
    if not client.auth_enabled:
        logger.warning(
            "OIDC credentials not configured — FairTier API calls are "
            "unauthenticated and will be rejected once the API enforces "
            "tenant-bound auth"
        )

    start_health_server(client, config.HEALTHZ_PORT, config.PIPELINES_DIR)

    logger.info(
        "dlt-worker started for customer=%s, polling every %ds",
        config.CUSTOMER_SLUG,
        config.POLL_INTERVAL_SECONDS,
    )

    last_sweep = time.monotonic()
    while not _shutdown:
        _tick(client)

        if _recorder and time.monotonic() - last_sweep >= _STALE_SWEEP_INTERVAL:
            _recorder.finalize_stale_runs()
            last_sweep = time.monotonic()

        # Sleep in small increments to respond to shutdown quickly — and to
        # answer source tests in between polls. A person is watching a
        # spinner for those, so they cannot wait for the next full tick.
        for elapsed in range(1, config.POLL_INTERVAL_SECONDS + 1):
            if _shutdown:
                break
            time.sleep(1)
            if (
                config.SOURCE_TEST_POLL_SECONDS > 0
                and elapsed % config.SOURCE_TEST_POLL_SECONDS == 0
            ):
                _serve_source_tests(client)

    telemetry.flush()
    logger.info("dlt-worker shut down cleanly")


def _serve_source_tests(client: APIClient) -> None:
    """Claim and run any queued source tests ("Test connection").

    Runs on the poll loop's own thread, so a probe delays the next tick by
    at most its own deadline (SOURCE_TEST_TIMEOUT_SECONDS, a minute by
    default). That is the right trade: scheduled work is minutes-granular,
    and a probe that cannot answer in a minute has answered.

    Never raises: a control plane too old to serve source tests, or one that
    is simply down, must not touch scheduling.
    """
    try:
        tests = client.get_pending_source_tests()
    except Exception:
        logger.warning("Failed to claim source tests", exc_info=True)
        return

    for test in tests:
        logger.info("Running source test %s (%s)", test.id, test.source_type)
        with telemetry.tracer.start_as_current_span(
            "dlt_worker.source_test",
            attributes={telemetry.ATTR_SOURCE_TYPE: test.source_type},
        ):
            report = run_source_test_isolated(test)
        logger.info("Source test %s: %s", test.id, report.status)
        client.report_source_test(report)


def _tick(client: APIClient) -> None:
    """One poll iteration, traced end to end.

    The tick span is the root of everything a run produces — the config
    fetch, each pipeline attempt (including the spans its subprocess emits)
    and each transformation hang off it, so one trace answers "what did the
    worker do at 04:00" without joining log lines by timestamp.
    """
    mode = "files"
    started = time.monotonic()
    outcome = "ok"

    with telemetry.tracer.start_as_current_span(
        "dlt_worker.poll", attributes={telemetry.ATTR_MODE: mode}
    ) as span:
        try:
            _poll_and_run(client)
        except Exception as exc:
            outcome = "error"
            # Type only, never the message: an exception escaping this far
            # can have quoted anything, credentials included. The scrubbed
            # detail is in the log line below.
            span.set_status(telemetry.error_status(type(exc).__name__))
            logger.exception("Unexpected error in poll loop")

    telemetry.poll_duration.record(
        time.monotonic() - started,
        {telemetry.ATTR_MODE: mode, telemetry.ATTR_OUTCOME: outcome},
    )


def _poll_and_run(client: APIClient) -> None:
    """One tick: pipelines, then workflows, then transformations.

    Workflows sit in the middle on purpose. After pipelines, so a step whose
    pipeline already ran by its own schedule this tick takes that run's
    outcome instead of loading the same data twice. Before transformations,
    so a pipeline a workflow ran still chains its trigger_after_pipeline
    transformations this tick, and a transformation a workflow already ran is
    not run again by its own schedule or chain.
    """
    tick = _Tick()
    _run_due_pipelines(client, tick)
    transformations = _fetch_transformation_configs(client)
    _run_due_workflows(client, tick, transformations)
    _run_due_transformations(client, tick.succeeded, transformations, tick)


def _execute_pipeline(
    cfg: PipelineConfig,
    now: datetime,
    client: APIClient,
    trigger: str | None = None,
) -> tuple[PipelineRunReport | None, str]:
    """Run one due pipeline; returns its final report and run id.

    ``now`` is the tick's time — for a workflow step too, not the step's
    start: the schedule baseline (it backs
    ``_last_failure_at`` here and ``last_run_at`` in the caller), which must
    not move with queueing or the schedule drifts later every run. It is
    NOT this run's start — pipelines in one tick run one after another, so
    a run queued behind a long one starts long after the tick did. The run
    stamps its own start, and every record of it (local start row, central
    "running" mark, final report) carries that one value: durations stay
    honest, and the stuck-run sweep, which ages rows from started_at,
    cannot fail a short run for the time it spent waiting its turn.
    """
    logger.info("Running pipeline: %s (%s)", cfg.name, cfg.source_type)
    run_started_at = _utcnow()

    # Local-first: the run exists in the workspace database before it
    # starts — under the central pending id when there is one (one run,
    # one identity in both stores), else a worker-generated UUID.
    local_run_id = cfg.pending_run_id or str(uuid.uuid4())
    trigger = trigger or telemetry.trigger_kind(cfg)
    started = time.monotonic()

    with telemetry.tracer.start_as_current_span(
        "dlt_worker.pipeline.run",
        attributes={
            telemetry.ATTR_PIPELINE_ID: cfg.id,
            telemetry.ATTR_PIPELINE_NAME: cfg.name,
            telemetry.ATTR_SOURCE_TYPE: cfg.source_type,
            telemetry.ATTR_DATASET: cfg.dataset_name,
            telemetry.ATTR_WRITE_DISPOSITION: cfg.write_disposition,
            telemetry.ATTR_TRIGGER: trigger,
            telemetry.ATTR_RUN_ID: local_run_id,
        },
    ) as span:
        if _recorder:
            _recorder.record_pipeline_run_start(local_run_id, cfg.id, run_started_at)

        # Mark a triggered run as "running" before execution, so the Console
        # stops showing it as queued. Only triggered runs: a scheduled run has
        # no row on the API side yet, and the final report creates it.
        # A failure here needs no fallback — the final report carries the same
        # id and is an upsert, so the row converges either way.
        if cfg.pending_run_id:
            ok = client.report_pipeline_run(
                PipelineRunReport(
                    pipeline_id=cfg.id,
                    status="running",
                    started_at=run_started_at.isoformat(),
                    completed_at="",
                    run_id=cfg.pending_run_id,
                )
            )
            if not ok:
                span.add_event("central.mark_running_failed")
                logger.warning(
                    "Failed to mark run %s as running; the final report will "
                    "reconcile it",
                    cfg.pending_run_id,
                )

        report = _run_with_retry(cfg, local_run_id, client, run_started_at)
        if report is not None:
            if report.status == "success":
                _last_failure_at.pop(cfg.id, None)
            else:
                _last_failure_at[cfg.id] = now
            span.set_attribute(telemetry.ATTR_STATUS, report.status)
            span.set_attribute(telemetry.ATTR_ROWS, report.rows_loaded)
            if report.status != "success":
                # error_message is scrubbed at the source (pipeline_runner)
                # — nothing credential-shaped reaches the span.
                span.set_status(telemetry.error_status(report.error_message))
            telemetry.record_pipeline_run(
                cfg, report, time.monotonic() - started, trigger
            )
    return report, local_run_id


def _run_due_pipelines(client: APIClient, tick: _Tick | None = None) -> set[str]:
    """Run all due pipelines. Returns the ids of pipelines that succeeded.

    Definitions and schedules come from the pipelines checkout; the poll is
    consumed only for Run-now triggers, the last-run watermark, and the
    source credentials that have no file to come from. A control-plane
    outage degrades (no history, no triggers, possibly no credentials)
    instead of stopping scheduled ingestion.

    There is no longer an alternative: the legacy poll-is-truth mode, in
    which the API served whole definitions and PIPELINES_DIR was optional,
    was retired in the pipelines-as-files Phase 2.5 cleanup along with the
    definition fields it read.

    ``tick`` (optional) receives what the later phases of the same tick need:
    the definitions, the poll, the scheduler state and every run's outcome.
    """
    if tick is None:
        tick = _Tick()

    files = load_pipeline_configs(config.PIPELINES_DIR, config.AGE_KEY_FILE)
    file_ids = {c.id for c in files.configs}
    state = SchedulerState.load(config.DLT_STATE_DIR)

    polled = client.try_get_pipeline_triggers()
    tick.pipelines = {c.id: c for c in files.configs}
    tick.polled = polled
    tick.state = state
    by_id: dict[str, PipelineTrigger] = {}
    if polled is None:
        telemetry.add_event("central.unreachable")
        logger.warning(
            "FairTier API unreachable — scheduling from files, "
            "credentials from .credentials.age files or the in-memory cache"
        )
    else:
        for p in polled:
            by_id[p.id] = p
            _creds_cache[p.id] = p.source_credentials
            if p.pending_run_id and p.id not in file_ids:
                logger.warning(
                    "Pending run for pipeline %s has no definition file yet "
                    "(mirror lag?) — will fire once the file lands",
                    p.id,
                )

    # Drop scheduler entries for deleted pipelines — but never on a
    # partial read: a transiently broken file must not lose its
    # last_run_at and re-fire on repair.
    if not files.had_errors:
        state.prune(file_ids)
        # Same standard of proof for cached credentials: evict only when
        # the pipeline is gone from this error-free file set AND from a
        # successful poll. Absence from just one poll keeps the entry for
        # the next outage.
        if polled is not None:
            for stale_id in list(_creds_cache):
                if stale_id not in file_ids and stale_id not in by_id:
                    del _creds_cache[stale_id]

    for cfg in files.configs:
        if _shutdown:
            break
        # One broken config must not abandon the rest of the tick.
        try:
            _process_file_pipeline(cfg, by_id, tick, client)
        except Exception as exc:
            _tick_error(cfg.name, exc)
            logger.exception(
                "Pipeline %s: tick processing failed — continuing with the rest",
                cfg.name,
            )

    return tick.succeeded


def _tick_error(name: str, exc: Exception) -> None:
    """Note on the tick span that one config blew up mid-tick.

    Exception *type* only: these handlers catch everything, and the message
    of an arbitrary exception can quote credentials. The full (scrubbed)
    traceback goes to the log.
    """
    telemetry.add_event(
        "config.tick_error",
        {"dlt_worker.config.name": name, "exception.type": type(exc).__name__},
    )


def _skipped(cfg: PipelineConfig, reason: str) -> None:
    """Note on the tick span that a due pipeline was not run.

    A skip is invisible in run history — there is no run row for something
    that never started — so the tick span is the only place it shows up.
    """
    telemetry.add_event(
        "pipeline.skipped",
        {
            telemetry.ATTR_PIPELINE_ID: cfg.id,
            telemetry.ATTR_PIPELINE_NAME: cfg.name,
            "dlt_worker.skip_reason": reason,
        },
    )


def _resolve_credentials(
    cfg: PipelineConfig, polled: list[PipelineTrigger] | None
) -> str | None:
    """Give ``cfg`` the credentials it runs with; returns why it cannot run,
    or None when it can.

    Precedence: the checkout's .credentials.age file (git truth, no
    fallback), then the in-memory poll cache, else nothing.
    """
    if cfg.has_file_credentials:
        # Decrypted from the checkout — git truth wins; no fallback.
        return None
    if cfg.id in _creds_cache:
        # A cached {} is a valid "known credential-less" entry.
        cfg.source_credentials = _creds_cache[cfg.id]
        return None
    if polled is None:
        return "no_credentials_central_unreachable"
    # The poll succeeded but this pipeline wasn't in the response (deleted
    # centrally, or file-mirror lag on delete). Running would produce a
    # guaranteed failed run with empty credentials. A genuinely
    # credential-less pipeline is covered above: its poll record caches {}.
    return "not_in_poll_response"


def _process_file_pipeline(
    cfg: PipelineConfig,
    by_id: dict[str, PipelineTrigger],
    tick: _Tick,
    client: APIClient,
) -> None:
    """Process one pipeline within a tick (schedule, run, record)."""
    state = tick.state
    assert state is not None  # set by _run_due_pipelines before this runs
    now = tick.now
    api_cfg = by_id.get(cfg.id)

    # One-time migration: adopt central's last_run_at so existing
    # pipelines don't re-fire on the first files-mode tick.
    if cfg.id not in state and api_cfg is not None and api_cfg.last_run_at:
        state.seed(cfg.id, api_cfg.last_run_at)
    cfg.last_run_at = state.get(cfg.id)

    # Triggers are the only definition-adjacent data taken from the
    # poll; polled schedule/enabled/definitions are ignored.
    if api_cfg is not None:
        cfg.trigger_now = api_cfg.trigger_now
        cfg.pending_run_id = api_cfg.pending_run_id

    if not _should_run(cfg, now):
        return

    reason = _resolve_credentials(cfg, tick.polled)
    if reason is not None:
        _skipped(cfg, reason)
        logger.warning(
            "Skipping pipeline %s: %s — retrying next tick",
            cfg.name,
            _CREDENTIAL_SKIP_REASONS[reason],
        )
        return

    report, run_id = _execute_pipeline(cfg, now, client)
    _record_pipeline_outcome(cfg, report, run_id, now, tick)


_CREDENTIAL_SKIP_REASONS = {
    "no_credentials_central_unreachable": (
        "no credential file, no cached credentials, and the FairTier API is unreachable"
    ),
    "not_in_poll_response": (
        "no credential file and the poll response does not include it"
    ),
}


def _record_pipeline_outcome(
    cfg: PipelineConfig,
    report: PipelineRunReport | None,
    run_id: str,
    started: datetime,
    tick: _Tick,
) -> None:
    """Book a finished pipeline run into the tick and the scheduler state.

    Shared by a standalone run and a workflow step, so a workflow-run
    pipeline moves its own cron baseline exactly as a standalone run would.
    """
    status = report.status if report is not None else "failed"
    tick.pipeline_runs[cfg.id] = _RunOutcome(
        run_id=run_id,
        status=status,
        error_message=report.error_message if report is not None else "",
        started_at=report.started_at if report is not None else "",
        completed_at=report.completed_at if report is not None else "",
    )
    if status != "success":
        return
    tick.succeeded.add(cfg.id)
    # Record the run *start* time, mirroring central semantics
    # (last_run_at = created_at of the last success). Success-only:
    # a failing scheduled pipeline keeps re-firing, exactly as in
    # legacy mode.
    if tick.state is not None:
        tick.state.record(cfg.id, started)
    # Extra best-effort snapshot so scheduler.json lands in the
    # post-run commit instead of waiting for the autosave window
    # (a crash in between would re-fire the run once).
    trigger_snapshot(cfg.name)


def _fetch_transformation_configs(client: APIClient) -> list[TransformationConfig]:
    """This tick's transformation configs; empty on any error.

    Transformation errors must never affect pipeline processing.
    """
    try:
        return client.get_transformation_configs()
    except Exception:
        logger.warning("Failed to fetch transformation configs", exc_info=True)
        return []


def _run_due_transformations(
    client: APIClient,
    succeeded_pipelines: set[str],
    configs: list[TransformationConfig] | None = None,
    tick: _Tick | None = None,
) -> None:
    """Run all due dbt transformations.

    A transformation is due on manual trigger, on its cron schedule, or
    chained after a pipeline that succeeded in this same iteration. Unlike
    pipelines there is no retry: dbt failures are code errors, not
    transient — report immediately.

    ``configs`` are fetched when not given. With a ``tick``, a
    transformation a workflow step already ran this tick is not run again —
    by schedule, chain or trigger: the step's run was that run.
    """
    if configs is None:
        configs = _fetch_transformation_configs(client)
    if not configs:
        return

    now = _utcnow()
    ran: set[str] = set(tick.transformation_runs) if tick is not None else set()

    for cfg in configs:
        if _shutdown:
            break
        if cfg.id in ran:
            continue

        # One broken config must not abandon the rest of the tick.
        try:
            chained = bool(
                cfg.enabled
                and cfg.trigger_after_pipeline_id
                and cfg.trigger_after_pipeline_id in succeeded_pipelines
            )
            due = _should_run(cfg, now)
            if not (due or chained):
                continue
            ran.add(cfg.id)
            trigger = "manual" if cfg.trigger_now else "schedule" if due else "chained"
            report = _execute_transformation(cfg, now, client, trigger)
            if tick is not None:
                tick.transformation_runs[cfg.id] = _transformation_outcome(report)
        except Exception as exc:
            _tick_error(cfg.name, exc)
            logger.exception(
                "Transformation %s: tick processing failed — continuing with the rest",
                cfg.name,
            )


def _transformation_outcome(report: TransformationRunReport) -> _RunOutcome:
    return _RunOutcome(
        run_id=report.run_id,
        status=report.status,
        error_message=report.error_message,
        started_at=report.started_at,
        completed_at=report.completed_at,
    )


def _execute_transformation(
    cfg: TransformationConfig, now: datetime, client: APIClient, trigger: str
) -> TransformationRunReport:
    """Run one transformation, record it locally first, report it centrally.

    The one code path for a standalone run and a workflow step alike.

    ``now`` is the schedule baseline (it backs ``_last_failure_at``), as for
    ``_execute_pipeline``; the run stamps its own start, because
    transformations run after every pipeline and workflow of the tick and one
    after another. The local start row and the central report carry that one
    value — the child's own clock would otherwise disagree with the local row.
    """
    logger.info("Running transformation: %s", cfg.name)
    run_started_at = _utcnow()

    # Local-first: same identity rules as pipelines (central pending id
    # when triggered, else a worker-generated UUID).
    local_run_id = cfg.pending_run_id or str(uuid.uuid4())
    started = time.monotonic()

    with telemetry.tracer.start_as_current_span(
        "dlt_worker.transformation.run",
        attributes={
            telemetry.ATTR_TRANSFORMATION_ID: cfg.id,
            telemetry.ATTR_TRANSFORMATION_NAME: cfg.name,
            telemetry.ATTR_TRIGGER: trigger,
            telemetry.ATTR_RUN_ID: local_run_id,
        },
    ) as span:
        if _recorder:
            _recorder.record_transformation_run_start(
                local_run_id, cfg.id, run_started_at
            )

        report = run_transformation_isolated(cfg)
        # Same single-identity rule as pipelines: the report is an
        # upsert on the id the run was recorded under, so a scheduled
        # run cannot end up as two rows.
        report.run_id = local_run_id
        report.started_at = run_started_at.isoformat()
        if report.status == "success":
            _last_failure_at.pop(cfg.id, None)
        else:
            _last_failure_at[cfg.id] = now

        span.set_attribute(telemetry.ATTR_STATUS, report.status)
        span.set_attribute("dlt_worker.dbt.commit_sha", report.commit_sha)
        span.set_attribute("dlt_worker.dbt.models_total", report.models_total)
        span.set_attribute("dlt_worker.dbt.models_failed", report.models_failed)
        span.set_attribute("dlt_worker.dbt.tests_total", report.tests_total)
        span.set_attribute("dlt_worker.dbt.tests_failed", report.tests_failed)
        if report.status != "success":
            # Sanitized at the source (transformation_runner) — no
            # git token can reach the span.
            span.set_status(telemetry.error_status(report.error_message))
        telemetry.record_transformation_run(cfg, report, time.monotonic() - started)

        if _recorder:
            _recorder.record_transformation_run_end(local_run_id, report)
        _report_run_central(
            lambda report=report: client.report_transformation_run(report),
            f"transformation {report.transformation_id} (run_id={report.run_id})",
        )
    return report


# --- workflows ---
#
# A workflow is a linear list of steps, each an existing pipeline or
# transformation. The worker does not get a second way to run either: a step
# goes through _execute_pipeline / _execute_transformation, so isolation,
# retries, local-first recording and the run's own report are exactly those
# of a standalone run. The workflow run itself is reported with
# ReportWorkflowRun — an idempotent upsert carrying every step every time —
# after each transition.


def _workflow_due(wf: WorkflowConfig, now: datetime) -> str | None:
    """The trigger that makes a workflow due ("manual" / "schedule"), or None.

    A Run-now outranks ``enabled``, as for pipelines: disabling stops the
    schedule, not a run a person asked for (the server serves a disabled
    workflow only when it has a pending run). The cron baseline is the
    server's last_run_at — the start of the last run whatever its outcome,
    so a failing workflow waits for its next slot like a successful one.
    """
    if wf.trigger_now:
        return "manual"
    if not wf.enabled or not wf.schedule:
        return None
    if not croniter.is_valid(wf.schedule):
        logger.warning(
            "Workflow %s has an invalid cron schedule %r — skipping",
            wf.name,
            wf.schedule,
        )
        return None

    baseline = wf.last_run_at
    local = _workflow_started_at.get(wf.id)
    if local is not None and (baseline is None or local > baseline):
        baseline = local
    if baseline is None:
        return "schedule"  # never run before
    try:
        next_run = croniter(wf.schedule, baseline).get_next(datetime)
    except ValueError:
        logger.warning(
            "Workflow %s: cron schedule %r failed to evaluate — skipping",
            wf.name,
            wf.schedule,
        )
        return None
    return "schedule" if now >= next_run else None


def _flush_workflow_reports(client: APIClient) -> None:
    """Re-send final workflow reports an earlier tick could not deliver.

    Bounded: a report the API keeps refusing (the workflow was deleted, say)
    is dropped after _WORKFLOW_REPORT_MAX_ATTEMPTS ticks rather than holding
    its workflow back forever.
    """
    for workflow_id, (report, attempts) in list(_unsent_workflow_reports.items()):
        if client.report_workflow_run(report):
            del _unsent_workflow_reports[workflow_id]
            continue
        attempts += 1
        if attempts >= _WORKFLOW_REPORT_MAX_ATTEMPTS:
            del _unsent_workflow_reports[workflow_id]
            telemetry.add_event(
                "workflow.report_dropped",
                {telemetry.ATTR_WORKFLOW_ID: workflow_id},
            )
            logger.error(
                "Giving up on the final report of workflow run %s after %d attempts",
                report.run_id,
                attempts,
            )
        else:
            _unsent_workflow_reports[workflow_id] = (report, attempts)


def _run_due_workflows(
    client: APIClient,
    tick: _Tick,
    transformations: list[TransformationConfig],
) -> None:
    """Run every due workflow, one after the other, each step in order.

    Never raises: a control plane that predates workflows answers "none"
    (get_workflow_configs), and one broken workflow must not abandon the
    others or the transformations that follow.
    """
    try:
        _flush_workflow_reports(client)
        workflows = client.get_workflow_configs()
    except Exception:
        logger.warning("Failed to fetch workflow configs", exc_info=True)
        return
    if not workflows:
        return

    transformations_by_id = {t.id: t for t in transformations}
    for wf in workflows:
        if _shutdown:
            break
        try:
            if wf.id in _unsent_workflow_reports:
                # The API has not taken the last run's outcome, so this
                # config may still show it pending — running now could run
                # one Run-now twice.
                logger.warning(
                    "Workflow %s: last run's final report not delivered yet — "
                    "holding the workflow back",
                    wf.name,
                )
                continue
            trigger = _workflow_due(wf, datetime.now(UTC))
            if trigger is None:
                continue
            _execute_workflow(wf, trigger, tick, transformations_by_id, client)
        except Exception as exc:
            _tick_error(wf.name, exc)
            logger.exception(
                "Workflow %s: tick processing failed — continuing with the rest",
                wf.name,
            )


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _send_workflow_report(client: APIClient, report: WorkflowRunReport) -> bool:
    """One best-effort attempt. A miss is not retried here: the next
    transition re-sends the whole run, and the final one falls back to
    _unsent_workflow_reports."""
    ok = client.report_workflow_run(report)
    if not ok:
        telemetry.add_event(
            "workflow.report_failed", {telemetry.ATTR_RUN_ID: report.run_id}
        )
    return ok


def _execute_workflow(
    wf: WorkflowConfig,
    trigger: str,
    tick: _Tick,
    transformations_by_id: dict[str, TransformationConfig],
    client: APIClient,
) -> WorkflowRunReport:
    """Run one due workflow to its end and report it."""
    # The pending id is the run the person queued; reporting it as running
    # is also what clears trigger_now server-side.
    run_id = wf.pending_run_id or str(uuid.uuid4())
    started = datetime.now(UTC)
    started_mono = time.monotonic()
    _workflow_started_at[wf.id] = started
    logger.info("Running workflow: %s (%d steps, %s)", wf.name, len(wf.steps), trigger)

    report = WorkflowRunReport(
        run_id=run_id,
        workflow_id=wf.id,
        status="running",
        trigger=trigger,
        started_at=started.isoformat(),
        step_runs=[
            WorkflowStepRunReport(
                position=s.position, kind=s.kind, ref_id=s.ref_id, name=s.name
            )
            for s in wf.steps
        ],
    )

    with telemetry.tracer.start_as_current_span(
        "dlt_worker.workflow.run",
        attributes={
            telemetry.ATTR_WORKFLOW_ID: wf.id,
            telemetry.ATTR_WORKFLOW_NAME: wf.name,
            telemetry.ATTR_TRIGGER: trigger,
            telemetry.ATTR_RUN_ID: run_id,
        },
    ) as span:
        _send_workflow_report(client, report)

        for step, step_run in zip(wf.steps, report.step_runs, strict=True):
            if _shutdown:
                report.status = "failed"
                report.error_message = (
                    f"worker shut down before step {step.position + 1} "
                    f"({step.kind} {step.name!r}) ran"
                )
                break

            step_run.status = "running"
            step_run.started_at = _iso_now()
            _send_workflow_report(client, report)

            with telemetry.tracer.start_as_current_span(
                "dlt_worker.workflow.step",
                attributes={
                    telemetry.ATTR_WORKFLOW_ID: wf.id,
                    telemetry.ATTR_STEP_POSITION: step.position,
                    telemetry.ATTR_STEP_KIND: step.kind,
                },
            ) as step_span:
                try:
                    _run_workflow_step(
                        step, step_run, tick, transformations_by_id, client
                    )
                except Exception as exc:
                    # Type only, never the message (it can quote anything).
                    step_run.status = "failed"
                    step_run.error_message = f"internal error ({type(exc).__name__})"
                    logger.exception(
                        "Workflow %s: step %d crashed", wf.name, step.position + 1
                    )
                step_span.set_attribute(telemetry.ATTR_STATUS, step_run.status)
                if step_run.status != "success":
                    step_span.set_status(telemetry.error_status(step_run.error_message))
            step_run.completed_at = step_run.completed_at or _iso_now()

            if step_run.status != "success":
                report.status = "failed"
                report.error_message = (
                    f"step {step.position + 1} ({step.kind} {step.name!r}) "
                    f"failed: {step_run.error_message or 'no error message'}"
                )
                break
            _send_workflow_report(client, report)
        else:
            report.status = "success"

        # Whatever did not get to run is skipped — the first failure (or a
        # shutdown) stops the workflow.
        for step_run in report.step_runs:
            if step_run.status == "pending":
                step_run.status = "skipped"
        report.completed_at = _iso_now()

        span.set_attribute(telemetry.ATTR_STATUS, report.status)
        if report.status != "success":
            span.set_status(telemetry.error_status(report.error_message))
        attrs = {
            telemetry.ATTR_WORKFLOW_NAME: wf.name,
            telemetry.ATTR_STATUS: report.status,
            telemetry.ATTR_TRIGGER: trigger,
        }
        telemetry.workflow_runs.add(1, attrs)
        telemetry.workflow_run_duration.record(time.monotonic() - started_mono, attrs)

        if not _send_workflow_report(client, report):
            _unsent_workflow_reports[wf.id] = (report, 1)
            logger.error(
                "Failed to report the outcome of workflow run %s; retrying next tick",
                run_id,
            )

    logger.info("Workflow %s: %s", wf.name, report.status)
    return report


def _fail_step(step_run: WorkflowStepRunReport, message: str) -> None:
    step_run.status = "failed"
    step_run.error_message = message


def _reuse_outcome(
    step: WorkflowStepConfig, step_run: WorkflowStepRunReport, prior: _RunOutcome
) -> None:
    """Satisfy a step with a run its target already made this tick.

    The target ran minutes ago by its own schedule, trigger or another
    workflow; running it again would load (or build) the same thing twice.
    A failure is taken over too — that run already exhausted its retries.
    """
    telemetry.add_event(
        "workflow.step_reused_run",
        {
            telemetry.ATTR_STEP_POSITION: step.position,
            telemetry.ATTR_STEP_KIND: step.kind,
            telemetry.ATTR_RUN_ID: prior.run_id,
        },
    )
    logger.info(
        "Workflow step %d (%s %s) already ran this tick (run %s, %s) — taking that run",
        step.position + 1,
        step.kind,
        step.name,
        prior.run_id,
        prior.status,
    )
    if step.kind == "pipeline":
        step_run.pipeline_run_id = prior.run_id
    else:
        step_run.transformation_run_id = prior.run_id
    step_run.status = "success" if prior.status == "success" else "failed"
    if prior.status != "success":
        step_run.error_message = (
            f"its run earlier in this tick failed: {prior.error_message}"
        )
    step_run.started_at = prior.started_at or step_run.started_at
    step_run.completed_at = prior.completed_at


def _run_workflow_step(
    step: WorkflowStepConfig,
    step_run: WorkflowStepRunReport,
    tick: _Tick,
    transformations_by_id: dict[str, TransformationConfig],
    client: APIClient,
) -> None:
    """Run one step and leave its outcome on ``step_run``."""
    if step.kind == "pipeline":
        _run_pipeline_step(step, step_run, tick, client)
    elif step.kind == "transformation":
        _run_transformation_step(step, step_run, tick, transformations_by_id, client)
    else:
        _fail_step(step_run, f"unknown step kind {step.kind!r}")


def _run_pipeline_step(
    step: WorkflowStepConfig,
    step_run: WorkflowStepRunReport,
    tick: _Tick,
    client: APIClient,
) -> None:
    if not step.ref_enabled:
        _fail_step(step_run, "pipeline is disabled")
        return
    prior = tick.pipeline_runs.get(step.ref_id)
    if prior is not None:
        _reuse_outcome(step, step_run, prior)
        return
    found = tick.pipelines.get(step.ref_id)
    if found is None:
        _fail_step(
            step_run,
            "pipeline not found: no definition in the pipelines checkout "
            "(not mirrored yet, deleted, or its file does not load)",
        )
        return
    if not found.enabled:
        _fail_step(step_run, "pipeline is disabled (in its definition file)")
        return
    # A copy: this tick's config object carries the pipeline's own trigger
    # fields, and a step is not the pipeline's Run-now.
    cfg = replace(found, trigger_now=False, pending_run_id="")
    reason = _resolve_credentials(cfg, tick.polled)
    if reason is not None:
        _fail_step(step_run, f"pipeline cannot run: {_CREDENTIAL_SKIP_REASONS[reason]}")
        return

    started = datetime.now(UTC)
    report, run_id = _execute_pipeline(cfg, started, client, trigger="workflow")
    _record_pipeline_outcome(cfg, report, run_id, started, tick)
    step_run.pipeline_run_id = run_id
    if report is not None and report.status == "success":
        step_run.status = "success"
    else:
        _fail_step(step_run, report.error_message if report is not None else "")
    if report is not None:
        step_run.completed_at = report.completed_at


def _run_transformation_step(
    step: WorkflowStepConfig,
    step_run: WorkflowStepRunReport,
    tick: _Tick,
    transformations_by_id: dict[str, TransformationConfig],
    client: APIClient,
) -> None:
    if not step.ref_enabled:
        _fail_step(step_run, "transformation is disabled")
        return
    prior = tick.transformation_runs.get(step.ref_id)
    if prior is not None:
        _reuse_outcome(step, step_run, prior)
        return
    cfg = transformations_by_id.get(step.ref_id)
    if cfg is None:
        _fail_step(
            step_run,
            "transformation not found in this tick's transformation configs",
        )
        return

    # The config as served, pending_run_id included: if the transformation
    # also has a Run-now queued, this run is that run — one run satisfies
    # both, and the transformation phase after this one skips it.
    report = _execute_transformation(cfg, datetime.now(UTC), client, "workflow")
    tick.transformation_runs[cfg.id] = _transformation_outcome(report)
    step_run.transformation_run_id = report.run_id
    if report.status == "success":
        step_run.status = "success"
    else:
        _fail_step(step_run, report.error_message)
    step_run.completed_at = report.completed_at


# Fields a retry's config re-read must NOT take from the checkout: the
# run's identity belongs to the tick that started it. `id` is the key the
# fresh copy was found by; the trigger fields were consumed when the run
# was admitted (re-reading them could re-arm a Run-now that already
# fired); `last_run_at` is the scheduler's, not the file's.
_RUN_IDENTITY_FIELDS = frozenset({"id", "trigger_now", "pending_run_id", "last_run_at"})


def _reload_definition(pipeline_id: str) -> PipelineConfig | None:
    """The checkout's *current* definition for one pipeline, or None.

    The filename is cosmetic, so there is no single file to stat — the
    whole set is reloaded and matched on `id`, exactly as a tick does.
    """
    files = load_pipeline_configs(config.PIPELINES_DIR, config.AGE_KEY_FILE)
    for cfg in files.configs:
        if cfg.id == pipeline_id:
            return cfg
    return None


def _repoll_credentials(cfg: PipelineConfig, client: APIClient) -> dict[str, Any]:
    """Re-fetch this pipeline's credentials from the control plane.

    Falls back to what the run started with: neither an unreachable API
    nor a poll that no longer lists the pipeline is evidence that the
    credentials changed, and blanking them would guarantee the next
    attempt fails.
    """
    polled = client.try_get_pipeline_triggers()
    if polled is None:
        return cfg.source_credentials
    for p in polled:
        if p.id == cfg.id:
            _creds_cache[p.id] = p.source_credentials
            return p.source_credentials
    return cfg.source_credentials


def _refresh_config(cfg: PipelineConfig, client: APIClient) -> bool:
    """Re-read one pipeline's definition and credentials before a retry.

    By the time a retry fires, the snapshot the run started from is a poll
    interval plus a backoff old, and the pipelines checkout syncs on its
    own ~1-minute cadence. So a save made moments before the run — a fixed
    dataset name, a reconnected OAuth credential — lands on disk *while*
    the attempts are still failing against the stale copy. Re-reading
    turns that into a self-healing second attempt instead of a run that
    fails every attempt against a problem already fixed. Two real runs on
    2026-08-22 lost exactly that race.

    Mutates ``cfg`` in place so the run keeps one config object end to end
    — the caller's span attributes and metrics then describe what actually
    ran. Returns True when anything changed.

    Never fails the run: any problem re-reading leaves ``cfg`` untouched,
    which is precisely the behaviour without this. A definition that has
    vanished from the checkout is one such case — the run in flight belongs
    to the tick that started it, and abandoning it mid-retry would leave it
    unreported.
    """
    try:
        fresh = _reload_definition(cfg.id)
        if fresh is None:
            return False
        if not fresh.has_file_credentials:
            # Same precedence as a tick: git truth wins outright, and only
            # a config without an .age file asks the poll.
            fresh.source_credentials = _repoll_credentials(cfg, client)

        changed = [
            f.name
            for f in fields(PipelineConfig)
            if f.name not in _RUN_IDENTITY_FIELDS
            and getattr(fresh, f.name) != getattr(cfg, f.name)
        ]
        if not changed:
            return False
        for name in changed:
            setattr(cfg, name, getattr(fresh, name))
    except Exception:
        logger.warning(
            "Pipeline %s: config re-read before retry failed — retrying with "
            "the run-start snapshot",
            cfg.name,
            exc_info=True,
        )
        return False

    # Field NAMES only: "source_credentials changed" is the whole signal
    # and carries nothing secret — the values never leave this process.
    telemetry.add_event(
        "pipeline.config_refreshed",
        {
            telemetry.ATTR_PIPELINE_ID: cfg.id,
            "dlt_worker.config.changed_fields": ",".join(changed),
        },
    )
    logger.info(
        "Pipeline %s: re-read before retry picked up a newer definition (%s)",
        cfg.name,
        ", ".join(changed),
    )
    return True


def _run_with_retry(
    cfg: PipelineConfig,
    local_run_id: str,
    client: APIClient,
    run_started_at: datetime | None = None,
) -> PipelineRunReport | None:
    """Run a pipeline with exponential-backoff retries on failure.

    Every retry re-reads the definition and credentials first
    (``_refresh_config``), so a fix that lands mid-run is picked up rather
    than slept through.

    ``run_started_at``, when given, replaces each attempt's own start on the
    report: the run row is one identity across attempts, and the local end
    write never touches started_at, so the central report has to carry the
    run's start too for the two stores to agree.

    Returns the final run report."""
    max_attempts = config.PIPELINE_MAX_RETRIES + 1
    report: PipelineRunReport | None = None

    for attempt in range(max_attempts):
        with telemetry.tracer.start_as_current_span(
            "dlt_worker.pipeline.attempt",
            attributes={
                telemetry.ATTR_PIPELINE_NAME: cfg.name,
                telemetry.ATTR_ATTEMPT: attempt + 1,
            },
        ) as attempt_span:
            report = run_pipeline_isolated(cfg)
            attempt_span.set_attribute(telemetry.ATTR_STATUS, report.status)
            if report.status != "success":
                attempt_span.set_status(telemetry.error_status(report.error_message))
        telemetry.pipeline_attempts.add(
            1,
            {
                telemetry.ATTR_PIPELINE_NAME: cfg.name,
                telemetry.ATTR_SOURCE_TYPE: cfg.source_type,
                telemetry.ATTR_STATUS: report.status,
            },
        )

        # Always the local id, not just a triggered run's: the run has ONE
        # identity, and the report is an upsert on it. Reporting without an
        # id lets the API mint a second one, which shows the customer two
        # rows for one run wherever the two stores are the same database
        # (a box serving its own workspace plane).
        report.run_id = local_run_id
        if run_started_at is not None:
            report.started_at = run_started_at.isoformat()

        if report.status == "success" or attempt == max_attempts - 1:
            # Success, or final attempt — record and return.
            _finalize_pipeline_run(report, local_run_id, client)
            return report

        # Intermediate failure — log and wait before retrying.
        delay = config.PIPELINE_RETRY_BASE_DELAY * (2**attempt)
        telemetry.add_event(
            "pipeline.retry",
            {
                telemetry.ATTR_ATTEMPT: attempt + 1,
                "dlt_worker.run.retry_delay_seconds": delay,
            },
        )
        logger.warning(
            "Pipeline %s failed (attempt %d/%d), retrying in %ds: %s",
            cfg.name,
            attempt + 1,
            max_attempts,
            delay,
            report.error_message,
        )

        for _ in range(delay):
            if _shutdown:
                # Shutting down — record the last failure and bail out.
                _finalize_pipeline_run(report, local_run_id, client)
                return report
            time.sleep(1)

        # After the wait, not before it: the backoff is exactly the window
        # in which a just-saved fix reaches the checkout, so re-reading on
        # the far side is what makes the next attempt a different attempt.
        _refresh_config(cfg, client)

    return report


def _finalize_pipeline_run(
    report: PipelineRunReport, local_run_id: str, client: APIClient
) -> None:
    """Record the finished run locally first, then report centrally."""
    if _recorder:
        _recorder.record_pipeline_run_end(local_run_id, report)
    _report_run_central(
        lambda: client.report_pipeline_run(report),
        f"pipeline {report.pipeline_id} (run_id={report.run_id})",
    )


def _report_run_central(send: Callable[[], bool], desc: str) -> None:
    """Report a finished run to the FairTier API, best-effort.

    With local-first recording active the local row is already the
    record, so a central failure only costs Console freshness: retry a
    bounded number of times with short pauses, then log-and-continue —
    never block the run loop for long, never fail the run. Without the
    recorder this stays a single attempt (legacy behavior).
    """
    attempts = _CENTRAL_REPORT_ATTEMPTS if _recorder else 1
    for attempt in range(attempts):
        if send():
            return
        if _shutdown or attempt + 1 == attempts:
            break
        for _ in range(_CENTRAL_REPORT_RETRY_DELAY):
            if _shutdown:
                break
            time.sleep(1)
    telemetry.add_event("central.report_failed", {"dlt_worker.attempts": attempts})
    logger.error("Failed to report final result for %s", desc)
