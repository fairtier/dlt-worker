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
from datetime import datetime, timezone
from typing import Any, Callable

from croniter import croniter

from dlt_worker import config, iceberg_stream, telemetry, workspace_db
from dlt_worker.health import start_health_server
from dlt_worker.pipeline_files import load_pipeline_configs
from dlt_worker.run_isolation import run_pipeline_isolated, run_transformation_isolated
from dlt_worker.scheduler_state import SchedulerState
from dlt_worker.snapshot import trigger_snapshot
from dlt_worker.api_client import (
    PipelineConfig,
    PipelineRunReport,
    PipelineTrigger,
    TransformationConfig,
    APIClient,
)

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

        # Sleep in small increments to respond to shutdown quickly.
        for _ in range(config.POLL_INTERVAL_SECONDS):
            if _shutdown:
                break
            time.sleep(1)

    telemetry.flush()
    logger.info("dlt-worker shut down cleanly")


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
    succeeded_pipelines = _run_due_pipelines(client)
    _run_due_transformations(client, succeeded_pipelines)


def _execute_pipeline(
    cfg: PipelineConfig, now: datetime, client: APIClient
) -> PipelineRunReport | None:
    """Run one due pipeline and return its final report."""
    logger.info("Running pipeline: %s (%s)", cfg.name, cfg.source_type)

    # Local-first: the run exists in the workspace database before it
    # starts — under the central pending id when there is one (one run,
    # one identity in both stores), else a worker-generated UUID.
    local_run_id = cfg.pending_run_id or str(uuid.uuid4())
    trigger = telemetry.trigger_kind(cfg)
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
            _recorder.record_pipeline_run_start(local_run_id, cfg.id, now)

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
                    started_at=now.isoformat(),
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

        report = _run_with_retry(cfg, local_run_id, client)
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
    return report


def _run_due_pipelines(client: APIClient) -> set[str]:
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
    """
    succeeded: set[str] = set()

    files = load_pipeline_configs(config.PIPELINES_DIR, config.AGE_KEY_FILE)
    file_ids = {c.id for c in files.configs}
    state = SchedulerState.load(config.DLT_STATE_DIR)

    polled = client.try_get_pipeline_triggers()
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

    now = datetime.now(timezone.utc)

    for cfg in files.configs:
        if _shutdown:
            break
        # One broken config must not abandon the rest of the tick.
        try:
            _process_file_pipeline(cfg, by_id, polled, state, now, client, succeeded)
        except Exception as exc:
            _tick_error(cfg.name, exc)
            logger.exception(
                "Pipeline %s: tick processing failed — continuing with the rest",
                cfg.name,
            )

    return succeeded


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


def _process_file_pipeline(
    cfg: PipelineConfig,
    by_id: dict[str, PipelineTrigger],
    polled: list[PipelineTrigger] | None,
    state: SchedulerState,
    now: datetime,
    client: APIClient,
    succeeded: set[str],
) -> None:
    """Process one pipeline within a tick (schedule, run, record)."""
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

    if cfg.has_file_credentials:
        # Decrypted from the checkout — git truth wins; no fallback.
        pass
    elif cfg.id in _creds_cache:
        # A cached {} is a valid "known credential-less" entry.
        cfg.source_credentials = _creds_cache[cfg.id]
    elif polled is None:
        _skipped(cfg, "no_credentials_central_unreachable")
        logger.warning(
            "Skipping pipeline %s: no credential file, no cached "
            "credentials, and the FairTier API is unreachable — "
            "retrying next tick",
            cfg.name,
        )
        return
    else:
        # The poll succeeded but this pipeline wasn't in the response
        # (deleted centrally, or file-mirror lag on delete). Running would
        # produce a guaranteed failed run with empty credentials — skip.
        # A genuinely credential-less pipeline is covered above: its poll
        # record caches {}.
        _skipped(cfg, "not_in_poll_response")
        logger.warning(
            "Skipping pipeline %s: no credential file and the poll "
            "response does not include it — retrying next tick",
            cfg.name,
        )
        return

    report = _execute_pipeline(cfg, now, client)
    if report is not None and report.status == "success":
        succeeded.add(cfg.id)
        # Record the run *start* time, mirroring central semantics
        # (last_run_at = created_at of the last success). Success-only:
        # a failing scheduled pipeline keeps re-firing, exactly as in
        # legacy mode.
        state.record(cfg.id, now)
        # Extra best-effort snapshot so scheduler.json lands in the
        # post-run commit instead of waiting for the autosave window
        # (a crash in between would re-fire the run once).
        trigger_snapshot(cfg.name)


def _run_due_transformations(client: APIClient, succeeded_pipelines: set[str]) -> None:
    """Run all due dbt transformations.

    A transformation is due on manual trigger, on its cron schedule, or
    chained after a pipeline that succeeded in this same iteration. Unlike
    pipelines there is no retry: dbt failures are code errors, not
    transient — report immediately.
    """
    try:
        configs = client.get_transformation_configs()
    except Exception:
        # Transformation errors must never affect pipeline processing.
        logger.warning("Failed to fetch transformation configs", exc_info=True)
        return
    if not configs:
        return

    now = datetime.now(timezone.utc)
    ran: set[str] = set()

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

            logger.info("Running transformation: %s", cfg.name)

            # Local-first: same identity rules as pipelines (central pending
            # id when triggered, else a worker-generated UUID).
            local_run_id = cfg.pending_run_id or str(uuid.uuid4())
            trigger = "manual" if cfg.trigger_now else "schedule" if due else "chained"
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
                        local_run_id, cfg.id, datetime.now(timezone.utc)
                    )

                report = run_transformation_isolated(cfg)
                # Same single-identity rule as pipelines: the report is an
                # upsert on the id the run was recorded under, so a scheduled
                # run cannot end up as two rows.
                report.run_id = local_run_id
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
                telemetry.record_transformation_run(
                    cfg, report, time.monotonic() - started
                )

                if _recorder:
                    _recorder.record_transformation_run_end(local_run_id, report)
                _report_run_central(
                    lambda report=report: client.report_transformation_run(report),
                    f"transformation {report.transformation_id} "
                    f"(run_id={report.run_id})",
                )
        except Exception as exc:
            _tick_error(cfg.name, exc)
            logger.exception(
                "Transformation %s: tick processing failed — continuing with the rest",
                cfg.name,
            )


def _run_with_retry(
    cfg: PipelineConfig, local_run_id: str, client: APIClient
) -> PipelineRunReport | None:
    """Run a pipeline with exponential-backoff retries on failure.

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
