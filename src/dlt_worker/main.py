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

from dlt_worker import config, iceberg_stream, workspace_db
from dlt_worker.health import start_health_server
from dlt_worker.pipeline_files import load_pipeline_configs
from dlt_worker.pipeline_runner import trigger_snapshot
from dlt_worker.run_isolation import run_pipeline_isolated
from dlt_worker.scheduler_state import SchedulerState
from dlt_worker.transformation_runner import run_transformation
from dlt_worker.api_client import (
    PipelineConfig,
    PipelineRunReport,
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
# disk unencrypted and must never be logged. Never shrunk: a pipeline
# briefly absent from one poll response keeps its last-known credentials
# for the next central outage. Fallback only: credentials decrypted from
# the checkout's .credentials.age files take precedence.
_creds_cache: dict[str, dict[str, Any]] = {}


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

    if cfg.last_run_at is None:
        return True  # never run before

    # is_valid can't catch date-arithmetic failures (croniter errors
    # subclass ValueError) — treat those as not-due too.
    try:
        next_run = croniter(cfg.schedule, cfg.last_run_at).get_next(datetime)
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

    _recorder = workspace_db.from_env()
    if _recorder:
        logger.info("Local-first run recording enabled (workspace database)")
        _recorder.finalize_stale_runs()

    if config.ICEBERG_LOAD_CHUNK_ROWS > 0:
        iceberg_stream.apply(
            config.ICEBERG_LOAD_CHUNK_ROWS, config.ICEBERG_LOAD_COMMIT_EVERY
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

    start_health_server(client, config.HEALTHZ_PORT)

    logger.info(
        "dlt-worker started for customer=%s, polling every %ds",
        config.CUSTOMER_SLUG,
        config.POLL_INTERVAL_SECONDS,
    )

    last_sweep = time.monotonic()
    while not _shutdown:
        try:
            _poll_and_run(client)
        except Exception:
            logger.exception("Unexpected error in poll loop")

        if _recorder and time.monotonic() - last_sweep >= _STALE_SWEEP_INTERVAL:
            _recorder.finalize_stale_runs()
            last_sweep = time.monotonic()

        # Sleep in small increments to respond to shutdown quickly.
        for _ in range(config.POLL_INTERVAL_SECONDS):
            if _shutdown:
                break
            time.sleep(1)

    logger.info("dlt-worker shut down cleanly")


def _poll_and_run(client: APIClient) -> None:
    succeeded_pipelines = _run_due_pipelines(client)
    _run_due_transformations(client, succeeded_pipelines)


def _run_due_pipelines(client: APIClient) -> set[str]:
    """Run all due pipelines. Returns the ids of pipelines that succeeded."""
    if config.PIPELINES_DIR:
        return _run_due_pipelines_files(client)
    return _run_due_pipelines_api(client)


def _execute_pipeline(
    cfg: PipelineConfig, now: datetime, client: APIClient
) -> PipelineRunReport | None:
    """Run one due pipeline and return its final report."""
    logger.info("Running pipeline: %s (%s)", cfg.name, cfg.source_type)

    run_id = cfg.pending_run_id

    # Local-first: the run exists in the workspace database before it
    # starts — under the central pending id when there is one (one run,
    # one identity in both stores), else a worker-generated UUID.
    local_run_id = cfg.pending_run_id or str(uuid.uuid4())
    if _recorder:
        _recorder.record_pipeline_run_start(local_run_id, cfg.id, now)

    # Mark triggered run as "running" before execution.
    # If the update fails (network blip, run already cleaned up),
    # fall back to creating a new run row.
    if run_id:
        ok = client.report_pipeline_run(
            PipelineRunReport(
                pipeline_id=cfg.id,
                status="running",
                started_at=now.isoformat(),
                completed_at="",
                run_id=run_id,
            )
        )
        if not ok:
            logger.warning(
                "Failed to mark run %s as running, will create new run", run_id
            )
            run_id = ""

    return _run_with_retry(cfg, run_id, local_run_id, client)


def _run_due_pipelines_api(client: APIClient) -> set[str]:
    """Legacy mode: the API poll is the full source of truth."""
    succeeded: set[str] = set()

    configs = client.get_pipeline_configs()
    if not configs:
        return succeeded

    now = datetime.now(timezone.utc)

    for cfg in configs:
        if _shutdown:
            break
        # One broken config must not abandon the rest of the tick.
        try:
            if not _should_run(cfg, now):
                continue

            report = _execute_pipeline(cfg, now, client)
            if report is not None and report.status == "success":
                succeeded.add(cfg.id)
        except Exception:
            logger.exception(
                "Pipeline %s: tick processing failed — continuing with the rest",
                cfg.name,
            )

    return succeeded


def _run_due_pipelines_files(client: APIClient) -> set[str]:
    """Files mode: definitions and schedules come from the pipelines
    checkout; the poll is consumed only for Run-now triggers and source
    credentials. A central outage degrades (no history, no triggers,
    possibly no credentials) instead of stopping scheduled ingestion.
    """
    succeeded: set[str] = set()

    files = load_pipeline_configs(config.PIPELINES_DIR, config.AGE_KEY_FILE)
    file_ids = {c.id for c in files.configs}
    state = SchedulerState.load(config.DLT_STATE_DIR)

    polled = client.try_get_pipeline_configs()
    by_id: dict[str, PipelineConfig] = {}
    if polled is None:
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

    now = datetime.now(timezone.utc)

    for cfg in files.configs:
        if _shutdown:
            break
        # One broken config must not abandon the rest of the tick.
        try:
            _process_file_pipeline(cfg, by_id, polled, state, now, client, succeeded)
        except Exception:
            logger.exception(
                "Pipeline %s: tick processing failed — continuing with the rest",
                cfg.name,
            )

    return succeeded


def _process_file_pipeline(
    cfg: PipelineConfig,
    by_id: dict[str, PipelineConfig],
    polled: list[PipelineConfig] | None,
    state: SchedulerState,
    now: datetime,
    client: APIClient,
    succeeded: set[str],
) -> None:
    """Process one files-mode pipeline within a tick (schedule, run, record)."""
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
        logger.warning(
            "Skipping pipeline %s: no credential file, no cached "
            "credentials, and the FairTier API is unreachable — "
            "retrying next tick",
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
            if not (_should_run(cfg, now) or chained):
                continue
            ran.add(cfg.id)

            logger.info("Running transformation: %s", cfg.name)

            # Local-first: same identity rules as pipelines (central pending
            # id when triggered, else a worker-generated UUID).
            local_run_id = cfg.pending_run_id or str(uuid.uuid4())
            if _recorder:
                _recorder.record_transformation_run_start(
                    local_run_id, cfg.id, datetime.now(timezone.utc)
                )

            report = run_transformation(cfg)

            if _recorder:
                _recorder.record_transformation_run_end(local_run_id, report)
            _report_run_central(
                lambda report=report: client.report_transformation_run(report),
                f"transformation {report.transformation_id} (run_id={report.run_id})",
            )
        except Exception:
            logger.exception(
                "Transformation %s: tick processing failed — continuing with the rest",
                cfg.name,
            )


def _run_with_retry(
    cfg: PipelineConfig, run_id: str, local_run_id: str, client: APIClient
) -> PipelineRunReport | None:
    """Run a pipeline with exponential-backoff retries on failure.

    Returns the final run report."""
    max_attempts = config.PIPELINE_MAX_RETRIES + 1
    report: PipelineRunReport | None = None

    for attempt in range(max_attempts):
        report = run_pipeline_isolated(cfg)
        if run_id:
            report.run_id = run_id

        if report.status == "success" or attempt == max_attempts - 1:
            # Success, or final attempt — record and return.
            _finalize_pipeline_run(report, local_run_id, client)
            return report

        # Intermediate failure — log and wait before retrying.
        delay = config.PIPELINE_RETRY_BASE_DELAY * (2**attempt)
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
    logger.error("Failed to report final result for %s", desc)
