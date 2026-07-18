"""Main poll loop with graceful shutdown.

Polls the FairTier API for pipeline and transformation configurations,
evaluates cron schedules, and runs work that is due. Handles
SIGTERM/SIGINT for graceful shutdown (important in Kubernetes).
"""

from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timezone
from typing import Any

from croniter import croniter

from dlt_worker import config
from dlt_worker.health import start_health_server
from dlt_worker.pipeline_files import load_pipeline_configs
from dlt_worker.pipeline_runner import run_pipeline, trigger_snapshot
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

# Files mode: last-known source credentials per pipeline id, refreshed on
# every successful poll. In-memory ONLY — credentials must never touch
# disk (that is Phase 3's encrypted-repo job) and must never be logged.
# Never shrunk: a pipeline briefly absent from one poll response keeps its
# last-known credentials for the next central outage.
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

    if cfg.last_run_at is None:
        return True  # never run before

    cron = croniter(cfg.schedule, cfg.last_run_at)
    next_run = cron.get_next(datetime)
    return now >= next_run


def run() -> None:
    """Main loop: poll for work, run pipelines, report results."""
    config.load()

    _configure_logging()

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

    while not _shutdown:
        try:
            _poll_and_run(client)
        except Exception:
            logger.exception("Unexpected error in poll loop")

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

    return _run_with_retry(cfg, run_id, client)


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
        if not _should_run(cfg, now):
            continue

        report = _execute_pipeline(cfg, now, client)
        if report is not None and report.status == "success":
            succeeded.add(cfg.id)

    return succeeded


def _run_due_pipelines_files(client: APIClient) -> set[str]:
    """Files mode: definitions and schedules come from the pipelines
    checkout; the poll is consumed only for Run-now triggers and source
    credentials. A central outage degrades (no history, no triggers,
    possibly no credentials) instead of stopping scheduled ingestion.
    """
    succeeded: set[str] = set()

    files = load_pipeline_configs(config.PIPELINES_DIR)
    file_ids = {c.id for c in files.configs}
    state = SchedulerState.load(config.DLT_STATE_DIR)

    polled = client.try_get_pipeline_configs()
    by_id: dict[str, PipelineConfig] = {}
    if polled is None:
        logger.warning(
            "FairTier API unreachable — scheduling from files, "
            "credentials from in-memory cache"
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
            continue

        if cfg.id in _creds_cache:
            # A cached {} is a valid "known credential-less" entry.
            cfg.source_credentials = _creds_cache[cfg.id]
        elif polled is None:
            logger.warning(
                "Skipping pipeline %s: no cached credentials and the "
                "FairTier API is unreachable — retrying next tick",
                cfg.name,
            )
            continue

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

    return succeeded


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

        chained = bool(
            cfg.enabled
            and cfg.trigger_after_pipeline_id
            and cfg.trigger_after_pipeline_id in succeeded_pipelines
        )
        if not (_should_run(cfg, now) or chained):
            continue
        ran.add(cfg.id)

        logger.info("Running transformation: %s", cfg.name)

        report = run_transformation(cfg)
        if not client.report_transformation_run(report):
            logger.error(
                "Failed to report result for transformation %s (run_id=%s)",
                cfg.id,
                report.run_id,
            )


def _run_with_retry(
    cfg: PipelineConfig, run_id: str, client: APIClient
) -> PipelineRunReport | None:
    """Run a pipeline with exponential-backoff retries on failure.

    Returns the final run report."""
    max_attempts = config.PIPELINE_MAX_RETRIES + 1
    last_report: PipelineRunReport | None = None

    for attempt in range(max_attempts):
        report = run_pipeline(cfg)
        if run_id:
            report.run_id = run_id

        if report.status == "success" or attempt == max_attempts - 1:
            # Success, or final attempt — report and return.
            if not client.report_pipeline_run(report):
                logger.error(
                    "Failed to report final result for pipeline %s (run_id=%s)",
                    cfg.id,
                    run_id,
                )
            return report

        # Intermediate failure — log and wait before retrying.
        last_report = report
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
                # Shutting down — report the last failure and bail out.
                if not client.report_pipeline_run(last_report):
                    logger.error(
                        "Failed to report result for pipeline %s (run_id=%s)",
                        cfg.id,
                        run_id,
                    )
                return last_report
            time.sleep(1)

    return last_report
