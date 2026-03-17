"""Main poll loop with graceful shutdown.

Polls the Platform API for pipeline configurations, evaluates cron
schedules, and runs pipelines that are due. Handles SIGTERM/SIGINT
for graceful shutdown (important in Kubernetes).
"""

from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timezone

from croniter import croniter

from dlt_worker import config
from dlt_worker.health import start_health_server
from dlt_worker.pipeline_runner import run_pipeline
from dlt_worker.platform_client import PipelineConfig, PipelineRunReport, PlatformClient

logger = logging.getLogger(__name__)

# Shutdown flag
_shutdown = False


def _handle_signal(signum: int, _frame: object) -> None:
    global _shutdown
    logger.info("Received signal %d, shutting down gracefully...", signum)
    _shutdown = True


def _should_run(cfg: PipelineConfig, now: datetime) -> bool:
    """Determine if a pipeline should run based on its cron schedule or trigger flag."""
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

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    client = PlatformClient(
        base_url=config.PLATFORM_API_URL,
        customer_slug=config.CUSTOMER_SLUG,
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


def _poll_and_run(client: PlatformClient) -> None:
    configs = client.get_pipeline_configs()
    if not configs:
        return

    now = datetime.now(timezone.utc)

    for cfg in configs:
        if _shutdown:
            break
        if not _should_run(cfg, now):
            continue

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

        _run_with_retry(cfg, run_id, client)


def _run_with_retry(cfg: PipelineConfig, run_id: str, client: PlatformClient) -> None:
    """Run a pipeline with exponential-backoff retries on failure."""
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
            return

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
                return
            time.sleep(1)
