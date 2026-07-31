"""Subprocess-per-run isolation for pipeline execution.

The worker is a long-lived poll loop, but a dlt run leaves a large footprint
behind: Arrow buffers, dlt schema/state caches, fsspec connection pools and
the allocator's free lists can keep ~1 GB resident long after the run
finished. Python never returns that memory to the OS — process exit does.
So each pipeline run executes in a short-lived spawned child process that
sends its run report back over a pipe and exits, releasing everything.

The child inherits the environment but not module state (spawn starts a
fresh interpreter), so it re-applies config, logging and the iceberg
streaming patch before running. The PipelineConfig — credentials included —
travels only through the pickled process arguments and the report only
through the pipe, both in-memory channels: never argv, env, or disk.

A child that dies without reporting (kernel OOM kill, segfault) becomes a
*failed run report* instead of a dead worker: the poll loop, health server
and scheduler state survive, and the failure lands in run history with the
exit code. Previously an OOM during a big load killed the whole worker.

``PIPELINE_SUBPROCESS=0`` restores in-process execution (the rollback
lever).
"""

from __future__ import annotations

import logging
import multiprocessing
import signal
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from dlt_worker import config, iceberg_stream
from dlt_worker.api_client import PipelineConfig, PipelineRunReport
from dlt_worker.pipeline_runner import run_pipeline

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

logger = logging.getLogger(__name__)


def _child_main(conn: Connection, cfg: PipelineConfig) -> None:
    """Entry point of the spawned child: re-apply process-wide setup, run
    the pipeline, send the report back."""
    config.load()
    # Same re-own-the-root-logger dance as main._configure_logging: importing
    # dlt installs a WARNING-level root handler; force=True takes it back so
    # the child's INFO logs reach the container's stdout.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    if config.ICEBERG_LOAD_CHUNK_ROWS > 0:
        iceberg_stream.apply(
            config.ICEBERG_LOAD_CHUNK_ROWS, config.ICEBERG_LOAD_COMMIT_EVERY
        )
    conn.send(run_pipeline(cfg))
    conn.close()


def run_pipeline_isolated(cfg: PipelineConfig) -> PipelineRunReport:
    """Run one pipeline in a short-lived child process and return its report.

    Falls back to in-process execution when PIPELINE_SUBPROCESS is disabled.
    """
    if not config.PIPELINE_SUBPROCESS:
        return run_pipeline(cfg)

    started_at = datetime.now(timezone.utc)

    # spawn, not fork: forking a process that holds the health-server thread
    # and live HTTP sessions is unsafe, and a forked child would start with
    # the parent's pages — the opposite of the point.
    ctx = multiprocessing.get_context("spawn")
    recv_conn, send_conn = ctx.Pipe(duplex=False)
    proc = ctx.Process(
        target=_child_main, args=(send_conn, cfg), name=f"run-{cfg.name}"
    )
    proc.start()
    # Drop the parent's handle on the send end so recv() sees EOF (instead
    # of blocking forever) when the child dies without sending.
    send_conn.close()

    # A wall-clock deadline on the run: dlt sources don't all enforce
    # network timeouts, and a child wedged in a read would block this
    # recv() — and with it the whole poll loop — forever.
    timeout = config.PIPELINE_RUN_TIMEOUT_SECONDS

    report: PipelineRunReport | None = None
    timed_out = False
    try:
        if timeout <= 0 or recv_conn.poll(timeout):
            report = recv_conn.recv()
        else:
            timed_out = True
    except EOFError:
        # The child died without sending — distinct from a timeout: fall
        # through to the exit-code report below. (A dead child's EOF can
        # arrive before is_alive() flips, so EOF must never be treated as
        # a deadline expiry.)
        pass
    finally:
        recv_conn.close()

    if timed_out:
        # Deadline expired with the child still running: terminate, then
        # kill if it ignores SIGTERM (e.g. stuck in uninterruptible IO).
        logger.error(
            "Pipeline %s: run exceeded the %ds wall-clock limit — killing "
            "the run subprocess",
            cfg.name,
            timeout,
        )
        proc.terminate()
        proc.join(30)
        if proc.is_alive():
            proc.kill()
            proc.join()
        return PipelineRunReport(
            pipeline_id=cfg.id,
            status="failed",
            started_at=started_at.isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            error_message=(
                f"pipeline run exceeded the {timeout}s wall-clock limit and was killed"
            ),
        )

    proc.join()

    if report is not None:
        return report

    # The child died before reporting. A negative exitcode is the signal
    # number; SIGKILL here is almost always the kernel OOM killer.
    code = proc.exitcode
    if code is not None and code < 0:
        detail = f"killed by signal {-code}"
        if -code == signal.SIGKILL:
            detail += " (SIGKILL — likely out of memory)"
    else:
        detail = f"exit code {code}"
    logger.error(
        "Pipeline %s: run subprocess died without reporting (%s)", cfg.name, detail
    )
    return PipelineRunReport(
        pipeline_id=cfg.id,
        status="failed",
        started_at=started_at.isoformat(),
        completed_at=datetime.now(timezone.utc).isoformat(),
        error_message=f"pipeline run subprocess died without reporting: {detail}",
    )
