"""Subprocess-per-run isolation for pipeline and dbt transformation runs.

The worker is a long-lived poll loop, but a run leaves a large footprint
behind: Arrow buffers, dlt schema/state caches, fsspec connection pools,
DuckDB's buffer manager and the allocator's free lists can keep ~1 GB
resident long after the run finished. Python never returns that memory to
the OS — process exit does. So each run executes in a short-lived spawned
child process that sends its report back over a pipe and exits, releasing
everything.

Both kinds of run go through here, and for the same reason. dbt is the
second one to arrive: on 2026-08-10 an in-process ``dbt build`` over an
85M-row Iceberg table left 800 MB of anonymous memory in the worker — under
its own container limit, so never OOM-killed, simply held until someone
restarted the pod. The box spent 30 minutes in reclaim thrash and lost DNS.

The child inherits the environment but not module state (spawn starts a
fresh interpreter), so it re-applies config, logging and telemetry — and,
for a pipeline, the iceberg streaming patch — before running. The config —
credentials included — travels only through the pickled process arguments
and the report only through the pipe, both in-memory channels: never argv,
env, or disk.

A child that dies without reporting (kernel OOM kill, segfault) becomes a
*failed run report* instead of a dead worker: the poll loop, health server
and scheduler state survive, and the failure lands in run history with the
exit code. Previously an OOM during a big load killed the whole worker.

Because only children run dlt and dbt, both imports are deferred to the
child: importing dlt costs ~115 MB of RSS and dbt another ~60 MB, and the
poll loop needs neither. A parent that never imports them idles at ~35 MB
instead of ~190 MB — memory that on a 4 GB box is the difference between
headroom and none. The cost is that a broken dependency surfaces as a
failed run rather than at startup.

``PIPELINE_SUBPROCESS=0`` / ``TRANSFORMATION_SUBPROCESS=0`` restore
in-process execution (the rollback levers).
"""

from __future__ import annotations

import logging
import multiprocessing
import signal
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Mapping, TypeVar

from dlt_worker import config, iceberg_stream, telemetry
from dlt_worker.api_client import (
    PipelineConfig,
    PipelineRunReport,
    SourceTest,
    SourceTestReport,
    TransformationConfig,
    TransformationRunReport,
)

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

logger = logging.getLogger(__name__)

_Report = TypeVar(
    "_Report", PipelineRunReport, TransformationRunReport, SourceTestReport
)


def _child_setup(trace_context: Mapping[str, str]) -> None:
    """Re-apply the process-wide setup a spawned child starts without."""
    config.load()
    # Same re-own-the-root-logger dance as main._configure_logging: importing
    # dlt installs a WARNING-level root handler; force=True takes it back so
    # the child's INFO logs reach the container's stdout.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    # Module state doesn't survive spawn, so telemetry is set up from
    # scratch here too; the propagated context is what keeps the child's
    # spans in the parent's trace instead of starting a second one.
    telemetry.setup("run", config.CUSTOMER_SLUG)
    telemetry.attach_trace_context(trace_context)


def _child_main(
    conn: Connection, cfg: PipelineConfig, trace_context: Mapping[str, str]
) -> None:
    """Entry point of a spawned pipeline child: set up, run, send the report."""
    _child_setup(trace_context)
    if config.ICEBERG_LOAD_CHUNK_ROWS > 0:
        iceberg_stream.apply(
            config.ICEBERG_LOAD_CHUNK_ROWS,
            config.ICEBERG_LOAD_COMMIT_EVERY,
            config.ICEBERG_CREDENTIAL_REFRESH_SECONDS,
        )
    from dlt_worker.pipeline_runner import run_pipeline

    try:
        conn.send(run_pipeline(cfg))
        conn.close()
    finally:
        # Bounded, and last: the parent is blocked on the report above, so
        # nothing about a slow or dead collector may delay it.
        telemetry.flush()


def _transformation_child_main(
    conn: Connection, cfg: TransformationConfig, trace_context: Mapping[str, str]
) -> None:
    """Entry point of a spawned dbt child: set up, run, send the report.

    No iceberg patch here — dbt reaches the warehouse through DuckDB's own
    iceberg extension, not PyIceberg.
    """
    _child_setup(trace_context)
    from dlt_worker.transformation_runner import run_transformation

    try:
        conn.send(run_transformation(cfg))
        conn.close()
    finally:
        telemetry.flush()


def _source_test_child_main(
    conn: Connection, test: SourceTest, trace_context: Mapping[str, str]
) -> None:
    """Entry point of a spawned probe child: set up, probe, send the report.

    No iceberg patch and no dlt import: a probe opens the source, reads a
    row and lands nothing.
    """
    _child_setup(trace_context)
    from dlt_worker.source_test import probe_source

    try:
        conn.send(probe_source(test))
        conn.close()
    finally:
        telemetry.flush()


def _supervise(
    target: Callable[..., None],
    cfg: Any,
    *,
    kind: str,
    label: str,
    timeout: int,
    failed: Callable[[datetime, str], _Report],
) -> _Report:
    """Run one child to completion and return its report.

    ``kind`` names the workload in messages ("pipeline"/"transformation"),
    ``label`` names the individual config. ``failed`` builds the report for
    the two ways a child can fail to produce one itself: the deadline
    expiring, and dying without sending.
    """
    started_at = datetime.now(timezone.utc)

    # spawn, not fork: forking a process that holds the health-server thread
    # and live HTTP sessions is unsafe, and a forked child would start with
    # the parent's pages — the opposite of the point.
    ctx = multiprocessing.get_context("spawn")
    recv_conn, send_conn = ctx.Pipe(duplex=False)
    proc = ctx.Process(
        target=target,
        args=(send_conn, cfg, telemetry.current_trace_context()),
        name=f"{kind}-{label}",
    )
    proc.start()
    # Drop the parent's handle on the send end so recv() sees EOF (instead
    # of blocking forever) when the child dies without sending.
    send_conn.close()

    report: _Report | None = None
    timed_out = False
    try:
        # A wall-clock deadline on the run: neither dlt sources nor a dbt
        # model's queries all enforce network timeouts, and a child wedged
        # in a read would block this recv() — and with it the whole poll
        # loop — forever.
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
        telemetry.add_event("run.timeout", {"dlt_worker.run.timeout_seconds": timeout})
        logger.error(
            "%s %s: run exceeded the %ds wall-clock limit — killing the run subprocess",
            kind.capitalize(),
            label,
            timeout,
        )
        proc.terminate()
        proc.join(30)
        if proc.is_alive():
            proc.kill()
            proc.join()
        return failed(
            started_at,
            f"{kind} run exceeded the {timeout}s wall-clock limit and was killed",
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
    # An OOM kill leaves nothing else behind — no report, no dlt log tail —
    # so the event carrying the exit code is the trace's only evidence.
    telemetry.add_event("run.subprocess_died", {"dlt_worker.run.exit_code": code or 0})
    logger.error(
        "%s %s: run subprocess died without reporting (%s)",
        kind.capitalize(),
        label,
        detail,
    )
    return failed(started_at, f"{kind} run subprocess died without reporting: {detail}")


def run_pipeline_isolated(cfg: PipelineConfig) -> PipelineRunReport:
    """Run one pipeline in a short-lived child process and return its report.

    Falls back to in-process execution when PIPELINE_SUBPROCESS is disabled.
    """
    if not config.PIPELINE_SUBPROCESS:
        from dlt_worker.pipeline_runner import run_pipeline

        return run_pipeline(cfg)

    def failed(started_at: datetime, message: str) -> PipelineRunReport:
        return PipelineRunReport(
            pipeline_id=cfg.id,
            status="failed",
            started_at=started_at.isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            error_message=message,
        )

    return _supervise(
        _child_main,
        cfg,
        kind="pipeline",
        label=cfg.name,
        timeout=config.PIPELINE_RUN_TIMEOUT_SECONDS,
        failed=failed,
    )


def run_source_test_isolated(test: SourceTest) -> SourceTestReport:
    """Probe one source in a short-lived child and return its report.

    Isolated for the usual reason (a DuckDB ATTACH or a SQLAlchemy engine
    leaves memory behind) and one of its own: a probe opens exactly the
    connections a wrong config makes hang, and the deadline here is what
    keeps a hung connect from stopping every schedule on the box.
    """
    if not config.SOURCE_TEST_SUBPROCESS:
        from dlt_worker.source_test import probe_source

        return probe_source(test)

    def failed(started_at: datetime, message: str) -> SourceTestReport:
        return SourceTestReport(id=test.id, status="failed", message=message)

    return _supervise(
        _source_test_child_main,
        test,
        kind="source test",
        label=test.id,
        timeout=config.SOURCE_TEST_TIMEOUT_SECONDS,
        failed=failed,
    )


def run_transformation_isolated(
    cfg: TransformationConfig,
) -> TransformationRunReport:
    """Run one dbt transformation in a short-lived child and return its report.

    Falls back to in-process execution when TRANSFORMATION_SUBPROCESS is
    disabled.
    """
    if not config.TRANSFORMATION_SUBPROCESS:
        from dlt_worker.transformation_runner import run_transformation

        return run_transformation(cfg)

    def failed(started_at: datetime, message: str) -> TransformationRunReport:
        return TransformationRunReport(
            transformation_id=cfg.id,
            status="failed",
            started_at=started_at.isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            error_message=message,
            run_id=cfg.pending_run_id,
        )

    return _supervise(
        _transformation_child_main,
        cfg,
        kind="transformation",
        label=cfg.name,
        timeout=config.TRANSFORMATION_RUN_TIMEOUT_SECONDS,
        failed=failed,
    )
