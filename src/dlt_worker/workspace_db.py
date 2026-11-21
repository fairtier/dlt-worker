"""Local-first run recording into a workspace database.

Run history must survive the FairTier API being unreachable: every run is
recorded in the local Postgres (``WORKSPACE_DB_URL``) first, and the
report to the API becomes best-effort.

Which database that is, is a deployment choice. Where the API is remote it
is a standalone store and the reported row is what the Console reads. Where
the API is local it can be the API's own database, and then both writes land
on ONE row — the run is recorded under an id, and the report carries that
same id for the API to upsert on.

The schema is owned by the box deployment (its migrations job); the worker
never runs migrations and writes with explicit column lists only, so
additive schema changes cannot break an older worker. Every write is
fail-safe: a database error is logged loudly and swallowed — local
recording must never skip or fail a run (degraded = central-only
reporting, the pre-0.4.0 behavior). ``WORKSPACE_DB_URL`` unset = feature
off entirely.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, LiteralString

import psycopg
from opentelemetry.trace import SpanKind

from dlt_worker import config, telemetry
from dlt_worker.api_client import (
    PipelineRunReport,
    TransformationRunReport,
    WorkflowRunReport,
    WorkflowStepRunReport,
)

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT_SECONDS = 10


def from_env() -> WorkspaceRecorder | None:
    """Build a recorder from config, or None when the feature is off."""
    if not config.WORKSPACE_DB_URL:
        return None
    return WorkspaceRecorder(config.WORKSPACE_DB_URL)


def _timestamp(value: str) -> datetime | None:
    """Parse a report's ISO timestamp; empty string means NULL."""
    if not value:
        return None
    return datetime.fromisoformat(value)


class WorkspaceRecorder:
    """Writes run rows to the workspace database, one connection per write.

    Runs are minutes apart at most, so a short-lived connection per
    statement beats keeping (and re-establishing) a long-lived one.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def _execute(
        self,
        query: LiteralString,
        params: tuple[Any, ...],
        desc: str,
        operation: str,
    ) -> int | None:
        """Run one statement; returns the affected row count, None on error.

        ``desc`` identifies the individual write for the log (it carries the
        run id); ``operation`` is its low-cardinality kind, for the span and
        the metric.
        """
        with telemetry.tracer.start_as_current_span(
            "dlt_worker.workspace_db.write",
            kind=SpanKind.CLIENT,
            attributes={
                "db.system": "postgresql",
                telemetry.ATTR_DB_OPERATION: operation,
            },
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            try:
                with psycopg.connect(
                    self._dsn,
                    autocommit=True,
                    connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                ) as conn:
                    rowcount = conn.execute(query, params).rowcount
            except Exception as exc:
                # Type only: a psycopg error quotes the failing statement,
                # and these statements carry run parameters.
                span.set_status(telemetry.error_status(type(exc).__name__))
                telemetry.workspace_db_operations.add(
                    1,
                    {
                        telemetry.ATTR_DB_OPERATION: operation,
                        telemetry.ATTR_OUTCOME: "error",
                    },
                )
                logger.exception(
                    "Workspace DB write failed (%s) — this run is only recorded"
                    " centrally",
                    desc,
                )
                return None
            telemetry.workspace_db_operations.add(
                1,
                {
                    telemetry.ATTR_DB_OPERATION: operation,
                    telemetry.ATTR_OUTCOME: "ok",
                },
            )
            return rowcount

    def record_pipeline_run_start(
        self, run_id: str, pipeline_id: str, started_at: datetime
    ) -> None:
        # Upsert: a Run-now id retried after a crash reuses its row — reset
        # the outcome columns so the row reads as a fresh attempt.
        #
        # Reset to the column DEFAULT, not NULL: rows_loaded and
        # error_message are NOT NULL in both schemas, so writing NULL fails
        # the whole statement with SQLSTATE 23502 — and _execute swallows
        # it, so the run would silently go unrecorded. completed_at is
        # nullable and genuinely means "not finished", so it stays NULL.
        self._execute(
            """
            INSERT INTO pipeline_runs (id, pipeline_id, status, started_at)
            VALUES (%s, %s, 'running', %s)
            ON CONFLICT (id) DO UPDATE SET
                status = 'running',
                started_at = EXCLUDED.started_at,
                completed_at = NULL,
                rows_loaded = DEFAULT,
                error_message = DEFAULT
            """,
            (run_id, pipeline_id, started_at),
            desc=f"pipeline run start {run_id}",
            operation="pipeline_run_start",
        )

    def record_pipeline_run_end(self, run_id: str, report: PipelineRunReport) -> None:
        # Upsert, not UPDATE: if the start write failed (db blip), the end
        # write alone still leaves a complete row behind.
        self._execute(
            """
            INSERT INTO pipeline_runs
                (id, pipeline_id, status, started_at, completed_at,
                 rows_loaded, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                status = EXCLUDED.status,
                completed_at = EXCLUDED.completed_at,
                rows_loaded = EXCLUDED.rows_loaded,
                error_message = EXCLUDED.error_message
            """,
            (
                run_id,
                report.pipeline_id,
                report.status,
                _timestamp(report.started_at),
                _timestamp(report.completed_at),
                report.rows_loaded,
                report.error_message,
            ),
            desc=f"pipeline run end {run_id}",
            operation="pipeline_run_end",
        )

    def record_transformation_run_start(
        self, run_id: str, transformation_id: str, started_at: datetime
    ) -> None:
        self._execute(
            """
            INSERT INTO transformation_runs
                (id, transformation_id, status, started_at)
            VALUES (%s, %s, 'running', %s)
            ON CONFLICT (id) DO UPDATE SET
                status = 'running',
                started_at = EXCLUDED.started_at,
                completed_at = NULL,
                commit_sha = DEFAULT,
                models_total = DEFAULT,
                models_failed = DEFAULT,
                tests_total = DEFAULT,
                tests_failed = DEFAULT,
                model_results = DEFAULT,
                error_message = DEFAULT
            """,
            (run_id, transformation_id, started_at),
            desc=f"transformation run start {run_id}",
            operation="transformation_run_start",
        )

    def record_transformation_run_end(
        self, run_id: str, report: TransformationRunReport
    ) -> None:
        # model_results is a JSON string already. NULLIF keeps an empty
        # report out of the jsonb cast, and COALESCE turns that back into
        # an empty array — the column is NOT NULL, and a run that produced
        # no per-node results (a clone or profile failure, where the report
        # defaults to "") is exactly when this path is taken.
        self._execute(
            """
            INSERT INTO transformation_runs
                (id, transformation_id, status, started_at, completed_at,
                 commit_sha, models_total, models_failed, tests_total,
                 tests_failed, model_results, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    COALESCE(NULLIF(%s, '')::jsonb, '[]'::jsonb), %s)
            ON CONFLICT (id) DO UPDATE SET
                status = EXCLUDED.status,
                completed_at = EXCLUDED.completed_at,
                commit_sha = EXCLUDED.commit_sha,
                models_total = EXCLUDED.models_total,
                models_failed = EXCLUDED.models_failed,
                tests_total = EXCLUDED.tests_total,
                tests_failed = EXCLUDED.tests_failed,
                model_results = EXCLUDED.model_results,
                error_message = EXCLUDED.error_message
            """,
            (
                run_id,
                report.transformation_id,
                report.status,
                _timestamp(report.started_at),
                _timestamp(report.completed_at),
                report.commit_sha,
                report.models_total,
                report.models_failed,
                report.tests_total,
                report.tests_failed,
                report.model_results,
                report.error_message,
            ),
            desc=f"transformation run end {run_id}",
            operation="transformation_run_end",
        )

    def finalize_stale_runs(self) -> None:
        """Fail orphaned ``running`` rows older than 2 hours (periodic).

        The central stuck-run sweep cannot see this database, so the worker
        finalizes its own orphans: all of them at startup
        (``claim_orphaned_runs``, no age margin — nothing can be in flight
        then), and these, older than 2 hours, between ticks — a backstop for
        a startup claim that could not reach the database.

        The 2-hour age is NOT a bound on how long a run may take: a pipeline
        run row stays ``running`` across all its attempts, each up to
        ``PIPELINE_RUN_TIMEOUT_SECONDS`` (6 h by default), so a live row can
        be far older. What keeps the sweep off live runs is that no live run
        can exist when it fires:

        * this worker runs everything synchronously inside a poll tick, and
          the sweep runs only between ticks (``main.run``);
        * there is one worker process per workspace database — the chart is
          a single-replica StatefulSet, so a new pod starts only after the
          old one is gone, and a run's subprocess (its own session, but the
          same container PID namespace, with the worker as PID 1) dies with
          the container that started it.

        So every ``running`` row the sweep sees is an orphan, and the age is
        only a margin against those invariants being broken (a second
        worker pointed at the same database, a force-deleted pod). If they
        are, a live row older than 2 hours reads ``failed`` until its run's
        end write — an upsert of the real status — overwrites it.
        """
        # Two literal statements (not an f-string over the table name) so
        # the SQL stays statically checkable.
        sweeps: tuple[tuple[str, LiteralString], ...] = (
            (
                "pipeline_runs",
                """
                UPDATE pipeline_runs
                   SET status = 'failed',
                       completed_at = now(),
                       error_message = 'worker restarted mid-run'
                 WHERE status = 'running'
                   AND started_at < now() - interval '2 hours'
                """,
            ),
            (
                "transformation_runs",
                """
                UPDATE transformation_runs
                   SET status = 'failed',
                       completed_at = now(),
                       error_message = 'worker restarted mid-run'
                 WHERE status = 'running'
                   AND started_at < now() - interval '2 hours'
                """,
            ),
        )
        for table, query in sweeps:
            count = self._execute(
                query,
                (),
                desc=f"stale-run sweep ({table})",
                operation="stale_run_sweep",
            )
            if count:
                logger.warning(
                    "Finalized %d orphaned %s row(s) from a previous worker",
                    count,
                    table,
                )

    def claim_orphaned_runs(
        self,
    ) -> tuple[list[PipelineRunReport], list[TransformationRunReport]]:
        """At startup: fail EVERY ``running`` pipeline/transformation row,
        and return each as the final report that says so.

        Startup only (``main._finalize_orphaned_runs``). No age margin, unlike
        ``finalize_stale_runs``: a worker that has just started has nothing
        in flight, and by the invariants spelled out there (one worker per
        database, runs execute synchronously inside a tick, a run's
        subprocess dies with the container) neither has anyone else, so
        every ``running`` row is an orphan of the previous worker. Leaving
        it ``running`` for the 2 h margin is not harmless: the API's
        ``TriggerPipeline`` answers Run now with the oldest pending-or-
        running run, so a dead row swallows every Run now until it is
        failed, and the Console shows the dead run as running.

        One ``UPDATE ... RETURNING`` per table, so the claim is atomic: a
        row the API's stuck-run sweep failed first is no longer ``running``
        and is not returned (no second report, so no second notification),
        and the API's sweep cannot fail a row claimed here. The SQL is the
        record; the returned reports exist so the caller sends them through
        ``ReportPipelineRun``/``ReportTransformationRun`` — for the central
        row (where this database is a standalone store) and the API's usual
        failure notification. They carry the row's own columns back,
        because the API's update overwrites them.

        A run subprocess that survived its worker is impossible on a box
        (it dies with the container), so it is not coded around. Were the
        invariants broken anyway, the live run's end write — an upsert of
        the real status — overwrites ``failed``, as with the periodic sweep.

        A database error is logged and swallowed: ``([], [])``, and the
        periodic sweep fails the rows once they are 2 hours old.
        """
        try:
            with psycopg.connect(
                self._dsn, autocommit=True, connect_timeout=_CONNECT_TIMEOUT_SECONDS
            ) as conn:
                pipeline_rows = conn.execute(
                    """
                    UPDATE pipeline_runs
                       SET status = 'failed',
                           completed_at = now(),
                           error_message = 'worker restarted mid-run'
                     WHERE status = 'running'
                    RETURNING id::text, pipeline_id::text, started_at,
                              completed_at, rows_loaded
                    """,
                    (),
                ).fetchall()
                transformation_rows = conn.execute(
                    """
                    UPDATE transformation_runs
                       SET status = 'failed',
                           completed_at = now(),
                           error_message = 'worker restarted mid-run'
                     WHERE status = 'running'
                    RETURNING id::text, transformation_id::text, started_at,
                              completed_at, commit_sha, models_total,
                              models_failed, tests_total, tests_failed,
                              model_results::text
                    """,
                    (),
                ).fetchall()
        except Exception:
            logger.warning(
                "Workspace DB: could not fail the runs a previous worker left "
                "running — the periodic stale-run sweep will, after 2 hours",
                exc_info=True,
            )
            return [], []
        pipelines = [
            PipelineRunReport(
                pipeline_id=pipeline_id,
                status="failed",
                started_at=_iso(started_at),
                completed_at=_iso(completed_at),
                rows_loaded=rows_loaded,
                error_message=_ORPHANED,
                run_id=run_id,
            )
            for run_id, pipeline_id, started_at, completed_at, rows_loaded in (
                pipeline_rows
            )
        ]
        transformations = [
            TransformationRunReport(
                transformation_id=row[1],
                status="failed",
                started_at=_iso(row[2]),
                completed_at=_iso(row[3]),
                commit_sha=row[4],
                models_total=row[5],
                models_failed=row[6],
                tests_total=row[7],
                tests_failed=row[8],
                model_results=row[9],
                error_message=_ORPHANED,
                run_id=row[0],
            )
            for row in transformation_rows
        ]
        for table, count in (
            ("pipeline_runs", len(pipelines)),
            ("transformation_runs", len(transformations)),
        ):
            if count:
                logger.warning(
                    "Failed %d %s row(s) a previous worker left running",
                    count,
                    table,
                )
        return pipelines, transformations

    def _workflow_tables_present(self) -> bool:
        """Whether this database has the workflow tables at all.

        They exist only where WORKSPACE_DB_URL is the workspace-api's own
        database (and only from its 000006 migration on); a standalone store
        has pipeline_runs/transformation_runs and nothing else. Asking first
        keeps the startup orphan check from logging a failed SELECT.
        """
        try:
            with psycopg.connect(
                self._dsn, autocommit=True, connect_timeout=_CONNECT_TIMEOUT_SECONDS
            ) as conn:
                row = conn.execute(
                    "SELECT to_regclass('workflow_runs') IS NOT NULL"
                    " AND to_regclass('workflow_step_runs') IS NOT NULL",
                    (),
                ).fetchone()
        except Exception:
            logger.warning(
                "Workspace DB: could not check for workflow tables — leaving "
                "orphaned workflow runs to the API's stuck-run sweep",
                exc_info=True,
            )
            return False
        return bool(row and row[0])

    def orphaned_workflow_runs(self) -> list[WorkflowRunReport]:
        """Every ``running`` workflow run, as the report that fails it.

        For startup only (``main._finalize_orphaned_workflow_runs`` sends
        them through ``ReportWorkflowRun``, so the API's failed transition
        raises the notification, once). Workflow runs are reported to the
        API only, but where this database is the API's own the rows are
        here; a worker that has just started has no workflow in flight, and
        by the invariants of ``finalize_stale_runs`` no other worker does,
        so every ``running`` row is an orphan — no age margin, which is the
        point: the run is failed (and its workflow served again) right
        after the restart, not hours later. The running step fails, steps
        that never ran are skipped, finished ones are reported as they
        were. No workflow tables, or no database: none (the API's own
        stuck-run sweep covers those deployments).
        """
        if not self._workflow_tables_present():
            return []
        try:
            with psycopg.connect(
                self._dsn, autocommit=True, connect_timeout=_CONNECT_TIMEOUT_SECONDS
            ) as conn:
                runs = conn.execute(
                    """
                    SELECT id::text, workflow_id::text, trigger, started_at
                      FROM workflow_runs
                     WHERE status = 'running'
                     ORDER BY created_at
                    """,
                    (),
                ).fetchall()
                if not runs:
                    return []
                steps = conn.execute(
                    """
                    SELECT workflow_run_id::text, position, kind, ref_id::text,
                           name, status,
                           COALESCE(pipeline_run_id::text, ''),
                           COALESCE(transformation_run_id::text, ''),
                           started_at, completed_at, error_message
                      FROM workflow_step_runs
                     WHERE workflow_run_id = ANY(%s::uuid[])
                     ORDER BY workflow_run_id, position
                    """,
                    ([row[0] for row in runs],),
                ).fetchall()
        except Exception:
            logger.warning(
                "Workspace DB: could not read running workflow runs — leaving "
                "them to the API's stuck-run sweep",
                exc_info=True,
            )
            return []
        now = datetime.now(UTC)
        return [
            _orphan_failure(run, [st for st in steps if st[0] == run[0]], now)
            for run in runs
        ]


_ORPHANED = "worker restarted mid-run"


def _iso(value: datetime | None) -> str:
    return value.astimezone(UTC).isoformat() if value is not None else ""


def _orphan_failure(
    run: tuple[Any, ...], steps: list[tuple[Any, ...]], now: datetime
) -> WorkflowRunReport:
    """The failure report of one orphaned workflow run (rows as selected in
    ``orphaned_workflow_runs``)."""
    run_id, workflow_id, trigger, started_at = run
    step_runs: list[WorkflowStepRunReport] = []
    interrupted: WorkflowStepRunReport | None = None
    for _, position, kind, ref_id, name, status, p_run, t_run, s_at, c_at, err in steps:
        step = WorkflowStepRunReport(
            position=position,
            kind=kind,
            ref_id=ref_id,
            name=name,
            status=status,
            pipeline_run_id=p_run,
            transformation_run_id=t_run,
            started_at=_iso(s_at),
            completed_at=_iso(c_at),
            error_message=err,
        )
        if status == "running":
            step.status = "failed"
            step.completed_at = now.isoformat()
            step.error_message = f"{_ORPHANED} (the worker stopped while this step ran)"
            interrupted = interrupted or step
        elif status == "pending":
            step.status = "skipped"
        step_runs.append(step)
    if interrupted is not None:
        message = (
            f"step {interrupted.position + 1} ({interrupted.kind} "
            f"{interrupted.name!r}) failed: {_ORPHANED}"
        )
    else:
        message = f"{_ORPHANED}, between steps"
    return WorkflowRunReport(
        run_id=run_id,
        workflow_id=workflow_id,
        status="failed",
        trigger=trigger,
        started_at=_iso(started_at),
        completed_at=now.isoformat(),
        error_message=message,
        step_runs=step_runs,
    )
