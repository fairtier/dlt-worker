"""Local-first run recording into the box-local ``workspace`` database.

Run history must survive the FairTier API being unreachable: every run is
recorded in the box's own Postgres (``WORKSPACE_DB_URL``) first, and the
central report becomes best-effort — the local row is the record, the
central row is only a cache for the Console.

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
from datetime import datetime
from typing import Any, LiteralString

import psycopg

from dlt_worker import config
from dlt_worker.api_client import PipelineRunReport, TransformationRunReport

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
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class WorkspaceRecorder:
    """Writes run rows to the workspace database, one connection per write.

    Runs are minutes apart at most, so a short-lived connection per
    statement beats keeping (and re-establishing) a long-lived one.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def _execute(
        self, query: LiteralString, params: tuple[Any, ...], desc: str
    ) -> int | None:
        """Run one statement; returns the affected row count, None on error."""
        try:
            with psycopg.connect(
                self._dsn,
                autocommit=True,
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
            ) as conn:
                return conn.execute(query, params).rowcount
        except Exception:
            logger.exception(
                "Workspace DB write failed (%s) — this run is only recorded centrally",
                desc,
            )
            return None

    def record_pipeline_run_start(
        self, run_id: str, pipeline_id: str, started_at: datetime
    ) -> None:
        # Upsert: a Run-now id retried after a crash reuses its row — reset
        # the outcome columns so the row reads as a fresh attempt.
        self._execute(
            """
            INSERT INTO pipeline_runs (id, pipeline_id, status, started_at)
            VALUES (%s, %s, 'running', %s)
            ON CONFLICT (id) DO UPDATE SET
                status = 'running',
                started_at = EXCLUDED.started_at,
                completed_at = NULL,
                rows_loaded = NULL,
                error_message = NULL
            """,
            (run_id, pipeline_id, started_at),
            desc=f"pipeline run start {run_id}",
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
                commit_sha = NULL,
                models_total = NULL,
                models_failed = NULL,
                tests_total = NULL,
                tests_failed = NULL,
                model_results = NULL,
                error_message = NULL
            """,
            (run_id, transformation_id, started_at),
            desc=f"transformation run start {run_id}",
        )

    def record_transformation_run_end(
        self, run_id: str, report: TransformationRunReport
    ) -> None:
        # model_results is a JSON string already; NULLIF keeps an empty
        # report out of the jsonb column instead of failing the cast.
        self._execute(
            """
            INSERT INTO transformation_runs
                (id, transformation_id, status, started_at, completed_at,
                 commit_sha, models_total, models_failed, tests_total,
                 tests_failed, model_results, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    NULLIF(%s, '')::jsonb, %s)
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
        )

    def finalize_stale_runs(self) -> None:
        """Fail orphaned ``running`` rows left behind by a crash/restart.

        The central stuck-run sweep cannot see this database, so the worker
        finalizes its own orphans on startup and periodically.
        """
        # Two literal statements (not an f-string over the table name) so
        # the SQL stays statically checkable. A `running` row older than
        # 2 hours is an orphan — no run is allowed to take that long.
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
            count = self._execute(query, (), desc=f"stale-run sweep ({table})")
            if count:
                logger.warning(
                    "Finalized %d orphaned %s row(s) from a previous worker",
                    count,
                    table,
                )
