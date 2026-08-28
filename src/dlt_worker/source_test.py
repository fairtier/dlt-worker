"""Probing a source: "can this thing read my X?", answered before a save.

The Console queues a test, this worker claims it on its next test poll,
opens the source the same way a run would, and reports what it found. The
probe runs HERE rather than in the FairTier API for the reason every part
of the extraction path runs here: this is where the drivers, the baked
DuckDB extensions, the box's own network path and the credentials are. A
test that ran anywhere else would be testing a different thing than the one
that will run at 03:00.

What a probe is allowed to do is deliberately narrow:

* it opens the connection and reads at most one row per table, so a test is
  bounded no matter how large the source is;
* it lands nothing — no dlt pipeline, no Iceberg table, no state;
* it reports per-table lines, because "connected" and "connected, and the
  table you named is there" are different answers and only the second one
  is the question the user asked.

Everything leaving here goes through scrub.scrub_credentials: a driver
error quotes the connection string it failed on, and this text is stored,
shipped and shown.
"""

from __future__ import annotations

import logging
from typing import Any

from dlt_worker import pgurl, scrub
from dlt_worker.api_client import SourceTest, SourceTestReport

logger = logging.getLogger(__name__)

# At most this many tables are probed. A source with 300 tables must not
# turn one button press into 300 queries; the first few answer the question.
MAX_PROBED_TABLES = 10

# The label the duckdb builder's shared validators put in their messages.
# They say "Pipeline <label>: …" because that is what they normally serve;
# a test has no pipeline, so the prefix is stripped back off rather than
# shown to someone who has not saved one yet.
_LABEL = "the source"
_LABEL_PREFIX = f"Pipeline {_LABEL!r}: "


def probe_source(test: SourceTest) -> SourceTestReport:
    """Run one probe and return its report. Never raises."""
    try:
        if test.source_type == "duckdb":
            message, details = _probe_duckdb(test)
        elif test.source_type == "sql_database":
            message, details = _probe_sql_database(test)
        else:
            return _failed(
                test, f"testing a {test.source_type!r} source is not supported here"
            )
        return SourceTestReport(
            id=test.id, status="success", message=message, details=details
        )
    except Exception as exc:  # noqa: BLE001 — a probe reports, it does not raise
        # Type and message both, scrubbed: the message is the whole value of
        # a failed test ("Access denied for user 'readonly'"), and losing it
        # would leave the user with "it did not work".
        text = f"{type(exc).__name__}: {exc}".replace(_LABEL_PREFIX, "")
        logger.info(
            "Source test %s failed: %s",
            test.id,
            scrub.scrub_credentials(text, test.source_credentials),
        )
        return _failed(test, text)


def _failed(test: SourceTest, message: str) -> SourceTestReport:
    return SourceTestReport(
        id=test.id,
        status="failed",
        message=scrub.scrub_credentials(message, test.source_credentials),
        details=[],
    )


def _probe_duckdb(test: SourceTest) -> tuple[str, list[str]]:
    """Open a duckdb source: LOAD, SECRET, ATTACH, then one row per table.

    The same four steps build_duckdb_source takes, in the same order, so a
    test that passes and a run that fails would be a real bug rather than
    two different code paths disagreeing.
    """
    from dlt_worker.duckdb_source import (
        _connect,
        _quote_ident,
        _secret_sql,
        _sql_string,
        render_attach,
        source_extensions,
    )

    cfg = test.source_config
    creds = test.source_credentials
    extensions = source_extensions(_LABEL, cfg)
    tables = cfg.get("tables") or []
    if not tables:
        raise ValueError("source_config has no tables to read")

    # A probe gets its own spill directory, keyed by the test id, so it can
    # never share (or wipe) a real pipeline's.
    con = _connect(f"source-test-{test.id}")
    try:
        for name in extensions:
            con.execute(f"LOAD {name}")

        secret = creds.get("secret")
        if secret:
            con.execute(_secret_sql(_LABEL, extensions[0], secret))

        attach_template = cfg.get("attach")
        if attach_template:
            rendered = render_attach(
                _LABEL, str(attach_template), creds.get("attach_params") or {}
            )
            con.execute(
                f"ATTACH '{_sql_string(rendered)}' "
                f"AS src (TYPE {extensions[0]}, READ_ONLY)"
            )

        details: list[str] = []
        failures: list[str] = []
        for table in tables[:MAX_PROBED_TABLES]:
            name = str(table.get("name") or "")
            query = table.get("query") or f"SELECT * FROM src.{_quote_ident(name)}"
            ok, line = _probe_query(con, name, query)
            details.append(line)
            if not ok:
                failures.append(line)
        if len(tables) > MAX_PROBED_TABLES:
            details.append(
                f"({len(tables) - MAX_PROBED_TABLES} more tables not checked)"
            )
    finally:
        con.close()

    if failures:
        raise ValueError("; ".join(failures))
    return f"Read {len(details)} table(s) with {', '.join(extensions)}", details


def _probe_query(con: Any, name: str, query: str) -> tuple[bool, str]:
    """Read at most one row, and describe what came back.

    One table failing is a detail line, not the end of the probe: "orders is
    there, customers is not" is a better answer than the first error.
    """
    try:
        cur = con.cursor()
        try:
            cur.execute(f"SELECT * FROM ({query}) AS _probe LIMIT 1")
            columns = [d[0] for d in cur.description or []]
            rows = cur.fetchall()
        finally:
            cur.close()
    except Exception as exc:  # noqa: BLE001 — reported, not raised
        return False, f"{name}: {type(exc).__name__}: {exc}"
    if not rows:
        return True, f"{name}: readable, {len(columns)} columns, no rows yet"
    return True, f"{name}: readable, {len(columns)} columns"


def _probe_sql_database(test: SourceTest) -> tuple[str, list[str]]:
    """Open the PostgreSQL connection and look for the configured tables.

    A wrong password, an unreachable host and a table that does not exist
    are three different answers, and all three used to arrive as one failed
    run hours later.
    """
    from sqlalchemy import create_engine, inspect, text

    connection_string = test.source_credentials.get("connection_string")
    if not connection_string:
        raise ValueError("source_credentials has no connection_string")

    engine = create_engine(pgurl.normalize_pg_connection_string(str(connection_string)))
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            inspector = inspect(conn)
            schema = test.source_config.get("schema") or None
            existing = set(inspector.get_table_names(schema=schema)) | set(
                inspector.get_view_names(schema=schema)
            )
    finally:
        engine.dispose()

    # Both shapes the sql_database source accepts: the plain name list and
    # the detailed tables_config the incremental form uses.
    wanted = [str(t) for t in (test.source_config.get("tables") or [])]
    for entry in test.source_config.get("tables_config") or []:
        if isinstance(entry, dict) and entry.get("name"):
            wanted.append(str(entry["name"]))
    details = [
        f"{name}: found" if name in existing else f"{name}: not found"
        for name in wanted[:MAX_PROBED_TABLES]
    ]
    missing = [name for name in wanted if name not in existing]
    if missing:
        raise ValueError(
            "connected, but these tables do not exist: " + ", ".join(missing)
        )
    if not wanted:
        return f"Connected. {len(existing)} tables visible", details
    return f"Connected. All {len(wanted)} configured table(s) found", details
