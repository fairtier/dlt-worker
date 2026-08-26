"""The `duckdb` source type: extract through a DuckDB extension, land via dlt.

DuckDB is the extractor, dlt is the loader. The builder opens an in-memory
DuckDB, LOADs one extension, optionally ATTACHes the external system, and
exposes each configured table as a dlt resource that streams Arrow record
batches out of a query — so the existing pipeline machinery (write
dispositions, merge, incremental cursor state, the chunked Iceberg load in
iceberg_stream.py, credential scrubbing) applies unchanged. The extension
only ever reads; landing in the lake is dlt's job.

source_config:
    extension: mysql              # which DuckDB extension to LOAD (required)
    attach: "host={host} user={user} password={password} database=shop"
                                  # ATTACH template (optional; see below)
    tables:
      - name: orders              # destination table (required)
        query: "SELECT * FROM src.orders"   # optional; defaults to
                                            # SELECT * FROM src."<name>"
        cursor_column: updated_at # optional -> incremental loading
        initial_value: null       # optional incremental start
        primary_key: id           # optional -> merge hint

source_credentials:
    attach_params: {host: "...", user: "...", password: "..."}
        # fill the {placeholders} in the attach template; values are part of
        # source_credentials, so pipeline_runner._scrub_credentials keeps
        # them out of every error message and log line
    secret: {type: gsheets, KEY: "VALUE", ...}
        # optional; rendered as CREATE SECRET (TYPE <type>, KEY 'VALUE', ...)
        # for extensions that authenticate via DuckDB secrets instead of an
        # ATTACH string. `type` defaults to the extension name.

When `attach` is present the external system is attached read-only as `src`
(ATTACH '...' AS src (TYPE <extension>, READ_ONLY)); table queries refer to
it as src.<table>. Extensions with no ATTACH concept (table functions like
read_gsheet) omit `attach` and give every table an explicit `query`.

Memory is bounded the way the dbt path bounds it (spill, don't shrink): a
`memory_limit` SET plus a per-pipeline temp directory that is wiped on the
next build, so DuckDB spills to disk instead of growing past the ceiling.
Batches leave DuckDB through an Arrow reader at DATA_WRITER_CHUNK_ROWS
rows, so peak RSS is a function of the chunk size, not the table size.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
from typing import Any, Iterator

import dlt

from dlt_worker import config
from dlt_worker.api_client import PipelineConfig

logger = logging.getLogger(__name__)

# The extensions baked into the image, mapping name -> DuckDB extension
# repository ("core" or "community"). The Dockerfile prefetches exactly this
# set into DUCKDB_EXTENSION_DIR at build time, keyed to the duckdb wheel's
# own version/platform — the DuckFlight extension image is NOT consumed
# here, its binaries are ABI-locked to a different DuckDB build. This is
# also the parity anchor for the FairTier API's `duckdb` extension
# allowlist (workspace-api pipeline_schema.go) and the drafter's capability
# prompt (workspace-api llm/drafter.go): the three move in the same change.
SUPPORTED_DUCKDB_EXTENSIONS: dict[str, str] = {
    # Databases (ATTACH-style: attach template + tables).
    "mysql": "core",
    "mssql": "community",
    # Document/file readers (table-function style: no attach, query-only —
    # read_pdf/read_pdf_tables, read_html/read_xml/html_extract_tables).
    "pdf": "community",
    "webbed": "community",
    # Helper: not a source by itself, but read_pdf('https://…') and friends
    # autoload it for the http(s) protocol, and autoinstall cannot write
    # into the read-only baked directory — so it must be baked. Also lets a
    # query-only pipeline read remote csv/parquet/json directly.
    "httpfs": "core",
}

# An extension name is interpolated into LOAD/ATTACH statements, so it must
# be a bare identifier — everything else is refused before touching SQL.
_IDENT_RE = re.compile(r"^[a-z0-9_]+$")

_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z0-9_]+)\}")

# Fallback batch size when DATA_WRITER_CHUNK_ROWS is configured 0 (which
# disables the parquet row-group bound, not the wish for bounded batches).
_DEFAULT_BATCH_ROWS = 100_000


def _sql_string(value: str) -> str:
    """Escape a value for embedding in a single-quoted SQL string literal."""
    return value.replace("'", "''")


def _quote_ident(name: str) -> str:
    """Double-quote an identifier, escaping embedded quotes."""
    return '"' + name.replace('"', '""') + '"'


def render_attach(pipeline_name: str, template: str, params: dict[str, Any]) -> str:
    """Fill {placeholder}s in the attach template from attach_params.

    Missing keys are reported by name only — never echo values, the
    template with placeholders filled is a credential.
    """
    missing = sorted(
        {m.group(1) for m in _PLACEHOLDER_RE.finditer(template)} - set(params)
    )
    if missing:
        raise ValueError(
            f"Pipeline {pipeline_name!r}: source_credentials attach_params "
            f"missing required {', '.join(repr(m) for m in missing)}"
        )
    return _PLACEHOLDER_RE.sub(lambda m: str(params[m.group(1)]), template)


def _secret_sql(pipeline_name: str, extension: str, secret: dict[str, Any]) -> str:
    """Render source_credentials.secret as a CREATE SECRET statement."""
    secret_type = str(secret.get("type") or extension)
    if not _IDENT_RE.match(secret_type):
        raise ValueError(
            f"Pipeline {pipeline_name!r}: invalid secret type {secret_type!r}"
        )
    pairs = []
    for key, value in secret.items():
        if key == "type":
            continue
        if not _IDENT_RE.match(str(key).lower()):
            raise ValueError(f"Pipeline {pipeline_name!r}: invalid secret key {key!r}")
        pairs.append(f"{key} '{_sql_string(str(value))}'")
    clauses = ", ".join([f"TYPE {secret_type}"] + pairs)
    return f"CREATE OR REPLACE SECRET pipeline_secret ({clauses})"


def _spill_dir(pipeline_id: str) -> str:
    """Per-pipeline DuckDB spill directory, wiped on every build.

    DuckDB removes its temp files on a clean close, but a SIGKILLed run
    child cannot — wiping at the next build keeps a long-lived worker pod
    from accumulating orphaned spill.
    """
    base = config.PIPELINE_DUCKDB_TEMP_DIR or tempfile.gettempdir()
    path = os.path.join(base, f"duckdb-pipeline-{pipeline_id}")
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    return path


def _connect(cfg: PipelineConfig) -> Any:
    """Open a bounded in-memory DuckDB for one extraction."""
    import duckdb

    ddb_config: dict[str, Any] = {}
    if config.DUCKDB_EXTENSION_DIR:
        ddb_config["extension_directory"] = config.DUCKDB_EXTENSION_DIR
    con = duckdb.connect(":memory:", config=ddb_config)
    if config.PIPELINE_DUCKDB_MEMORY_LIMIT:
        con.execute(
            f"SET memory_limit = '{_sql_string(config.PIPELINE_DUCKDB_MEMORY_LIMIT)}'"
        )
    con.execute(f"SET temp_directory = '{_sql_string(_spill_dir(cfg.id))}'")
    if config.PIPELINE_DUCKDB_MAX_TEMP_SIZE:
        con.execute(
            "SET max_temp_directory_size = "
            f"'{_sql_string(config.PIPELINE_DUCKDB_MAX_TEMP_SIZE)}'"
        )
    return con


def _stream_batches(
    con: Any, query: str, params: list[Any], batch_rows: int
) -> Iterator[Any]:
    """Execute a query on its own cursor and yield Arrow record batches.

    A cursor per resource: dlt may pull resources from worker threads, and
    a duckdb cursor is a connection clone that is safe to drive
    independently of its siblings.
    """
    cur = con.cursor()
    try:
        cur.execute(query, params or None)
        reader = cur.to_arrow_reader(batch_rows)
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break
            if batch.num_rows:
                yield batch
    finally:
        cur.close()


def _table_resource(
    pipeline_name: str, con: Any, table: dict[str, Any], batch_rows: int
) -> Any:
    name = table.get("name")
    if not name:
        raise ValueError(
            f"Pipeline {pipeline_name!r}: tables entry missing required 'name'"
        )
    base_query = table.get("query") or f"SELECT * FROM src.{_quote_ident(name)}"
    cursor_column = table.get("cursor_column")

    hints: dict[str, Any] = {"name": name}
    if table.get("primary_key"):
        hints["primary_key"] = table["primary_key"]

    if cursor_column:
        if not _IDENT_RE.match(str(cursor_column).lower()):
            raise ValueError(
                f"Pipeline {pipeline_name!r}: invalid cursor_column "
                f"{cursor_column!r} for table {name!r}"
            )

        def read(
            incremental: Any = dlt.sources.incremental(
                cursor_path=str(cursor_column),
                initial_value=table.get("initial_value"),
            ),
        ) -> Iterator[Any]:
            # Push the cursor down into the query so the source only sends
            # new rows; dlt's own incremental filter stays as the state
            # keeper and the exact boundary arbiter.
            query, params = base_query, []
            last = incremental.last_value if incremental else None
            if last is not None:
                query = (
                    f"SELECT * FROM ({base_query}) AS _q "
                    f"WHERE {_quote_ident(str(cursor_column))} > ?"
                )
                params = [last]
            yield from _stream_batches(con, query, params, batch_rows)

    else:

        def read() -> Iterator[Any]:
            yield from _stream_batches(con, base_query, [], batch_rows)

    return dlt.resource(read, **hints)


def build_duckdb_source(cfg: PipelineConfig) -> Any:
    """Build the list of dlt resources for a `duckdb` pipeline."""
    src_cfg = cfg.source_config

    extension = src_cfg.get("extension")
    if not extension:
        raise ValueError(
            f"Pipeline {cfg.name!r}: source_config missing required 'extension'"
        )
    if not _IDENT_RE.match(str(extension)):
        raise ValueError(f"Pipeline {cfg.name!r}: invalid extension name {extension!r}")
    if extension not in SUPPORTED_DUCKDB_EXTENSIONS:
        # Not fatal on purpose: the image only *bakes* the supported set,
        # but DuckDB can still autoinstall a signed extension over the
        # box's open 443 — a self-hoster's escape hatch. The FairTier API
        # enforces its own allowlist at save time.
        logger.warning(
            "Pipeline %s: extension %r is not in the baked set %s; "
            "DuckDB will try to install it at run time",
            cfg.name,
            extension,
            sorted(SUPPORTED_DUCKDB_EXTENSIONS),
        )

    tables = src_cfg.get("tables")
    if not tables:
        raise ValueError(
            f"Pipeline {cfg.name!r}: source_config missing required 'tables'"
        )

    attach_template = src_cfg.get("attach")
    if not attach_template:
        for table in tables:
            if not table.get("query"):
                raise ValueError(
                    f"Pipeline {cfg.name!r}: table "
                    f"{table.get('name')!r} needs an explicit 'query' when "
                    "source_config has no 'attach'"
                )

    con = _connect(cfg)

    try:
        con.execute(f"LOAD {extension}")
    except Exception as exc:
        raise ValueError(
            f"Pipeline {cfg.name!r}: failed to load DuckDB extension "
            f"{extension!r}: {exc}"
        ) from exc

    secret = cfg.source_credentials.get("secret")
    if secret:
        if not isinstance(secret, dict):
            raise ValueError(
                f"Pipeline {cfg.name!r}: source_credentials 'secret' must be an object"
            )
        con.execute(_secret_sql(cfg.name, str(extension), secret))

    if attach_template:
        attach_params = cfg.source_credentials.get("attach_params") or {}
        if not isinstance(attach_params, dict):
            raise ValueError(
                f"Pipeline {cfg.name!r}: source_credentials 'attach_params' "
                "must be an object"
            )
        rendered = render_attach(cfg.name, str(attach_template), attach_params)
        con.execute(
            f"ATTACH '{_sql_string(rendered)}' AS src (TYPE {extension}, READ_ONLY)"
        )

    batch_rows = config.DATA_WRITER_CHUNK_ROWS or _DEFAULT_BATCH_ROWS
    return [_table_resource(cfg.name, con, table, batch_rows) for table in tables]
