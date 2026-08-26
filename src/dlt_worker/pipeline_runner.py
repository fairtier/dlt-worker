"""Constructs and executes dlt pipelines from PipelineConfig objects."""

from __future__ import annotations

import json
import logging
import re
import traceback
from datetime import datetime, timezone
from typing import Any, cast
from urllib.parse import quote

import dlt

from dlt.common.schema.typing import TMergeDispositionDict, TWriteDispositionConfig

from dlt_worker import config, telemetry
from dlt_worker.api_client import PipelineConfig, PipelineRunReport
from dlt_worker.snapshot import trigger_snapshot

logger = logging.getLogger(__name__)


# Credential values shorter than this are not scrubbed: replacing every
# occurrence of a 1-3 char string would mangle unrelated message text.
_MIN_SCRUB_LENGTH = 4


def _credential_values(obj: Any) -> list[str]:
    """Collect every string value nested anywhere in a credentials structure."""
    values: list[str] = []
    if isinstance(obj, dict):
        for item in obj.values():
            values.extend(_credential_values(item))
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            values.extend(_credential_values(item))
    elif isinstance(obj, str) and len(obj) >= _MIN_SCRUB_LENGTH:
        values.append(obj)
    return values


def _scrub_credentials(text: str, credentials: dict[str, Any]) -> str:
    """Replace credential values (and their URL-encoded forms) with ***.

    Second line of defense mirroring transformation_runner._sanitize: dlt
    source exceptions routinely echo config (SQLAlchemy errors include the
    connection URL, requests errors the full request URL), and the resulting
    error_message is persisted to the workspace database, sent to the
    central API, and logged — nothing credential-shaped may reach any of
    them. Longest values first so a value that contains another is scrubbed
    whole before its substring punches a hole in it.
    """
    values = sorted(
        set(_credential_values(credentials)), key=lambda v: len(v), reverse=True
    )
    for value in values:
        for variant in (value, quote(value, safe="")):
            text = text.replace(variant, "***")
    return text


def _count_rows(normalize_info: Any) -> int:
    """Count rows from normalize metrics, excluding dlt internal tables."""
    if not normalize_info:
        return 0
    rows = 0
    for metrics_list in normalize_info.metrics.values():
        for metrics in metrics_list:
            for table_name, table_metrics in metrics["table_metrics"].items():
                if table_name.startswith("_dlt_"):
                    continue
                count = table_metrics.items_count
                if isinstance(count, (int, float)):
                    rows += int(count)
    return rows


def run_pipeline(cfg: PipelineConfig) -> PipelineRunReport:
    """Build and run a dlt pipeline from the given config. Returns a run report."""
    started_at = datetime.now(timezone.utc)
    rows_loaded = 0

    # The inner spans below are created with exception recording OFF on
    # purpose: a dlt source exception quotes config freely (SQLAlchemy puts
    # the whole connection URL in its message), and a span's recorded
    # exception is not something _scrub_credentials can reach. The one
    # message that lands on a span is the scrubbed one, set by hand.
    with telemetry.tracer.start_as_current_span(
        "dlt_worker.pipeline.execute",
        attributes={
            telemetry.ATTR_PIPELINE_ID: cfg.id,
            telemetry.ATTR_PIPELINE_NAME: cfg.name,
            telemetry.ATTR_SOURCE_TYPE: cfg.source_type,
            telemetry.ATTR_DATASET: cfg.dataset_name,
            telemetry.ATTR_WRITE_DISPOSITION: cfg.write_disposition,
        },
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        try:
            # destination="filesystem" writes Iceberg files to S3; unrelated to
            # source_type="filesystem" which *reads* files from S3.
            pipeline = dlt.pipeline(
                pipeline_name=cfg.name,
                destination="filesystem",
                dataset_name=cfg.dataset_name,
                pipelines_dir=config.DLT_STATE_DIR,
            )

            with telemetry.tracer.start_as_current_span(
                "dlt_worker.source.build",
                attributes={telemetry.ATTR_SOURCE_TYPE: cfg.source_type},
                record_exception=False,
                set_status_on_exception=False,
            ):
                source = _build_source(cfg)

            # Apply merge strategy when write_disposition is "merge" and
            # strategy is set
            write_disp: TWriteDispositionConfig = cfg.write_disposition
            if cfg.write_disposition == "merge" and cfg.merge_strategy:
                write_disp = TMergeDispositionDict(
                    disposition="merge",
                    strategy=cfg.merge_strategy,  # type: ignore[arg-type]
                )

            # dlt's own extract/normalize/load stages live under this span;
            # it is where a run spends essentially all of its time.
            with telemetry.tracer.start_as_current_span(
                "dlt_worker.dlt.run",
                attributes={telemetry.ATTR_PIPELINE_NAME: cfg.name},
                record_exception=False,
                set_status_on_exception=False,
            ):
                pipeline.run(
                    source,
                    write_disposition=write_disp,
                    table_format="iceberg",
                )

            rows_loaded = _count_rows(pipeline.last_trace.last_normalize_info)

            logger.info("Pipeline %s completed: %d rows loaded", cfg.name, rows_loaded)

            span.set_attribute(telemetry.ATTR_ROWS, rows_loaded)
            span.set_attribute(telemetry.ATTR_STATUS, "success")
            # What the run left resident is the number that decides whether
            # the next one gets OOM-killed (see run_isolation).
            span.set_attribute("dlt_worker.process.memory.rss", telemetry.rss_bytes())

            trigger_snapshot(cfg.name)

            return PipelineRunReport(
                pipeline_id=cfg.id,
                status="success",
                started_at=started_at.isoformat(),
                completed_at=datetime.now(timezone.utc).isoformat(),
                rows_loaded=rows_loaded,
            )

        except Exception as exc:
            # Scrub the log too — the traceback quotes the same exception text.
            logger.error(
                "Pipeline %s failed:\n%s",
                cfg.name,
                _scrub_credentials(traceback.format_exc(), cfg.source_credentials),
            )
            error_message = _scrub_credentials(str(exc), cfg.source_credentials)
            span.set_attribute(telemetry.ATTR_STATUS, "failed")
            span.set_attribute("exception.type", type(exc).__name__)
            span.set_status(telemetry.error_status(error_message))
            return PipelineRunReport(
                pipeline_id=cfg.id,
                status="failed",
                started_at=started_at.isoformat(),
                completed_at=datetime.now(timezone.utc).isoformat(),
                error_message=error_message,
            )


def _build_source(cfg: PipelineConfig) -> Any:
    """Build a dlt source based on the pipeline's source_type."""
    if cfg.source_type == "rest_api":
        return _build_rest_api_source(cfg)
    if cfg.source_type == "sql_database":
        return _build_sql_database_source(cfg)
    if cfg.source_type == "filesystem":
        return _build_filesystem_source(cfg)
    if cfg.source_type == "google_sheets":
        return _build_google_sheets_source(cfg)
    if cfg.source_type == "duckdb":
        # Deferred import like the dlt sources above: duckdb costs real
        # memory and only the runs that use it should pay.
        from dlt_worker.duckdb_source import build_duckdb_source

        return build_duckdb_source(cfg)

    raise ValueError(f"Unsupported source type: {cfg.source_type}")


def _build_rest_api_source(cfg: PipelineConfig) -> Any:
    from dlt.sources.rest_api import rest_api_source
    from dlt.sources.rest_api.typing import RESTAPIConfig

    client_config: dict[str, Any] = {
        "base_url": cfg.source_config["base_url"],
    }

    # Inject auth from credentials
    auth = None
    if "auth" in cfg.source_credentials:
        auth = cfg.source_credentials["auth"]  # pass through to dlt as-is
    elif "api_key" in cfg.source_credentials:
        # Backwards-compatible: simple bearer token
        auth = {"type": "bearer", "token": cfg.source_credentials["api_key"]}

    if auth:
        client_config["auth"] = auth
    if "headers" in cfg.source_credentials:
        client_config.setdefault("headers", {}).update(
            cfg.source_credentials["headers"]
        )

    # A source-level `incremental` (the shape platform_api validates and the
    # Console emits — a single top-level object, not per-endpoint) applies to
    # every resource; a per-resource `incremental` overrides it for that
    # resource. A fresh dlt.sources.incremental is built per resource below so
    # they never share cursor state.
    source_incremental = cfg.source_config.get("incremental")

    # Build resource definitions
    resources = []
    for res_cfg in cfg.source_config.get("resources", []):
        resource_def: dict[str, Any] = {
            "name": res_cfg["name"],
            "endpoint": {
                "path": res_cfg["endpoint"],
            },
        }
        if "params" in res_cfg:
            resource_def["endpoint"]["params"] = res_cfg["params"]
        if "paginator" in res_cfg:
            resource_def["endpoint"]["paginator"] = res_cfg["paginator"]
        if "primary_key" in res_cfg:
            resource_def["primary_key"] = res_cfg["primary_key"]
        if "write_disposition" in res_cfg:
            resource_def["write_disposition"] = res_cfg["write_disposition"]
        inc = res_cfg.get("incremental", source_incremental)
        if inc:
            # dlt rest_api takes incremental as a config dict UNDER `endpoint`
            # (Endpoint.incremental: IncrementalConfig) — an Incremental object,
            # or `incremental` at the resource level, is rejected by
            # EndpointResource validation ("received unexpected fields").
            inc_cfg: dict[str, Any] = {"cursor_path": inc["cursor_path"]}
            if inc.get("initial_value") is not None:
                inc_cfg["initial_value"] = inc["initial_value"]
            resource_def["endpoint"]["incremental"] = inc_cfg
        resources.append(resource_def)

    return rest_api_source(
        cast(RESTAPIConfig, {"client": client_config, "resources": resources})
    )


def _normalize_pg_connection_string(connection_string: str) -> str:
    """Map the common PostgreSQL URL schemes onto the installed driver.

    This image deliberately ships psycopg (v3) as its only database driver,
    but SQLAlchemy resolves a plain ``postgresql://`` to psycopg2 and knows
    no ``postgres`` dialect at all — so the natural forms users paste would
    crash at engine creation. Anything that is not a PostgreSQL scheme is
    left untouched (the FairTier API rejects those at save time; a
    self-hoster who installed extra drivers keeps them working).
    """
    scheme, sep, rest = connection_string.partition("://")
    if sep and scheme.lower() in ("postgres", "postgresql"):
        return "postgresql+psycopg://" + rest
    return connection_string


def _build_sql_database_source(cfg: PipelineConfig) -> Any:
    from dlt.sources.sql_database import sql_database

    try:
        connection_string = _normalize_pg_connection_string(
            cfg.source_credentials["connection_string"]
        )
    except KeyError:
        raise ValueError(
            f"Pipeline {cfg.name!r}: source_credentials missing required 'connection_string'"
        ) from None

    src_cfg = cfg.source_config

    # Detailed per-table config with optional incremental loading
    tables_config = src_cfg.get("tables_config")
    if tables_config:
        if "tables" in src_cfg:
            logger.warning(
                "Pipeline %s: both 'tables' and 'tables_config' provided; "
                "'tables' will be ignored",
                cfg.name,
            )

        has_incremental = False
        table_names = []
        for tc in tables_config:
            if "name" not in tc:
                raise ValueError(
                    f"Pipeline {cfg.name!r}: tables_config entry missing required 'name'"
                )
            table_names.append(tc["name"])
            if "incremental" in tc:
                if "cursor_path" not in tc["incremental"]:
                    raise ValueError(
                        f"Pipeline {cfg.name!r}: incremental config for table "
                        f"{tc['name']!r} missing required 'cursor_path'"
                    )
                has_incremental = True

        if has_incremental and cfg.write_disposition == "replace":
            logger.warning(
                "Pipeline %s: incremental loading configured but write_disposition "
                "is 'replace' — rows will be replaced on every run; "
                "consider 'append' or 'merge'",
                cfg.name,
            )

        source = sql_database(credentials=connection_string, table_names=table_names)

        for tc in tables_config:
            if "incremental" in tc:
                inc = tc["incremental"]
                source.resources[tc["name"]].apply_hints(
                    incremental=dlt.sources.incremental(
                        cursor_path=inc["cursor_path"],
                        initial_value=inc.get("initial_value"),
                    ),
                )

        return source

    # Simple table list (full load)
    table_names = src_cfg.get("tables")
    return sql_database(credentials=connection_string, table_names=table_names)


_SPREADSHEET_URL_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9_-]+)")

# Google Sheets API v4; readonly scope — the worker never writes to sheets.
_SHEETS_API_BASE = "https://sheets.googleapis.com/v4/spreadsheets"
_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"


def _spreadsheet_id(url_or_id: str) -> str:
    """Extract the spreadsheet ID from a docs.google.com URL, or pass an ID through."""
    m = _SPREADSHEET_URL_RE.search(url_or_id)
    return m.group(1) if m else url_or_id


def _raise_for_sheets_status(resp: Any, pipeline_name: str) -> None:
    """Like resp.raise_for_status(), but surface the Sheets API error message.

    requests' default HTTPError includes only the status + URL, discarding the
    JSON body where Google explains *why* (e.g. "Unable to parse range:
    Sheet1!A1:D" when a range names a tab that doesn't exist). Re-raise as a
    ValueError carrying that message so the failure is actionable.
    """
    if resp.ok:
        return
    message = ""
    try:
        message = resp.json().get("error", {}).get("message", "")
    except ValueError:
        message = (resp.text or "").strip()
    detail = f": {message}" if message else ""
    hint = ""
    if "unable to parse range" in message.lower():
        hint = (
            " — check that each range's tab name exists in the spreadsheet "
            "(tab names are case-sensitive; clear range_names to load every tab)"
        )
    raise ValueError(
        f"Pipeline {pipeline_name!r}: Google Sheets API returned "
        f"{resp.status_code}{detail}{hint}"
    )


def _range_table_name(range_name: str) -> str:
    """Derive a table name from a range: "Orders 2024!A1:D" -> "orders_2024"."""
    sheet = range_name.split("!", 1)[0].strip().strip("'")
    name = re.sub(r"[^a-zA-Z0-9]+", "_", sheet).strip("_").lower()
    return name or "sheet"


def _normalize_headers(header_row: list[Any]) -> list[str]:
    """First-row cells -> column names: blanks become col_N, duplicates get _N."""
    headers: list[str] = []
    seen: dict[str, int] = {}
    for i, cell in enumerate(header_row):
        name = str(cell).strip() if cell not in (None, "") else ""
        if not name:
            name = f"col_{i + 1}"
        count = seen.get(name, 0)
        seen[name] = count + 1
        if count:
            name = f"{name}_{count + 1}"
        headers.append(name)
    return headers


def _rows_to_records(
    pipeline_name: str, range_name: str, rows: list[list[Any]]
) -> list[dict[str, Any]]:
    """Header row + data rows -> dicts. Short rows are padded (the API omits
    trailing empty cells); cells beyond the header width are dropped; fully
    empty rows are skipped."""
    if len(rows) < 2:
        logger.warning(
            "Pipeline %s: range %r has a header but no data rows",
            pipeline_name,
            range_name,
        )
        return []
    headers = _normalize_headers(rows[0])
    records = []
    for row in rows[1:]:
        if all(cell in (None, "") for cell in row):
            continue
        padded = list(row) + [None] * (len(headers) - len(row))
        records.append(dict(zip(headers, padded)))
    return records


def _google_sheets_credentials(cfg: PipelineConfig) -> Any:
    """Build read-only Sheets credentials from either OAuth or a service-account
    key. Exactly one of the two credential methods must be present.
    """
    oauth = cfg.source_credentials.get("oauth")
    key_info = cfg.source_credentials.get("service_account_key")

    if oauth:
        from google.oauth2.credentials import Credentials as OAuthCredentials

        missing = [
            k
            for k in ("client_id", "client_secret", "refresh_token")
            if not oauth.get(k)
        ]
        if missing:
            raise ValueError(
                f"Pipeline {cfg.name!r}: source_credentials oauth missing "
                f"required {', '.join(missing)!r}"
            )
        # token=None forces AuthorizedSession to mint an access token from the
        # refresh token on the first request.
        return OAuthCredentials(
            None,
            refresh_token=oauth["refresh_token"],
            client_id=oauth["client_id"],
            client_secret=oauth["client_secret"],
            token_uri="https://oauth2.googleapis.com/token",
            scopes=[_SHEETS_SCOPE],
        )

    if key_info:
        from google.oauth2.service_account import Credentials

        if isinstance(key_info, str):
            key_info = json.loads(key_info)
        return Credentials.from_service_account_info(key_info, scopes=[_SHEETS_SCOPE])

    raise ValueError(
        f"Pipeline {cfg.name!r}: source_credentials missing required "
        "'oauth' or 'service_account_key'"
    )


def _build_google_sheets_source(cfg: PipelineConfig) -> Any:
    """Read spreadsheet ranges via the Sheets API into one resource per range.

    source_config: spreadsheet_url_or_id (required), range_names (optional —
    tab names, "Tab!A1:D" ranges, or named ranges; defaults to every tab).
    source_credentials carries exactly one method:
      - oauth: a delegated-user grant from the "Sign in with Google" flow —
        {client_id, client_secret, refresh_token}; the FairTier API injects the
        central client pair before serving. The easy path for ordinary users.
      - service_account_key: the GCP service-account key JSON (object or
        string); the spreadsheet must be shared read-only with the key's
        client_email. The advanced/automation path.
    The first row of each range is the header row.
    """
    from google.auth.transport.requests import AuthorizedSession

    src_cfg = cfg.source_config

    try:
        spreadsheet = src_cfg["spreadsheet_url_or_id"]
    except KeyError:
        raise ValueError(
            f"Pipeline {cfg.name!r}: source_config missing required "
            "'spreadsheet_url_or_id'"
        ) from None

    credentials = _google_sheets_credentials(cfg)
    session = AuthorizedSession(credentials)
    base = f"{_SHEETS_API_BASE}/{_spreadsheet_id(spreadsheet)}"

    range_names = src_cfg.get("range_names")
    if not range_names:
        resp = session.get(
            base, params={"fields": "sheets.properties.title"}, timeout=60
        )
        _raise_for_sheets_status(resp, cfg.name)
        range_names = [s["properties"]["title"] for s in resp.json().get("sheets", [])]
    if not range_names:
        raise ValueError(f"Pipeline {cfg.name!r}: spreadsheet has no sheets to load")

    # UNFORMATTED_VALUE keeps numbers/booleans typed; date/time cells come back
    # as locale-formatted strings (FORMATTED_STRING) instead of raw serials.
    resp = session.get(
        f"{base}/values:batchGet",
        params=[
            ("majorDimension", "ROWS"),
            ("valueRenderOption", "UNFORMATTED_VALUE"),
            ("dateTimeRenderOption", "FORMATTED_STRING"),
            *[("ranges", r) for r in range_names],
        ],
        timeout=300,
    )
    _raise_for_sheets_status(resp, cfg.name)
    value_ranges = resp.json().get("valueRanges", [])

    resources = []
    used: dict[str, int] = {}
    for range_name, value_range in zip(range_names, value_ranges):
        records = _rows_to_records(cfg.name, range_name, value_range.get("values", []))
        table = _range_table_name(range_name)
        count = used.get(table, 0)
        used[table] = count + 1
        if count:
            table = f"{table}_{count + 1}"
        resources.append(dlt.resource(records, name=table))
    return resources


def _reader_for(pipeline_name: str, file_glob: str) -> tuple[Any, dict[str, Any]]:
    """Pick the dlt reader transformer (and its kwargs) for a file glob.

    The format is chosen from the glob's extension, matching the FairTier API's
    upload allowlist: csv/tsv → read_csv, parquet → read_parquet,
    jsonl/ndjson → read_jsonl.
    """
    from dlt.sources.filesystem import read_csv, read_jsonl, read_parquet

    lower = file_glob.lower()
    if lower.endswith(".csv"):
        return read_csv, {}
    if lower.endswith(".tsv"):
        return read_csv, {"sep": "\t"}
    if lower.endswith(".parquet"):
        # Yield native pyarrow RecordBatches (use_pyarrow) rather than Python
        # lists of dicts, so dlt takes its Arrow-native path and skips the
        # single-threaded, row-by-row normalize — the bottleneck that made a
        # few-million-row parquet backfill take many minutes on the box's one
        # core. Larger batches cut per-batch overhead. Parquet is already typed
        # columnar data, so nothing is lost by keeping it in Arrow.
        #
        # chunksize == the downstream row-group cap so the whole Arrow pipeline
        # flows in uniform, memory-bounded chunks (see config.DATA_WRITER_CHUNK_ROWS
        # / iceberg_stream.py). Fall back to a bounded default when the row-group
        # cap is disabled (0): the reader must stay bounded regardless.
        chunk = config.DATA_WRITER_CHUNK_ROWS or 100_000
        return read_parquet, {"use_pyarrow": True, "chunksize": chunk}
    if lower.endswith((".jsonl", ".ndjson")):
        return read_jsonl, {}
    raise ValueError(
        f"Pipeline {pipeline_name!r}: unsupported file type in glob {file_glob!r}"
    )


def _http_file_resources(cfg: PipelineConfig, base_url: str) -> list[Any]:
    """Build resources for named files served over plain HTTP(S).

    This exists because dlt's own filesystem source cannot read a host that
    serves objects by key and refuses to list a directory — the shape of a
    public object-storage bucket. Both of its behaviours there are traps: a
    wildcard glob finds nothing and yields **zero rows without an error**,
    and an exact filename fails inside fsspec on a doubled URL scheme.

    So the file names are not discovered, they are declared: each table
    carries an explicit `files` list, and nothing here ever lists. That is
    also the only thing that CAN work — there is no listing to do.

    Reads stream through pyarrow's batch iterator at the same row cap as
    every other stage, so a multi-gigabyte file is bounded the same way a
    bucket-backed one is.
    """
    import fsspec

    tables = cfg.source_config.get("tables")
    if not tables:
        raise ValueError(
            f"Pipeline {cfg.name!r}: an http(s) bucket_url requires 'tables' with "
            f"an explicit 'files' list — a listing-less host cannot be globbed"
        )

    fs = fsspec.filesystem("http")
    resources = []
    for i, table in enumerate(tables):
        name = table.get("name")
        files = table.get("files")
        if not name:
            raise ValueError(f"Pipeline {cfg.name!r}: tables[{i}] missing 'name'")
        if not files:
            raise ValueError(
                f"Pipeline {cfg.name!r}: tables[{i}] missing 'files' — an http(s) "
                f"source names its files, it cannot discover them"
            )
        urls = [f"{base_url.rstrip('/')}/{f.lstrip('/')}" for f in files]
        resources.append(dlt.resource(_http_batches(cfg.name, fs, urls), name=name))
    return resources


def _http_batches(pipeline_name: str, fs: Any, urls: list[str]) -> Any:
    """Yield Arrow record batches from each named URL, one bounded batch at
    a time.

    A missing file is fatal. The caller *declared* this file, so quietly
    loading fewer rows than asked for is the one outcome worth ruling out —
    it is precisely the failure that made the glob path unusable here.
    """
    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq

    chunk = config.DATA_WRITER_CHUNK_ROWS or 100_000

    def batches_for(url: str, handle: Any) -> Any:
        lower = url.lower()
        if lower.endswith(".parquet"):
            yield from pq.ParquetFile(handle).iter_batches(batch_size=chunk)
        elif lower.endswith((".csv", ".tsv")):
            # open_csv streams; read_csv would materialise the whole file.
            options = pacsv.ParseOptions(
                delimiter="\t" if lower.endswith(".tsv") else ","
            )
            reader = pacsv.open_csv(
                handle,
                read_options=pacsv.ReadOptions(block_size=1 << 20),
                parse_options=options,
            )
            for batch in reader:
                yield batch
        else:
            raise ValueError(
                f"Pipeline {pipeline_name!r}: unsupported file type over http(s): "
                f"{url!r} (expected .parquet, .csv or .tsv)"
            )

    def generator() -> Any:
        for url in urls:
            if not fs.exists(url):
                raise ValueError(f"Pipeline {pipeline_name!r}: {url} not found")
            with fs.open(url, "rb") as handle:
                yield from batches_for(url, handle)

    return generator


def _build_filesystem_source(cfg: PipelineConfig) -> Any:
    from dlt.common.configuration.specs.aws_credentials import AwsCredentials
    from dlt.sources.filesystem import filesystem

    creds = cfg.source_credentials
    src_cfg = cfg.source_config

    try:
        bucket_url = src_cfg["bucket_url"]
    except KeyError:
        raise ValueError(
            f"Pipeline {cfg.name!r}: source_config missing required 'bucket_url'"
        ) from None

    # A public HTTP(S) host needs no credentials and cannot be listed, so it
    # takes a different path entirely — see _http_file_resources.
    if bucket_url.lower().startswith(("http://", "https://")):
        return _http_file_resources(cfg, bucket_url)

    for key in ("access_key_id", "secret_access_key"):
        if key not in creds:
            raise ValueError(
                f"Pipeline {cfg.name!r}: source_credentials missing required {key!r}"
            ) from None

    aws_creds = AwsCredentials(
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
        endpoint_url=creds.get("endpoint_url"),
        region_name=creds.get("region", "auto"),
    )

    # Tables mode (FairTier file drop): each entry maps files matching a glob
    # to one parsed table via the matching reader transformer. Without it, the
    # bare filesystem resource is returned unchanged — that loads file
    # *listings*, which is only useful for inventory-style pipelines.
    tables = src_cfg.get("tables")
    if not tables:
        return filesystem(
            bucket_url=bucket_url,
            file_glob=src_cfg.get("file_glob", "**/*"),
            credentials=aws_creds,
        )

    resources = []
    for i, table in enumerate(tables):
        for key in ("name", "file_glob"):
            if not table.get(key):
                raise ValueError(
                    f"Pipeline {cfg.name!r}: tables[{i}] missing required {key!r}"
                )
        reader, reader_kwargs = _reader_for(cfg.name, table["file_glob"])
        files = filesystem(
            bucket_url=bucket_url,
            file_glob=table["file_glob"],
            credentials=aws_creds,
        )
        resources.append((files | reader(**reader_kwargs)).with_name(table["name"]))
    return resources
