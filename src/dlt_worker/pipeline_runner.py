"""Constructs and executes dlt pipelines from PipelineConfig objects."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, cast

import dlt
import requests

from dlt.common.schema.typing import TMergeDispositionDict, TWriteDispositionConfig

from dlt_worker import config
from dlt_worker.api_client import PipelineConfig, PipelineRunReport

logger = logging.getLogger(__name__)


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

    try:
        # destination="filesystem" writes Iceberg files to S3; unrelated to
        # source_type="filesystem" which *reads* files from S3.
        pipeline = dlt.pipeline(
            pipeline_name=cfg.name,
            destination="filesystem",
            dataset_name=cfg.dataset_name,
            pipelines_dir=config.DLT_STATE_DIR,
        )

        source = _build_source(cfg)

        # Apply merge strategy when write_disposition is "merge" and strategy is set
        write_disp: TWriteDispositionConfig = cfg.write_disposition
        if cfg.write_disposition == "merge" and cfg.merge_strategy:
            write_disp = TMergeDispositionDict(
                disposition="merge",
                strategy=cfg.merge_strategy,  # type: ignore[arg-type]
            )

        pipeline.run(
            source,
            write_disposition=write_disp,
            table_format="iceberg",
        )

        rows_loaded = _count_rows(pipeline.last_trace.last_normalize_info)

        logger.info("Pipeline %s completed: %d rows loaded", cfg.name, rows_loaded)

        _trigger_snapshot(cfg.name)

        return PipelineRunReport(
            pipeline_id=cfg.id,
            status="success",
            started_at=started_at.isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            rows_loaded=rows_loaded,
        )

    except Exception as exc:
        logger.exception("Pipeline %s failed", cfg.name)
        return PipelineRunReport(
            pipeline_id=cfg.id,
            status="failed",
            started_at=started_at.isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            error_message=str(exc),
        )


def _trigger_snapshot(pipeline_name: str) -> None:
    """POST to a configured webhook after each successful pipeline run. Best-effort.

    Designed for use with a state-snapshot sidecar (e.g. snapshot-sidecar) but
    works with any endpoint that accepts an empty JSON POST.
    Requires SNAPSHOT_URL to be set; silently skips when not configured.
    """
    if not config.SNAPSHOT_URL:
        return
    try:
        resp = requests.post(
            config.SNAPSHOT_URL,
            json={},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        status = resp.json().get("status", "unknown")
        logger.info("Pipeline %s: snapshot %s", pipeline_name, status)
    except requests.RequestException:
        logger.warning(
            "Pipeline %s: failed to trigger snapshot webhook",
            pipeline_name,
            exc_info=True,
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
        if "incremental" in res_cfg:
            inc = res_cfg["incremental"]
            resource_def["incremental"] = dlt.sources.incremental(
                cursor_path=inc["cursor_path"],
                initial_value=inc.get("initial_value"),
            )
        resources.append(resource_def)

    return rest_api_source(
        cast(RESTAPIConfig, {"client": client_config, "resources": resources})
    )


def _build_sql_database_source(cfg: PipelineConfig) -> Any:
    from dlt.sources.sql_database import sql_database

    try:
        connection_string = cfg.source_credentials["connection_string"]
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


def _build_google_sheets_source(cfg: PipelineConfig) -> Any:
    """Read spreadsheet ranges via the Sheets API into one resource per range.

    source_config: spreadsheet_url_or_id (required), range_names (optional —
    tab names, "Tab!A1:D" ranges, or named ranges; defaults to every tab).
    source_credentials: service_account_key — the GCP service-account key JSON
    (object or string); the spreadsheet must be shared read-only with the
    key's client_email. The first row of each range is the header row.
    """
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    src_cfg = cfg.source_config

    try:
        spreadsheet = src_cfg["spreadsheet_url_or_id"]
    except KeyError:
        raise ValueError(
            f"Pipeline {cfg.name!r}: source_config missing required "
            "'spreadsheet_url_or_id'"
        ) from None

    key_info = cfg.source_credentials.get("service_account_key")
    if not key_info:
        raise ValueError(
            f"Pipeline {cfg.name!r}: source_credentials missing required "
            "'service_account_key'"
        )
    if isinstance(key_info, str):
        key_info = json.loads(key_info)

    credentials = Credentials.from_service_account_info(
        key_info, scopes=[_SHEETS_SCOPE]
    )
    session = AuthorizedSession(credentials)
    base = f"{_SHEETS_API_BASE}/{_spreadsheet_id(spreadsheet)}"

    range_names = src_cfg.get("range_names")
    if not range_names:
        resp = session.get(
            base, params={"fields": "sheets.properties.title"}, timeout=60
        )
        resp.raise_for_status()
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
    resp.raise_for_status()
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
        return read_parquet, {}
    if lower.endswith((".jsonl", ".ndjson")):
        return read_jsonl, {}
    raise ValueError(
        f"Pipeline {pipeline_name!r}: unsupported file type in glob {file_glob!r}"
    )


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
