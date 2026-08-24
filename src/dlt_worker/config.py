"""Configuration loaded from environment variables.

Maps standard AWS env vars and dlt-worker-specific env vars into the
dlt filesystem destination + Iceberg catalog config so that dlt picks
up the storage and catalog settings automatically.
"""

import json
import os
import sys


def _require(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        print(
            f"FATAL: required environment variable {name} is not set", file=sys.stderr
        )
        sys.exit(1)
    return value


def _int(name: str, default: int) -> int:
    """Integer env var with the same clean FATAL a missing _require gives —
    a typo'd value must not crash with a raw traceback."""
    raw = os.environ.get(name, "")
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(
            f"FATAL: environment variable {name} must be an integer, got {raw!r}",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Worker config
# ---------------------------------------------------------------------------
CUSTOMER_SLUG: str = ""
FAIRTIER_API_URL: str = ""
LAKEKEEPER_URL: str = ""
POLL_INTERVAL_SECONDS: int = 60
DLT_STATE_DIR: str = "/dlt-state"
HEALTHZ_PORT: int = 8080
PIPELINE_MAX_RETRIES: int = 2
PIPELINE_RETRY_BASE_DELAY: int = 30
SNAPSHOT_URL: str = ""
# Pipeline definitions and schedules are read from
# <PIPELINES_DIR>/pipelines/*.yaml (a checkout kept in sync by a sidecar);
# the API poll carries only triggers, the last-run watermark and the
# credentials that have no file. Required as of 0.9.0 — the legacy
# poll-is-truth mode it used to be a lever over was retired with the
# definition fields it read (pipelines-as-files Phase 2.5).
PIPELINES_DIR: str = ""
# Age credentials (files mode): path to the box age identity file (the
# dlt-age Secret mount). When set, pipelines/<name>.credentials.age files
# in the checkout are decrypted and take precedence over polled
# credentials. Unset = credentials come from the poll only (0.1.0
# behavior). This is the Phase-3 rollback lever.
AGE_KEY_FILE: str = ""
# Rows per chunked Iceberg append (see iceberg_stream.py). Bounds the
# worker's peak memory during the load stage; 0 disables the streamed
# patch and restores dlt's materialize-everything behavior.
ICEBERG_LOAD_CHUNK_ROWS: int = 200_000
# Commit the streamed Iceberg load every N appends (see iceberg_stream.py).
# PyIceberg holds every appended data file's metadata in the open transaction
# until commit; across hundreds of chunks that alone can OOM a small worker,
# so we flush to a real snapshot periodically. 0 = single atomic commit at the
# end (pre-0.2.5 behavior). Snapshot expiry in the maintenance CronJob reaps
# the extra snapshots.
ICEBERG_LOAD_COMMIT_EVERY: int = 20
# Re-open the Iceberg table mid-load once its storage credentials are this
# old, in seconds (see iceberg_stream.py). With credential vending the
# catalog mints temporary S3 credentials and PyIceberg never refreshes them,
# so a load that outlives them fails on a metadata write and dlt retries the
# whole package — for `replace`, forever. The response advertises no expiry,
# so this is a conservative interval rather than a derived one. Only takes
# effect at an interim commit, so it needs ICEBERG_LOAD_COMMIT_EVERY > 0.
# 0 disables (pre-0.6.1 behavior).
ICEBERG_CREDENTIAL_REFRESH_SECONDS: int = 900
# Run each pipeline in a short-lived spawned subprocess (see
# run_isolation.py). A big dlt run leaves ~1 GB resident (Arrow buffers,
# dlt caches, allocator free lists) that Python never returns to the OS —
# process exit does. Also contains OOM kills to the run: the kernel kills
# the child, the worker reports a failed run and lives on. 0/false = run
# in-process (pre-0.3.0 behavior). This is the rollback lever.
PIPELINE_SUBPROCESS: bool = True
# Wall-clock limit for one pipeline run attempt, in seconds. A run child
# stuck in an un-timeouted network read would otherwise wedge the poll
# loop forever (no schedules, no triggers) while /healthz stays green.
# On expiry the child is terminated (then killed) and a failed run is
# reported. Only enforced in subprocess mode — an in-process run
# (PIPELINE_SUBPROCESS=0) cannot be safely interrupted. 0 disables.
PIPELINE_RUN_TIMEOUT_SECONDS: int = 21_600
# Run each dbt transformation in a short-lived spawned subprocess, for the
# same reason pipelines are (see run_isolation.py). dbt + DuckDB over a big
# table retains hundreds of MB the worker never gives back; on 2026-08-10
# an in-process build over 85M rows left 800 MB resident — under the
# container limit, so never OOM-killed, and it starved the box until the
# pod was restarted by hand. 0/false = run in-process (pre-0.7.0
# behavior). This is the rollback lever.
TRANSFORMATION_SUBPROCESS: bool = True
# Wall-clock limit for one dbt transformation run, in seconds. Same purpose
# as PIPELINE_RUN_TIMEOUT_SECONDS and a separate knob because a dbt build is
# a different workload: a model that queries a warehouse table has no
# timeout of its own. Only enforced in subprocess mode. 0 disables.
TRANSFORMATION_RUN_TIMEOUT_SECONDS: int = 7_200
# DuckDB memory ceiling for a dbt run (a `memory_limit` SET in the generated
# profile). Past it DuckDB spills to DBT_DUCKDB_TEMP_DIR instead of growing,
# which is the difference between a slow model and a box in reclaim thrash —
# the subprocess gives the memory back afterwards, this bounds what it takes
# in the first place. Empty = leave DuckDB's default (80% of RAM, which on a
# box means 80% of everyone else's RAM too).
DBT_DUCKDB_MEMORY_LIMIT: str = "512MB"
# Where DuckDB spills once past that ceiling. Empty = the run's temp dir
# (an emptyDir on the box), which is where a spill belongs: it dies with
# the run.
DBT_DUCKDB_TEMP_DIR: str = ""
# Cap on that spill, so a runaway model fills neither RAM nor the box's
# disk — disk is the box's other silently-exhausted resource. Empty = leave
# DuckDB's default (90% of the filesystem).
DBT_DUCKDB_MAX_TEMP_SIZE: str = "4GB"
# Local-first run recording: Postgres DSN of the box-local `workspace`
# database (see workspace_db.py). When set, every run is recorded there
# first and the FairTier API report becomes best-effort. Unset = feature
# off (central-only reporting, pre-0.4.0 behavior). This is the rollback
# lever.
WORKSPACE_DB_URL: str = ""
# Rows per parquet row group for dlt's intermediate extract/normalize files.
# Bounds the worker's peak memory *before* the load stage: normalize rewrites
# one row group at a time, so an uncapped row group (dlt lets pyarrow default
# to ~1M rows) is read whole into RAM — enough to OOM a small worker before the
# (already-bounded) load stage runs. Capping it makes normalize's per-read a
# function of this value, not the dataset size. Matches the filesystem reader's
# chunksize so the whole Arrow pipeline flows in uniform chunks. 0 disables the
# bound and restores dlt defaults. See iceberg_stream.py for the matching
# load-stage bound.
DATA_WRITER_CHUNK_ROWS: int = 100_000

# AWS / S3
AWS_ACCESS_KEY_ID: str = ""
AWS_SECRET_ACCESS_KEY: str = ""
AWS_ENDPOINT_URL: str = ""
AWS_REGION: str = ""
S3_BUCKET: str = ""

# OIDC (for Lakekeeper REST catalog auth)
OIDC_CLIENT_ID: str = ""
OIDC_CLIENT_SECRET: str = ""
OIDC_TOKEN_URL: str = ""

# Lakekeeper warehouse name
LAKEKEEPER_WAREHOUSE: str = "default"

# Transformations (dbt) — optional, only used when transformations are
# configured. The hosted-repo fallback for transformations without a
# connected git repo of their own.
TRANSFORM_REPO_URL: str = ""
TRANSFORM_GIT_USERNAME: str = ""
TRANSFORM_GIT_TOKEN: str = ""


def load() -> None:
    """Read env vars and set module-level config. Also injects dlt env vars."""
    global CUSTOMER_SLUG, FAIRTIER_API_URL, LAKEKEEPER_URL
    global POLL_INTERVAL_SECONDS, DLT_STATE_DIR, HEALTHZ_PORT
    global PIPELINE_MAX_RETRIES, PIPELINE_RETRY_BASE_DELAY, SNAPSHOT_URL
    global PIPELINES_DIR, AGE_KEY_FILE, ICEBERG_LOAD_CHUNK_ROWS
    global ICEBERG_LOAD_COMMIT_EVERY
    global ICEBERG_CREDENTIAL_REFRESH_SECONDS
    global PIPELINE_SUBPROCESS
    global PIPELINE_RUN_TIMEOUT_SECONDS
    global TRANSFORMATION_SUBPROCESS
    global TRANSFORMATION_RUN_TIMEOUT_SECONDS
    global DBT_DUCKDB_MEMORY_LIMIT, DBT_DUCKDB_TEMP_DIR, DBT_DUCKDB_MAX_TEMP_SIZE
    global WORKSPACE_DB_URL
    global DATA_WRITER_CHUNK_ROWS
    global \
        AWS_ACCESS_KEY_ID, \
        AWS_SECRET_ACCESS_KEY, \
        AWS_ENDPOINT_URL, \
        AWS_REGION, \
        S3_BUCKET
    global OIDC_CLIENT_ID, OIDC_CLIENT_SECRET, OIDC_TOKEN_URL
    global LAKEKEEPER_WAREHOUSE
    global TRANSFORM_REPO_URL, TRANSFORM_GIT_USERNAME, TRANSFORM_GIT_TOKEN

    CUSTOMER_SLUG = _require("CUSTOMER_SLUG")
    FAIRTIER_API_URL = _require("FAIRTIER_API_URL")
    LAKEKEEPER_URL = _require("LAKEKEEPER_URL")
    POLL_INTERVAL_SECONDS = _int("POLL_INTERVAL_SECONDS", 60)
    DLT_STATE_DIR = os.environ.get("DLT_STATE_DIR", "/dlt-state")
    HEALTHZ_PORT = _int("HEALTHZ_PORT", 8080)
    PIPELINE_MAX_RETRIES = _int("PIPELINE_MAX_RETRIES", 2)
    PIPELINE_RETRY_BASE_DELAY = _int("PIPELINE_RETRY_BASE_DELAY", 30)
    SNAPSHOT_URL = os.environ.get("SNAPSHOT_URL", "")
    PIPELINES_DIR = _require("PIPELINES_DIR")
    AGE_KEY_FILE = os.environ.get("AGE_KEY_FILE", "")
    ICEBERG_LOAD_CHUNK_ROWS = _int("ICEBERG_LOAD_CHUNK_ROWS", 200_000)
    ICEBERG_LOAD_COMMIT_EVERY = _int("ICEBERG_LOAD_COMMIT_EVERY", 20)
    ICEBERG_CREDENTIAL_REFRESH_SECONDS = _int("ICEBERG_CREDENTIAL_REFRESH_SECONDS", 900)
    PIPELINE_SUBPROCESS = os.environ.get("PIPELINE_SUBPROCESS", "1").lower() not in (
        "0",
        "false",
        "no",
    )
    PIPELINE_RUN_TIMEOUT_SECONDS = _int("PIPELINE_RUN_TIMEOUT_SECONDS", 21_600)
    TRANSFORMATION_SUBPROCESS = os.environ.get(
        "TRANSFORMATION_SUBPROCESS", "1"
    ).lower() not in (
        "0",
        "false",
        "no",
    )
    TRANSFORMATION_RUN_TIMEOUT_SECONDS = _int(
        "TRANSFORMATION_RUN_TIMEOUT_SECONDS", 7_200
    )
    DBT_DUCKDB_MEMORY_LIMIT = os.environ.get("DBT_DUCKDB_MEMORY_LIMIT", "512MB")
    DBT_DUCKDB_TEMP_DIR = os.environ.get("DBT_DUCKDB_TEMP_DIR", "")
    DBT_DUCKDB_MAX_TEMP_SIZE = os.environ.get("DBT_DUCKDB_MAX_TEMP_SIZE", "4GB")
    WORKSPACE_DB_URL = os.environ.get("WORKSPACE_DB_URL", "")
    DATA_WRITER_CHUNK_ROWS = _int("DATA_WRITER_CHUNK_ROWS", 100_000)

    AWS_ACCESS_KEY_ID = _require("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = _require("AWS_SECRET_ACCESS_KEY")
    AWS_ENDPOINT_URL = _require("AWS_ENDPOINT_URL")
    AWS_REGION = _require("AWS_REGION")
    S3_BUCKET = _require("S3_BUCKET")

    # OIDC (optional — only needed when Lakekeeper OIDC is enabled)
    OIDC_CLIENT_ID = os.environ.get("OIDC_CLIENT_ID", "")
    OIDC_CLIENT_SECRET = os.environ.get("OIDC_CLIENT_SECRET", "")
    OIDC_TOKEN_URL = os.environ.get("OIDC_TOKEN_URL", "")

    LAKEKEEPER_WAREHOUSE = os.environ.get("LAKEKEEPER_WAREHOUSE", "default")

    # Transformations (optional — hosted-repo fallback for dbt transformations)
    TRANSFORM_REPO_URL = os.environ.get("TRANSFORM_REPO_URL", "")
    TRANSFORM_GIT_USERNAME = os.environ.get("TRANSFORM_GIT_USERNAME", "")
    TRANSFORM_GIT_TOKEN = os.environ.get("TRANSFORM_GIT_TOKEN", "")

    # Filesystem destination: bucket URL for S3/R2 storage.
    os.environ.setdefault("DESTINATION__FILESYSTEM__BUCKET_URL", f"s3://{S3_BUCKET}")

    # Filesystem credentials: dlt needs these to pass S3 endpoint and region
    # through to PyIceberg's catalog IO (used when writing Iceberg metadata).
    os.environ.setdefault(
        "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID
    )
    os.environ.setdefault(
        "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY",
        AWS_SECRET_ACCESS_KEY,
    )
    os.environ.setdefault(
        "DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL", AWS_ENDPOINT_URL
    )
    os.environ.setdefault(
        "DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME", AWS_REGION
    )

    # Bound extract- and normalize-stage memory so peak RSS is a function of
    # the chunk size, not the dataset size. Without this a source that yields a
    # large Arrow table makes dlt write a single million-row parquet row group,
    # and normalize (which rewrites one row group at a time) reads that whole
    # group into RAM — enough to OOM a small worker before the load stage even
    # starts. Capping the parquet row-group size makes normalize's per-read (and
    # the load scanner's) working set a function of the cap, not the data size.
    # This is the GLOBAL `data_writer` section on purpose: the extract writer
    # resolves its parquet config under `sources.*` and falls back to the global
    # section — a stage-scoped `EXTRACT__`/`NORMALIZE__` prefix is silently
    # ignored there (verified against dlt 1.22). We deliberately leave
    # buffer_max_items at dlt's default (5000): raising it would only *increase*
    # memory for row/dict sources, and the row-group cap already bounds the
    # Arrow path. setdefault so an explicit dlt env override still wins. See
    # iceberg_stream.py for the matching load-stage bound.
    if DATA_WRITER_CHUNK_ROWS > 0:
        os.environ.setdefault(
            "DATA_WRITER__ROW_GROUP_SIZE", str(DATA_WRITER_CHUNK_ROWS)
        )

    # Iceberg REST catalog config — dlt requires a single JSON env var for
    # dict-typed config (individual __-nested env vars are not resolved).
    catalog_uri = f"{LAKEKEEPER_URL}/catalog"
    catalog_config: dict[str, str] = {
        "type": "rest",
        "uri": catalog_uri,
        "warehouse": LAKEKEEPER_WAREHOUSE,
    }
    if OIDC_CLIENT_ID and OIDC_CLIENT_SECRET and OIDC_TOKEN_URL:
        catalog_config["oauth2-server-uri"] = OIDC_TOKEN_URL
        catalog_config["credential"] = f"{OIDC_CLIENT_ID}:{OIDC_CLIENT_SECRET}"
    os.environ.setdefault(
        "ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG",
        json.dumps(catalog_config),
    )
    os.environ.setdefault("ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE", "rest")
