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
# Files mode: when set, pipeline definitions and schedules are read from
# <PIPELINES_DIR>/pipelines/*.yaml (a checkout kept in sync by a sidecar)
# and the API poll shrinks to triggers + credentials. Unset = legacy mode,
# the poll is the full source of truth. This is the rollback lever.
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
    POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
    DLT_STATE_DIR = os.environ.get("DLT_STATE_DIR", "/dlt-state")
    HEALTHZ_PORT = int(os.environ.get("HEALTHZ_PORT", "8080"))
    PIPELINE_MAX_RETRIES = int(os.environ.get("PIPELINE_MAX_RETRIES", "2"))
    PIPELINE_RETRY_BASE_DELAY = int(os.environ.get("PIPELINE_RETRY_BASE_DELAY", "30"))
    SNAPSHOT_URL = os.environ.get("SNAPSHOT_URL", "")
    PIPELINES_DIR = os.environ.get("PIPELINES_DIR", "")
    AGE_KEY_FILE = os.environ.get("AGE_KEY_FILE", "")
    ICEBERG_LOAD_CHUNK_ROWS = int(os.environ.get("ICEBERG_LOAD_CHUNK_ROWS", "200000"))
    ICEBERG_LOAD_COMMIT_EVERY = int(os.environ.get("ICEBERG_LOAD_COMMIT_EVERY", "20"))
    DATA_WRITER_CHUNK_ROWS = int(os.environ.get("DATA_WRITER_CHUNK_ROWS", "100000"))

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
