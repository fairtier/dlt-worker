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
PLATFORM_API_URL: str = ""
LAKEKEEPER_URL: str = ""
POLL_INTERVAL_SECONDS: int = 60
DLT_STATE_DIR: str = "/dlt-state"
HEALTHZ_PORT: int = 8080
PIPELINE_MAX_RETRIES: int = 2
PIPELINE_RETRY_BASE_DELAY: int = 30
SNAPSHOT_URL: str = ""

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


def load() -> None:
    """Read env vars and set module-level config. Also injects dlt env vars."""
    global CUSTOMER_SLUG, PLATFORM_API_URL, LAKEKEEPER_URL
    global POLL_INTERVAL_SECONDS, DLT_STATE_DIR, HEALTHZ_PORT
    global PIPELINE_MAX_RETRIES, PIPELINE_RETRY_BASE_DELAY, SNAPSHOT_URL
    global \
        AWS_ACCESS_KEY_ID, \
        AWS_SECRET_ACCESS_KEY, \
        AWS_ENDPOINT_URL, \
        AWS_REGION, \
        S3_BUCKET
    global OIDC_CLIENT_ID, OIDC_CLIENT_SECRET, OIDC_TOKEN_URL

    CUSTOMER_SLUG = _require("CUSTOMER_SLUG")
    PLATFORM_API_URL = _require("PLATFORM_API_URL")
    LAKEKEEPER_URL = _require("LAKEKEEPER_URL")
    POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
    DLT_STATE_DIR = os.environ.get("DLT_STATE_DIR", "/dlt-state")
    HEALTHZ_PORT = int(os.environ.get("HEALTHZ_PORT", "8080"))
    PIPELINE_MAX_RETRIES = int(os.environ.get("PIPELINE_MAX_RETRIES", "2"))
    PIPELINE_RETRY_BASE_DELAY = int(os.environ.get("PIPELINE_RETRY_BASE_DELAY", "30"))
    SNAPSHOT_URL = os.environ.get("SNAPSHOT_URL", "")

    AWS_ACCESS_KEY_ID = _require("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = _require("AWS_SECRET_ACCESS_KEY")
    AWS_ENDPOINT_URL = _require("AWS_ENDPOINT_URL")
    AWS_REGION = _require("AWS_REGION")
    S3_BUCKET = _require("S3_BUCKET")

    # OIDC (optional — only needed when Lakekeeper OIDC is enabled)
    OIDC_CLIENT_ID = os.environ.get("OIDC_CLIENT_ID", "")
    OIDC_CLIENT_SECRET = os.environ.get("OIDC_CLIENT_SECRET", "")
    OIDC_TOKEN_URL = os.environ.get("OIDC_TOKEN_URL", "")

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

    # Iceberg REST catalog config — dlt requires a single JSON env var for
    # dict-typed config (individual __-nested env vars are not resolved).
    catalog_uri = f"{LAKEKEEPER_URL}/catalog"
    catalog_config: dict[str, str] = {
        "type": "rest",
        "uri": catalog_uri,
        "warehouse": os.environ.get("LAKEKEEPER_WAREHOUSE", "default"),
    }
    if OIDC_CLIENT_ID and OIDC_CLIENT_SECRET and OIDC_TOKEN_URL:
        catalog_config["oauth2-server-uri"] = OIDC_TOKEN_URL
        catalog_config["credential"] = f"{OIDC_CLIENT_ID}:{OIDC_CLIENT_SECRET}"
    os.environ.setdefault(
        "ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG",
        json.dumps(catalog_config),
    )
    os.environ.setdefault("ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE", "rest")
