# dlt-worker

[![CI](https://github.com/fairtier/dlt-worker/actions/workflows/ci.yml/badge.svg)](https://github.com/fairtier/dlt-worker/actions/workflows/ci.yml)
[![Coverage](https://fairtier.github.io/dlt-worker/badges/coverage.svg)](https://github.com/fairtier/dlt-worker/actions)
[![License](https://img.shields.io/github/license/fairtier/dlt-worker)](LICENSE)

Worker that polls a control plane for pipeline configurations and runs them via [dlt](https://dlthub.com/), writing Iceberg tables to S3-compatible storage.

## How it works

1. Polls a control plane on a configurable interval for pipeline definitions
2. Evaluates cron schedules (or manual triggers) to decide which pipelines to run
3. Runs each due pipeline using [dlt](https://dlthub.com/) with Iceberg table format
4. Reports results (rows loaded, errors) back to the control plane
5. Exposes a `/healthz` endpoint for Kubernetes readiness probes

## Quick start

```bash
docker run --rm \
  -e CUSTOMER_SLUG=acme \
  -e FAIRTIER_API_URL=https://api.example.com \
  -e LAKEKEEPER_URL=https://lakekeeper.example.com \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -e AWS_ENDPOINT_URL=https://s3.example.com \
  -e AWS_REGION=us-east-1 \
  -e S3_BUCKET=my-data-lake \
  ghcr.io/fairtier/dlt-worker:latest
```

## Configuration

All configuration is via environment variables.

### Required

| Variable                | Description                                                          |
|-------------------------|----------------------------------------------------------------------|
| `CUSTOMER_SLUG`         | Customer identifier sent to the control plane                        |
| `FAIRTIER_API_URL`      | Base URL of the control plane API (Connect/JSON)                     |
| `LAKEKEEPER_URL`        | URL of the [Lakekeeper](https://lakekeeper.io/) Iceberg REST catalog |
| `AWS_ACCESS_KEY_ID`     | AWS (or S3-compatible) access key                                    |
| `AWS_SECRET_ACCESS_KEY` | AWS (or S3-compatible) secret key                                    |
| `AWS_ENDPOINT_URL`      | S3 endpoint URL (e.g. `https://s3.amazonaws.com`)                    |
| `AWS_REGION`            | S3 region (e.g. `us-east-1`)                                         |
| `S3_BUCKET`             | Target S3 bucket for Iceberg data                                    |

### Optional

| Variable                    | Default      | Description                                                                           |
|-----------------------------|--------------|---------------------------------------------------------------------------------------|
| `POLL_INTERVAL_SECONDS`     | `60`         | Seconds between control plane polls                                                   |
| `DLT_STATE_DIR`             | `/dlt-state` | Directory for dlt pipeline state (mount a volume for persistence)                     |
| `HEALTHZ_PORT`              | `8080`       | Port for the `/healthz` HTTP endpoint                                                 |
| `PIPELINE_MAX_RETRIES`      | `2`          | Max retry attempts per pipeline on failure                                            |
| `PIPELINE_RETRY_BASE_DELAY` | `30`         | Base delay in seconds for exponential backoff                                         |
| `SNAPSHOT_URL`              | _(empty)_    | URL to trigger a state snapshot sidecar after each pipeline run (disabled when empty) |
| `OIDC_CLIENT_ID`            | _(empty)_    | OIDC client ID for Lakekeeper catalog auth **and** FairTier API bearer auth           |
| `OIDC_CLIENT_SECRET`        | _(empty)_    | OIDC client secret for Lakekeeper catalog auth **and** FairTier API bearer auth       |
| `OIDC_TOKEN_URL`            | _(empty)_    | OIDC token endpoint for Lakekeeper catalog auth **and** FairTier API bearer auth      |
| `LAKEKEEPER_WAREHOUSE`      | `default`    | Lakekeeper warehouse name                                                             |
| `TRANSFORM_REPO_URL`        | _(empty)_    | HTTPS clone URL of the hosted dbt repo (used when a transformation has no repo of its own) |
| `TRANSFORM_GIT_USERNAME`    | _(empty)_    | Git username for the hosted dbt repo                                                  |
| `TRANSFORM_GIT_TOKEN`       | _(empty)_    | Git token for the hosted dbt repo                                                     |

## Supported source types

The control plane provides pipeline configurations that specify one of these source types:

- **`sql_database`** -- Reads from SQL databases (PostgreSQL, MySQL, etc.) via [dlt's sql_database source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database). Supports incremental loading with cursor-based tracking.
- **`rest_api`** -- Reads from HTTP/REST APIs via [dlt's rest_api source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api). Supports pagination, auth (bearer, OAuth2, HTTP basic), and incremental loading.
- **`filesystem`** -- Reads files from S3-compatible storage via [dlt's filesystem source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem).

## Transformations (dbt)

Besides ingestion pipelines, the worker runs [dbt](https://www.getdbt.com/) transformations. The control plane provides transformation configs (git repo, ref, schedule); for each due transformation the worker:

1. Shallow-clones the dbt project -- either a connected repo with its own credentials, or the hosted repo from `TRANSFORM_REPO_URL`
2. Generates `profiles.yml` at run time -- credentials never live in git; data-file access uses credentials vended by the [Lakekeeper](https://lakekeeper.io/) REST catalog
3. Runs `dbt build` against DuckDB with the Iceberg catalog attached
4. Reports per-model and per-test results back to the control plane

Transformations run on a cron schedule, on manual trigger, or chained after a successful pipeline run.

## Development

Requires Python 3.12+ and [uv](https://github.com/astral-sh/uv).

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest tests/ -v

# Type check (using ty from Astral)
uv run ty check

# Run locally
uv run python -m dlt_worker
```

## License

Apache-2.0 -- see [LICENSE](LICENSE).
