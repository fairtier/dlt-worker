# dlt-worker

Worker that polls FairTier API for pipeline configurations and runs them via [dlt](https://dlthub.com/).

Published as `github.com/fairtier/dlt-worker`. Docker images at `ghcr.io/fairtier/dlt-worker`.

## Commands

```bash
# Run tests
uv run pytest -v

# Run tests with coverage
uv run pytest -v --cov=dlt_worker --cov-report=term-missing

# Lint & format
uv run ruff check .
uv run ruff format --check .

# Type check
uv run ty check

# Run locally (requires env vars — see README.md for configuration)
uv run python -m dlt_worker
```

## Linting & Type Checking

We use [ruff](https://github.com/astral-sh/ruff) for linting/formatting and [ty](https://github.com/astral-sh/ty) for type checking (both from Astral). Config is in [`pyproject.toml`](./pyproject.toml).

All code must pass `uv run ruff check .`, `uv run ruff format --check .`, and `uv run ty check` with zero errors before committing.

## Package Management

We use [uv](https://github.com/astral-sh/uv) for dependency management. The lockfile (`uv.lock`) is committed.

## Project Structure

```
src/dlt_worker/       # Application source
  __main__.py         # Entry point (python -m dlt_worker)
  config.py           # Environment variable configuration
  health.py           # Kubernetes health check server
  telemetry.py        # OpenTelemetry traces + metrics (no-op unless an OTLP endpoint is set)
  main.py             # Main poll loop with graceful shutdown
  pipeline_files.py   # Files mode: load pipeline definitions from a git checkout
  scheduler_state.py  # Files mode: worker-owned last_run_at (scheduler.json)
  pipeline_runner.py  # dlt pipeline execution
  run_isolation.py    # Subprocess-per-run: releases post-run memory, contains OOM kills
  transformation_runner.py  # dbt transformation execution (git clone + dbt build)
  workspace_db.py     # Local-first run recording into the workspace Postgres database
  api_client.py  # FairTier API HTTP client
tests/                # pytest test suite
```

## CI/CD

- **CI**: GitHub Actions runs `ruff check`, `ruff format`, `ty check`, and `pytest` on push to master and PRs; on master a separate job publishes a coverage badge to the `gh-pages` branch
- **Release**: Tag `v*` triggers Docker build + push to GHCR (linux/amd64 + linux/arm64)
