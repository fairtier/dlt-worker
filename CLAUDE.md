# dlt-worker

Worker that polls Platform API for pipeline configurations and runs them via [dlt](https://dlthub.com/).

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
  main.py             # Main poll loop with graceful shutdown
  pipeline_runner.py  # dlt pipeline execution
  platform_client.py  # Platform API HTTP client
tests/                # pytest test suite
```

## CI/CD

- **CI**: GitHub Actions runs `ruff check`, `ruff format`, `ty check`, and `pytest` (with coverage upload to Codecov) on push to master and PRs
- **Release**: Tag `v*` triggers Docker build + push to GHCR (linux/amd64 + linux/arm64)
