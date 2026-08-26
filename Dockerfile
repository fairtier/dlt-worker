############################
# STEP 1: Build with uv
############################
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.10.10 /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first (cache layer)
COPY pyproject.toml uv.lock ./

# Install dependencies only, skip building the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --no-install-project

# Copy source code and metadata
COPY README.md ./
COPY src/ src/

# Install the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# Bake the DuckDB extensions the `duckdb` source type LOADs
# (SUPPORTED_DUCKDB_EXTENSIONS in duckdb_source.py — the single source of
# truth), keyed to the duckdb wheel's own version/platform. No run-time
# egress to extensions.duckdb.org, and deliberately NOT the DuckFlight
# extension image: those binaries are ABI-locked to a different DuckDB.
RUN /app/.venv/bin/python -c "\
import duckdb; \
from dlt_worker.duckdb_source import SUPPORTED_DUCKDB_EXTENSIONS; \
con = duckdb.connect(config={'extension_directory': '/opt/duckdb-extensions'}); \
[con.install_extension(name) if repo == 'core' else con.install_extension(name, repository=repo) \
 for name, repo in SUPPORTED_DUCKDB_EXTENSIONS.items()]"

############################
# STEP 2: Runtime image
############################
FROM python:3.12-slim

# git is needed at run time to shallow-clone dbt transformation repos
# (the builder stage doesn't need it — no git dependencies in uv.lock)
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Copy the virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# The baked DuckDB extension set (read-only for the runtime user, which is
# the point: LOAD finds them, nothing can grow the set at run time).
COPY --from=builder /opt/duckdb-extensions /opt/duckdb-extensions
ENV DUCKDB_EXTENSION_DIR=/opt/duckdb-extensions

# Set PATH to use the venv
ENV PATH="/app/.venv/bin:$PATH"

# Unbuffer stdout/stderr so poll-loop and pipeline logs stream to the
# container log in real time. Without it Python block-buffers when stdout is a
# pipe (k8s), so quiet polls emit nothing and only a multi-KB crash traceback
# flushes — which is why the 0.0.6 file-drop pandas crash was invisible in
# central Loki and only surfaced via live SSH.
ENV PYTHONUNBUFFERED=1

# Non-root user (pinned UID/GID for K8s fsGroup)
RUN groupadd --gid 1000 dlt && useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash dlt
USER dlt:dlt

WORKDIR /app

EXPOSE 8080

ENTRYPOINT ["python", "-m", "dlt_worker"]
