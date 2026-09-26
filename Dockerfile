############################
# STEP 1: Build with uv
############################
FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.12.15 /uv /usr/local/bin/uv

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
# STEP 1b: dbt-oss + the DuckDB driver it runs on
############################
# dbt runs as a subprocess of the run child, not as a Python package: the
# Apache-2.0 dbt-oss binary, a DuckDB ADBC driver installed by dbc (the
# driver bundled with dbt loads no extensions), and the extensions dbt's
# profile LOADs, baked for exactly that DuckDB build. Nothing is fetched at
# run time — the box has no business reaching extensions.duckdb.org.
FROM python:3.14-slim AS dbt
ARG TARGETARCH
ARG DBT_VERSION=2.0.5
ARG DBC_VERSION=0.3.0
ARG DUCKDB_DRIVER_VERSION=1.5.4
ARG DRIVER_SHA256_AMD64=d7f30ef2ef4b813edb94ce82906329cc689672624a4161617ea33431040ce174
ARG DRIVER_SHA256_ARM64=e8cb8c234ade5e6ccd722d727725cb38582ebe4554630dbec66f874155db246a
# curl (not duckdb.install_extension()) fetches the three extensions below:
# DuckDB's own minimal HTTP client is unreliable against
# extensions.duckdb.org from some networks (a bare connection-read failure
# partway through the transfer, no retry), where curl with retries succeeds.
# The extension files it serves are DuckDB Labs' own signed builds either
# way — curl only replaces the transport, LOAD still verifies the signature.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) T=x86_64-unknown-linux-gnu; SUM=$DRIVER_SHA256_AMD64 ;; \
      arm64) T=aarch64-unknown-linux-gnu; SUM=$DRIVER_SHA256_ARM64 ;; \
      *) echo "unsupported arch $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    F="dbt-core-${DBT_VERSION}-${T}.tar.gz"; \
    B="https://github.com/dbt-labs/dbt/releases/download/v${DBT_VERSION}"; \
    cd /tmp; \
    curl -fSL --connect-timeout 20 --max-time 180 --retry 3 --retry-delay 2 -o "$F" "$B/$F"; \
    curl -fSL --connect-timeout 20 --max-time 180 --retry 3 --retry-delay 2 -o SHA256SUMS "$B/SHA256SUMS"; \
    grep " \*\?${F}\$" SHA256SUMS | sed 's/ \*/  /' | sha256sum -c -; \
    mkdir -p /opt/dbt && tar -xzf "$F" -C /opt/dbt; \
    cp "$(find /opt/dbt -type f -name dbt -perm -u+x | head -n1)" /usr/local/bin/dbt; \
    /usr/local/bin/dbt --version; \
    pip install --no-cache-dir "dbc==${DBC_VERSION}" "duckdb==${DUCKDB_DRIVER_VERSION}"; \
    dbc install --level system "duckdb=${DUCKDB_DRIVER_VERSION}"; \
    echo "${SUM}  /etc/adbc/drivers/duckdb_linux_${TARGETARCH}_v${DUCKDB_DRIVER_VERSION}/libduckdb.so" | sha256sum -c -; \
    EXTDIR="/opt/dbt-duckdb/.duckdb/extensions/v${DUCKDB_DRIVER_VERSION}/linux_${TARGETARCH}"; \
    mkdir -p "$EXTDIR"; \
    for ext in iceberg httpfs avro; do \
      curl -fsSL --connect-timeout 20 --max-time 180 --retry 5 --retry-delay 2 \
        "https://extensions.duckdb.org/v${DUCKDB_DRIVER_VERSION}/linux_${TARGETARCH}/${ext}.duckdb_extension.gz" \
        | gunzip > "$EXTDIR/${ext}.duckdb_extension"; \
    done; \
    HOME=/opt/dbt-duckdb python -c "\
import duckdb; \
c = duckdb.connect(); \
c.execute('SET autoinstall_known_extensions=false'); \
c.execute('SET autoload_known_extensions=false'); \
[c.execute(f'LOAD {e}') for e in ('iceberg', 'httpfs', 'avro')]"; \
    ls "$EXTDIR"

############################
# STEP 2: Runtime image
############################
FROM python:3.14-slim

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

# dbt-oss and its DuckDB (see the dbt stage). /opt/dbt-duckdb is the
# read-only extension home a run's HOME links to (transformation_runner).
COPY --from=dbt /usr/local/bin/dbt /usr/local/bin/dbt
COPY --from=dbt /etc/adbc /etc/adbc
COPY --from=dbt /opt/dbt-duckdb /opt/dbt-duckdb

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
