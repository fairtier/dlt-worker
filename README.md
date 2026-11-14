# dlt-worker

[![CI](https://github.com/fairtier/dlt-worker/actions/workflows/ci.yml/badge.svg)](https://github.com/fairtier/dlt-worker/actions/workflows/ci.yml)
[![Coverage](https://fairtier.github.io/dlt-worker/badges/coverage.svg)](https://github.com/fairtier/dlt-worker/actions)
[![License](https://img.shields.io/github/license/fairtier/dlt-worker)](LICENSE)

Worker that runs declarative data pipelines via [dlt](https://dlthub.com/), writing Iceberg tables to S3-compatible storage. Pipeline definitions come from local YAML files kept in sync by a git sidecar; the control plane poll carries only what those files cannot.

## How it works

1. Reads pipeline definitions from `$PIPELINES_DIR/pipelines/*.yaml` (a checkout kept in sync by a git sidecar)
2. Evaluates cron schedules (or manual triggers) to decide which pipelines to run; the worker owns `last_run_at` locally (`scheduler.json` in the state dir), so scheduling keeps working when the control plane is unreachable
3. Runs each due pipeline using [dlt](https://dlthub.com/) with Iceberg table format
4. Runs due workflows — linear sequences of pipelines and transformations (see [Workflows](#workflows))
5. Records every run in a local `workspace` Postgres database first when `WORKSPACE_DB_URL` is set (local-first mode), then reports results (rows loaded, errors) back to the control plane (best-effort)
6. Exposes a `/healthz` endpoint for Kubernetes readiness probes, gated on what it actually schedules from — the checkout (see below)

The control plane poll is still made every tick, but only manual ("Run now") triggers, the last-run watermark and source credentials are consumed from it — definitions, schedules, and enablement come from the files. Credentials are cached in memory only, so a pipeline whose credentials were seen at least once keeps running through a control plane outage.

As of 0.9.0 this is the only mode: `PIPELINES_DIR` is required, and the legacy poll-is-truth mode was retired together with the definition fields it read (`source_config`, `schedule`, `write_disposition`, `enabled`) — `GetPipelineConfigs` no longer returns them. Running 0.9.0 against a pre-shrink control plane is fine; the extra fields are ignored. Running an older worker against a shrunk one is not: it sees every pipeline as scheduleless.

Because of that split, `/healthz` no longer follows the poll (as of 0.9.0). An unreachable control plane costs triggers and run history while scheduled ingestion keeps firing, so the endpoint stays `200` and reports the outage as `"api_healthy": false` alongside `"mode": "files"`. What gates readiness instead is the checkout itself — a missing `$PIPELINES_DIR/pipelines` is the one state in which the worker has no schedule at all, and is the only `503` it returns. Only a readiness probe reads this, so an unready worker is reported, never restarted.

A manual "Run now" also outranks `enabled` on a **pipeline** (0.9.0): disabling stops the schedule, not a run someone just asked for, and the control plane already hands out a pending run for a disabled pipeline — so refusing it here left that run pending forever. This does not weaken file truth: the poll cannot carry an `enabled` flag at all, and the file's is what every other path reads. Transformations are deliberately stricter — disabled means never, by schedule, chain, or trigger.

When `AGE_KEY_FILE` additionally points at an [age](https://age-encryption.org/) identity file, source credentials are read from `pipelines/<name>.credentials.age` in the checkout (armored age ciphertext of the credentials JSON, encrypted to this worker's public key) and take precedence over polled credentials. A pipeline with a credential file then runs fully control-plane-independent — even a fresh worker process during an outage. A missing or undecryptable credential file degrades that one pipeline to polled/cached credentials. The companion `python -m dlt_worker.agekey <outdir>` command generates the keypair (used by the box seed job).

When `WORKSPACE_DB_URL` is set (local-first run recording), every pipeline and transformation run is written to that Postgres database — a `running` row before execution, the outcome after — and the control plane report becomes strictly best-effort (a few bounded retries, then log-and-continue). The local row is the record; the central one is only a cache. A run picked up from a central "Run now" trigger reuses the trigger's run id locally, so the same run has one identity in both stores.

**Run start vs. schedule baseline.** Everything in a tick runs one after another, so a run queued behind a long one starts long after the tick did. Each pipeline and transformation run — standalone or as a workflow step — stamps its own start, and the local `running` row, the central "running" mark and the final report all carry that one value (across retries too), so durations are honest and the stuck-run sweep, which ages rows from `started_at`, cannot fail a short run for the time it waited. The schedule baseline is the tick's time instead: `scheduler.json`'s `last_run_at` and the failure backoff record the tick that admitted the run, so an hourly pipeline queued behind a 20-minute one still fires on the hour.

The worker never migrates that schema (it's owned by the deployment) and writes explicit column lists only; on startup and hourly it finalizes its own orphaned `running` rows older than 2 hours (a crashed worker's leftovers). The 2 hours is not a limit on run length (a run may take `PIPELINE_RUN_TIMEOUT_SECONDS` per attempt). The sweep never fires while this worker has a run in flight, because it runs only at startup and between poll ticks, and the chart runs one worker per workspace database. So every `running` row it sees is an orphan. Combined with the definition checkout and age credential files, ingestion, scheduling, and run history all keep working with no control plane at all.

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
  -e PIPELINES_DIR=/pipelines \
  -v "$PWD/checkout:/pipelines:ro" \
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
| `PIPELINES_DIR`         | Checkout root holding the `pipelines/*.yaml` definitions the worker schedules from (required since 0.9.0) |

### Optional

| Variable                    | Default      | Description                                                                           |
|-----------------------------|--------------|---------------------------------------------------------------------------------------|
| `POLL_INTERVAL_SECONDS`     | `60`         | Seconds between control plane polls                                                   |
| `DLT_STATE_DIR`             | `/dlt-state` | Directory for dlt pipeline state (mount a volume for persistence)                     |
| `HEALTHZ_PORT`              | `8080`       | Port for the `/healthz` HTTP endpoint                                                 |
| `PIPELINE_MAX_RETRIES`      | `2`          | Max retry attempts per pipeline on failure. Since 0.16.0 each retry re-reads the pipeline's definition from the checkout and re-polls its credentials first, so a fix saved while the run was failing is picked up by the next attempt instead of being repeated against — the checkout syncs on its own ~1-minute cadence, which the backoff is long enough to cross |
| `PIPELINE_RETRY_BASE_DELAY` | `30`         | Base delay in seconds for exponential backoff                                         |
| `SNAPSHOT_URL`              | _(empty)_    | URL to trigger a state snapshot sidecar after each pipeline run (disabled when empty) |
| `AGE_KEY_FILE`              | _(empty)_    | Path to the age identity file for decrypting `pipelines/*.credentials.age`; unset = credentials come from the poll only |
| `WORKSPACE_DB_URL`          | _(empty)_    | Postgres DSN of the local `workspace` database for local-first run recording — every run is recorded there first and the control plane report becomes best-effort (bounded retries); unset = central-only reporting |
| `ICEBERG_LOAD_CHUNK_ROWS`   | `200000`     | Rows per chunked Iceberg append — bounds peak memory during the load stage so large loads stream instead of materializing in RAM; `0` restores dlt's load-everything behavior. This is a real ceiling as of 0.8.0: before it, a source batch whose tail left a partial buffer let the next chunk reach `2×` this value (observed 298,963 against a nominal 200,000) |
| `ICEBERG_LOAD_COMMIT_EVERY` | `20`         | Commit the streamed Iceberg load every N appends — PyIceberg holds each appended data file's metadata in the open transaction until commit, so across hundreds of chunks that alone can OOM a small worker; `0` keeps a single atomic commit at the end. Extra snapshots are reaped by the maintenance CronJob's snapshot expiry |
| `ICEBERG_CREDENTIAL_REFRESH_SECONDS` | `900` | Re-open the Iceberg table once its storage credentials are this old. With credential vending the catalog mints temporary S3 credentials and PyIceberg never renews them (the response carries no expiry to renew against), so a load that outlives them fails on a metadata write with `ACCESS_DENIED`/`SIGNATURE_DOES_NOT_MATCH` — which dlt retries as transient, restarting a `replace` from zero, forever. Only takes effect at an interim commit, so it needs `ICEBERG_LOAD_COMMIT_EVERY > 0`; `0` disables |
| `DATA_WRITER_CHUNK_ROWS`    | `100000`     | Rows per parquet row group for dlt's intermediate extract/normalize files (sets the global `DATA_WRITER__ROW_GROUP_SIZE`) — bounds peak memory *before* the load stage, since normalize rewrites one row group at a time; `0` restores dlt's default (unbounded row groups) |
| `PIPELINE_SUBPROCESS`       | `1`          | Run each pipeline in a short-lived spawned subprocess so the memory a big run leaves behind (Arrow buffers, dlt caches) is returned to the OS at process exit, and an OOM kill takes down the run — reported as a failed run — instead of the worker; `0` runs pipelines in-process (pre-0.3.0 behavior) |
| `PIPELINE_RUN_TIMEOUT_SECONDS` | `21600`   | Wall-clock limit for one pipeline run attempt — on expiry the run subprocess is killed and a failed run is reported, so a source stuck in a read can't wedge the poll loop forever; only enforced in subprocess mode; `0` disables |
| `TRANSFORMATION_RUN_TIMEOUT_SECONDS` | `7200` | Wall-clock limit for one dbt run — a model querying the warehouse has no timeout of its own; on expiry the run's whole process group is killed, dbt included; `0` disables |
| `SOURCE_TEST_POLL_SECONDS`  | `10`         | How often to claim queued source tests ("Test connection" in the Console) between config polls — much shorter than `POLL_INTERVAL_SECONDS` because a person is watching a spinner. `0` disables source tests |
| `SOURCE_TEST_TIMEOUT_SECONDS` | `60`       | Wall-clock limit for one probe. A test that has not answered in a minute has answered: the source is not reachable from this box. Only enforced in subprocess mode; `0` disables |
| `SOURCE_TEST_SUBPROCESS`    | `1`         | Run each probe in a short-lived spawned subprocess. A probe opens exactly the connections a wrong config makes hang; `0` runs it in the poll loop |
| `DBT_DUCKDB_MEMORY_LIMIT`   | `512MB`      | DuckDB `memory_limit` for a dbt run. Unbounded, DuckDB sizes its buffer manager from *host* RAM — which on a single-node box is everyone else's RAM too; bounded, it spills instead. Empty leaves DuckDB's default |
| `DBT_DUCKDB_TEMP_DIR`       | _(empty)_    | Where DuckDB spills past that ceiling; empty = the run's temp dir, so a killed build leaves no spill behind |
| `DBT_DUCKDB_MAX_TEMP_SIZE`  | `4GB`        | Cap on that spill, so a runaway model exhausts neither RAM nor the box's disk. Empty leaves DuckDB's default (90% of the filesystem) |
| `DBT_DUCKDB_THREADS`        | `2`          | DuckDB's own thread count for a dbt run. Left alone DuckDB takes the host's core count, and each thread holds its own scan and writer state — enough, on a many-core host, to run a tightly bounded build out of memory. Not the profile's `threads:`, which is dbt's model concurrency. Empty leaves DuckDB's default |
| `DBT_S3_UPLOADER_MAX_FILESIZE` | `1GB`     | Sizes httpfs's S3 multipart part buffer (this ÷ 10000), one per file being written. DuckDB's default (`800GB`) makes it 76.5 MiB per Iceberg data file; `1GB` clamps it to 5 MiB. Applied with `SET GLOBAL` from an `on-run-start` hook, because a profile `SET` of it does not reach dbt's statements. Empty leaves DuckDB's default |
| `DBT_STAGE_CREATE_TABLES`   | `0`          | `1` builds `lake` tables with a CTAS in place (`stage_create_tables`) instead of an empty `CREATE` followed by `INSERT` |
| `DBT_DUCKDB_HOME`           | `/opt/dbt-duckdb` | Directory whose `.duckdb/extensions` holds the extensions baked for dbt's DuckDB; a run's `HOME` links to it |
| `PIPELINE_DUCKDB_MEMORY_LIMIT` | `512MB`   | DuckDB `memory_limit` for a `duckdb`-source extraction — same reasoning as the dbt bound, and safe to keep tight because the extraction streams batches out rather than materializing. Empty leaves DuckDB's default |
| `PIPELINE_DUCKDB_TEMP_DIR`  | _(empty)_    | Where a `duckdb`-source extraction spills past that ceiling; empty = a per-pipeline directory under the system temp dir, wiped on the next run |
| `PIPELINE_DUCKDB_MAX_TEMP_SIZE` | `4GB`    | Cap on that spill. Empty leaves DuckDB's default |
| `DUCKDB_EXTENSION_DIR`      | _(empty)_    | Where the `duckdb` source type finds its extension binaries; the image bakes its supported set there at build time. Empty = DuckDB's default directory, where a missing extension is autoinstalled from the official repositories |
| `OIDC_CLIENT_ID`            | _(empty)_    | OIDC client ID for Lakekeeper catalog auth **and** FairTier API bearer auth           |
| `OIDC_CLIENT_SECRET`        | _(empty)_    | OIDC client secret for Lakekeeper catalog auth **and** FairTier API bearer auth       |
| `OIDC_TOKEN_URL`            | _(empty)_    | OIDC token endpoint for Lakekeeper catalog auth **and** FairTier API bearer auth      |
| `LAKEKEEPER_WAREHOUSE`      | `default`    | Lakekeeper warehouse name                                                             |
| `TRANSFORM_REPO_URL`        | _(empty)_    | HTTPS clone URL of the hosted dbt repo (used when a transformation has no repo of its own) |
| `TRANSFORM_GIT_USERNAME`    | _(empty)_    | Git username for the hosted dbt repo                                                  |
| `TRANSFORM_GIT_TOKEN`       | _(empty)_    | Git token for the hosted dbt repo                                                     |

### OpenTelemetry

Telemetry is configured with the [standard OTel environment variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/) — the worker mirrors none of them into its own config.

| Variable                       | Default      | Description                                                                                     |
|--------------------------------|--------------|-------------------------------------------------------------------------------------------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT`  | _(empty)_    | OTLP/HTTP collector base URL (e.g. `http://otel-collector:4318`). **This is the on switch**: unset = no SDK is installed at all and instrumentation is a no-op |
| `OTEL_EXPORTER_OTLP_HEADERS`   | _(empty)_    | Extra OTLP headers, e.g. authentication for a hosted collector                                  |
| `OTEL_SERVICE_NAME`            | `dlt-worker` | Service name on every span and metric                                                           |
| `OTEL_RESOURCE_ATTRIBUTES`     | _(empty)_    | Extra resource attributes (`k8s.namespace.name=…,deployment.environment=…`)                     |
| `OTEL_METRIC_EXPORT_INTERVAL`  | `60000`      | Metric export interval in ms                                                                    |
| `OTEL_TRACES_SAMPLER`          | `parentbased_always_on` | Sampler; a run trace is a handful of spans, so sampling is rarely worth it            |
| `OTEL_SDK_DISABLED`            | `false`      | Kill switch — `true` disables export even with an endpoint configured                           |

See [Observability](#observability) for what is emitted.

## Supported source types

The control plane provides pipeline configurations that specify one of these source types:

- **`sql_database`** -- Reads from SQL databases (PostgreSQL, MySQL, etc.) via [dlt's sql_database source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database). Supports incremental loading with cursor-based tracking.
- **`rest_api`** -- Reads from HTTP/REST APIs via [dlt's rest_api source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api). Supports pagination, auth (bearer, OAuth2, HTTP basic), and incremental loading.
- **`filesystem`** -- Reads files from S3-compatible storage via [dlt's filesystem source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem).
- **`duckdb`** -- Reads through a [DuckDB extension](https://duckdb.org/docs/stable/core_extensions/overview) and streams Arrow batches into the normal dlt load path. Config: `extension` (the image bakes `mysql`, `mssql`, `pdf`, `webbed`, `gdrive` (a `gdrive://` virtual filesystem for the readers), and the `httpfs` helper — anything else is autoinstalled at run time when egress allows) or `extensions` (a list, for when one extension's reader has to run over another's filesystem: `["gdrive", "pdf"]` reads a PDF in Google Drive, because DuckDB autoloads no community extension's functions — ordered, the first is the ATTACH `TYPE` and a secret's default type). Exactly one of the two, never both. `attach` (optional ATTACH template with `{placeholder}`s, attached read-only as `src`), `tables` (required; each with `name`, optional `query` defaulting to `SELECT * FROM src."<name>"`, optional `cursor_column`/`initial_value` for incremental loading, optional `primary_key` for merge). Credentials: `attach_params` (fills the ATTACH `{placeholder}`s) and/or `secret` (rendered as a DuckDB `CREATE SECRET`, `type` defaulting to the extension name). Extraction runs in a bounded in-memory DuckDB (`PIPELINE_DUCKDB_*` knobs) that spills to disk rather than growing.
- **`google_sheets`** -- Reads spreadsheet ranges via the [Google Sheets API v4](https://developers.google.com/sheets/api) (read-only scope). Config: `spreadsheet_url_or_id` (required), `range_names` (optional -- tab names, `Tab!A1:D` ranges, or named ranges; defaults to every tab). Credentials: exactly one of `oauth` (a `{client_id, client_secret, refresh_token}` delegated-user grant from the "Sign in with Google" flow — the FairTier API injects the central client pair before serving) or `service_account_key` (a GCP service-account key JSON; share the spreadsheet read-only with the key's `client_email`). The first row of each range is used as the header row; one table is loaded per range.

## Source tests ("Test connection")

The Console can ask "can this thing read my source?" before a pipeline is ever
saved. The probe runs **here**, not in the FairTier API: this is where the
drivers, the baked DuckDB extensions, the box's network path and the
credentials are, and a test that ran anywhere else would be testing a different
thing than the one that runs at 03:00.

So it is a queued job rather than a call. Between config polls the worker
claims its tenant's queued tests (`GetPendingSourceTests`, every
`SOURCE_TEST_POLL_SECONDS`), probes each one in the same subprocess isolation a
run uses, and reports back (`ReportSourceTest`). Claiming happens server-side in
one statement, so two overlapping polls never probe the same source twice.

A probe opens the source, reads **at most one row per table** (10 tables at
most), lands nothing, and reports a line per table — "connected" and "connected,
and the table you named is there" being different answers. `duckdb` and
`sql_database` have probes; any other type reports that it cannot be tested
here. Every message goes through the same credential scrubber a failed run does.

## Transformations (dbt)

Besides ingestion pipelines, the worker runs [dbt](https://www.getdbt.com/) transformations. The control plane provides transformation configs (git repo, ref, schedule); for each due transformation the worker:

1. Shallow-clones the dbt project -- either a connected repo with its own credentials, or the hosted repo from `TRANSFORM_REPO_URL`
2. Prepares the clone (never the repo): merges a `lake` catalog pointing at the box's Lakekeeper into `catalogs.yml` (the repo's other catalogs are kept; its own `lake`, if any, is replaced), sets `flags.use_catalogs_v2` and a project-wide `+catalog_name: lake` when the project doesn't, and puts `on-run-start` hooks ahead of the project's own that apply `DBT_S3_UPLOADER_MAX_FILESIZE` and fail the run if a DuckDB bound did not take
3. Generates `profiles.yml` (DuckDB bounds + the catalog's OAuth2 client) and runs `dbt deps` if the project declares packages, then `dbt build` — the [dbt-oss](https://github.com/dbt-labs/dbt) 2.x binary, as a subprocess whose environment is an allowlist (no worker credential is reachable from a model's `env_var()`), with telemetry off and the DuckDB driver and extensions baked into the image (nothing is fetched at run time)
4. Reads the per-node outcome from `target/run_results.json` and reports it

Transformations run on a cron schedule, on manual trigger, or chained after a successful pipeline run.

## Workflows

A workflow (workspace-api `workflow.v1.WorkflowService`) is a named, **linear** list of steps, each an existing pipeline or transformation, with one optional cron schedule. The worker fetches them every tick with `GetWorkflowConfigs` and runs the due ones; a workspace-api that predates workflows answers 404/Unimplemented, which the worker treats as "no workflows" (logged once).

- **Due:** a manual trigger (`trigger_now`) is due even when the workflow is disabled; otherwise a disabled or schedule-less workflow is not; otherwise its cron is evaluated from the server's `last_run_at` (the start of the last run *whatever its outcome*, so a failing workflow waits for its next slot); never run means due. The server stores only five-field cron for workflows (`@daily`-style macros are refused at save time), so the worker's evaluation needs nothing extra.
- **One run, one id:** the pending run's id for a manual run, otherwise a fresh UUID, used for every report. The first report (`running`, every step `pending`) is what clears the trigger server-side.
- **Steps run exactly like standalone runs** — same subprocess isolation, pipeline retries, local-first rows and `ReportPipelineRun` / `ReportTransformationRun`; the step records the id of the run it produced, and reports it already with the step's `running` report (sent once that run has its id and local row, before it executes), so a worker killed mid-step leaves the step linked to its run. A step that fails or reuses a run without running anything skips the `running` report and goes straight to its outcome. Pipelines are looked up in the checkout (definitions) with credentials resolved as for a scheduled run; transformations among the tick's `GetTransformationConfigs`. A target that is missing, disabled or has no usable credentials fails its step without running. "Disabled" is the definition file's `enabled` for a pipeline — it wins over the server's database-derived `ref_enabled`, which can lag the checkout — and `ref_enabled` for a transformation.
- **One run at a time:** the server leaves a workflow whose latest run is still `running` out of `GetWorkflowConfigs` (until that run is reported finished or its stuck-run sweep fails it after 8h), so the worker never starts a second run beside one. Workflow and step timestamps are the run's own (see "Run start vs. schedule baseline" above).
- **Stop on first failure:** the remaining steps are `skipped` and the run is `failed`, its error naming the step. The workflow run is re-reported (all steps) after every transition; a report that fails is not retried in place — the next transition carries everything. An undelivered *final* report is retried at the start of later ticks (up to 10), holding that workflow back meanwhile so a manual run cannot run twice — but only when the failure is transient. A report the API refuses for good (`InvalidArgument`: malformed; `NotFound`: the workflow was deleted or the run id is another workflow's) fails the same way every time, so it is dropped at once with an error log and a `workflow.report_dropped` event. `Unauthenticated` / `PermissionDenied` count as transient: an operator fix can land within the retry window.

**Order within a tick and double runs.** A tick runs pipelines, then workflows, then transformations, and nothing runs twice in it:

- A step whose pipeline already ran this tick by its own schedule or trigger (or whose target an earlier workflow ran) takes that run's outcome instead of running again — a failure included; that run already exhausted its retries.
- A transformation a step ran is not run again by its own schedule, chain or trigger in the same tick. If it had a manual trigger pending, the step's run *is* that run (reported under the pending id).
- A pipeline a step ran moves its own `scheduler.json` baseline on success (and its failure backoff on failure) exactly like a standalone run — to the tick's time, not the step's start, so its own cron neither fires again right after nor drifts later; a transformation's baseline is server-side and moves with the reported run.
- A pipeline a step ran successfully still triggers its `trigger_after_pipeline` transformations later in the same tick — unless that transformation was itself a step, which already ran it.

The intended setup is still to clear the step targets' own schedules: across ticks a target with its own cron can run both on its own and inside the workflow.

**Crash safety.** Workflow runs are reported to the API only; there is no local-first workflow row. When `WORKSPACE_DB_URL` is the workspace-api's own database (the workflow tables exist there), the startup/hourly stale-run sweep also fails `running` workflow runs older than 2 hours — the step that was running `failed`, the ones that never ran `skipped`. Against a standalone store without those tables the sweep skips them.

## Observability

Besides `/healthz` and structured logs, the worker emits OpenTelemetry traces and metrics over OTLP/HTTP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. With it unset nothing is exported and no SDK is installed — the instrumentation compiles down to no-op API calls, so leaving telemetry off costs nothing.

**Traces.** One trace per poll tick. `dlt_worker.poll` is the root; under it sit the control plane RPCs (`pipeline.v1.PipelineService/GetPipelineConfigs`, …), one `dlt_worker.pipeline.run` per due pipeline (with a `dlt_worker.pipeline.attempt` child per retry), and one `dlt_worker.transformation.run` per due transformation (with `dlt_worker.git.clone`, `dlt_worker.dbt.deps`, `dlt_worker.dbt.build` children), and one `dlt_worker.workflow.run` per due workflow with a `dlt_worker.workflow.step` per step, under which the step's own pipeline or transformation run span sits with trigger `workflow`. Pipeline runs execute in a subprocess, and the trace context is propagated into it, so `dlt_worker.pipeline.execute`, `dlt_worker.source.build`, `dlt_worker.dlt.run` and one `dlt_worker.iceberg.load` per destination table join the same trace. Events mark the things that are worth seeing inside a run but do not deserve a span of their own: `pipeline.retry`, `pipeline.config_refreshed` (a retry's re-read found a newer definition or credentials — the changed field *names*, never values), `run.timeout`, `run.subprocess_died` (with the exit code — an OOM kill leaves nothing else behind), `pipeline.resumed_pending` (see below), `pipeline.skipped`, `central.unreachable`, `iceberg.credentials_refreshed`, `workflow.step_reused_run` (a step took a run its target already made this tick), `workflow.report_failed`, `workflow.report_dropped`.

**Resumed runs.** A run that dies after extract or normalize (an OOM kill, the deadline kill) leaves a pending load package in the dlt state. dlt's `run()` then only finishes that package on the next run and returns without extracting the data it was given. The worker detects this (the dlt trace has no `extract` step), logs it, adds `pipeline.resumed_pending` to the span and calls `run()` once more in the same run, so the run delivers its own data as well as the earlier one's. A second `dlt_worker.dlt.run` span, marked `dlt_worker.run.after_resume`, covers that call. `rows_loaded` is the sum of both. An incremental cursor was already advanced by the earlier extract, so the second call extracts only what is new. If the second call fails, the run fails, but the resumed package stays loaded and counted, and the error message says so. The worker never makes more than one extra call.

**Metrics.** `dlt_worker.pipeline.runs` / `.run.duration` / `.rows_loaded` / `.attempts`, `dlt_worker.transformation.runs` / `.run.duration` / `.nodes`, `dlt_worker.workflow.runs` / `.run.duration`, `dlt_worker.poll.duration`, `dlt_worker.api.requests` / `.request.duration`, `dlt_worker.workspace_db.operations`, `dlt_worker.iceberg.rows_appended` / `.append.duration` / `.credential_refreshes`, and `dlt_worker.process.memory.rss` (the number that decides whether the next run gets OOM-killed). Attributes are deliberately low-cardinality: pipeline/transformation/workflow name, source type, status, trigger kind — never run ids or error messages, which live on spans.

**Secrets.** Source credentials, connection strings and git tokens reach exception text routinely, so spans never record raw exceptions on any path that touches them: what reaches a span is the same scrubbed message that goes into the run report, or the exception type alone.

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
