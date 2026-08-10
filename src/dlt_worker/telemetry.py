"""OpenTelemetry traces and metrics for the worker.

Instrumentation is unconditional; *exporting* is not. The call sites
throughout the worker always create spans and record measurements against
the OTel API, which is a no-op (a proxy tracer/meter that drops everything)
until :func:`setup` installs real SDK providers. ``setup`` installs them
only when an OTLP endpoint is configured — so a deployment that sets no
``OTEL_*`` env var pays nothing and behaves exactly as before, and turning
telemetry on is a deployment change rather than a code change.

Configuration is the standard OTel environment: ``OTEL_EXPORTER_OTLP_ENDPOINT``
(the on switch), ``OTEL_EXPORTER_OTLP_HEADERS``, ``OTEL_SERVICE_NAME``,
``OTEL_RESOURCE_ATTRIBUTES``, ``OTEL_METRIC_EXPORT_INTERVAL``,
``OTEL_TRACES_SAMPLER``, and ``OTEL_SDK_DISABLED`` — all read by the SDK
itself, so nothing here needs to mirror them into :mod:`dlt_worker.config`.

Two rules the call sites must keep:

* **Never put a credential-bearing string on a span.** Source credentials,
  git tokens and connection strings reach exception text routinely. Only
  already-scrubbed text (``PipelineRunReport.error_message`` and friends)
  may be recorded, which is why the runners set span status by hand instead
  of letting ``record_exception`` capture the raw exception.
* **Keep metric attributes low-cardinality.** Pipeline/transformation
  *names* are bounded per customer and are fine; run ids, table names and
  error messages are not, and belong on spans only.

A pipeline run happens in a spawned child process (see
:mod:`dlt_worker.run_isolation`), which starts with no SDK at all. The
parent hands it the current trace context via :func:`current_trace_context`,
the child re-runs ``setup`` and calls :func:`attach_trace_context`, so its
spans join the parent's trace. Child exports are flushed with
:func:`flush` before it exits; both exporter threads are daemons, so a
collector that has gone away can never keep the child (and with it the
poll loop) alive.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from opentelemetry import context as otel_context
from opentelemetry import metrics, trace
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

if TYPE_CHECKING:
    from dlt_worker.api_client import (
        PipelineConfig,
        PipelineRunReport,
        TransformationConfig,
        TransformationRunReport,
    )

logger = logging.getLogger(__name__)

SERVICE_NAME = "dlt-worker"

# Custom attributes are namespaced so they can never collide with a
# semantic convention that gains the same short name later.
ATTR_ROLE = "dlt_worker.role"
ATTR_CUSTOMER = "dlt_worker.customer"
ATTR_MODE = "dlt_worker.mode"  # "files" | "api"
ATTR_OUTCOME = "dlt_worker.outcome"  # "ok" | "error"
ATTR_PIPELINE_ID = "dlt_worker.pipeline.id"
ATTR_PIPELINE_NAME = "dlt_worker.pipeline.name"
ATTR_SOURCE_TYPE = "dlt_worker.pipeline.source_type"
ATTR_DATASET = "dlt_worker.pipeline.dataset"
ATTR_WRITE_DISPOSITION = "dlt_worker.pipeline.write_disposition"
ATTR_TRANSFORMATION_ID = "dlt_worker.transformation.id"
ATTR_TRANSFORMATION_NAME = "dlt_worker.transformation.name"
ATTR_RUN_ID = "dlt_worker.run.id"
ATTR_TRIGGER = "dlt_worker.run.trigger"  # "manual" | "schedule" | "chained"
ATTR_STATUS = "dlt_worker.run.status"  # "success" | "failed"
ATTR_ATTEMPT = "dlt_worker.run.attempt"
ATTR_ROWS = "dlt_worker.run.rows_loaded"
ATTR_TABLE = "dlt_worker.iceberg.table"
ATTR_NODE_KIND = "dlt_worker.dbt.node_kind"  # "model" | "test"
ATTR_DB_OPERATION = "dlt_worker.db.operation"

# The worker's own instrumentation scope. Created before any provider
# exists on purpose: the API hands back proxies that start delegating to
# the real provider the moment setup() installs one.
tracer = trace.get_tracer("dlt_worker")
_meter = metrics.get_meter("dlt_worker")

poll_duration = _meter.create_histogram(
    "dlt_worker.poll.duration",
    unit="s",
    description="Duration of one poll tick (fetch configs, run everything due)",
)
pipeline_runs = _meter.create_counter(
    "dlt_worker.pipeline.runs",
    unit="{run}",
    description="Finished pipeline runs by outcome",
)
pipeline_run_duration = _meter.create_histogram(
    "dlt_worker.pipeline.run.duration",
    unit="s",
    description="Wall-clock duration of a pipeline run, retries included",
)
pipeline_rows_loaded = _meter.create_counter(
    "dlt_worker.pipeline.rows_loaded",
    unit="{row}",
    description="Rows loaded by successful pipeline runs",
)
pipeline_attempts = _meter.create_counter(
    "dlt_worker.pipeline.attempts",
    unit="{attempt}",
    description="Individual pipeline run attempts, retries included",
)
transformation_runs = _meter.create_counter(
    "dlt_worker.transformation.runs",
    unit="{run}",
    description="Finished dbt transformation runs by outcome",
)
transformation_run_duration = _meter.create_histogram(
    "dlt_worker.transformation.run.duration",
    unit="s",
    description="Wall-clock duration of a dbt transformation run",
)
transformation_nodes = _meter.create_counter(
    "dlt_worker.transformation.nodes",
    unit="{node}",
    description="dbt nodes executed, by kind and outcome",
)
api_requests = _meter.create_counter(
    "dlt_worker.api.requests",
    unit="{request}",
    description="Control plane API calls by RPC method and outcome",
)
api_request_duration = _meter.create_histogram(
    "dlt_worker.api.request.duration",
    unit="s",
    description="Duration of control plane API calls",
)
workspace_db_operations = _meter.create_counter(
    "dlt_worker.workspace_db.operations",
    unit="{operation}",
    description="Workspace database writes by operation and outcome",
)
iceberg_rows_appended = _meter.create_counter(
    "dlt_worker.iceberg.rows_appended",
    unit="{row}",
    description="Rows appended by the streamed Iceberg load",
)
iceberg_append_duration = _meter.create_histogram(
    "dlt_worker.iceberg.append.duration",
    unit="s",
    description="Duration of one chunked Iceberg append",
)
iceberg_credential_refreshes = _meter.create_counter(
    "dlt_worker.iceberg.credential_refreshes",
    unit="{refresh}",
    description="Mid-load table re-opens that re-vended storage credentials",
)

_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096

_configured = False
_tracer_provider: Any = None
_meter_provider: Any = None


def rss_bytes() -> int:
    """Current resident set size in bytes from /proc; 0 where unavailable."""
    try:
        with open("/proc/self/statm") as f:
            resident_pages = int(f.read().split()[1])
    except (OSError, ValueError, IndexError):
        return 0
    return resident_pages * _PAGE_SIZE


def _observe_rss(_options: CallbackOptions) -> Sequence[Observation]:
    """Callback for the RSS gauge.

    Worth a metric of its own: a pipeline run leaves ~1 GB resident that
    Python never returns to the OS (the reason runs are isolated in a
    subprocess), so the shape of this series over a run is the difference
    between a healthy worker and one about to be OOM-killed.
    """
    rss = rss_bytes()
    return [Observation(rss)] if rss else []


_meter.create_observable_gauge(
    "dlt_worker.process.memory.rss",
    callbacks=[_observe_rss],
    unit="By",
    description="Resident set size of the worker process",
)


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def export_enabled() -> bool:
    """Whether an OTLP endpoint is configured (and the SDK not disabled)."""
    if _truthy("OTEL_SDK_DISABLED"):
        return False
    return any(
        os.environ.get(name, "").strip()
        for name in (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        )
    )


def _service_version() -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("dlt-worker")
    except PackageNotFoundError:
        return "unknown"


def setup(role: str, customer_slug: str = "") -> None:
    """Install SDK providers when OTLP export is configured. Idempotent.

    ``role`` separates the poll loop ("worker") from a run subprocess
    ("run") on otherwise identical resource attributes.
    """
    global _configured, _tracer_provider, _meter_provider
    if _configured:
        return
    _configured = True

    if not export_enabled():
        return

    # Imported here, not at module scope: the SDK and the OTLP exporter are
    # only needed when export is on, and an import failure must degrade to
    # no telemetry rather than take the worker down.
    try:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create(
            {
                # Explicit rather than left to the env detector so the
                # default is ours; OTEL_SERVICE_NAME still wins when set.
                "service.name": os.environ.get("OTEL_SERVICE_NAME") or SERVICE_NAME,
                "service.version": _service_version(),
                ATTR_ROLE: role,
                ATTR_CUSTOMER: customer_slug,
            }
        )

        _tracer_provider = TracerProvider(resource=resource)
        _tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(_tracer_provider)

        _meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())],
        )
        metrics.set_meter_provider(_meter_provider)
    except Exception:
        logger.warning("OpenTelemetry setup failed — continuing without", exc_info=True)
        _tracer_provider = _meter_provider = None
        return

    logger.info("OpenTelemetry export enabled (role=%s)", role)


def flush(timeout_millis: int = 5000) -> None:
    """Best-effort flush of pending spans and metrics.

    Bounded on purpose and never followed by a provider shutdown: this runs
    on the exit path of a run subprocess, and an unreachable collector must
    not delay the report the poll loop is waiting on. Both exporter threads
    are daemons, so anything still queued dies with the process.
    """
    for provider in (_tracer_provider, _meter_provider):
        if provider is None:
            continue
        try:
            provider.force_flush(timeout_millis)
        except Exception:
            logger.warning("OpenTelemetry flush failed", exc_info=True)


def current_trace_context() -> dict[str, str]:
    """Serialize the active span context for a child process (W3C traceparent)."""
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier)
    return carrier


def attach_trace_context(carrier: Mapping[str, str]) -> None:
    """Make a carrier produced by :func:`current_trace_context` the active context.

    Deliberately not detached: the caller is a child process whose whole
    life is the one run this context belongs to.
    """
    if not carrier:
        return
    otel_context.attach(TraceContextTextMapPropagator().extract(dict(carrier)))


# Span status descriptions are bounded: a dbt failure or a stack-quoting
# source error can run to kilobytes, and every byte of it ships on every
# export.
_MAX_STATUS_CHARS = 500


def add_event(name: str, attributes: Mapping[str, Any] | None = None) -> None:
    """Add an event to whatever span is active — a no-op when none is.

    Events are the right weight for things that are worth seeing inside a
    run but would be noise as their own span: a retry, a killed subprocess,
    a pipeline skipped for missing credentials, a mid-load credential
    re-vend.
    """
    trace.get_current_span().add_event(name, dict(attributes or {}))


def set_attributes(attributes: Mapping[str, Any]) -> None:
    """Set attributes on whatever span is active — a no-op when none is."""
    span = trace.get_current_span()
    for key, value in attributes.items():
        span.set_attribute(key, value)


def error_status(message: str) -> Status:
    """An ERROR status carrying a bounded, ALREADY-SCRUBBED message.

    Callers pass a run report's error_message (scrubbed at the source) or a
    bare exception type name — never raw exception text, which routinely
    quotes connection strings and tokens.
    """
    return Status(StatusCode.ERROR, message[:_MAX_STATUS_CHARS])


def trigger_kind(cfg: PipelineConfig | TransformationConfig) -> str:
    """Low-cardinality label for what made a config due."""
    return "manual" if cfg.trigger_now else "schedule"


def record_pipeline_run(
    cfg: PipelineConfig, report: PipelineRunReport, duration_s: float, trigger: str
) -> None:
    """Record the metrics for one finished pipeline run (retries included)."""
    attrs = {
        ATTR_PIPELINE_NAME: cfg.name,
        ATTR_SOURCE_TYPE: cfg.source_type,
        ATTR_STATUS: report.status,
        ATTR_TRIGGER: trigger,
    }
    pipeline_runs.add(1, attrs)
    pipeline_run_duration.record(duration_s, attrs)
    if report.rows_loaded:
        pipeline_rows_loaded.add(
            report.rows_loaded,
            {ATTR_PIPELINE_NAME: cfg.name, ATTR_SOURCE_TYPE: cfg.source_type},
        )


def record_transformation_run(
    cfg: TransformationConfig, report: TransformationRunReport, duration_s: float
) -> None:
    """Record the metrics for one finished dbt transformation run."""
    attrs = {
        ATTR_TRANSFORMATION_NAME: cfg.name,
        ATTR_STATUS: report.status,
    }
    transformation_runs.add(1, attrs)
    transformation_run_duration.record(duration_s, attrs)
    for kind, total, failed in (
        ("model", report.models_total, report.models_failed),
        ("test", report.tests_total, report.tests_failed),
    ):
        base = {ATTR_TRANSFORMATION_NAME: cfg.name, ATTR_NODE_KIND: kind}
        if total - failed:
            transformation_nodes.add(total - failed, {**base, ATTR_OUTCOME: "ok"})
        if failed:
            transformation_nodes.add(failed, {**base, ATTR_OUTCOME: "error"})
