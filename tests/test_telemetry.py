"""Tests for OpenTelemetry wiring.

Two things matter beyond "spans exist": the worker must behave identically
with no collector configured (the default), and nothing credential-shaped
may ever reach a span.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from typing import Any, Iterator
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry import context as otel_context
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from dlt_worker import main, telemetry
from dlt_worker.api_client import (
    PipelineConfig,
    PipelineRunReport,
    TransformationConfig,
    TransformationRunReport,
)

_OTEL_ENV = (
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_SDK_DISABLED",
)


@pytest.fixture
def no_otel_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _OTEL_ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def spans(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemorySpanExporter]:
    """Record spans from the worker's tracer without touching the global
    provider (which can only ever be set once per process)."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(telemetry, "tracer", provider.get_tracer("test"))
    yield exporter
    exporter.clear()


@pytest.fixture(scope="module")
def metric_reader() -> InMemoryMetricReader:
    """Install a real meter provider once; the module-level proxy
    instruments bind to it on first use."""
    reader = InMemoryMetricReader()
    metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    return reader


def _metric_points(reader: InMemoryMetricReader, name: str) -> list[Any]:
    data = reader.get_metrics_data()
    points = []
    for resource_metrics in data.resource_metrics if data else []:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name == name:
                    points.extend(metric.data.data_points)
    return points


def _named(exporter: InMemorySpanExporter, name: str) -> ReadableSpan:
    matches = [s for s in exporter.get_finished_spans() if s.name == name]
    assert matches, (
        f"no {name} span in {[s.name for s in exporter.get_finished_spans()]}"
    )
    return matches[0]


def _make_config(**overrides: Any) -> PipelineConfig:
    defaults: dict[str, Any] = {
        "id": "p1",
        "name": "test-pipeline",
        "source_type": "sql_database",
        "source_config": {"tables": ["orders"]},
        "source_credentials": {"connection_string": "postgresql://u:hunter2@host/db"},
        "dataset_name": "raw",
        "schedule": None,
        "write_disposition": "append",
        "enabled": True,
    }
    defaults.update(overrides)
    return PipelineConfig(**defaults)


# --- export enablement ---


def test_export_disabled_without_endpoint(no_otel_env: None) -> None:
    assert telemetry.export_enabled() is False


def test_export_enabled_with_endpoint(
    no_otel_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    assert telemetry.export_enabled() is True


def test_export_disabled_by_sdk_flag(
    no_otel_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """OTEL_SDK_DISABLED is the kill switch even with an endpoint set."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    assert telemetry.export_enabled() is False


def test_setup_installs_nothing_without_endpoint(
    no_otel_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default deployment configures no collector: setup must leave the
    API's no-op providers in place, and instrumentation must still run."""
    monkeypatch.setattr(telemetry, "_configured", False)
    monkeypatch.setattr(telemetry, "_tracer_provider", None)

    telemetry.setup("worker", "acme")

    assert telemetry._tracer_provider is None
    # Instrumentation stays callable — no branch guards the call sites.
    with telemetry.tracer.start_as_current_span("noop"):
        telemetry.add_event("still-fine")
        telemetry.set_attributes({"k": "v"})
    telemetry.flush()


def test_setup_installs_sdk_providers_when_enabled() -> None:
    """Exercised in a subprocess: installing global providers is a one-way
    door per process, and a real exporter aimed at a dead collector would
    outlive the test that created it."""
    code = textwrap.dedent(
        """
        import os, sys
        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://127.0.0.1:14318"
        os.environ.pop("OTEL_SDK_DISABLED", None)
        from opentelemetry import trace
        from dlt_worker import telemetry

        telemetry.setup("worker", "acme")
        assert telemetry._tracer_provider is not None
        assert telemetry._meter_provider is not None
        assert type(trace.get_tracer_provider()).__name__ == "TracerProvider"

        resource = telemetry._tracer_provider.resource.attributes
        assert resource["service.name"] == "dlt-worker"
        assert resource[telemetry.ATTR_ROLE] == "worker"
        assert resource[telemetry.ATTR_CUSTOMER] == "acme"

        # The module-level proxy tracer now records for real.
        with telemetry.tracer.start_as_current_span("probe") as span:
            assert span.get_span_context().is_valid
            assert telemetry.current_trace_context()["traceparent"]

        print("ok")
        sys.stdout.flush()
        # Skip the atexit flush: nothing is listening on 14318 and the
        # exporter would spend its retry budget discovering that.
        os._exit(0)
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout


# --- trace context propagation into the run subprocess ---


def test_trace_context_roundtrip() -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("test")

    with tracer.start_as_current_span("parent") as parent:
        carrier = telemetry.current_trace_context()
    assert "traceparent" in carrier

    # Stand in for the child process: attach the carrier, and the parent's
    # trace id becomes the ambient one.
    token = otel_context.attach(otel_context.get_current())
    try:
        telemetry.attach_trace_context(carrier)
        current = trace.get_current_span().get_span_context()
        assert current.trace_id == parent.get_span_context().trace_id
        assert current.span_id == parent.get_span_context().span_id
    finally:
        otel_context.detach(token)


def test_attach_empty_context_is_a_noop() -> None:
    """No active span (telemetry off in the parent) means an empty carrier."""
    telemetry.attach_trace_context({})
    assert not trace.get_current_span().get_span_context().is_valid


def test_current_trace_context_empty_without_active_span() -> None:
    assert telemetry.current_trace_context() == {}


# --- process memory ---


def test_rss_bytes_reports_something() -> None:
    assert telemetry.rss_bytes() > 0


# --- spans emitted by a run ---


def test_pipeline_run_span_carries_run_attributes(
    spans: InMemorySpanExporter, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_config(trigger_now=True)
    report = PipelineRunReport(
        pipeline_id=cfg.id,
        status="success",
        started_at=datetime.now(timezone.utc).isoformat(),
        completed_at=datetime.now(timezone.utc).isoformat(),
        rows_loaded=42,
    )
    client = MagicMock()
    client.report_pipeline_run.return_value = True

    with patch("dlt_worker.main.run_pipeline_isolated", return_value=report):
        main._execute_pipeline(cfg, datetime.now(timezone.utc), client)

    span = _named(spans, "dlt_worker.pipeline.run")
    assert span.attributes is not None
    assert span.attributes[telemetry.ATTR_PIPELINE_NAME] == "test-pipeline"
    assert span.attributes[telemetry.ATTR_SOURCE_TYPE] == "sql_database"
    assert span.attributes[telemetry.ATTR_TRIGGER] == "manual"
    assert span.attributes[telemetry.ATTR_STATUS] == "success"
    assert span.attributes[telemetry.ATTR_ROWS] == 42
    assert span.status.status_code is not StatusCode.ERROR

    attempt = _named(spans, "dlt_worker.pipeline.attempt")
    assert attempt.attributes is not None
    assert attempt.attributes[telemetry.ATTR_ATTEMPT] == 1
    # The attempt is a child of the run: one trace per run, subprocess included.
    assert attempt.parent is not None
    assert attempt.parent.span_id == span.context.span_id


def test_failed_run_span_carries_only_the_scrubbed_message(
    spans: InMemorySpanExporter, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The status description comes from the report, which pipeline_runner
    already scrubbed — a credential must never reach the collector."""
    from dlt_worker import config

    monkeypatch.setattr(config, "PIPELINE_MAX_RETRIES", 0)
    cfg = _make_config()
    report = PipelineRunReport(
        pipeline_id=cfg.id,
        status="failed",
        started_at=datetime.now(timezone.utc).isoformat(),
        completed_at=datetime.now(timezone.utc).isoformat(),
        error_message="could not connect to postgresql://u:***@host/db",
    )
    client = MagicMock()

    with patch("dlt_worker.main.run_pipeline_isolated", return_value=report):
        main._execute_pipeline(cfg, datetime.now(timezone.utc), client)

    span = _named(spans, "dlt_worker.pipeline.run")
    assert span.status.status_code is StatusCode.ERROR
    assert span.status.description is not None
    assert "hunter2" not in span.status.description
    for finished in spans.get_finished_spans():
        assert "hunter2" not in str(finished.attributes)
        assert "hunter2" not in str(finished.status.description)


def test_retry_is_an_event_on_the_run_span(
    spans: InMemorySpanExporter, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A retry is a moment inside the run, not a run of its own."""
    from dlt_worker import config

    monkeypatch.setattr(config, "PIPELINE_MAX_RETRIES", 1)
    monkeypatch.setattr(config, "PIPELINE_RETRY_BASE_DELAY", 0)
    cfg = _make_config()
    failed = PipelineRunReport(
        pipeline_id=cfg.id,
        status="failed",
        started_at=datetime.now(timezone.utc).isoformat(),
        completed_at=datetime.now(timezone.utc).isoformat(),
        error_message="boom",
    )
    client = MagicMock()

    with patch("dlt_worker.main.run_pipeline_isolated", return_value=failed):
        main._execute_pipeline(cfg, datetime.now(timezone.utc), client)

    span = _named(spans, "dlt_worker.pipeline.run")
    assert [e.name for e in span.events] == ["pipeline.retry"]
    assert (
        len([s for s in spans.get_finished_spans() if s.name.endswith("attempt")]) == 2
    )


def test_tick_span_records_a_skipped_pipeline(spans: InMemorySpanExporter) -> None:
    """A skip leaves no run row anywhere — the tick span is its only trace."""
    with telemetry.tracer.start_as_current_span("dlt_worker.poll"):
        main._skipped(_make_config(), "not_in_poll_response")

    span = _named(spans, "dlt_worker.poll")
    assert [e.name for e in span.events] == ["pipeline.skipped"]
    assert span.events[0].attributes is not None
    assert span.events[0].attributes["dlt_worker.skip_reason"] == "not_in_poll_response"


# --- metrics ---


def test_pipeline_run_metrics(metric_reader: InMemoryMetricReader) -> None:
    cfg = _make_config(name="metric-pipeline")
    report = PipelineRunReport(
        pipeline_id=cfg.id,
        status="success",
        started_at="",
        completed_at="",
        rows_loaded=7,
    )

    telemetry.record_pipeline_run(cfg, report, 1.5, "schedule")

    runs = [
        p
        for p in _metric_points(metric_reader, "dlt_worker.pipeline.runs")
        if p.attributes and p.attributes[telemetry.ATTR_PIPELINE_NAME] == cfg.name
    ]
    assert len(runs) == 1
    assert runs[0].value == 1
    assert runs[0].attributes[telemetry.ATTR_STATUS] == "success"
    assert runs[0].attributes[telemetry.ATTR_TRIGGER] == "schedule"

    rows = [
        p
        for p in _metric_points(metric_reader, "dlt_worker.pipeline.rows_loaded")
        if p.attributes and p.attributes[telemetry.ATTR_PIPELINE_NAME] == cfg.name
    ]
    assert rows and rows[0].value == 7

    duration = [
        p
        for p in _metric_points(metric_reader, "dlt_worker.pipeline.run.duration")
        if p.attributes and p.attributes[telemetry.ATTR_PIPELINE_NAME] == cfg.name
    ]
    assert duration and duration[0].sum == pytest.approx(1.5)


def test_transformation_node_metrics(metric_reader: InMemoryMetricReader) -> None:
    cfg = TransformationConfig(
        id="t1",
        name="metric-transformation",
        repo_url="https://example.com/repo.git",
        repo_ref="main",
        git_credentials={},
        schedule=None,
        trigger_after_pipeline_id="",
        dbt_selector="",
        enabled=True,
    )
    report = TransformationRunReport(
        transformation_id=cfg.id,
        status="failed",
        started_at="",
        completed_at="",
        models_total=5,
        models_failed=2,
        tests_total=3,
        tests_failed=0,
    )

    telemetry.record_transformation_run(cfg, report, 2.0)

    points = {
        (
            p.attributes[telemetry.ATTR_NODE_KIND],
            p.attributes[telemetry.ATTR_OUTCOME],
        ): (p.value)
        for p in _metric_points(metric_reader, "dlt_worker.transformation.nodes")
        if p.attributes and p.attributes[telemetry.ATTR_TRANSFORMATION_NAME] == cfg.name
    }
    assert points == {("model", "ok"): 3, ("model", "error"): 2, ("test", "ok"): 3}


def test_process_memory_gauge_is_collected(metric_reader: InMemoryMetricReader) -> None:
    points = _metric_points(metric_reader, "dlt_worker.process.memory.rss")
    assert points and points[0].value > 0
