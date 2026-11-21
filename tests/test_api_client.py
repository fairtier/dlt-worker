"""Tests for APIClient HTTP interactions."""

from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from dlt_worker.api_client import (
    APIClient,
    PipelineRunReport,
    _parse_source_test,
    _parse_transformation_record,
    _parse_trigger_record,
)


def _make_client() -> APIClient:
    return APIClient(base_url="http://localhost:8080", customer_slug="acme")


def _make_auth_client() -> APIClient:
    return APIClient(
        base_url="http://localhost:8080",
        customer_slug="acme",
        oidc_token_url="http://casdoor/api/login/oauth/access_token",
        oidc_client_id="cid",
        oidc_client_secret="secret",
    )


def _token_response(token: str, expires_in: int = 3600) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"access_token": token, "expires_in": expires_in}
    resp.raise_for_status = MagicMock()
    return resp


def _api_response(payload: dict | None = None, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload or {"pipelines": []}
    resp.raise_for_status = MagicMock()
    return resp


class TestTryGetPipelineTriggers:
    """try_get_pipeline_triggers carries what the checkout cannot: the manual
    trigger, file_upload credentials and the last-run watermark. It also
    distinguishes "nothing pending" from "unreachable" — the caller needs
    that to decide between pruning cached credentials and keeping them."""

    def test_parses_full_response(self) -> None:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response(
            {
                "pipelines": [
                    {
                        "id": "p1",
                        "sourceCredentials": '{"connection_string":"pg://host/db"}',
                        "triggerNow": True,
                        "pendingRunId": "run-1",
                        "lastRunAt": "2025-06-01T12:00:00Z",
                    },
                ],
            }
        )

        triggers = client.try_get_pipeline_triggers()

        assert triggers is not None
        assert len(triggers) == 1
        t = triggers[0]
        assert t.id == "p1"
        assert t.source_credentials == {"connection_string": "pg://host/db"}
        assert t.trigger_now is True
        assert t.pending_run_id == "run-1"
        assert t.last_run_at is not None
        assert t.last_run_at.year == 2025

    def test_definition_fields_from_an_older_server_are_ignored(self) -> None:
        """A pre-0.9.0 control plane still sends definitions. They must be
        dropped, not treated as a malformed record — the checkout is the
        only definition the worker reads."""
        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response(
            {
                "pipelines": [
                    {
                        "id": "p1",
                        "name": "orders",
                        "sourceType": "sql_database",
                        "sourceConfig": '{"tables":["orders"]}',
                        "datasetName": "raw",
                        "schedule": "*/5 * * * *",
                        "writeDisposition": "replace",
                        "enabled": False,
                    },
                ],
            }
        )

        triggers = client.try_get_pipeline_triggers()

        assert triggers is not None
        assert [t.id for t in triggers] == ["p1"]
        assert not hasattr(triggers[0], "schedule")

    def test_returns_empty_list_when_nothing_pending(self) -> None:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response({"pipelines": []})

        assert client.try_get_pipeline_triggers() == []

    def test_missing_optional_fields(self) -> None:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response({"pipelines": [{"id": "p2"}]})

        triggers = client.try_get_pipeline_triggers()

        assert triggers is not None
        t = triggers[0]
        assert t.source_credentials == {}
        assert t.last_run_at is None
        assert t.trigger_now is False
        assert t.pending_run_id == ""

    def test_returns_none_on_connection_error(self) -> None:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.side_effect = requests.ConnectionError("refused")

        assert client.try_get_pipeline_triggers() is None
        healthy, details = client.health_status()
        assert healthy is False
        assert "refused" in details["last_error"]


class TestReportPipelineRun:
    """Tests for APIClient.report_pipeline_run."""

    def test_success_returns_true(self) -> None:
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        client._session = MagicMock()
        client._session.post.return_value = mock_resp

        report = PipelineRunReport(
            pipeline_id="p1",
            status="success",
            started_at="2025-06-01T12:00:00Z",
            completed_at="2025-06-01T12:05:00Z",
            rows_loaded=42,
        )
        result = client.report_pipeline_run(report)

        assert result is True
        call_kwargs = client._session.post.call_args
        payload = call_kwargs.kwargs["json"]
        assert payload["pipelineId"] == "p1"
        assert payload["status"] == "success"
        assert payload["rowsLoaded"] == 42

    def test_http_error_returns_false_and_unhealthy(self) -> None:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.side_effect = requests.ConnectionError("timeout")

        report = PipelineRunReport(
            pipeline_id="p1",
            status="failed",
            started_at="2025-06-01T12:00:00Z",
            completed_at="2025-06-01T12:01:00Z",
            error_message="connection refused",
        )
        result = client.report_pipeline_run(report)

        assert result is False
        healthy, _ = client.health_status()
        assert healthy is False


class TestBearerAuth:
    """Tests for the OIDC client-credentials bearer token."""

    def test_auth_disabled_without_credentials(self) -> None:
        client = _make_client()
        assert client.auth_enabled is False

        client._session = MagicMock()
        client._session.post.return_value = _api_response()
        client.try_get_pipeline_triggers()

        headers = client._session.post.call_args.kwargs["headers"]
        assert "Authorization" not in headers

    def test_token_fetched_and_sent(self) -> None:
        client = _make_auth_client()
        client._session = MagicMock()
        client._session.post.side_effect = [
            _token_response("tok-1"),
            _api_response(),
        ]

        client.try_get_pipeline_triggers()

        token_call, api_call = client._session.post.call_args_list
        assert token_call.args[0] == client.oidc_token_url
        assert token_call.kwargs["data"]["grant_type"] == "client_credentials"
        assert api_call.kwargs["headers"]["Authorization"] == "Bearer tok-1"

    def test_token_cached_across_calls(self) -> None:
        client = _make_auth_client()
        client._session = MagicMock()
        client._session.post.side_effect = [
            _token_response("tok-1"),
            _api_response(),
            _api_response(),
        ]

        client.try_get_pipeline_triggers()
        client.try_get_pipeline_triggers()

        # One token fetch, two API calls.
        assert client._session.post.call_count == 3
        last_call = client._session.post.call_args_list[-1]
        assert last_call.kwargs["headers"]["Authorization"] == "Bearer tok-1"

    def test_retries_once_with_fresh_token_on_401(self) -> None:
        client = _make_auth_client()
        client._session = MagicMock()
        client._session.post.side_effect = [
            _token_response("tok-stale"),
            _api_response(status_code=401),
            _token_response("tok-fresh"),
            _api_response(),
        ]

        configs = client.try_get_pipeline_triggers()

        assert configs == []
        assert client._session.post.call_count == 4
        retry_call = client._session.post.call_args_list[-1]
        assert retry_call.kwargs["headers"]["Authorization"] == "Bearer tok-fresh"
        healthy, _ = client.health_status()
        assert healthy is True

    def test_token_fetch_failure_marks_unhealthy(self) -> None:
        client = _make_auth_client()
        client._session = MagicMock()
        client._session.post.side_effect = requests.ConnectionError("casdoor down")

        assert client.try_get_pipeline_triggers() is None
        healthy, details = client.health_status()
        assert healthy is False
        assert "casdoor down" in details["last_error"]

    def test_report_pipeline_run_sends_token(self) -> None:
        client = _make_auth_client()
        client._session = MagicMock()
        client._session.post.side_effect = [
            _token_response("tok-1"),
            _api_response(),
        ]

        report = PipelineRunReport(
            pipeline_id="p1",
            status="success",
            started_at="2025-06-01T12:00:00Z",
            completed_at="2025-06-01T12:05:00Z",
        )
        assert client.report_pipeline_run(report) is True

        api_call = client._session.post.call_args_list[-1]
        assert api_call.kwargs["headers"]["Authorization"] == "Bearer tok-1"


class TestHealthStatus:
    """Tests for APIClient health tracking."""

    def test_initially_healthy(self) -> None:
        client = _make_client()
        healthy, details = client.health_status()
        assert healthy is True
        assert details["last_error"] == ""

    def test_unhealthy_after_failure(self) -> None:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.side_effect = requests.ConnectionError("refused")

        client.try_get_pipeline_triggers()

        healthy, details = client.health_status()
        assert healthy is False
        assert "refused" in details["last_error"]
        assert details["last_check_at"] != ""


class TestGetTransformationConfigs:
    """Tests for APIClient.get_transformation_configs."""

    def test_parses_full_response(self) -> None:
        client = _make_client()
        mock_resp = _api_response(
            {
                "transformations": [
                    {
                        "id": "t1",
                        "name": "nightly",
                        "repoUrl": "https://git/x.git",
                        "repoRef": "prod",
                        "gitCredentials": '{"username":"u","token":"tok"}',
                        "schedule": "0 3 * * *",
                        "triggerAfterPipelineId": "p1",
                        "dbtSelector": "tag:daily",
                        "enabled": True,
                        "triggerNow": True,
                        "pendingRunId": "run-1",
                        "lastRunAt": "2025-06-01T12:00:00Z",
                    }
                ]
            }
        )
        client._session = MagicMock()
        client._session.post.return_value = mock_resp

        (cfg,) = client.get_transformation_configs()
        assert cfg.id == "t1"
        assert cfg.repo_ref == "prod"
        assert cfg.git_credentials == {"username": "u", "token": "tok"}
        assert cfg.trigger_after_pipeline_id == "p1"
        assert cfg.trigger_now is True
        assert cfg.pending_run_id == "run-1"
        assert cfg.last_run_at is not None

    def test_missing_optional_fields_default(self) -> None:
        # Connect omits zero-value fields from the JSON.
        client = _make_client()
        mock_resp = _api_response({"transformations": [{"id": "t1", "name": "n"}]})
        client._session = MagicMock()
        client._session.post.return_value = mock_resp

        (cfg,) = client.get_transformation_configs()
        assert cfg.repo_url == ""
        assert cfg.repo_ref == "main"
        assert cfg.git_credentials == {}
        assert cfg.schedule is None
        assert cfg.trigger_now is False
        assert cfg.last_run_at is None

    def test_http_error_returns_empty(self) -> None:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.side_effect = requests.ConnectionError("refused")

        assert client.get_transformation_configs() == []

    def test_omitted_enabled_is_false(self) -> None:
        """proto3 JSON omits a false bool, so an absent `enabled` IS false:
        a disabled transformation must not parse as enabled."""
        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response(
            {"transformations": [{"id": "t1", "name": "n"}]}
        )

        (cfg,) = client.get_transformation_configs()
        assert cfg.enabled is False


class TestProto3JsonDefaults:
    """proto3 JSON: a zero value may be omitted OR sent as null, and both
    mean the field's zero value ("" / false), never a type-confused None."""

    def test_transformation_nulls_are_zero_values(self) -> None:
        cfg = _parse_transformation_record(
            {
                "id": "t1",
                "name": "n",
                "repoUrl": None,
                "repoRef": None,
                "gitCredentials": None,
                "schedule": None,
                "triggerAfterPipelineId": None,
                "dbtSelector": None,
                "enabled": None,
                "triggerNow": None,
                "pendingRunId": None,
                "lastRunAt": None,
            }
        )
        assert cfg.repo_url == ""
        assert cfg.repo_ref == "main"
        assert cfg.git_credentials == {}
        assert cfg.schedule is None
        assert cfg.trigger_after_pipeline_id == ""
        assert cfg.dbt_selector == ""
        assert cfg.enabled is False
        assert cfg.trigger_now is False
        assert cfg.pending_run_id == ""
        assert cfg.last_run_at is None

    def test_trigger_nulls_are_zero_values(self) -> None:
        trig = _parse_trigger_record(
            {
                "id": "p1",
                "sourceCredentials": None,
                "triggerNow": None,
                "pendingRunId": None,
                "lastRunAt": None,
            }
        )
        assert trig.source_credentials == {}
        assert trig.trigger_now is False
        assert trig.pending_run_id == ""
        assert trig.last_run_at is None

    def test_source_test_nulls_are_zero_values(self) -> None:
        t = _parse_source_test(
            {
                "id": "s1",
                "sourceType": None,
                "sourceConfig": None,
                "sourceCredentials": None,
            }
        )
        assert t.source_type == ""
        assert t.source_config == {}
        assert t.source_credentials == {}


class TestReportTransformationRun:
    """Tests for APIClient.report_transformation_run."""

    def test_payload_shape(self) -> None:
        from dlt_worker.api_client import TransformationRunReport

        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response({})

        report = TransformationRunReport(
            transformation_id="t1",
            status="success",
            started_at="2025-06-01T12:00:00Z",
            completed_at="2025-06-01T12:05:00Z",
            commit_sha="abc123",
            models_total=3,
            tests_total=2,
            model_results='[{"name":"m"}]',
            run_id="run-1",
        )
        assert client.report_transformation_run(report) is True

        payload = client._session.post.call_args.kwargs["json"]
        assert payload["transformationId"] == "t1"
        assert payload["commitSha"] == "abc123"
        assert payload["modelsTotal"] == 3
        assert payload["testsTotal"] == 2
        assert payload["modelResults"] == '[{"name":"m"}]'
        assert payload["runId"] == "run-1"

    def test_http_error_returns_false(self) -> None:
        from dlt_worker.api_client import TransformationRunReport

        client = _make_client()
        client._session = MagicMock()
        client._session.post.side_effect = requests.ConnectionError("refused")

        report = TransformationRunReport(
            transformation_id="t1",
            status="failed",
            started_at="2025-06-01T12:00:00Z",
            completed_at="2025-06-01T12:05:00Z",
        )
        assert client.report_transformation_run(report) is False


class TestMalformedCentralResponses:
    """A garbage-returning central must degrade exactly like an unreachable
    one (B1): 200-with-non-JSON → None/unhealthy, one bad record skipped."""

    def test_non_json_200_body_returns_none_and_unhealthy(self) -> None:
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.side_effect = ValueError("Expecting value: line 1 column 1")
        client._session = MagicMock()
        client._session.post.return_value = mock_resp

        assert client.try_get_pipeline_triggers() is None
        healthy, _ = client.health_status()
        assert healthy is False

    def test_pipelines_not_a_list_returns_none(self) -> None:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response({"pipelines": "oops"})

        assert client.try_get_pipeline_triggers() is None

    def test_bad_record_is_skipped_others_survive(self) -> None:
        client = _make_client()
        good = {"id": "p2"}
        bad = {"id": "p1", "sourceCredentials": "{not json"}
        client._session = MagicMock()
        client._session.post.return_value = _api_response({"pipelines": [bad, good]})

        triggers = client.try_get_pipeline_triggers()

        assert triggers is not None
        assert [t.id for t in triggers] == ["p2"]

    def test_transformations_non_json_body_returns_empty(self) -> None:
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.side_effect = ValueError("not json")
        client._session = MagicMock()
        client._session.post.return_value = mock_resp

        assert client.get_transformation_configs() == []

    def test_bad_transformation_record_is_skipped(self) -> None:
        client = _make_client()
        good = {"id": "t2", "name": "good"}
        bad = {"name": "no id"}
        client._session = MagicMock()
        client._session.post.return_value = _api_response(
            {"transformations": [bad, good]}
        )

        configs = client.get_transformation_configs()

        assert [c.id for c in configs] == ["t2"]

    def test_token_response_missing_access_token_degrades_cleanly(self) -> None:
        """A 200 token response without access_token must surface as a
        RequestException (the callers' degradation path), not a KeyError."""
        client = _make_auth_client()
        bad_token_resp = MagicMock()
        bad_token_resp.raise_for_status = MagicMock()
        bad_token_resp.json.return_value = {"unexpected": True}
        client._session = MagicMock()
        client._session.post.return_value = bad_token_resp

        assert client.try_get_pipeline_triggers() is None
        healthy, _ = client.health_status()
        assert healthy is False


class TestGetWorkflowConfigs:
    """get_workflow_configs: the worker-facing view of linear workflows."""

    def test_parses_full_response(self) -> None:
        from datetime import UTC, datetime

        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response(
            {
                "workflows": [
                    {
                        "id": "w1",
                        "name": "nightly",
                        "schedule": "0 3 * * *",
                        "enabled": True,
                        # Out of order on purpose: the worker sorts by position.
                        "steps": [
                            {
                                "position": 1,
                                "kind": "transformation",
                                "refId": "t1",
                                "name": "marts",
                                "refEnabled": True,
                            },
                            # proto3 JSON omits zero values: position 0 and
                            # refEnabled false arrive as absent keys.
                            {"kind": "pipeline", "refId": "p1", "name": "orders"},
                        ],
                        "triggerNow": True,
                        "pendingRunId": "run-1",
                        "lastRunAt": "2025-06-01T03:00:00Z",
                        "lastRunStatus": "failed",
                    }
                ]
            }
        )

        [wf] = client.get_workflow_configs()

        assert client._session.post.call_args.args[0].endswith(
            "/workflow.v1.WorkflowService/GetWorkflowConfigs"
        )
        assert client._session.post.call_args.kwargs["json"] == {"customerSlug": "acme"}
        assert wf.id == "w1"
        assert wf.schedule == "0 3 * * *"
        assert wf.enabled is True
        assert [(s.position, s.kind, s.ref_id) for s in wf.steps] == [
            (0, "pipeline", "p1"),
            (1, "transformation", "t1"),
        ]
        assert wf.steps[0].ref_enabled is False
        assert wf.steps[1].ref_enabled is True
        assert wf.trigger_now is True
        assert wf.pending_run_id == "run-1"
        assert wf.last_run_at == datetime(2025, 6, 1, 3, tzinfo=UTC)
        assert wf.last_run_status == "failed"

    def test_zero_values_default(self) -> None:
        """A disabled, manual-only, never-run workflow is mostly absent keys."""
        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response(
            {
                "workflows": [
                    {"id": "w1", "steps": [{"kind": "pipeline", "refId": "p1"}]}
                ]
            }
        )

        [wf] = client.get_workflow_configs()

        assert wf.enabled is False
        assert wf.schedule is None
        assert wf.trigger_now is False
        assert wf.pending_run_id == ""
        assert wf.last_run_at is None

    def test_unimplemented_is_no_workflows_logged_once(self, caplog: Any) -> None:
        """A workspace-api that predates workflows answers 404 (Connect
        unimplemented). That is "no workflows", not an error — and it is
        logged once, not every tick."""
        client = _make_client()
        client._session = MagicMock()
        resp = _api_response({"code": "unimplemented"}, status_code=404)
        resp.raise_for_status.side_effect = requests.HTTPError("404")
        client._session.post.return_value = resp

        with caplog.at_level(logging.INFO, logger="dlt_worker.api_client"):
            assert client.get_workflow_configs() == []
            assert client.get_workflow_configs() == []

        notes = [r for r in caplog.records if "does not serve workflows" in r.message]
        assert len(notes) == 1
        assert notes[0].levelno == logging.INFO
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        # Optional feature: never flips the pod unready.
        assert client.health_status()[0] is True

    def test_api_without_workflow_service_over_real_http(self, caplog: Any) -> None:
        """End to end against a server shaped like a workspace-api that
        predates workflows: its internal mux serves the pipeline and
        transformation services and answers any other path with the Go
        ServeMux's plain-text 404. Workflows read as none, the services it
        does serve keep working, and repeated ticks log nothing more."""
        paths: list[str] = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                paths.append(self.path)
                if self.path.startswith("/transformation.v1.TransformationService/"):
                    body, ctype, code = b"{}", "application/json", 200
                else:
                    body, ctype, code = b"404 page not found\n", "text/plain", 404
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = APIClient(
                base_url=f"http://127.0.0.1:{server.server_address[1]}",
                customer_slug="acme",
            )
            with caplog.at_level(logging.INFO, logger="dlt_worker.api_client"):
                for _ in range(3):
                    assert client.get_workflow_configs() == []
                    assert client.get_transformation_configs() == []
        finally:
            server.shutdown()
            server.server_close()

        assert paths.count("/workflow.v1.WorkflowService/GetWorkflowConfigs") == 3
        notes = [r for r in caplog.records if "does not serve workflows" in r.message]
        assert len(notes) == 1
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert client.health_status()[0] is True

    def test_transport_error_returns_empty(self) -> None:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.side_effect = requests.ConnectionError("refused")

        assert client.get_workflow_configs() == []
        assert client.health_status()[0] is True

    def test_bad_record_is_skipped_others_survive(self) -> None:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response(
            {
                "workflows": [
                    {"id": "bad", "steps": [{"kind": "pipeline"}]},  # no refId
                    {"id": "w2", "steps": [{"kind": "pipeline", "refId": "p1"}]},
                ]
            }
        )

        assert [w.id for w in client.get_workflow_configs()] == ["w2"]


class TestReportWorkflowRun:
    def test_payload_shape(self) -> None:
        from dlt_worker.api_client import (
            ReportResult,
            WorkflowRunReport,
            WorkflowStepRunReport,
        )

        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response({})

        report = WorkflowRunReport(
            run_id="run-1",
            workflow_id="w1",
            status="failed",
            trigger="manual",
            started_at="2025-06-01T03:00:00+00:00",
            completed_at="2025-06-01T03:10:00+00:00",
            error_message="step 1 failed",
            step_runs=[
                WorkflowStepRunReport(
                    position=0,
                    kind="pipeline",
                    ref_id="p1",
                    name="orders",
                    status="failed",
                    pipeline_run_id="prun-1",
                    error_message="boom",
                ),
                WorkflowStepRunReport(
                    position=1,
                    kind="transformation",
                    ref_id="t1",
                    name="marts",
                    status="skipped",
                ),
            ],
        )
        assert client.report_workflow_run(report) is ReportResult.DELIVERED

        assert client._session.post.call_args.args[0].endswith(
            "/workflow.v1.WorkflowService/ReportWorkflowRun"
        )
        payload = client._session.post.call_args.kwargs["json"]
        assert payload["runId"] == "run-1"
        assert payload["workflowId"] == "w1"
        assert payload["status"] == "failed"
        assert payload["trigger"] == "manual"
        assert payload["errorMessage"] == "step 1 failed"
        assert payload["stepRuns"][0] == {
            "position": 0,
            "kind": "pipeline",
            "refId": "p1",
            "name": "orders",
            "status": "failed",
            "pipelineRunId": "prun-1",
            "transformationRunId": "",
            "startedAt": "",
            "completedAt": "",
            "errorMessage": "boom",
        }
        assert payload["stepRuns"][1]["status"] == "skipped"

    def test_http_error_is_transient(self) -> None:
        from dlt_worker.api_client import ReportResult, WorkflowRunReport

        client = _make_client()
        client._session = MagicMock()
        client._session.post.side_effect = requests.ConnectionError("refused")

        report = WorkflowRunReport(
            run_id="run-1",
            workflow_id="w1",
            status="running",
            trigger="schedule",
            started_at="2025-06-01T03:00:00+00:00",
        )
        assert client.report_workflow_run(report) is ReportResult.TRANSIENT
        assert client.health_status()[0] is True

    def test_wrongly_typed_scalar_is_malformed(self) -> None:
        """Not a proto3 bool: the record is malformed (the caller skips it),
        not silently read as true or false."""
        with pytest.raises(TypeError):
            _parse_transformation_record({"id": "t1", "name": "n", "enabled": "false"})


class TestWorkflowZeroValues:
    """The Go server encodes with protojson, which omits every zero value —
    and a proto3 JSON parser must also take an explicit null as that zero
    value. A workflow record must read the same either way."""

    def _parse(self, record: dict[str, Any]) -> Any:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = _api_response({"workflows": [record]})
        return client.get_workflow_configs()

    def test_omitted_zero_values(self) -> None:
        """What the server really sends for a disabled, manual-only, never-run
        workflow whose first step's target is disabled: no position, no
        refEnabled, no enabled, no triggerNow, no schedule."""
        [wf] = self._parse(
            {
                "id": "w1",
                "name": "nightly",
                "steps": [
                    {"kind": "pipeline", "refId": "p1", "name": "orders"},
                    {
                        "position": 1,
                        "kind": "transformation",
                        "refId": "t1",
                        "refEnabled": True,
                    },
                ],
            }
        )
        assert (wf.enabled, wf.trigger_now, wf.schedule) == (False, False, None)
        assert (wf.pending_run_id, wf.last_run_at, wf.last_run_status) == ("", None, "")
        assert [(s.position, s.ref_enabled, s.name) for s in wf.steps] == [
            (0, False, "orders"),
            (1, True, ""),
        ]

    def test_explicit_nulls_are_zero_values(self) -> None:
        [wf] = self._parse(
            {
                "id": "w1",
                "name": None,
                "schedule": None,
                "enabled": None,
                "triggerNow": None,
                "pendingRunId": None,
                "lastRunAt": None,
                "lastRunStatus": None,
                "steps": [
                    {
                        "position": None,
                        "kind": "pipeline",
                        "refId": "p1",
                        "name": None,
                        "refEnabled": None,
                    }
                ],
            }
        )
        assert (wf.name, wf.schedule, wf.enabled, wf.trigger_now) == (
            "",
            None,
            False,
            False,
        )
        assert (wf.pending_run_id, wf.last_run_status) == ("", "")
        [step] = wf.steps
        assert (step.position, step.name, step.ref_enabled) == (0, "", False)

    def test_null_steps_is_no_steps(self) -> None:
        [wf] = self._parse({"id": "w1", "steps": None})
        assert wf.steps == []

    def test_int32_as_string_is_accepted(self) -> None:
        """protojson parsers accept an int32 as a JSON string too."""
        [wf] = self._parse(
            {"id": "w1", "steps": [{"position": "2", "kind": "pipeline", "refId": "p"}]}
        )
        assert wf.steps[0].position == 2

    @pytest.mark.parametrize(
        "record",
        [
            {"id": "w1", "enabled": "false", "steps": []},
            {"id": "w1", "triggerNow": 1, "steps": []},
            {"id": "w1", "pendingRunId": 7, "steps": []},
            {
                "id": "w1",
                "steps": [{"kind": "pipeline", "refId": "p", "refEnabled": "true"}],
            },
            {
                "id": "w1",
                "steps": [{"kind": "pipeline", "refId": "p", "position": 1.5}],
            },
            {
                "id": "w1",
                "steps": [{"kind": "pipeline", "refId": "p", "position": True}],
            },
            {"id": "w1", "steps": [{"kind": "pipeline", "refId": ""}]},
            {"id": "", "steps": []},
        ],
    )
    def test_wrong_types_make_the_record_malformed(
        self, record: dict[str, Any]
    ) -> None:
        """Never read "false" as true: a wrongly typed scalar skips the
        record, as for every other record type."""
        assert self._parse(record) == []


def _connect_error(status_code: int, body: Any) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    if isinstance(body, Exception):
        resp.json.side_effect = body
    else:
        resp.json.return_value = body
    resp.raise_for_status.side_effect = requests.HTTPError(
        f"{status_code}", response=resp
    )
    return resp


class TestReportWorkflowRunErrors:
    """ReportWorkflowRun documents which errors are permanent (the same
    report fails the same way: InvalidArgument, NotFound) and which are
    worth retrying. The client tells them apart for the caller."""

    def _report(self, resp: MagicMock) -> Any:
        from dlt_worker.api_client import WorkflowRunReport

        client = _make_client()
        client._session = MagicMock()
        client._session.post.return_value = resp
        return client.report_workflow_run(
            WorkflowRunReport(
                run_id="run-1",
                workflow_id="w1",
                status="success",
                trigger="schedule",
                started_at="2025-06-01T03:00:00+00:00",
            )
        )

    def test_delivered_is_truthy(self) -> None:
        from dlt_worker.api_client import ReportResult

        result = self._report(_api_response({}))
        assert result is ReportResult.DELIVERED
        assert result

    @pytest.mark.parametrize(
        ("status_code", "code"),
        [(400, "invalid_argument"), (404, "not_found")],
    )
    def test_permanent_codes_are_rejected(self, status_code: int, code: str) -> None:
        from dlt_worker.api_client import ReportResult

        result = self._report(
            _connect_error(status_code, {"code": code, "message": "workflow gone"})
        )
        assert result is ReportResult.REJECTED
        assert not result

    @pytest.mark.parametrize(
        ("status_code", "body"),
        [
            (503, {"code": "unavailable", "message": "down"}),
            (504, {"code": "deadline_exceeded"}),
            (500, {"code": "internal"}),
            (500, {"code": "unknown"}),
            # Operator problems: not fixed by this report, but by a fix that
            # may land any minute — bounded retries, not an immediate drop.
            (401, {"code": "unauthenticated"}),
            (403, {"code": "permission_denied"}),
            # No Connect error body (a proxy's page): Connect's HTTP mapping
            # makes 400 "internal" and 404 "unimplemented" — neither is the
            # server saying "this report is wrong".
            (400, ValueError("not json")),
            (404, {"error": "no route"}),
            (502, ValueError("not json")),
        ],
    )
    def test_everything_else_is_transient(self, status_code: int, body: Any) -> None:
        from dlt_worker.api_client import ReportResult

        result = self._report(_connect_error(status_code, body))
        assert result is ReportResult.TRANSIENT
        assert not result

    def test_transport_error_is_transient(self) -> None:
        from dlt_worker.api_client import ReportResult, WorkflowRunReport

        client = _make_client()
        client._session = MagicMock()
        client._session.post.side_effect = requests.ConnectionError("refused")
        result = client.report_workflow_run(
            WorkflowRunReport(
                run_id="r", workflow_id="w", status="running", trigger="", started_at=""
            )
        )
        assert result is ReportResult.TRANSIENT
