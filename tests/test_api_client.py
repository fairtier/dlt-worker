"""Tests for APIClient HTTP interactions."""

from __future__ import annotations

from unittest.mock import MagicMock

import requests

from dlt_worker.api_client import PipelineRunReport, APIClient


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


class TestGetPipelineConfigs:
    """Tests for APIClient.get_pipeline_configs."""

    def test_parses_full_response(self) -> None:
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "pipelines": [
                {
                    "id": "p1",
                    "name": "orders",
                    "sourceType": "sql_database",
                    "sourceConfig": '{"tables":["orders"]}',
                    "sourceCredentials": '{"connection_string":"pg://host/db"}',
                    "datasetName": "raw",
                    "schedule": "*/5 * * * *",
                    "writeDisposition": "append",
                    "mergeStrategy": "",
                    "enabled": True,
                    "triggerNow": True,
                    "pendingRunId": "run-1",
                    "lastRunAt": "2025-06-01T12:00:00Z",
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        client._session = MagicMock()
        client._session.post.return_value = mock_resp

        configs = client.get_pipeline_configs()

        assert len(configs) == 1
        cfg = configs[0]
        assert cfg.id == "p1"
        assert cfg.name == "orders"
        assert cfg.source_type == "sql_database"
        assert cfg.source_config == {"tables": ["orders"]}
        assert cfg.source_credentials == {"connection_string": "pg://host/db"}
        assert cfg.dataset_name == "raw"
        assert cfg.schedule == "*/5 * * * *"
        assert cfg.write_disposition == "append"
        assert cfg.enabled is True
        assert cfg.trigger_now is True
        assert cfg.pending_run_id == "run-1"
        assert cfg.last_run_at is not None
        assert cfg.last_run_at.year == 2025

    def test_empty_pipelines_list(self) -> None:
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"pipelines": []}
        mock_resp.raise_for_status = MagicMock()
        client._session = MagicMock()
        client._session.post.return_value = mock_resp

        configs = client.get_pipeline_configs()

        assert configs == []

    def test_missing_optional_fields(self) -> None:
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "pipelines": [
                {
                    "id": "p2",
                    "name": "minimal",
                    "sourceType": "rest_api",
                    "datasetName": "raw",
                },
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        client._session = MagicMock()
        client._session.post.return_value = mock_resp

        configs = client.get_pipeline_configs()

        assert len(configs) == 1
        cfg = configs[0]
        assert cfg.schedule is None
        assert cfg.last_run_at is None
        assert cfg.trigger_now is False
        assert cfg.pending_run_id == ""

    def test_http_error_returns_empty_and_unhealthy(self) -> None:
        client = _make_client()
        client._session = MagicMock()
        client._session.post.side_effect = requests.ConnectionError("refused")

        configs = client.get_pipeline_configs()

        assert configs == []
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
        client.get_pipeline_configs()

        headers = client._session.post.call_args.kwargs["headers"]
        assert "Authorization" not in headers

    def test_token_fetched_and_sent(self) -> None:
        client = _make_auth_client()
        client._session = MagicMock()
        client._session.post.side_effect = [
            _token_response("tok-1"),
            _api_response(),
        ]

        client.get_pipeline_configs()

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

        client.get_pipeline_configs()
        client.get_pipeline_configs()

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

        configs = client.get_pipeline_configs()

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

        configs = client.get_pipeline_configs()

        assert configs == []
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

        client.get_pipeline_configs()

        healthy, details = client.health_status()
        assert healthy is False
        assert "refused" in details["last_error"]
        assert details["last_check_at"] != ""
