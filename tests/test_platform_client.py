"""Tests for PlatformClient HTTP interactions."""

from __future__ import annotations

from unittest.mock import MagicMock

import requests

from dlt_worker.platform_client import PipelineRunReport, PlatformClient


def _make_client() -> PlatformClient:
    return PlatformClient(base_url="http://localhost:8080", customer_slug="acme")


class TestGetPipelineConfigs:
    """Tests for PlatformClient.get_pipeline_configs."""

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
    """Tests for PlatformClient.report_pipeline_run."""

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


class TestHealthStatus:
    """Tests for PlatformClient health tracking."""

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
