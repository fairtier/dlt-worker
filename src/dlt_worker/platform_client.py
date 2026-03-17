"""HTTP client for the Platform API.

Fetches pipeline configurations and reports run results. Uses plain HTTP
(requests) rather than gRPC to avoid pulling in a heavy protobuf/grpc
dependency on the Python side.  The Platform API exposes a Connect
(JSON) endpoint that works with regular HTTP POST + JSON bodies.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

import requests

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    id: str
    name: str
    source_type: str
    source_config: dict[str, Any]
    source_credentials: dict[str, Any]
    dataset_name: str
    schedule: str | None
    write_disposition: Literal["skip", "append", "replace", "merge"]
    enabled: bool
    merge_strategy: str = ""
    trigger_now: bool = False
    pending_run_id: str = ""
    last_run_at: datetime | None = None


@dataclass
class PipelineRunReport:
    pipeline_id: str
    status: str  # "running", "success", or "failed"
    started_at: str
    completed_at: str
    rows_loaded: int = 0
    error_message: str = ""
    run_id: str = ""


@dataclass
class PlatformClient:
    base_url: str
    customer_slug: str
    timeout: int = 30
    _session: requests.Session = field(default_factory=requests.Session, repr=False)
    _healthy: bool = field(default=True, repr=False)
    _last_error: str = field(default="", repr=False)
    _last_check_at: str = field(default="", repr=False)

    def health_status(self) -> tuple[bool, dict[str, Any]]:
        """Return health status and details dict."""
        return self._healthy, {
            "healthy": self._healthy,
            "last_error": self._last_error,
            "last_check_at": self._last_check_at,
        }

    def _mark_healthy(self) -> None:
        self._healthy = True
        self._last_error = ""
        self._last_check_at = datetime.now(timezone.utc).isoformat()

    def _mark_unhealthy(self, err: Exception) -> None:
        self._healthy = False
        self._last_error = str(err)
        self._last_check_at = datetime.now(timezone.utc).isoformat()

    def get_pipeline_configs(self) -> list[PipelineConfig]:
        """Fetch all enabled pipeline configs for this customer."""
        url = f"{self.base_url}/pipeline.v1.PipelineService/GetPipelineConfigs"
        try:
            resp = self._session.post(
                url,
                json={"customerSlug": self.customer_slug},
                timeout=self.timeout,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.exception("Failed to fetch pipeline configs")
            self._mark_unhealthy(e)
            return []

        self._mark_healthy()

        data = resp.json()
        configs = []
        for p in data.get("pipelines", []):
            source_config = p.get("sourceConfig", "{}")
            source_credentials = p.get("sourceCredentials", "{}")
            last_run_at_str = p.get("lastRunAt", "")
            last_run_at = (
                datetime.fromisoformat(last_run_at_str.replace("Z", "+00:00"))
                if last_run_at_str
                else None
            )
            configs.append(
                PipelineConfig(
                    id=p["id"],
                    name=p["name"],
                    source_type=p["sourceType"],
                    source_config=json.loads(source_config) if source_config else {},
                    source_credentials=json.loads(source_credentials)
                    if source_credentials
                    else {},
                    dataset_name=p["datasetName"],
                    schedule=p.get("schedule") or None,
                    write_disposition=p.get("writeDisposition", "append"),
                    merge_strategy=p.get("mergeStrategy", ""),
                    enabled=p.get("enabled", True),
                    trigger_now=p.get("triggerNow", False),
                    pending_run_id=p.get("pendingRunId", ""),
                    last_run_at=last_run_at,
                )
            )
        return configs

    def report_pipeline_run(self, report: PipelineRunReport) -> bool:
        """Report the result of a pipeline run back to Platform API.

        Returns True on success, False on failure.
        """
        url = f"{self.base_url}/pipeline.v1.PipelineService/ReportPipelineRun"
        try:
            resp = self._session.post(
                url,
                json={
                    "pipelineId": report.pipeline_id,
                    "status": report.status,
                    "startedAt": report.started_at,
                    "completedAt": report.completed_at,
                    "rowsLoaded": report.rows_loaded,
                    "errorMessage": report.error_message,
                    "runId": report.run_id,
                },
                timeout=self.timeout,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            self._mark_healthy()
            return True
        except requests.RequestException as e:
            logger.exception("Failed to report pipeline run for %s", report.pipeline_id)
            self._mark_unhealthy(e)
            return False
