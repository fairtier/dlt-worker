"""HTTP client for the FairTier API.

Fetches pipeline and transformation configurations and reports run
results. Uses plain HTTP
(requests) rather than gRPC to avoid pulling in a heavy protobuf/grpc
dependency on the Python side.  The FairTier API exposes a Connect
(JSON) endpoint that works with regular HTTP POST + JSON bodies.

Authentication: when OIDC credentials are configured, every call carries a
Casdoor client-credentials bearer token. The FairTier API binds the token's
tenant (the app name in its subject, ``dlt-worker-<slug>``) to the requested
customer slug, so one tenant's worker can never read another's pipeline
configs. The same OIDC app is used for the Lakekeeper catalog.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

import requests

logger = logging.getLogger(__name__)

# Refresh the cached token this many seconds before it expires.
_TOKEN_REFRESH_MARGIN = 60


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
    # Files mode: True when source_credentials were decrypted from the
    # pipeline's .credentials.age file in the checkout (git truth). Such a
    # config never falls back to poll/cached credentials.
    has_file_credentials: bool = False


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
class TransformationConfig:
    id: str
    name: str
    repo_url: str  # empty string means the hosted repo (TRANSFORM_REPO_URL)
    repo_ref: str
    git_credentials: dict[str, Any]
    schedule: str | None
    trigger_after_pipeline_id: str
    dbt_selector: str
    enabled: bool
    trigger_now: bool = False
    pending_run_id: str = ""
    last_run_at: datetime | None = None


@dataclass
class TransformationRunReport:
    transformation_id: str
    status: str  # "success" or "failed"
    started_at: str
    completed_at: str
    commit_sha: str = ""
    models_total: int = 0
    models_failed: int = 0
    tests_total: int = 0
    tests_failed: int = 0
    model_results: str = ""  # JSON array of per-node results
    error_message: str = ""
    run_id: str = ""


@dataclass
class APIClient:
    base_url: str
    customer_slug: str
    oidc_token_url: str = ""
    oidc_client_id: str = ""
    oidc_client_secret: str = ""
    timeout: int = 30
    _session: requests.Session = field(default_factory=requests.Session, repr=False)
    _healthy: bool = field(default=True, repr=False)
    _last_error: str = field(default="", repr=False)
    _last_check_at: str = field(default="", repr=False)
    _token: str = field(default="", repr=False)
    _token_expires_at: float = field(default=0.0, repr=False)

    @property
    def auth_enabled(self) -> bool:
        """Whether FairTier API calls carry a bearer token."""
        return bool(
            self.oidc_token_url and self.oidc_client_id and self.oidc_client_secret
        )

    def _get_token(self) -> str:
        """Return a cached client-credentials token, fetching when stale."""
        if self._token and time.monotonic() < self._token_expires_at:
            return self._token

        resp = self._session.post(
            self.oidc_token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.oidc_client_id,
                "client_secret": self.oidc_client_secret,
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        # Re-raise parse failures (non-JSON body, missing access_token) as
        # RequestException so callers' "central unreachable" degradation
        # paths handle them instead of a raw traceback losing the tick.
        try:
            data = resp.json()
            token = data["access_token"]
            expires_in = int(data.get("expires_in", 3600))
        except (ValueError, KeyError, TypeError) as e:
            raise requests.RequestException(
                f"OIDC token response malformed: {type(e).__name__}"
            ) from e
        self._token = token
        self._token_expires_at = time.monotonic() + max(
            expires_in - _TOKEN_REFRESH_MARGIN, 30
        )
        return self._token

    def _post(self, url: str, payload: dict[str, Any]) -> requests.Response:
        """POST JSON with bearer auth when configured.

        Retries once with a fresh token on 401 (token revoked/expired early,
        e.g. after a Casdoor credential rotation).
        """
        headers = {"Content-Type": "application/json"}
        if self.auth_enabled:
            headers["Authorization"] = f"Bearer {self._get_token()}"

        resp = self._session.post(
            url, json=payload, timeout=self.timeout, headers=headers
        )
        if resp.status_code == 401 and self.auth_enabled:
            self._token = ""
            self._token_expires_at = 0.0
            headers["Authorization"] = f"Bearer {self._get_token()}"
            resp = self._session.post(
                url, json=payload, timeout=self.timeout, headers=headers
            )
        return resp

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
        """Fetch all enabled pipeline configs for this customer.

        Legacy-mode entry point: a fetch failure is indistinguishable from
        "no pipelines" (empty list). Files mode needs the distinction —
        use try_get_pipeline_configs there.
        """
        configs = self.try_get_pipeline_configs()
        return configs if configs is not None else []

    def try_get_pipeline_configs(self) -> list[PipelineConfig] | None:
        """Fetch pipeline configs, or None when the FairTier API is unreachable.

        A 200 with a non-JSON body (proxy error page, truncated response)
        counts as unreachable; one malformed pipeline record is skipped so
        the rest still run — a garbage-returning central must degrade
        exactly like an unreachable one, never halt scheduling.
        """
        url = f"{self.base_url}/pipeline.v1.PipelineService/GetPipelineConfigs"
        try:
            resp = self._post(url, {"customerSlug": self.customer_slug})
            resp.raise_for_status()
            data = resp.json()
            pipelines = data.get("pipelines", [])
            if not isinstance(pipelines, list):
                raise ValueError("'pipelines' is not a list")
        except (requests.RequestException, ValueError) as e:
            logger.exception("Failed to fetch pipeline configs")
            self._mark_unhealthy(e)
            return None

        self._mark_healthy()

        configs = []
        for p in pipelines:
            try:
                configs.append(_parse_pipeline_record(p))
            except Exception:
                logger.warning(
                    "Skipping malformed pipeline record (id=%s)",
                    p.get("id", "?") if isinstance(p, dict) else "?",
                    exc_info=True,
                )
        return configs

    def get_transformation_configs(self) -> list[TransformationConfig]:
        """Fetch all dbt transformation configs for this customer.

        Deliberately does not touch health status: transformations are
        optional, and a FairTier API without the TransformationService yet
        must not flip the pod unready while pipelines still work.
        """
        url = (
            f"{self.base_url}"
            "/transformation.v1.TransformationService/GetTransformationConfigs"
        )
        try:
            resp = self._post(url, {"customerSlug": self.customer_slug})
            resp.raise_for_status()
            data = resp.json()
            transformations = data.get("transformations", [])
            if not isinstance(transformations, list):
                raise ValueError("'transformations' is not a list")
        except (requests.RequestException, ValueError):
            logger.warning("Failed to fetch transformation configs", exc_info=True)
            return []

        configs = []
        for t in transformations:
            try:
                configs.append(_parse_transformation_record(t))
            except Exception:
                logger.warning(
                    "Skipping malformed transformation record (id=%s)",
                    t.get("id", "?") if isinstance(t, dict) else "?",
                    exc_info=True,
                )
        return configs

    def report_transformation_run(self, report: TransformationRunReport) -> bool:
        """Report the result of a transformation run back to FairTier API.

        Returns True on success, False on failure.
        """
        url = (
            f"{self.base_url}"
            "/transformation.v1.TransformationService/ReportTransformationRun"
        )
        try:
            resp = self._post(
                url,
                {
                    "transformationId": report.transformation_id,
                    "status": report.status,
                    "startedAt": report.started_at,
                    "completedAt": report.completed_at,
                    "commitSha": report.commit_sha,
                    "modelsTotal": report.models_total,
                    "modelsFailed": report.models_failed,
                    "testsTotal": report.tests_total,
                    "testsFailed": report.tests_failed,
                    "modelResults": report.model_results,
                    "errorMessage": report.error_message,
                    "runId": report.run_id,
                },
            )
            resp.raise_for_status()
            return True
        except requests.RequestException:
            logger.exception(
                "Failed to report transformation run for %s", report.transformation_id
            )
            return False

    def report_pipeline_run(self, report: PipelineRunReport) -> bool:
        """Report the result of a pipeline run back to FairTier API.

        Returns True on success, False on failure.
        """
        url = f"{self.base_url}/pipeline.v1.PipelineService/ReportPipelineRun"
        try:
            resp = self._post(
                url,
                {
                    "pipelineId": report.pipeline_id,
                    "status": report.status,
                    "startedAt": report.started_at,
                    "completedAt": report.completed_at,
                    "rowsLoaded": report.rows_loaded,
                    "errorMessage": report.error_message,
                    "runId": report.run_id,
                },
            )
            resp.raise_for_status()
            self._mark_healthy()
            return True
        except requests.RequestException as e:
            logger.exception("Failed to report pipeline run for %s", report.pipeline_id)
            self._mark_unhealthy(e)
            return False


def _parse_last_run_at(record: dict[str, Any]) -> datetime | None:
    last_run_at_str = record.get("lastRunAt", "")
    if not last_run_at_str:
        return None
    return datetime.fromisoformat(last_run_at_str.replace("Z", "+00:00"))


def _parse_pipeline_record(p: dict[str, Any]) -> PipelineConfig:
    """Map one API pipeline record to a PipelineConfig. Raises on malformed
    input — the caller skips the record and keeps the rest."""
    source_config = p.get("sourceConfig", "{}")
    source_credentials = p.get("sourceCredentials", "{}")
    return PipelineConfig(
        id=p["id"],
        name=p["name"],
        source_type=p["sourceType"],
        source_config=json.loads(source_config) if source_config else {},
        source_credentials=json.loads(source_credentials) if source_credentials else {},
        dataset_name=p["datasetName"],
        schedule=p.get("schedule") or None,
        write_disposition=p.get("writeDisposition", "append"),
        merge_strategy=p.get("mergeStrategy", ""),
        enabled=p.get("enabled", True),
        trigger_now=p.get("triggerNow", False),
        pending_run_id=p.get("pendingRunId", ""),
        last_run_at=_parse_last_run_at(p),
    )


def _parse_transformation_record(t: dict[str, Any]) -> TransformationConfig:
    """Map one API transformation record to a TransformationConfig. Raises on
    malformed input — the caller skips the record and keeps the rest."""
    git_credentials = t.get("gitCredentials", "{}")
    return TransformationConfig(
        id=t["id"],
        name=t["name"],
        repo_url=t.get("repoUrl", ""),
        repo_ref=t.get("repoRef") or "main",
        git_credentials=json.loads(git_credentials) if git_credentials else {},
        schedule=t.get("schedule") or None,
        trigger_after_pipeline_id=t.get("triggerAfterPipelineId", ""),
        dbt_selector=t.get("dbtSelector", ""),
        enabled=t.get("enabled", True),
        trigger_now=t.get("triggerNow", False),
        pending_run_id=t.get("pendingRunId", ""),
        last_run_at=_parse_last_run_at(t),
    )
