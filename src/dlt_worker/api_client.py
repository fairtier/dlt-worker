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
from urllib.parse import urlsplit

import requests
from opentelemetry.trace import SpanKind

from dlt_worker import telemetry

logger = logging.getLogger(__name__)

# Refresh the cached token this many seconds before it expires.
_TOKEN_REFRESH_MARGIN = 60


def _rpc_target(url: str) -> tuple[str, str]:
    """Split a Connect endpoint URL into (service, method).

    ``.../pipeline.v1.PipelineService/GetPipelineConfigs`` ->
    ``("pipeline.v1.PipelineService", "GetPipelineConfigs")``. Both halves
    are from a fixed set, so they are safe as metric attributes.
    """
    parts = urlsplit(url).path.rsplit("/", 2)
    if len(parts) < 3:
        return "", parts[-1]
    return parts[-2], parts[-1]


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
class PipelineTrigger:
    """What the control-plane poll still carries for one pipeline.

    Not a definition: since the pipelines-as-files Phase 2.5 cleanup the
    checkout is the only source of a pipeline's definition and schedule, and
    GetPipelineConfigs was shrunk to the three things a checkout cannot
    provide — the manual trigger, the credentials of a synthesized
    file_upload pipeline (no .age file exists for those), and the last-run
    watermark that seeds a worker with no local scheduler entry.
    """

    id: str
    source_credentials: dict[str, Any] = field(default_factory=dict)
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
class SourceTest:
    """One queued "can this thing read my X?" probe, claimed from the API.

    Config and credentials arrive in exactly the shape a run would receive
    them — a Google connection reference is already resolved server-side —
    so the probe exercises the same thing the run will.
    """

    id: str
    source_type: str
    source_config: dict[str, Any]
    source_credentials: dict[str, Any]


@dataclass
class SourceTestReport:
    id: str
    status: str  # "success" or "failed"
    message: str
    details: list[str] = field(default_factory=list)


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

        # Traced because "the worker went quiet" is as often a broken OIDC
        # app as it is a broken API. requests keeps neither headers nor the
        # form body (the client secret) in its exception messages, so the
        # default exception recording is safe here.
        with telemetry.tracer.start_as_current_span(
            "dlt_worker.oidc.token",
            kind=SpanKind.CLIENT,
            attributes={"server.address": urlsplit(self.oidc_token_url).hostname or ""},
        ):
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

        Every control plane call funnels through here, so this is also where
        they are traced and counted — one CLIENT span and one duration
        sample per call, method and status code as attributes.
        """
        service, method = _rpc_target(url)
        started = time.monotonic()
        status_code = 0

        with telemetry.tracer.start_as_current_span(
            f"{service}/{method}" if service else method,
            kind=SpanKind.CLIENT,
            attributes={
                "rpc.system": "connect_rpc",
                "rpc.service": service,
                "rpc.method": method,
                "server.address": urlsplit(url).hostname or "",
            },
        ) as span:
            try:
                headers = {"Content-Type": "application/json"}
                if self.auth_enabled:
                    headers["Authorization"] = f"Bearer {self._get_token()}"

                resp = self._session.post(
                    url, json=payload, timeout=self.timeout, headers=headers
                )
                if resp.status_code == 401 and self.auth_enabled:
                    span.add_event("auth.token_refreshed")
                    self._token = ""
                    self._token_expires_at = 0.0
                    headers["Authorization"] = f"Bearer {self._get_token()}"
                    resp = self._session.post(
                        url, json=payload, timeout=self.timeout, headers=headers
                    )
                status_code = resp.status_code
                span.set_attribute("http.response.status_code", status_code)
                return resp
            finally:
                # Counted on the way out so a transport failure (no
                # response, status 0) is counted too — that is the shape a
                # central outage takes.
                telemetry.api_requests.add(
                    1,
                    {
                        "rpc.method": method,
                        "http.response.status_code": status_code,
                    },
                )
                telemetry.api_request_duration.record(
                    time.monotonic() - started, {"rpc.method": method}
                )

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

    def try_get_pipeline_triggers(self) -> list[PipelineTrigger] | None:
        """Fetch the poll's per-pipeline triggers, or None when the FairTier
        API is unreachable.

        The None is the point: scheduling comes from the checkout, so the
        caller must tell "the control plane says there is nothing pending"
        apart from "the control plane did not answer" — the first prunes
        cached credentials, the second must not.

        A 200 with a non-JSON body (proxy error page, truncated response)
        counts as unreachable; one malformed record is skipped so the rest
        still run — a garbage-returning control plane must degrade exactly
        like an unreachable one, never halt scheduling.
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
                configs.append(_parse_trigger_record(p))
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

    def get_pending_source_tests(self) -> list[SourceTest]:
        """Claim this customer's queued source tests.

        Claiming happens server-side in the same statement as the read, so
        two overlapping polls cannot probe one test twice — with a database
        password, twice is how an account gets locked out.

        Deliberately does not touch health status: source tests are a
        convenience, and a FairTier API too old to serve them (404/501) must
        not flip the pod unready while pipelines still run.
        """
        url = f"{self.base_url}/pipeline.v1.PipelineService/GetPendingSourceTests"
        try:
            resp = self._post(url, {"customerSlug": self.customer_slug})
            resp.raise_for_status()
            data = resp.json()
            tests = data.get("tests", [])
            if not isinstance(tests, list):
                raise ValueError("'tests' is not a list")
        except (requests.RequestException, ValueError):
            logger.debug("Failed to fetch source tests", exc_info=True)
            return []

        out = []
        for t in tests:
            try:
                out.append(_parse_source_test(t))
            except Exception:
                logger.warning(
                    "Skipping malformed source test (id=%s)",
                    t.get("id", "?") if isinstance(t, dict) else "?",
                    exc_info=True,
                )
        return out

    def report_source_test(self, report: SourceTestReport) -> bool:
        """Report a probe's outcome. Returns True on success."""
        url = f"{self.base_url}/pipeline.v1.PipelineService/ReportSourceTest"
        try:
            resp = self._post(
                url,
                {
                    "id": report.id,
                    "status": report.status,
                    "message": report.message,
                    "details": report.details,
                },
            )
            resp.raise_for_status()
            return True
        except requests.RequestException:
            logger.exception("Failed to report source test %s", report.id)
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


def _parse_source_test(t: dict[str, Any]) -> SourceTest:
    return SourceTest(
        id=t["id"],
        source_type=t.get("sourceType", ""),
        source_config=_json_object(t.get("sourceConfig")),
        source_credentials=_json_object(t.get("sourceCredentials")),
    )


def _json_object(raw: Any) -> dict[str, Any]:
    """Parse a JSON-string field into an object; anything else is empty.

    The wire carries these as strings (they are free-form per source type),
    and a null, an empty string and a JSON array all mean the same thing
    here: nothing this probe can use.
    """
    if not raw:
        return {}
    value = json.loads(raw) if isinstance(raw, str) else raw
    return value if isinstance(value, dict) else {}


def _parse_last_run_at(record: dict[str, Any]) -> datetime | None:
    last_run_at_str = record.get("lastRunAt", "")
    if not last_run_at_str:
        return None
    return datetime.fromisoformat(last_run_at_str.replace("Z", "+00:00"))


def _parse_trigger_record(p: dict[str, Any]) -> PipelineTrigger:
    """Map one API pipeline record to a PipelineTrigger. Raises on malformed
    input — the caller skips the record and keeps the rest.

    Definition fields a pre-0.9.0 response may still carry are ignored
    rather than rejected: an older control plane must degrade to "triggers
    only", not to "every record is malformed".
    """
    source_credentials = p.get("sourceCredentials", "{}")
    return PipelineTrigger(
        id=p["id"],
        source_credentials=json.loads(source_credentials) if source_credentials else {},
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
