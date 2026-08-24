"""Minimal health-check HTTP server for Kubernetes readiness probes.

Runs in a daemon thread so it doesn't block the main poll loop.

What "ready" means depends on the worker's mode, because the two modes
schedule from different sources:

- **api mode** (``PIPELINES_DIR`` unset): the FairTier API *is* the schedule.
  A poll failure means the worker cannot know what to run, so it is not
  ready — 503.
- **files mode** (pipelines-as-files Phase 2): the pipelines checkout is the
  schedule, and the poll carries only Run-now triggers and fallback source
  credentials. An API outage degrades — no triggers, no run history — while
  scheduled ingestion keeps firing, so reporting NotReady there describes a
  worker that is in fact working. The checkout takes the API's place as the
  gate; API reachability stays in the body as ``api_healthy``, which is a
  diagnostic and not a verdict.

Only a readiness probe reads this (the chart sets no liveness probe on the
worker), so an unready worker is reported, never restarted.
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from dlt_worker.api_client import APIClient
from dlt_worker.pipeline_files import checkout_present

logger = logging.getLogger(__name__)


def _readiness(client: APIClient, pipelines_dir: str) -> tuple[bool, dict[str, Any]]:
    """Return (ready, body) for the current mode.

    last_error is deliberately never echoed: the server binds on 0.0.0.0 and
    error strings can quote internal URLs and config details. The full error
    stays in the worker's logs.
    """
    api_healthy, details = client.health_status()
    if not pipelines_dir:
        return api_healthy, {
            "healthy": api_healthy,
            "last_check_at": details["last_check_at"],
        }

    ready = checkout_present(pipelines_dir)
    return ready, {
        "healthy": ready,
        "last_check_at": details["last_check_at"],
        "mode": "files",
        "api_healthy": api_healthy,
    }


def start_health_server(
    client: APIClient, port: int, pipelines_dir: str = ""
) -> HTTPServer:
    """Start the /healthz HTTP server in a daemon thread.

    ``pipelines_dir`` is the worker's ``PIPELINES_DIR``: empty selects api
    mode, set selects files mode and is the directory the readiness gate
    checks.
    """

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path != "/healthz":
                self.send_response(404)
                self.end_headers()
                return

            ready, payload = _readiness(client, pipelines_dir)
            status = 200 if ready else 503
            body = json.dumps(payload).encode()

            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            # Suppress default access logs (K8s probes are noisy).
            pass

    server = HTTPServer(("0.0.0.0", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(
        "Health server listening on :%d/healthz (mode=%s)",
        server.server_port,
        "files" if pipelines_dir else "api",
    )
    return server
