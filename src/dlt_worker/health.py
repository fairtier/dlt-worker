"""Minimal health-check HTTP server for Kubernetes readiness probes.

Runs in a daemon thread so it doesn't block the main poll loop.
Returns 200 when the Platform API connection is healthy, 503 otherwise.
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from dlt_worker.platform_client import PlatformClient

logger = logging.getLogger(__name__)


def start_health_server(client: PlatformClient, port: int) -> None:
    """Start the /healthz HTTP server in a daemon thread."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path != "/healthz":
                self.send_response(404)
                self.end_headers()
                return

            healthy, details = client.health_status()
            status = 200 if healthy else 503
            body = json.dumps(details).encode()

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
    logger.info("Health server listening on :%d/healthz", port)
