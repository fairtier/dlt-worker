"""Tests for the /healthz endpoint."""

from __future__ import annotations

import json
import urllib.request
from urllib.error import HTTPError

import requests

from dlt_worker.api_client import APIClient
from dlt_worker.health import start_health_server


def _get(port: int, path: str) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}") as resp:
            return resp.status, resp.read()
    except HTTPError as e:
        return e.code, e.read()


def test_healthz_does_not_echo_error_details() -> None:
    """S6: /healthz binds on 0.0.0.0 — error strings can quote internal
    URLs and must never be served, only logged."""
    client = APIClient(base_url="http://internal-api.svc:8080", customer_slug="acme")
    client._mark_unhealthy(
        requests.ConnectionError("http://internal-api.svc:8080 refused")
    )
    server = start_health_server(client, 0)
    try:
        status, body = _get(server.server_port, "/healthz")
    finally:
        server.shutdown()

    assert status == 503
    payload = json.loads(body)
    assert payload["healthy"] is False
    assert set(payload) == {"healthy", "last_check_at"}
    assert b"internal-api" not in body


def test_healthz_healthy_and_unknown_path() -> None:
    client = APIClient(base_url="http://api", customer_slug="acme")
    server = start_health_server(client, 0)
    try:
        status, body = _get(server.server_port, "/healthz")
        assert status == 200
        assert json.loads(body)["healthy"] is True

        status, _ = _get(server.server_port, "/other")
        assert status == 404
    finally:
        server.shutdown()
