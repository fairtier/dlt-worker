"""State-snapshot webhook, called by the poll loop after a successful run.

Its own module rather than part of :mod:`dlt_worker.pipeline_runner`
because of who calls it: this runs in the *parent* poll loop, while
everything else in that module runs in a child (see
:mod:`dlt_worker.run_isolation`). Importing it from there would drag dlt —
~115 MB of resident memory — into a process that never runs a pipeline.
"""

from __future__ import annotations

import logging

import requests

from dlt_worker import config, telemetry

logger = logging.getLogger(__name__)


def trigger_snapshot(pipeline_name: str) -> None:
    """POST to a configured webhook after each successful pipeline run. Best-effort.

    Designed for use with a state-snapshot sidecar (e.g. snapshot-sidecar) but
    works with any endpoint that accepts an empty JSON POST.
    Requires SNAPSHOT_URL to be set; silently skips when not configured.
    """
    if not config.SNAPSHOT_URL:
        return
    try:
        resp = requests.post(
            config.SNAPSHOT_URL,
            json={},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        status = resp.json().get("status", "unknown")
        logger.info("Pipeline %s: snapshot %s", pipeline_name, status)
    except requests.RequestException:
        telemetry.add_event("snapshot.failed")
        logger.warning(
            "Pipeline %s: failed to trigger snapshot webhook",
            pipeline_name,
            exc_info=True,
        )
