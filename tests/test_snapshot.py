"""Tests for the state-snapshot webhook."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
import requests

from dlt_worker import config
from dlt_worker.snapshot import trigger_snapshot


class TestTriggerSnapshot:
    """Tests for trigger_snapshot."""

    SNAPSHOT_URL = "http://localhost:9999/snapshot"

    def setup_method(self) -> None:
        config.SNAPSHOT_URL = self.SNAPSHOT_URL

    def teardown_method(self) -> None:
        config.SNAPSHOT_URL = ""

    @patch("dlt_worker.snapshot.requests.post")
    def test_successful_snapshot(
        self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Successful snapshot logs the status."""
        mock_post.return_value.json.return_value = {"status": "created"}
        mock_post.return_value.raise_for_status = MagicMock()

        with caplog.at_level(logging.INFO, logger="dlt_worker.snapshot"):
            trigger_snapshot("my-pipeline")

        mock_post.assert_called_once_with(
            self.SNAPSHOT_URL,
            json={},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        assert "snapshot created" in caplog.text

    @patch("dlt_worker.snapshot.requests.post")
    def test_unchanged_snapshot(
        self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Unchanged snapshot is logged normally (not a warning)."""
        mock_post.return_value.json.return_value = {"status": "unchanged"}
        mock_post.return_value.raise_for_status = MagicMock()

        with caplog.at_level(logging.INFO, logger="dlt_worker.snapshot"):
            trigger_snapshot("my-pipeline")

        assert "snapshot unchanged" in caplog.text

    @patch("dlt_worker.snapshot.requests.post")
    def test_sidecar_unreachable_warns(
        self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Connection failure logs a warning but does not raise."""
        mock_post.side_effect = requests.ConnectionError("sidecar down")

        with caplog.at_level(logging.WARNING, logger="dlt_worker.snapshot"):
            trigger_snapshot("my-pipeline")

        assert "failed to trigger snapshot webhook" in caplog.text

    @patch("dlt_worker.snapshot.requests.post")
    def test_sidecar_error_response_warns(
        self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """HTTP error response logs a warning but does not raise."""
        mock_post.return_value.raise_for_status.side_effect = requests.HTTPError("500")

        with caplog.at_level(logging.WARNING, logger="dlt_worker.snapshot"):
            trigger_snapshot("my-pipeline")

        assert "failed to trigger snapshot webhook" in caplog.text

    @patch("dlt_worker.snapshot.requests.post")
    def test_skips_when_url_not_configured(
        self,
        mock_post: MagicMock,
    ) -> None:
        """No HTTP call when SNAPSHOT_URL is empty."""
        config.SNAPSHOT_URL = ""

        trigger_snapshot("my-pipeline")

        mock_post.assert_not_called()
