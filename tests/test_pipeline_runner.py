"""Tests for pipeline_runner: incremental loading and snapshot triggering."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock, patch

import dlt
import pytest
import requests

from dlt_worker import config
from dlt_worker.pipeline_runner import (
    _build_filesystem_source,
    _build_rest_api_source,
    _build_sql_database_source,
    _count_rows,
    _trigger_snapshot,
)
from dlt_worker.platform_client import PipelineConfig


def _make_config(**overrides: Any) -> PipelineConfig:
    defaults: dict[str, Any] = {
        "id": "p1",
        "name": "test-pipeline",
        "source_type": "sql_database",
        "source_config": {"tables": ["orders"]},
        "source_credentials": {"connection_string": "postgresql://u:p@host/db"},
        "dataset_name": "raw",
        "schedule": None,
        "write_disposition": "append",
        "enabled": True,
    }
    defaults.update(overrides)
    return PipelineConfig(**defaults)


@patch("dlt.sources.sql_database.sql_database")
class TestBuildSqlDatabaseSource:
    """Tests for _build_sql_database_source."""

    def test_simple_tables_list(self, mock_sql_db: MagicMock) -> None:
        """Simple tables config works unchanged (full load)."""
        cfg = _make_config(source_config={"tables": ["orders", "customers"]})

        _build_sql_database_source(cfg)

        mock_sql_db.assert_called_once_with(
            credentials="postgresql://u:p@host/db",
            table_names=["orders", "customers"],
        )

    def test_tables_config_without_incremental(self, mock_sql_db: MagicMock) -> None:
        """tables_config with no incremental behaves like a simple table list."""
        cfg = _make_config(
            source_config={
                "tables_config": [
                    {"name": "orders"},
                    {"name": "customers"},
                ],
            },
        )

        _build_sql_database_source(cfg)

        mock_sql_db.assert_called_once_with(
            credentials="postgresql://u:p@host/db",
            table_names=["orders", "customers"],
        )
        # No apply_hints called when there's no incremental config
        source = mock_sql_db.return_value
        source.resources.__getitem__.return_value.apply_hints.assert_not_called()

    def test_tables_config_with_incremental(self, mock_sql_db: MagicMock) -> None:
        """tables_config with incremental applies hints to the right resources."""
        cfg = _make_config(
            source_config={
                "tables_config": [
                    {
                        "name": "orders",
                        "incremental": {
                            "cursor_path": "updated_at",
                            "initial_value": "2024-01-01T00:00:00Z",
                        },
                    },
                    {"name": "customers"},
                ],
            },
        )

        source = mock_sql_db.return_value
        orders_resource = MagicMock()
        customers_resource = MagicMock()
        source.resources.__getitem__.side_effect = lambda name: {
            "orders": orders_resource,
            "customers": customers_resource,
        }[name]

        _build_sql_database_source(cfg)

        mock_sql_db.assert_called_once_with(
            credentials="postgresql://u:p@host/db",
            table_names=["orders", "customers"],
        )

        # Only orders should get apply_hints (it has incremental config)
        orders_resource.apply_hints.assert_called_once()
        call_kwargs = orders_resource.apply_hints.call_args.kwargs
        inc = call_kwargs["incremental"]
        assert isinstance(inc, dlt.sources.incremental)
        assert inc.cursor_path == "updated_at"
        assert inc.initial_value == "2024-01-01T00:00:00Z"

        # customers has no incremental — no apply_hints
        customers_resource.apply_hints.assert_not_called()

    def test_tables_config_incremental_without_initial_value(
        self, mock_sql_db: MagicMock
    ) -> None:
        """Incremental without initial_value loads all rows on first run."""
        cfg = _make_config(
            source_config={
                "tables_config": [
                    {"name": "events", "incremental": {"cursor_path": "id"}},
                ],
            },
        )

        resource = MagicMock()
        source = mock_sql_db.return_value
        source.resources.__getitem__.return_value = resource

        _build_sql_database_source(cfg)

        resource.apply_hints.assert_called_once()
        inc = resource.apply_hints.call_args.kwargs["incremental"]
        assert inc.cursor_path == "id"
        assert inc.initial_value is None

    def test_tables_config_takes_precedence_over_tables(
        self, mock_sql_db: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """When both tables and tables_config are present, tables_config wins with a warning."""
        cfg = _make_config(
            source_config={
                "tables": ["ignored_table"],
                "tables_config": [{"name": "used_table"}],
            },
        )

        with caplog.at_level(logging.WARNING, logger="dlt_worker.pipeline_runner"):
            _build_sql_database_source(cfg)

        mock_sql_db.assert_called_once_with(
            credentials="postgresql://u:p@host/db",
            table_names=["used_table"],
        )
        assert "'tables' will be ignored" in caplog.text

    def test_no_tables_and_no_tables_config(self, mock_sql_db: MagicMock) -> None:
        """When neither tables nor tables_config is set, table_names is None."""
        cfg = _make_config(source_config={})

        _build_sql_database_source(cfg)

        mock_sql_db.assert_called_once_with(
            credentials="postgresql://u:p@host/db",
            table_names=None,
        )

    def test_missing_connection_string_raises(self, mock_sql_db: MagicMock) -> None:
        """Missing connection_string gives a descriptive ValueError."""
        cfg = _make_config(source_credentials={})

        with pytest.raises(ValueError, match="missing required 'connection_string'"):
            _build_sql_database_source(cfg)

    def test_tables_config_missing_name_raises(self, mock_sql_db: MagicMock) -> None:
        """tables_config entry without 'name' gives a descriptive ValueError."""
        cfg = _make_config(
            source_config={"tables_config": [{"incremental": {"cursor_path": "id"}}]},
        )

        with pytest.raises(ValueError, match="missing required 'name'"):
            _build_sql_database_source(cfg)

    def test_incremental_missing_cursor_path_raises(
        self, mock_sql_db: MagicMock
    ) -> None:
        """Incremental config without cursor_path gives a descriptive ValueError."""
        cfg = _make_config(
            source_config={
                "tables_config": [
                    {"name": "orders", "incremental": {"initial_value": "2024-01-01"}},
                ],
            },
        )

        with pytest.raises(ValueError, match="missing required 'cursor_path'"):
            _build_sql_database_source(cfg)

    def test_incremental_with_replace_warns(
        self, mock_sql_db: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Incremental + write_disposition=replace logs a warning."""
        cfg = _make_config(
            write_disposition="replace",
            source_config={
                "tables_config": [
                    {"name": "orders", "incremental": {"cursor_path": "updated_at"}},
                ],
            },
        )

        source = mock_sql_db.return_value
        source.resources.__getitem__.return_value = MagicMock()

        with caplog.at_level(logging.WARNING, logger="dlt_worker.pipeline_runner"):
            _build_sql_database_source(cfg)

        assert "write_disposition is 'replace'" in caplog.text
        assert "consider 'append' or 'merge'" in caplog.text

    def test_incremental_with_append_no_warning(
        self, mock_sql_db: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Incremental + write_disposition=append does not warn."""
        cfg = _make_config(
            write_disposition="append",
            source_config={
                "tables_config": [
                    {"name": "orders", "incremental": {"cursor_path": "updated_at"}},
                ],
            },
        )

        source = mock_sql_db.return_value
        source.resources.__getitem__.return_value = MagicMock()

        with caplog.at_level(logging.WARNING, logger="dlt_worker.pipeline_runner"):
            _build_sql_database_source(cfg)

        assert "write_disposition" not in caplog.text


@patch("dlt.sources.filesystem.filesystem")
class TestBuildFilesystemSource:
    """Tests for _build_filesystem_source."""

    def _fs_config(self, **overrides: Any) -> PipelineConfig:
        defaults: dict[str, Any] = {
            "id": "p1",
            "name": "test-fs-pipeline",
            "source_type": "filesystem",
            "source_config": {"bucket_url": "s3://my-bucket/data"},
            "source_credentials": {
                "access_key_id": "AKID",
                "secret_access_key": "SECRET",
                "endpoint_url": "https://s3.example.com",
                "region": "eu-central-1",
            },
            "dataset_name": "raw",
            "schedule": None,
            "write_disposition": "append",
            "enabled": True,
        }
        defaults.update(overrides)
        return PipelineConfig(**defaults)

    def test_basic_config(self, mock_fs: MagicMock) -> None:
        """Happy path with all fields provided."""
        cfg = self._fs_config()

        _build_filesystem_source(cfg)

        mock_fs.assert_called_once()
        call_kwargs = mock_fs.call_args.kwargs
        assert call_kwargs["bucket_url"] == "s3://my-bucket/data"
        assert call_kwargs["file_glob"] == "**/*"
        creds = call_kwargs["credentials"]
        assert creds.aws_access_key_id == "AKID"
        assert creds.aws_secret_access_key == "SECRET"
        assert creds.endpoint_url == "https://s3.example.com"
        assert creds.region_name == "eu-central-1"

    def test_default_file_glob(self, mock_fs: MagicMock) -> None:
        """Omitted file_glob defaults to **/*."""
        cfg = self._fs_config(source_config={"bucket_url": "s3://b"})

        _build_filesystem_source(cfg)

        assert mock_fs.call_args.kwargs["file_glob"] == "**/*"

    def test_custom_file_glob(self, mock_fs: MagicMock) -> None:
        """Explicit file_glob is passed through."""
        cfg = self._fs_config(
            source_config={"bucket_url": "s3://b", "file_glob": "*.csv"},
        )

        _build_filesystem_source(cfg)

        assert mock_fs.call_args.kwargs["file_glob"] == "*.csv"

    def test_optional_credentials_fields(self, mock_fs: MagicMock) -> None:
        """Omitted endpoint_url → None, omitted region → 'auto'."""
        cfg = self._fs_config(
            source_credentials={
                "access_key_id": "AKID",
                "secret_access_key": "SECRET",
            },
        )

        _build_filesystem_source(cfg)

        creds = mock_fs.call_args.kwargs["credentials"]
        assert creds.endpoint_url is None
        assert creds.region_name == "auto"

    def test_missing_bucket_url_raises(self, mock_fs: MagicMock) -> None:
        """Missing bucket_url gives a descriptive ValueError."""
        cfg = self._fs_config(source_config={})

        with pytest.raises(ValueError, match="missing required 'bucket_url'"):
            _build_filesystem_source(cfg)

    def test_missing_access_key_id_raises(self, mock_fs: MagicMock) -> None:
        """Missing access_key_id gives a descriptive ValueError."""
        cfg = self._fs_config(
            source_credentials={"secret_access_key": "SECRET"},
        )

        with pytest.raises(ValueError, match="missing required 'access_key_id'"):
            _build_filesystem_source(cfg)

    def test_missing_secret_access_key_raises(self, mock_fs: MagicMock) -> None:
        """Missing secret_access_key gives a descriptive ValueError."""
        cfg = self._fs_config(
            source_credentials={"access_key_id": "AKID"},
        )

        with pytest.raises(ValueError, match="missing required 'secret_access_key'"):
            _build_filesystem_source(cfg)


@patch("dlt.sources.rest_api.rest_api_source")
class TestBuildRestApiSource:
    """Tests for _build_rest_api_source."""

    def _rest_cfg(self, **resource_overrides: Any) -> PipelineConfig:
        resource: dict[str, Any] = {
            "name": "users",
            "endpoint": "/users",
        }
        resource.update(resource_overrides)
        return _make_config(
            source_type="rest_api",
            source_config={
                "base_url": "https://api.example.com",
                "resources": [resource],
            },
            source_credentials={},
        )

    def test_paginator_string_shorthand(self, mock_rest: MagicMock) -> None:
        """String paginator shorthand is passed through."""
        cfg = self._rest_cfg(paginator="header_link")

        _build_rest_api_source(cfg)

        call_config = mock_rest.call_args[0][0]
        endpoint = call_config["resources"][0]["endpoint"]
        assert endpoint["paginator"] == "header_link"

    def test_paginator_dict_form(self, mock_rest: MagicMock) -> None:
        """Dict paginator config is passed through."""
        paginator = {"type": "offset", "limit": 100}
        cfg = self._rest_cfg(paginator=paginator)

        _build_rest_api_source(cfg)

        call_config = mock_rest.call_args[0][0]
        endpoint = call_config["resources"][0]["endpoint"]
        assert endpoint["paginator"] == {"type": "offset", "limit": 100}

    def test_no_paginator_defaults_to_auto(self, mock_rest: MagicMock) -> None:
        """Without paginator field, endpoint has no paginator key (auto-detection)."""
        cfg = self._rest_cfg()

        _build_rest_api_source(cfg)

        call_config = mock_rest.call_args[0][0]
        endpoint = call_config["resources"][0]["endpoint"]
        assert "paginator" not in endpoint


class TestTriggerSnapshot:
    """Tests for _trigger_snapshot."""

    SNAPSHOT_URL = "http://localhost:9999/snapshot"

    def setup_method(self) -> None:
        config.SNAPSHOT_URL = self.SNAPSHOT_URL

    def teardown_method(self) -> None:
        config.SNAPSHOT_URL = ""

    @patch("dlt_worker.pipeline_runner.requests.post")
    def test_successful_snapshot(
        self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Successful snapshot logs the status."""
        mock_post.return_value.json.return_value = {"status": "created"}
        mock_post.return_value.raise_for_status = MagicMock()

        with caplog.at_level(logging.INFO, logger="dlt_worker.pipeline_runner"):
            _trigger_snapshot("my-pipeline")

        mock_post.assert_called_once_with(
            self.SNAPSHOT_URL,
            json={},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        assert "snapshot created" in caplog.text

    @patch("dlt_worker.pipeline_runner.requests.post")
    def test_unchanged_snapshot(
        self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Unchanged snapshot is logged normally (not a warning)."""
        mock_post.return_value.json.return_value = {"status": "unchanged"}
        mock_post.return_value.raise_for_status = MagicMock()

        with caplog.at_level(logging.INFO, logger="dlt_worker.pipeline_runner"):
            _trigger_snapshot("my-pipeline")

        assert "snapshot unchanged" in caplog.text

    @patch("dlt_worker.pipeline_runner.requests.post")
    def test_sidecar_unreachable_warns(
        self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Connection failure logs a warning but does not raise."""
        mock_post.side_effect = requests.ConnectionError("sidecar down")

        with caplog.at_level(logging.WARNING, logger="dlt_worker.pipeline_runner"):
            _trigger_snapshot("my-pipeline")

        assert "failed to trigger snapshot webhook" in caplog.text

    @patch("dlt_worker.pipeline_runner.requests.post")
    def test_sidecar_error_response_warns(
        self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """HTTP error response logs a warning but does not raise."""
        mock_post.return_value.raise_for_status.side_effect = requests.HTTPError("500")

        with caplog.at_level(logging.WARNING, logger="dlt_worker.pipeline_runner"):
            _trigger_snapshot("my-pipeline")

        assert "failed to trigger snapshot webhook" in caplog.text

    @patch("dlt_worker.pipeline_runner.requests.post")
    def test_skips_when_url_not_configured(
        self,
        mock_post: MagicMock,
    ) -> None:
        """No HTTP call when SNAPSHOT_URL is empty."""
        config.SNAPSHOT_URL = ""

        _trigger_snapshot("my-pipeline")

        mock_post.assert_not_called()


@patch("dlt.sources.rest_api.rest_api_source")
class TestBuildRestApiSourceAuth:
    """Tests for _build_rest_api_source auth handling."""

    def _rest_config(self, **overrides: Any) -> PipelineConfig:
        defaults: dict[str, Any] = {
            "id": "p1",
            "name": "test-rest-pipeline",
            "source_type": "rest_api",
            "source_config": {
                "base_url": "https://api.example.com",
                "resources": [{"name": "items", "endpoint": "/items"}],
            },
            "source_credentials": {},
            "dataset_name": "raw",
            "schedule": None,
            "write_disposition": "append",
            "enabled": True,
        }
        defaults.update(overrides)
        return PipelineConfig(**defaults)

    def test_bearer_via_api_key(self, mock_rest: MagicMock) -> None:
        """Backwards compat: api_key becomes bearer auth dict."""
        cfg = self._rest_config(source_credentials={"api_key": "tok123"})

        _build_rest_api_source(cfg)

        client = mock_rest.call_args[0][0]["client"]
        assert client["auth"] == {"type": "bearer", "token": "tok123"}
        assert "headers" not in client

    def test_explicit_auth_dict(self, mock_rest: MagicMock) -> None:
        """Explicit auth dict is passed through as-is."""
        auth_cfg = {
            "type": "oauth2_client_credentials",
            "access_token_url": "https://auth.example.com/token",
            "client_id": "cid",
            "client_secret": "csec",
        }
        cfg = self._rest_config(source_credentials={"auth": auth_cfg})

        _build_rest_api_source(cfg)

        client = mock_rest.call_args[0][0]["client"]
        assert client["auth"] is auth_cfg

    def test_auth_takes_precedence_over_api_key(self, mock_rest: MagicMock) -> None:
        """When both auth and api_key are present, auth wins."""
        auth_cfg = {"type": "http_basic", "username": "u", "password": "p"}
        cfg = self._rest_config(
            source_credentials={"auth": auth_cfg, "api_key": "ignored"},
        )

        _build_rest_api_source(cfg)

        client = mock_rest.call_args[0][0]["client"]
        assert client["auth"] is auth_cfg

    def test_headers_alongside_auth(self, mock_rest: MagicMock) -> None:
        """Custom headers work alongside any auth method."""
        cfg = self._rest_config(
            source_credentials={
                "api_key": "tok",
                "headers": {"X-Custom": "val"},
            },
        )

        _build_rest_api_source(cfg)

        client = mock_rest.call_args[0][0]["client"]
        assert client["auth"] == {"type": "bearer", "token": "tok"}
        assert client["headers"] == {"X-Custom": "val"}

    def test_no_auth(self, mock_rest: MagicMock) -> None:
        """No auth or api_key — no auth key in client config."""
        cfg = self._rest_config(source_credentials={})

        _build_rest_api_source(cfg)

        client = mock_rest.call_args[0][0]["client"]
        assert "auth" not in client
        assert "headers" not in client


def _table_metrics(items_count: Any) -> MagicMock:
    """Helper: create a mock table_metrics object with given items_count."""
    m = MagicMock()
    m.items_count = items_count
    return m


class TestCountRows:
    """Tests for _count_rows."""

    def test_none_normalize_info(self) -> None:
        """None normalize_info returns 0."""
        assert _count_rows(None) == 0

    def test_excludes_dlt_internal_tables(self) -> None:
        """Tables starting with _dlt_ are excluded from the count."""
        normalize_info = MagicMock()
        normalize_info.metrics = {
            "job1": [
                {
                    "table_metrics": {
                        "_dlt_loads": _table_metrics(3),
                        "_dlt_version": _table_metrics(1),
                        "orders": _table_metrics(100),
                    },
                },
            ],
        }
        assert _count_rows(normalize_info) == 100

    def test_counts_data_tables(self) -> None:
        """Normal data tables are counted correctly across jobs and metrics."""
        normalize_info = MagicMock()
        normalize_info.metrics = {
            "job1": [
                {"table_metrics": {"orders": _table_metrics(50)}},
                {"table_metrics": {"customers": _table_metrics(30)}},
            ],
            "job2": [
                {"table_metrics": {"products": _table_metrics(20)}},
            ],
        }
        assert _count_rows(normalize_info) == 100

    def test_handles_none_items_count(self) -> None:
        """None items_count is safely skipped (not an int/float)."""
        normalize_info = MagicMock()
        normalize_info.metrics = {
            "job1": [
                {
                    "table_metrics": {
                        "orders": _table_metrics(None),
                        "customers": _table_metrics(10),
                    },
                },
            ],
        }
        assert _count_rows(normalize_info) == 10

    def test_empty_table_metrics(self) -> None:
        """Empty table_metrics dict returns 0."""
        normalize_info = MagicMock()
        normalize_info.metrics = {
            "job1": [{"table_metrics": {}}],
        }
        assert _count_rows(normalize_info) == 0
