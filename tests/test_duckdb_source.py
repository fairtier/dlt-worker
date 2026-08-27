"""Tests for the `duckdb` source type (duckdb_source.py).

These run against a real in-memory DuckDB but load only the `json`
extension (statically linked into the duckdb wheel), so they need no
network and none of the baked extension binaries.
"""

from __future__ import annotations

from typing import Any

import pytest

from dlt_worker import config
from dlt_worker.api_client import PipelineConfig
from dlt_worker.duckdb_source import (
    SUPPORTED_DUCKDB_EXTENSIONS,
    _connect,
    _secret_sql,
    build_duckdb_source,
    render_attach,
    source_extensions,
)


def _make_config(**overrides: Any) -> PipelineConfig:
    defaults: dict[str, Any] = {
        "id": "p1",
        "name": "duck-pipeline",
        "source_type": "duckdb",
        "source_config": {
            "extension": "json",
            "tables": [{"name": "t", "query": "SELECT 1 AS id"}],
        },
        "source_credentials": {},
        "dataset_name": "raw",
        "schedule": None,
        "write_disposition": "append",
        "enabled": True,
    }
    defaults.update(overrides)
    return PipelineConfig(**defaults)


class TestRenderAttach:
    def test_substitutes_params(self) -> None:
        out = render_attach(
            "p",
            "host={host} user={user} password={password} database=shop",
            {"host": "db.example.com", "user": "u", "password": "s3cret"},
        )
        assert out == "host=db.example.com user=u password=s3cret database=shop"

    def test_no_placeholders_passthrough(self) -> None:
        assert render_attach("p", "database=shop", {}) == "database=shop"

    def test_missing_params_named_but_never_valued(self) -> None:
        with pytest.raises(ValueError) as exc_info:
            render_attach("p", "host={host} password={password}", {"host": "h"})
        msg = str(exc_info.value)
        assert "'password'" in msg
        # The one provided value must not leak into the error either.
        assert "h" not in msg.replace("host", "").replace("attach", "")

    def test_extra_params_ignored(self) -> None:
        assert render_attach("p", "host={host}", {"host": "h", "junk": "x"}) == "host=h"


class TestSecretSql:
    def test_type_defaults_to_extension(self) -> None:
        sql = _secret_sql("p", "gsheets", {"provider": "access_token"})
        assert sql == (
            "CREATE OR REPLACE SECRET pipeline_secret "
            "(TYPE gsheets, provider 'access_token')"
        )

    def test_explicit_type_wins(self) -> None:
        sql = _secret_sql("p", "whatever", {"type": "s3", "region": "auto"})
        assert "TYPE s3" in sql
        assert "region 'auto'" in sql

    def test_values_are_quote_escaped(self) -> None:
        sql = _secret_sql("p", "x", {"token": "it's"})
        assert "token 'it''s'" in sql

    def test_invalid_key_rejected(self) -> None:
        with pytest.raises(ValueError, match="invalid secret key"):
            _secret_sql("p", "x", {"bad key; DROP": "v"})

    def test_invalid_type_rejected(self) -> None:
        with pytest.raises(ValueError, match="invalid secret type"):
            _secret_sql("p", "x", {"type": "s3; DROP"})


class TestSourceExtensions:
    """The LOAD list. One key or the other, ordered, first is primary."""

    def test_single_extension(self) -> None:
        assert source_extensions("p", {"extension": "gdrive"}) == ["gdrive"]

    def test_list_keeps_order(self) -> None:
        # Order is the contract: the first is the ATTACH TYPE and the
        # default secret type, so [gdrive, pdf] is not [pdf, gdrive].
        assert source_extensions("p", {"extensions": ["gdrive", "pdf"]}) == [
            "gdrive",
            "pdf",
        ]

    def test_duplicates_named_once(self) -> None:
        assert source_extensions("p", {"extensions": ["pdf", "pdf"]}) == ["pdf"]

    def test_both_keys_refused(self) -> None:
        with pytest.raises(ValueError, match="both 'extension' and 'extensions'"):
            source_extensions("p", {"extension": "pdf", "extensions": ["gdrive"]})

    def test_list_must_be_a_list(self) -> None:
        with pytest.raises(ValueError, match="'extensions' must be a list"):
            source_extensions("p", {"extensions": "gdrive"})

    def test_every_name_is_an_identifier(self) -> None:
        with pytest.raises(ValueError, match="invalid extension name"):
            source_extensions("p", {"extensions": ["gdrive", "pdf; DROP"]})

    def test_empty_is_missing(self) -> None:
        with pytest.raises(ValueError, match="missing required 'extension'"):
            source_extensions("p", {"extensions": []})


class TestBuildValidation:
    def test_missing_extension(self) -> None:
        cfg = _make_config(source_config={"tables": [{"name": "t"}]})
        with pytest.raises(ValueError, match="missing required 'extension'"):
            build_duckdb_source(cfg)

    def test_invalid_extension_name(self) -> None:
        cfg = _make_config(
            source_config={"extension": "my ext; DROP", "tables": [{"name": "t"}]}
        )
        with pytest.raises(ValueError, match="invalid extension name"):
            build_duckdb_source(cfg)

    def test_missing_tables(self) -> None:
        cfg = _make_config(source_config={"extension": "json"})
        with pytest.raises(ValueError, match="missing required 'tables'"):
            build_duckdb_source(cfg)

    def test_table_without_query_needs_attach(self) -> None:
        cfg = _make_config(
            source_config={"extension": "json", "tables": [{"name": "orders"}]}
        )
        with pytest.raises(ValueError, match="needs an explicit 'query'"):
            build_duckdb_source(cfg)

    def test_table_without_name(self) -> None:
        cfg = _make_config(
            source_config={
                "extension": "json",
                "tables": [{"query": "SELECT 1"}],
            }
        )
        with pytest.raises(ValueError, match="missing required 'name'"):
            build_duckdb_source(cfg)

    def test_every_extension_is_loaded(self) -> None:
        # Two statically-linked extensions, so the assertion is about the
        # LOAD list rather than about any binary: both names reach DuckDB.
        cfg = _make_config(
            source_config={
                "extensions": ["json", "icu"],
                "tables": [{"name": "t", "query": "SELECT 1 AS id"}],
            }
        )
        assert len(build_duckdb_source(cfg)) == 1

    def test_unsupported_extension_warns_not_fails(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        assert "json" not in SUPPORTED_DUCKDB_EXTENSIONS
        cfg = _make_config()
        with caplog.at_level("WARNING"):
            build_duckdb_source(cfg)
        assert any("not in the baked set" in r.message for r in caplog.records)


class TestExtraction:
    def test_streams_arrow_batches(self) -> None:
        cfg = _make_config(
            source_config={
                "extension": "json",
                "tables": [
                    {
                        "name": "nums",
                        "query": (
                            "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) "
                            "AS t(id, label)"
                        ),
                    }
                ],
            }
        )
        resources = build_duckdb_source(cfg)
        assert len(resources) == 1
        assert resources[0].name == "nums"
        batches = list(resources[0])
        assert sum(b.num_rows for b in batches) == 3
        assert batches[0].schema.names == ["id", "label"]

    def test_multiple_tables_multiple_resources(self) -> None:
        cfg = _make_config(
            source_config={
                "extension": "json",
                "tables": [
                    {"name": "a", "query": "SELECT 1 AS x"},
                    {"name": "b", "query": "SELECT 2 AS y", "primary_key": "y"},
                ],
            }
        )
        resources = build_duckdb_source(cfg)
        assert [r.name for r in resources] == ["a", "b"]

    def test_incremental_pushdown(self) -> None:
        cfg = _make_config(
            source_config={
                "extension": "json",
                "tables": [
                    {
                        "name": "events",
                        "query": (
                            "SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) "
                            "AS t(id, v)"
                        ),
                        "cursor_column": "id",
                        "initial_value": 1,
                    }
                ],
            }
        )
        resources = build_duckdb_source(cfg)
        rows = sum(b.num_rows for b in resources[0])
        # initial_value=1 becomes WHERE id > 1: only ids 2 and 3 come back.
        assert rows == 2

    def test_incremental_invalid_cursor_column(self) -> None:
        cfg = _make_config(
            source_config={
                "extension": "json",
                "tables": [
                    {
                        "name": "t",
                        "query": "SELECT 1 AS id",
                        "cursor_column": "id; DROP TABLE x",
                    }
                ],
            }
        )
        with pytest.raises(ValueError, match="invalid cursor_column"):
            build_duckdb_source(cfg)


class TestConnect:
    def test_memory_and_spill_bounds_applied(self, tmp_path: Any) -> None:
        cfg = _make_config()
        old_tmp = config.PIPELINE_DUCKDB_TEMP_DIR
        config.PIPELINE_DUCKDB_TEMP_DIR = str(tmp_path)
        try:
            con = _connect(cfg.id)
            temp_dir = con.execute(
                "SELECT current_setting('temp_directory')"
            ).fetchone()[0]
            assert temp_dir.endswith(f"duckdb-pipeline-{cfg.id}")
            assert temp_dir.startswith(str(tmp_path))
            limit = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            assert limit  # bounded, not DuckDB's 80%-of-RAM default
            con.close()
        finally:
            config.PIPELINE_DUCKDB_TEMP_DIR = old_tmp


class TestSourceTypeDispatch:
    def test_build_source_routes_duckdb(self) -> None:
        from dlt_worker.pipeline_runner import _build_source

        cfg = _make_config()
        resources = _build_source(cfg)
        assert resources[0].name == "t"
