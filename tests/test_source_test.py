"""Tests for the source-test probe (source_test.py).

The duckdb half runs against a real in-memory DuckDB loading only `json`
(statically linked into the wheel), so it needs no network and none of the
baked extension binaries. The sql_database half is not exercised here: it
needs a live PostgreSQL, which the run path's own tests do not have either.
"""

from __future__ import annotations

from typing import Any

import pytest

from dlt_worker import config
from dlt_worker.api_client import SourceTest
from dlt_worker.source_test import MAX_PROBED_TABLES, probe_source


def _test(**overrides: Any) -> SourceTest:
    defaults: dict[str, Any] = {
        "id": "t1",
        "source_type": "duckdb",
        "source_config": {
            "extension": "json",
            "tables": [{"name": "t", "query": "SELECT 1 AS id"}],
        },
        "source_credentials": {},
    }
    defaults.update(overrides)
    return SourceTest(**defaults)


@pytest.fixture(autouse=True)
def _config(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(config, "PIPELINE_DUCKDB_TEMP_DIR", str(tmp_path))
    monkeypatch.setattr(config, "DUCKDB_EXTENSION_DIR", "")


class TestDuckDBProbe:
    def test_reads_one_row_per_table(self) -> None:
        report = probe_source(
            _test(
                source_config={
                    "extension": "json",
                    "tables": [
                        {"name": "a", "query": "SELECT 1 AS id, 'x' AS name"},
                        {"name": "b", "query": "SELECT 2 AS id"},
                    ],
                }
            )
        )
        assert report.status == "success"
        assert report.details == [
            "a: readable, 2 columns",
            "b: readable, 1 columns",
        ]

    def test_an_empty_table_is_still_readable(self) -> None:
        # "Connected, and there is nothing in it yet" is a pass: an empty
        # source table is a normal state, not a broken connection.
        report = probe_source(
            _test(
                source_config={
                    "extension": "json",
                    "tables": [{"name": "a", "query": "SELECT 1 AS id WHERE false"}],
                }
            )
        )
        assert report.status == "success"
        assert "no rows yet" in report.details[0]

    def test_one_bad_table_fails_the_test_but_reports_the_others(self) -> None:
        report = probe_source(
            _test(
                source_config={
                    "extension": "json",
                    "tables": [
                        {"name": "good", "query": "SELECT 1 AS id"},
                        {"name": "bad", "query": "SELECT * FROM no_such_table"},
                    ],
                }
            )
        )
        assert report.status == "failed"
        assert "no_such_table" in report.message

    def test_a_probe_is_bounded_by_table_count(self) -> None:
        tables = [
            {"name": f"t{i}", "query": "SELECT 1 AS id"}
            for i in range(MAX_PROBED_TABLES + 5)
        ]
        report = probe_source(
            _test(source_config={"extension": "json", "tables": tables})
        )
        assert report.status == "success"
        assert report.details[-1] == "(5 more tables not checked)"

    def test_a_config_problem_is_reported_not_raised(self) -> None:
        report = probe_source(_test(source_config={"extension": "json"}))
        assert report.status == "failed"
        assert "no tables" in report.message
        # The shared validators say "Pipeline 'the source': …"; a test has no
        # pipeline, and the person running it has not saved one.
        assert "Pipeline" not in report.message

    def test_credentials_never_reach_the_message(self) -> None:
        # A driver error quotes what it failed on — an ATTACH echoes the
        # rendered connection string, a query the identifier it could not
        # resolve — and this message is stored, shipped to central Loki and
        # shown in a toast. Here the failing identifier IS the credential
        # value, which is the shape that has to come back scrubbed.
        report = probe_source(
            _test(
                source_config={
                    "extension": "json",
                    "tables": [{"name": "t", "query": "SELECT * FROM sup3rsecret"}],
                },
                source_credentials={"attach_params": {"password": "sup3rsecret"}},
            )
        )
        assert report.status == "failed"
        assert "sup3rsecret" not in report.message
        assert "***" in report.message

    def test_an_attach_failure_is_reported_without_the_template(self) -> None:
        report = probe_source(
            _test(
                source_config={
                    "extension": "json",
                    "attach": "host=nope password={password}",
                    "tables": [{"name": "t"}],
                },
                source_credentials={"attach_params": {"password": "sup3rsecret"}},
            )
        )
        assert report.status == "failed"
        assert "sup3rsecret" not in report.message


class TestUnsupported:
    def test_a_type_without_a_probe_says_so(self) -> None:
        report = probe_source(_test(source_type="rest_api"))
        assert report.status == "failed"
        assert "not supported" in report.message
