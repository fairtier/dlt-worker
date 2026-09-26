"""Tests for dbt_project: how the worker's clone of a repo is made v2-ready."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from dlt_worker import config
from dlt_worker.dbt_project import prepare_project


@pytest.fixture(autouse=True)
def _box(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "LAKEKEEPER_URL", "http://lakekeeper:8181")
    monkeypatch.setattr(config, "LAKEKEEPER_WAREHOUSE", "default")
    monkeypatch.setattr(config, "DBT_DUCKDB_THREADS", "2")
    monkeypatch.setattr(config, "DBT_S3_UPLOADER_MAX_FILESIZE", "1GB")
    monkeypatch.setattr(config, "DBT_STAGE_CREATE_TABLES", False)


def _project(tmp_path: Path, body: dict) -> Path:
    (tmp_path / "dbt_project.yml").write_text(yaml.safe_dump(body, sort_keys=False))
    return tmp_path


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


STARTER = {
    "name": "fairtier",
    "profile": "fairtier",
    "models": {"fairtier": {"+materialized": "table", "+database": "lake"}},
}


def test_old_repo_gets_v2_flags_and_catalog_name(tmp_path: Path) -> None:
    """A repo written for dbt 1.x runs unchanged on the v2 worker."""
    p = _project(tmp_path, STARTER)
    prepare_project(str(p))
    cfg = _load(p / "dbt_project.yml")
    assert cfg["flags"]["use_catalogs_v2"] is True
    assert cfg["models"]["fairtier"]["+catalog_name"] == "lake"
    assert cfg["models"]["fairtier"]["+materialized"] == "table"


def test_repo_settings_win_over_injection(tmp_path: Path) -> None:
    p = _project(
        tmp_path,
        {
            **STARTER,
            "flags": {"use_catalogs_v2": False, "send_anonymous_usage_stats": False},
            "models": {"fairtier": {"+catalog_name": "other"}},
        },
    )
    prepare_project(str(p))
    cfg = _load(p / "dbt_project.yml")
    assert cfg["flags"] == {
        "use_catalogs_v2": False,
        "send_anonymous_usage_stats": False,
    }
    assert cfg["models"]["fairtier"]["+catalog_name"] == "other"


def test_lake_catalog_is_written(tmp_path: Path) -> None:
    p = _project(tmp_path, STARTER)
    prepare_project(str(p))
    (lake,) = _load(p / "catalogs.yml")["catalogs"]
    assert lake == {
        "name": "lake",
        "type": "iceberg_rest",
        "table_format": "iceberg",
        "config": {
            "duckdb": {
                "endpoint": "http://lakekeeper:8181/catalog",
                "warehouse": "default",
                "secret": "lakekeeper",
                "authorization_type": "OAUTH2",
                "access_delegation_mode": "VENDED_CREDENTIALS",
            }
        },
    }


def test_stage_create_tables_when_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "DBT_STAGE_CREATE_TABLES", True)
    p = _project(tmp_path, STARTER)
    prepare_project(str(p))
    (lake,) = _load(p / "catalogs.yml")["catalogs"]
    assert lake["config"]["duckdb"]["stage_create_tables"] is True


def test_repo_catalogs_are_kept_but_the_worker_owns_lake(tmp_path: Path) -> None:
    """A BYO repo carries its own `lake` pointing at the box from a laptop;
    on the box the worker's entry replaces it. Other catalogs stay."""
    p = _project(tmp_path, STARTER)
    (p / "catalogs.yml").write_text(
        yaml.safe_dump(
            {
                "catalogs": [
                    {
                        "name": "lake",
                        "type": "iceberg_rest",
                        "config": {"duckdb": {"endpoint": "https://elsewhere"}},
                    },
                    {"name": "scratch", "type": "iceberg_rest"},
                ]
            }
        )
    )
    prepare_project(str(p))
    catalogs = _load(p / "catalogs.yml")["catalogs"]
    assert [c["name"] for c in catalogs] == ["scratch", "lake"]
    assert (
        catalogs[1]["config"]["duckdb"]["endpoint"] == "http://lakekeeper:8181/catalog"
    )


def test_run_start_hooks_come_first_and_keep_the_repos(tmp_path: Path) -> None:
    p = _project(
        tmp_path, {**STARTER, "on-run-start": "create schema if not exists lake.x"}
    )
    prepare_project(str(p))
    hooks = _load(p / "dbt_project.yml")["on-run-start"]
    assert hooks[0] == "SET GLOBAL s3_uploader_max_filesize = '1GB'"
    assert "current_setting('s3_uploader_max_filesize') <> '1GB'" in hooks[1]
    assert "error(" in hooks[1]
    assert "current_setting('threads') <> 2" in hooks[2]
    assert hooks[-1] == "create schema if not exists lake.x"


def test_empty_settings_add_no_hooks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "DBT_S3_UPLOADER_MAX_FILESIZE", "")
    monkeypatch.setattr(config, "DBT_DUCKDB_THREADS", "")
    p = _project(tmp_path, STARTER)
    prepare_project(str(p))
    assert _load(p / "dbt_project.yml")["on-run-start"] == []


def test_quote_in_a_size_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "DBT_S3_UPLOADER_MAX_FILESIZE", "1GB'; drop")
    p = _project(tmp_path, STARTER)
    with pytest.raises(ValueError, match="DBT_S3_UPLOADER_MAX_FILESIZE"):
        prepare_project(str(p))


def test_non_integer_threads_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "DBT_DUCKDB_THREADS", "two")
    p = _project(tmp_path, STARTER)
    with pytest.raises(ValueError, match="DBT_DUCKDB_THREADS"):
        prepare_project(str(p))


def test_missing_project_file_is_a_clear_error(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="dbt_project.yml"):
        prepare_project(str(tmp_path))


def test_stale_run_results_are_removed(tmp_path: Path) -> None:
    """A repo that commits target/ must not have its old results reported
    as this run's."""
    p = _project(tmp_path, STARTER)
    (p / "target").mkdir()
    (p / "target" / "run_results.json").write_text("{}")
    prepare_project(str(p))
    assert not (p / "target" / "run_results.json").exists()
