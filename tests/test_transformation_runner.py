"""Tests for transformation_runner: repo resolution, profile generation, dbt runs."""

from __future__ import annotations

import base64
import json
import os
import tempfile
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

from dlt_worker import config
from dlt_worker.api_client import TransformationConfig
from dlt_worker.transformation_runner import (
    _clone_repo,
    _git_auth_env,
    _count_nodes,
    _read_profile_name,
    _resolve_repo,
    _sanitize,
    _write_profiles,
    run_transformation,
)


def _make_config(**overrides: Any) -> TransformationConfig:
    defaults: dict[str, Any] = {
        "id": "t1",
        "name": "nightly",
        "repo_url": "",
        "repo_ref": "main",
        "git_credentials": {},
        "schedule": None,
        "trigger_after_pipeline_id": "",
        "dbt_selector": "",
        "enabled": True,
    }
    defaults.update(overrides)
    return TransformationConfig(**defaults)


# --- _resolve_repo ---


def test_resolve_repo_connected_uses_own_credentials() -> None:
    cfg = _make_config(
        repo_url="https://github.com/acme/dbt.git",
        git_credentials={"username": "deploy", "token": "tok123"},
    )
    url, username, token = _resolve_repo(cfg)
    assert url == "https://github.com/acme/dbt.git"
    assert username == "deploy"
    assert token == "tok123"


def test_resolve_repo_hosted_falls_back_to_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config,
        "TRANSFORM_REPO_URL",
        "http://gitea:3000/fairtier-admin/transformations.git",
    )
    monkeypatch.setattr(config, "TRANSFORM_GIT_USERNAME", "fairtier-admin")
    monkeypatch.setattr(config, "TRANSFORM_GIT_TOKEN", "hostedtok")

    url, username, token = _resolve_repo(_make_config())
    assert url == "http://gitea:3000/fairtier-admin/transformations.git"
    assert username == "fairtier-admin"
    assert token == "hostedtok"


def test_resolve_repo_unconfigured_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "TRANSFORM_REPO_URL", "")
    with pytest.raises(ValueError, match="no repo_url"):
        _resolve_repo(_make_config())


# --- _git_auth_env / _sanitize ---


def test_git_auth_env_builds_basic_auth_header() -> None:
    """S2: credentials travel as an http.extraheader via GIT_CONFIG_* env
    vars — never in argv (readable in /proc/<pid>/cmdline) and never
    persisted to the checkout's .git/config."""
    env = _git_auth_env("user", "t@k/1")
    assert env["GIT_CONFIG_COUNT"] == "1"
    assert env["GIT_CONFIG_KEY_0"] == "http.extraheader"
    decoded = base64.b64decode(
        env["GIT_CONFIG_VALUE_0"].removeprefix("Authorization: Basic ")
    ).decode()
    assert decoded == "user:t@k/1"


def test_git_auth_env_without_credentials_is_empty() -> None:
    assert _git_auth_env("", "") == {}


def test_clone_repo_keeps_token_out_of_argv() -> None:
    calls = []

    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        calls.append((argv, kwargs))
        result = MagicMock()
        result.returncode = 0
        result.stdout = "abc123\n"
        result.stderr = ""
        return result

    with patch("dlt_worker.transformation_runner.subprocess.run", fake_run):
        sha = _clone_repo(
            "https://gitea/acme/dbt.git", "main", "user", "s3cret", "/tmp/x"
        )

    assert sha == "abc123"
    clone_argv, clone_kwargs = calls[0]
    assert all("s3cret" not in arg for arg in clone_argv)
    assert "https://gitea/acme/dbt.git" in clone_argv
    env = clone_kwargs["env"]
    assert env["GIT_CONFIG_KEY_0"] == "http.extraheader"


def test_sanitize_masks_raw_and_quoted_token() -> None:
    assert _sanitize("fatal: http://u:t%40k@host and t@k", "t@k") == (
        "fatal: http://u:***@host and ***"
    )
    assert _sanitize("no token here", "") == "no token here"


# --- profiles.yml generation ---


def test_read_profile_name(tmp_path: Any) -> None:
    (tmp_path / "dbt_project.yml").write_text("name: acme\nprofile: custom\n")
    assert _read_profile_name(str(tmp_path)) == "custom"


def test_read_profile_name_fallback(tmp_path: Any) -> None:
    assert _read_profile_name(str(tmp_path)) == "fairtier"


def test_write_profiles_shape(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "OIDC_CLIENT_ID", "cid")
    monkeypatch.setattr(config, "OIDC_CLIENT_SECRET", "secret")
    monkeypatch.setattr(config, "OIDC_TOKEN_URL", "https://auth/token")
    monkeypatch.setattr(config, "LAKEKEEPER_URL", "http://lakekeeper:8181")
    monkeypatch.setattr(config, "LAKEKEEPER_WAREHOUSE", "default")

    project_dir = tmp_path / "repo"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text("profile: fairtier\n")

    profiles_dir = _write_profiles(str(project_dir), str(tmp_path))
    with open(os.path.join(profiles_dir, "profiles.yml")) as f:
        profiles = yaml.safe_load(f)

    output = profiles["fairtier"]["outputs"]["box"]
    assert output["type"] == "duckdb"
    assert output["extensions"] == ["iceberg", "httpfs"]

    (secret,) = output["secrets"]
    assert secret["type"] == "iceberg"
    assert secret["client_id"] == "cid"
    assert secret["oauth2_server_uri"] == "https://auth/token"

    (attach,) = output["attach"]
    assert attach["path"] == "default"
    assert attach["alias"] == "lake"
    assert attach["options"] == {
        "type": "iceberg",
        "secret": "lakekeeper",
        "endpoint": "http://lakekeeper:8181/catalog",
    }


def test_write_profiles_bounds_duckdb_memory(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DuckDB must be told a ceiling: unbounded it sizes its buffer manager
    from host RAM, which on a single-node box is everyone else's RAM too."""
    monkeypatch.setattr(config, "DBT_DUCKDB_MEMORY_LIMIT", "512MB")
    monkeypatch.setattr(config, "DBT_DUCKDB_TEMP_DIR", "")
    monkeypatch.setattr(config, "DBT_DUCKDB_MAX_TEMP_SIZE", "4GB")

    project_dir = tmp_path / "repo"
    project_dir.mkdir()

    profiles_dir = _write_profiles(str(project_dir), str(tmp_path))
    with open(os.path.join(profiles_dir, "profiles.yml")) as f:
        profiles = yaml.safe_load(f)

    settings = profiles["fairtier"]["outputs"]["box"]["settings"]
    assert settings["memory_limit"] == "512MB"
    assert settings["max_temp_directory_size"] == "4GB"
    # Spilling inside the run's temp dir means a killed build leaves no
    # spill behind on the box.
    assert settings["temp_directory"] == os.path.join(str(tmp_path), "duckdb-temp")


def test_write_profiles_empty_bounds_leave_duckdb_defaults(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each bound is skippable — the rollback if one is too tight."""
    monkeypatch.setattr(config, "DBT_DUCKDB_MEMORY_LIMIT", "")
    monkeypatch.setattr(config, "DBT_DUCKDB_TEMP_DIR", "/spill")
    monkeypatch.setattr(config, "DBT_DUCKDB_MAX_TEMP_SIZE", "")

    project_dir = tmp_path / "repo"
    project_dir.mkdir()

    profiles_dir = _write_profiles(str(project_dir), str(tmp_path))
    with open(os.path.join(profiles_dir, "profiles.yml")) as f:
        profiles = yaml.safe_load(f)

    settings = profiles["fairtier"]["outputs"]["box"]["settings"]
    assert settings == {"temp_directory": "/spill"}


# --- node counting ---


def test_count_nodes() -> None:
    nodes = [
        {"resource_type": "model", "status": "success"},
        {"resource_type": "model", "status": "error"},
        {"resource_type": "seed", "status": "success"},
        {"resource_type": "test", "status": "pass"},
        {"resource_type": "test", "status": "fail"},
        {"resource_type": "unit_test", "status": "pass"},
    ]
    assert _count_nodes(nodes) == (3, 1, 3, 1)


# --- run_transformation ---


def _fake_node(resource_type: str, name: str, status: str) -> SimpleNamespace:
    return SimpleNamespace(
        node=SimpleNamespace(resource_type=resource_type, name=name),
        status=status,
        execution_time=0.5,
        message="ok" if status in ("success", "pass") else "boom",
    )


def _fake_dbt_result(nodes: list[SimpleNamespace], success: bool) -> SimpleNamespace:
    return SimpleNamespace(
        success=success,
        exception=None,
        result=SimpleNamespace(results=nodes),
    )


def test_run_transformation_success(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _make_config(
        repo_url="https://git/x.git",
        git_credentials={"username": "u", "token": "tok"},
        dbt_selector="tag:daily",
        pending_run_id="run-9",
    )
    runner = MagicMock()
    invoke_cwds: list[str] = []

    def _invoke(args: list[str]) -> SimpleNamespace:
        invoke_cwds.append(os.getcwd())
        return _fake_dbt_result(
            [
                _fake_node("model", "stg_orders", "success"),
                _fake_node("test", "unique_id", "pass"),
            ],
            success=True,
        )

    runner.invoke.side_effect = _invoke
    cwd_before = os.getcwd()

    with (
        patch("dlt_worker.transformation_runner._clone_repo", return_value="abc123"),
        patch("dlt_worker.transformation_runner.dbtRunner", return_value=runner),
    ):
        report = run_transformation(cfg)

    assert report.status == "success"
    assert report.commit_sha == "abc123"
    assert report.models_total == 1
    assert report.tests_total == 1
    assert report.run_id == "run-9"
    results = json.loads(report.model_results)
    assert results[0]["name"] == "stg_orders"

    build_args = runner.invoke.call_args[0][0]
    assert build_args[0] == "build"
    assert build_args[-2:] == ["--select", "tag:daily"]
    assert "--target" in build_args

    # dbt must run from the writable temp dir (the container cwd is
    # read-only and DuckDB's iceberg extension mkdirs relative to cwd),
    # and the previous cwd must be restored afterwards.
    assert all(c.startswith(tempfile.gettempdir()) for c in invoke_cwds)
    assert os.getcwd() == cwd_before


def test_run_transformation_dbt_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _make_config(repo_url="https://git/x.git")
    runner = MagicMock()
    runner.invoke.return_value = _fake_dbt_result(
        [_fake_node("model", "bad_model", "error")], success=False
    )

    with (
        patch("dlt_worker.transformation_runner._clone_repo", return_value="abc123"),
        patch("dlt_worker.transformation_runner.dbtRunner", return_value=runner),
    ):
        report = run_transformation(cfg)

    assert report.status == "failed"
    assert report.models_failed == 1
    assert "1 models" in report.error_message


def test_run_transformation_clone_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_config(
        repo_url="https://git/x.git",
        git_credentials={"username": "u", "token": "sekret"},
    )
    failed = SimpleNamespace(
        returncode=128,
        stdout="",
        stderr="fatal: unable to access https://u:sekret@git/x.git",
    )

    with patch("dlt_worker.transformation_runner.subprocess.run", return_value=failed):
        report = run_transformation(cfg)

    assert report.status == "failed"
    assert "sekret" not in report.error_message
    assert "***" in report.error_message
