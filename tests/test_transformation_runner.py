"""Tests for transformation_runner: repo resolution, profile generation, dbt runs."""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

from dlt_worker import config
from dlt_worker.api_client import TransformationConfig
from dlt_worker.transformation_runner import (
    _OUTPUT_TAIL_CHARS,
    _clone_repo,
    _dbt_env,
    _dbt_home,
    _git_auth_env,
    _read_profile_name,
    _resolve_repo,
    _run_dbt,
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


def test_sanitize_masks_every_secret_and_skips_empty_ones() -> None:
    text = "tok=a/b cat=c-sec aws=d-sec"
    assert _sanitize(text, "a/b", "", "c-sec", "d-sec") == "tok=*** cat=*** aws=***"
    assert _sanitize(text, "", "") == text


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

    project_dir = tmp_path / "repo"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text("profile: fairtier\n")

    profiles_dir = _write_profiles(str(project_dir), str(tmp_path))
    with open(os.path.join(profiles_dir, "profiles.yml")) as f:
        profiles = yaml.safe_load(f)

    output = profiles["fairtier"]["outputs"]["box"]
    assert output["type"] == "duckdb"
    assert output["extensions"] == ["iceberg", "httpfs"]
    # The catalog is attached from catalogs.yml (dbt_project.py), not here.
    assert "attach" not in output

    (secret,) = output["secrets"]
    assert secret["type"] == "iceberg"
    assert secret["name"] == "lakekeeper"
    assert secret["client_id"] == "cid"
    assert secret["oauth2_server_uri"] == "https://auth/token"


def test_write_profiles_bounds_duckdb(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DuckDB must be told a ceiling and a thread count: unbounded it sizes
    its buffer manager from host RAM and its threads from host cores."""
    monkeypatch.setattr(config, "DBT_DUCKDB_MEMORY_LIMIT", "192MB")
    monkeypatch.setattr(config, "DBT_DUCKDB_THREADS", "2")
    monkeypatch.setattr(config, "DBT_DUCKDB_TEMP_DIR", "")
    monkeypatch.setattr(config, "DBT_DUCKDB_MAX_TEMP_SIZE", "4GB")

    project_dir = tmp_path / "repo"
    project_dir.mkdir()

    profiles_dir = _write_profiles(str(project_dir), str(tmp_path))
    with open(os.path.join(profiles_dir, "profiles.yml")) as f:
        profiles = yaml.safe_load(f)

    settings = profiles["fairtier"]["outputs"]["box"]["settings"]
    assert settings == {
        "memory_limit": "192MB",
        "threads": 2,
        "temp_directory": os.path.join(str(tmp_path), "duckdb-temp"),
        "max_temp_directory_size": "4GB",
        "enable_external_file_cache": False,
        "autoinstall_known_extensions": False,
    }


def test_write_profiles_empty_bounds_leave_duckdb_defaults(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each bound is skippable — the rollback if one is too tight."""
    monkeypatch.setattr(config, "DBT_DUCKDB_MEMORY_LIMIT", "")
    monkeypatch.setattr(config, "DBT_DUCKDB_THREADS", "")
    monkeypatch.setattr(config, "DBT_DUCKDB_TEMP_DIR", "/spill")
    monkeypatch.setattr(config, "DBT_DUCKDB_MAX_TEMP_SIZE", "")

    project_dir = tmp_path / "repo"
    project_dir.mkdir()

    profiles_dir = _write_profiles(str(project_dir), str(tmp_path))
    with open(os.path.join(profiles_dir, "profiles.yml")) as f:
        profiles = yaml.safe_load(f)

    settings = profiles["fairtier"]["outputs"]["box"]["settings"]
    assert settings == {
        "temp_directory": "/spill",
        "enable_external_file_cache": False,
        "autoinstall_known_extensions": False,
    }


def test_dbt_env_passes_only_what_dbt_needs(monkeypatch: pytest.MonkeyPatch) -> None:
    """S3: a customer model can read any env var through env_var(), so the
    worker's credentials must never be in dbt's environment."""
    monkeypatch.setenv("PATH", "/usr/local/bin:/usr/bin")
    monkeypatch.setenv("OIDC_CLIENT_SECRET", "oidc-secret")
    monkeypatch.setenv("TRANSFORM_GIT_TOKEN", "git-token")
    monkeypatch.setenv("WORKSPACE_DB_URL", "postgresql://u:p@db/ws")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy:3128")

    env = _dbt_env("/tmp/run/home")

    assert env["PATH"] == "/usr/local/bin:/usr/bin"
    assert env["HOME"] == "/tmp/run/home"
    assert env["HTTPS_PROXY"] == "http://proxy:3128"
    # v2 sends usage data by default and ignores DO_NOT_TRACK.
    assert env["DBT_ENGINE_SEND_ANONYMOUS_USAGE_STATS"] == "false"
    leaked = {"oidc-secret", "git-token", "postgresql://u:p@db/ws", "aws-secret"}
    assert leaked.isdisjoint(env.values())


def test_dbt_home_links_the_baked_extensions(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    baked = tmp_path / "baked"
    (baked / ".duckdb" / "extensions").mkdir(parents=True)
    monkeypatch.setattr(config, "DBT_DUCKDB_HOME", str(baked))
    run = tmp_path / "run"
    run.mkdir()

    home = _dbt_home(str(run))

    assert home == str(run / "home")
    link = run / "home" / ".duckdb"
    assert link.is_symlink()
    assert os.path.realpath(link) == str(baked / ".duckdb")


def test_dbt_home_without_baked_extensions_is_plain(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "DBT_DUCKDB_HOME", str(tmp_path / "absent"))
    home = _dbt_home(str(tmp_path))
    assert os.path.isdir(home)
    assert not os.path.exists(os.path.join(home, ".duckdb"))


# --- run_transformation ---

FIXTURES = Path(__file__).parent / "fixtures"


class _FakeProc:
    """A finished dbt process as subprocess.Popen returns it."""

    def __init__(self, returncode: int, output: str) -> None:
        self.returncode = returncode
        self.stdout = io.StringIO(output)

    def wait(self, timeout: float | None = None) -> int:
        return self.returncode

    def poll(self) -> int:
        return self.returncode


class _FakeDbt:
    """Stands in for subprocess.Popen of the dbt binary: records each call and
    writes the given run_results.json fixture where dbt would."""

    def __init__(self, fixture: str | None, returncode: int, output: str = "") -> None:
        self.fixture = fixture
        self.returncode = returncode
        self.output = output
        self.calls: list[tuple[list[str], dict[str, Any]]] = []

    def __call__(self, argv: list[str], **kwargs: Any) -> _FakeProc:
        self.calls.append((argv, kwargs))
        if argv[1] == "build" and self.fixture:
            project_dir = argv[argv.index("--project-dir") + 1]
            target = Path(project_dir) / "target"
            target.mkdir(exist_ok=True)
            shutil.copy(FIXTURES / self.fixture, target / "run_results.json")
        return _FakeProc(self.returncode, self.output)


def _fake_clone(url: str, ref: str, username: str, token: str, dest: str) -> str:
    os.makedirs(dest)
    with open(os.path.join(dest, "dbt_project.yml"), "w") as f:
        yaml.safe_dump(
            {
                "name": "fairtier",
                "profile": "fairtier",
                "models": {"fairtier": {"+materialized": "table"}},
            },
            f,
        )
    return "abc123"


def test_run_transformation_success() -> None:
    cfg = _make_config(
        repo_url="https://git/x.git",
        git_credentials={"username": "u", "token": "tok"},
        dbt_selector="tag:daily",
        pending_run_id="run-9",
    )
    dbt = _FakeDbt("run_results_v2_build.json", 0)
    cwd_before = os.getcwd()

    with (
        patch("dlt_worker.transformation_runner._clone_repo", _fake_clone),
        patch("dlt_worker.transformation_runner.subprocess.Popen", dbt),
    ):
        report = run_transformation(cfg)

    assert report.status == "success"
    assert report.commit_sha == "abc123"
    assert (report.models_total, report.tests_total) == (8, 5)
    assert report.run_id == "run-9"
    names = {n["name"] for n in json.loads(report.model_results)}
    assert "stg_trips" in names

    ((argv, kwargs),) = dbt.calls  # no packages.yml → no deps call
    assert argv[:2] == ["dbt", "build"]
    assert argv[argv.index("--select") + 1] == "tag:daily"
    assert argv[argv.index("--target") + 1] == "box"
    # dbt runs from the writable temp dir (the container cwd is read-only
    # and DuckDB's iceberg extension mkdirs relative to cwd) ...
    assert kwargs["cwd"].startswith(tempfile.gettempdir())
    # ... with the allowlisted env, never the worker's own ...
    assert kwargs["env"]["DBT_ENGINE_SEND_ANONYMOUS_USAGE_STATS"] == "false"
    # ... and its output streamed through one pipe, line by line.
    assert kwargs["stdout"] is subprocess.PIPE
    assert kwargs["stderr"] is subprocess.STDOUT
    assert kwargs["env"]["HOME"].startswith(kwargs["cwd"])
    assert os.getcwd() == cwd_before


def test_run_transformation_rewrites_the_clone_before_building() -> None:
    cfg = _make_config(repo_url="https://git/x.git")
    seen: dict[str, Any] = {}

    def fake_popen(argv: list[str], **kwargs: Any) -> _FakeProc:
        project_dir = argv[argv.index("--project-dir") + 1]
        with open(os.path.join(project_dir, "dbt_project.yml")) as f:
            seen["project"] = yaml.safe_load(f)
        seen["catalogs"] = os.path.exists(os.path.join(project_dir, "catalogs.yml"))
        return _FakeProc(0, "")

    with (
        patch("dlt_worker.transformation_runner._clone_repo", _fake_clone),
        patch("dlt_worker.transformation_runner.subprocess.Popen", fake_popen),
    ):
        run_transformation(cfg)

    assert seen["project"]["flags"]["use_catalogs_v2"] is True
    assert seen["catalogs"] is True


def test_run_transformation_node_failure() -> None:
    cfg = _make_config(repo_url="https://git/x.git")
    dbt = _FakeDbt("run_results_v2_error.json", 1)

    with (
        patch("dlt_worker.transformation_runner._clone_repo", _fake_clone),
        patch("dlt_worker.transformation_runner.subprocess.Popen", dbt),
    ):
        report = run_transformation(cfg)

    assert report.status == "failed"
    assert report.models_failed == 1
    assert report.error_message == "dbt build failed: 1 models and 0 tests failed"


def test_run_transformation_failure_before_any_node_reports_dbt_output() -> None:
    """dbt that fails before building anything (bad YAML, unreachable
    catalog) writes no results: its own output is the only explanation."""
    cfg = _make_config(
        repo_url="https://git/x.git",
        git_credentials={"username": "u", "token": "sekret"},
    )
    dbt = _FakeDbt(None, 2, output="Error: could not reach https://u:sekret@git/x\n")

    with (
        patch("dlt_worker.transformation_runner._clone_repo", _fake_clone),
        patch("dlt_worker.transformation_runner.subprocess.Popen", dbt),
    ):
        report = run_transformation(cfg)

    assert report.status == "failed"
    assert report.error_message.startswith("dbt build failed (exit 2): ")
    assert "could not reach" in report.error_message
    assert "sekret" not in report.error_message


def test_run_transformation_runs_deps_when_packages_declared() -> None:
    cfg = _make_config(repo_url="https://git/x.git")
    dbt = _FakeDbt("run_results_v2_build.json", 0)

    def clone_with_packages(*args: Any) -> str:
        sha = _fake_clone(*args)
        with open(os.path.join(args[4], "packages.yml"), "w") as f:
            f.write("packages: []\n")
        return sha

    with (
        patch("dlt_worker.transformation_runner._clone_repo", clone_with_packages),
        patch("dlt_worker.transformation_runner.subprocess.Popen", dbt),
    ):
        report = run_transformation(cfg)

    assert report.status == "success"
    assert [argv[1] for argv, _ in dbt.calls] == ["deps", "build"]


def test_run_transformation_deps_failure_is_sanitized() -> None:
    cfg = _make_config(
        repo_url="https://git/x.git",
        git_credentials={"username": "u", "token": "sekret"},
    )
    dbt = _FakeDbt(None, 1, output="deps: 401 for https://u:sekret@hub\n")

    def clone_with_packages(*args: Any) -> str:
        sha = _fake_clone(*args)
        with open(os.path.join(args[4], "packages.yml"), "w") as f:
            f.write("packages: []\n")
        return sha

    with (
        patch("dlt_worker.transformation_runner._clone_repo", clone_with_packages),
        patch("dlt_worker.transformation_runner.subprocess.Popen", dbt),
    ):
        report = run_transformation(cfg)

    assert report.status == "failed"
    assert "dbt deps failed" in report.error_message
    assert "sekret" not in report.error_message


def test_run_transformation_logs_dbt_output_sanitized(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A successful run's dbt output reaches the log as it arrives — the
    only trace of a run's progress — with the git token scrubbed."""
    cfg = _make_config(
        repo_url="https://git/x.git",
        git_credentials={"username": "u", "token": "sekret"},
    )
    dbt = _FakeDbt(
        "run_results_v2_build.json",
        0,
        output="1 of 8 OK created stg_trips\nfetched https://u:sekret@git/x\n",
    )

    with (
        caplog.at_level(logging.INFO, logger="dlt_worker.transformation_runner"),
        patch("dlt_worker.transformation_runner._clone_repo", _fake_clone),
        patch("dlt_worker.transformation_runner.subprocess.Popen", dbt),
    ):
        report = run_transformation(cfg)

    assert report.status == "success"
    messages = [r.getMessage() for r in caplog.records]
    assert "dbt: 1 of 8 OK created stg_trips" in messages
    assert "dbt: fetched https://u:***@git/x" in messages
    assert "sekret" not in caplog.text


@pytest.mark.parametrize("step", ["deps", "build"])
def test_run_transformation_scrubs_catalog_and_storage_secrets(
    step: str, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The profile carries the catalog's client secret and dbt can quote it:
    it must reach neither the run report nor the log, whichever step fails."""
    monkeypatch.setattr(config, "OIDC_CLIENT_SECRET", "oidc-shh")
    monkeypatch.setattr(config, "AWS_SECRET_ACCESS_KEY", "aws-shh")
    cfg = _make_config(repo_url="https://git/x.git")
    dbt = _FakeDbt(None, 1, output="secret: oidc-shh key=aws-shh\n")

    def clone(*args: Any) -> str:
        sha = _fake_clone(*args)
        if step == "deps":
            with open(os.path.join(args[4], "packages.yml"), "w") as f:
                f.write("packages: []\n")
        return sha

    with (
        caplog.at_level(logging.INFO, logger="dlt_worker.transformation_runner"),
        patch("dlt_worker.transformation_runner._clone_repo", clone),
        patch("dlt_worker.transformation_runner.subprocess.Popen", dbt),
    ):
        report = run_transformation(cfg)

    assert report.status == "failed"
    assert f"dbt {step} failed" in report.error_message
    assert "secret: *** key=***" in report.error_message
    for secret in ("oidc-shh", "aws-shh"):
        assert secret not in report.error_message
        assert secret not in caplog.text


def test_run_dbt_keeps_a_bounded_tail() -> None:
    """Only the end of dbt's output is kept for the report, never all of it."""
    lines = "".join(f"line {i}\n" for i in range(5000))
    with patch(
        "dlt_worker.transformation_runner.subprocess.Popen",
        return_value=_FakeProc(2, lines),
    ):
        res = _run_dbt(["build"], "/p", "/pr", "/c", {}, ())

    assert res.returncode == 2
    assert res.tail.endswith("line 4999\n")
    assert len(res.tail) == _OUTPUT_TAIL_CHARS


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
