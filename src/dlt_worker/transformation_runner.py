"""Clones dbt projects and runs them from TransformationConfig objects.

The execution half of FairTier's git-backed dbt transformation layer:
shallow-clones the configured git repo, generates ``profiles.yml`` at run
time (credentials never live in git), and runs ``dbt build`` against the
box's DuckDB with the Lakekeeper Iceberg catalog attached. Data-file
access uses credentials vended by the Lakekeeper REST catalog, so no S3
secret is needed.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import yaml
from dbt.cli.main import dbtRunner

from dlt_worker import config
from dlt_worker.api_client import TransformationConfig, TransformationRunReport

logger = logging.getLogger(__name__)

# Fallback dbt profile name when dbt_project.yml doesn't declare one.
_DEFAULT_PROFILE_NAME = "fairtier"

# Seconds before a git operation is aborted.
_GIT_TIMEOUT = 300

# Bounds for the serialized per-node results (keep the report payload small).
_MAX_NODE_RESULTS = 200
_MAX_MESSAGE_CHARS = 500

_MODEL_RESOURCE_TYPES = {"model", "seed", "snapshot"}
_TEST_RESOURCE_TYPES = {"test", "unit_test"}
_FAILED_STATUSES = {"error", "fail"}


def run_transformation(cfg: TransformationConfig) -> TransformationRunReport:
    """Clone and run a dbt project from the given config. Returns a run report."""
    started_at = datetime.now(timezone.utc)
    token = ""
    commit_sha = ""
    tmpdir = tempfile.mkdtemp(prefix="dbt-run-")
    prev_cwd = os.getcwd()

    try:
        repo_url, username, token = _resolve_repo(cfg)
        clone_dir = os.path.join(tmpdir, "repo")
        commit_sha = _clone_repo(
            repo_url, cfg.repo_ref or "main", username, token, clone_dir
        )
        profiles_dir = _write_profiles(clone_dir, tmpdir)

        # dbt (and DuckDB's iceberg extension) resolve relative paths against
        # the process cwd, which in the container is a non-writable /app —
        # the extension's staging mkdir fails there. Run from the temp dir.
        os.chdir(tmpdir)

        runner = dbtRunner()
        _run_deps(runner, clone_dir, profiles_dir, token)

        build_args = [
            "build",
            "--project-dir",
            clone_dir,
            "--profiles-dir",
            profiles_dir,
            "--target",
            "box",
        ]
        if cfg.dbt_selector:
            build_args += ["--select", cfg.dbt_selector]
        res = runner.invoke(build_args)

        nodes = _node_results(res)
        models_total, models_failed, tests_total, tests_failed = _count_nodes(nodes)

        success = res.success and res.exception is None
        error_message = ""
        if success:
            logger.info(
                "Transformation %s completed: %d models, %d tests (commit %s)",
                cfg.name,
                models_total,
                tests_total,
                commit_sha,
            )
        else:
            error_message = (
                _sanitize(str(res.exception), token)
                if res.exception
                else f"dbt build failed: {models_failed} models "
                f"and {tests_failed} tests failed"
            )
            logger.error("Transformation %s failed: %s", cfg.name, error_message)

        return TransformationRunReport(
            transformation_id=cfg.id,
            status="success" if success else "failed",
            started_at=started_at.isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            commit_sha=commit_sha,
            models_total=models_total,
            models_failed=models_failed,
            tests_total=tests_total,
            tests_failed=tests_failed,
            model_results=json.dumps(nodes[:_MAX_NODE_RESULTS]),
            error_message=error_message,
            run_id=cfg.pending_run_id,
        )

    except Exception as exc:
        # _sanitize is a second line of defense: git errors are already
        # sanitized at the source, but nothing token-shaped may ever
        # reach the run report.
        logger.exception("Transformation %s failed", cfg.name)
        return TransformationRunReport(
            transformation_id=cfg.id,
            status="failed",
            started_at=started_at.isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            commit_sha=commit_sha,
            error_message=_sanitize(str(exc), token),
            run_id=cfg.pending_run_id,
        )

    finally:
        os.chdir(prev_cwd)
        shutil.rmtree(tmpdir, ignore_errors=True)


def _resolve_repo(cfg: TransformationConfig) -> tuple[str, str, str]:
    """Resolve the clone URL and git credentials for a transformation.

    A connected repo (repo_url set) uses its own credentials; an empty
    repo_url means the hosted repo — fall back to TRANSFORM_REPO_URL and
    the TRANSFORM_GIT_* env credentials. Returns (url, username, token).
    """
    if cfg.repo_url:
        creds = cfg.git_credentials
        return cfg.repo_url, creds.get("username", ""), creds.get("token", "")

    if config.TRANSFORM_REPO_URL:
        return (
            config.TRANSFORM_REPO_URL,
            config.TRANSFORM_GIT_USERNAME,
            config.TRANSFORM_GIT_TOKEN,
        )

    raise ValueError(
        f"Transformation {cfg.name!r}: no repo_url configured and "
        "TRANSFORM_REPO_URL is not set"
    )


def _git_auth_env(username: str, token: str) -> dict[str, str]:
    """Git credentials as GIT_CONFIG_* env vars (http.extraheader Basic auth).

    Credentials embedded in the clone URL would be readable in
    /proc/<pid>/cmdline by anything sharing the PID namespace while the
    clone runs, and git persists that URL in the checkout's .git/config
    for the run's duration. A header delivered through the environment is
    visible to neither.
    """
    if not username and not token:
        return {}
    basic = base64.b64encode(f"{username}:{token}".encode()).decode("ascii")
    return {
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "http.extraheader",
        "GIT_CONFIG_VALUE_0": f"Authorization: Basic {basic}",
    }


def _sanitize(text: str, token: str) -> str:
    """Replace the git token in text with *** so it never leaks into
    logs or run reports (git echoes the clone URL in its errors)."""
    if not token:
        return text
    return text.replace(token, "***").replace(quote(token, safe=""), "***")


def _clone_repo(url: str, ref: str, username: str, token: str, dest: str) -> str:
    """Shallow-clone ref of url into dest. Returns the commit SHA.

    Credentials travel via the environment (see _git_auth_env), never
    argv; stderr is still sanitized before it can reach a run report as a
    second line of defense.
    """
    env = {**os.environ, **_git_auth_env(username, token)}
    try:
        result = subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", ref, url, dest],
            capture_output=True,
            text=True,
            timeout=_GIT_TIMEOUT,
            env=env,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"git clone timed out after {_GIT_TIMEOUT}s") from None
    if result.returncode != 0:
        raise RuntimeError(
            f"git clone failed: {_sanitize(result.stderr.strip(), token)}"
        )

    result = subprocess.run(
        ["git", "-C", dest, "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        timeout=_GIT_TIMEOUT,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git rev-parse failed: {_sanitize(result.stderr.strip(), token)}"
        )
    return result.stdout.strip()


def _read_profile_name(project_dir: str) -> str:
    """Read the profile name from dbt_project.yml, with a fallback default."""
    path = os.path.join(project_dir, "dbt_project.yml")
    try:
        with open(path) as f:
            project = yaml.safe_load(f) or {}
    except FileNotFoundError:
        return _DEFAULT_PROFILE_NAME
    return project.get("profile") or _DEFAULT_PROFILE_NAME


def _write_profiles(project_dir: str, tmpdir: str) -> str:
    """Generate profiles.yml in the temp dir and return its directory.

    Always generated at run time — never taken from the repo — so catalog
    credentials stay out of git. The profile targets the box's DuckDB with
    the Lakekeeper Iceberg catalog attached; data-file access uses
    credentials vended by the catalog, so no S3 secret is written.
    """
    profile_name = _read_profile_name(project_dir)
    profiles = {
        profile_name: {
            "target": "box",
            "outputs": {
                "box": {
                    "type": "duckdb",
                    "path": os.path.join(tmpdir, "dbt.duckdb"),
                    "threads": 2,
                    "extensions": ["iceberg", "httpfs"],
                    "secrets": [
                        {
                            "type": "iceberg",
                            "name": "lakekeeper",
                            "client_id": config.OIDC_CLIENT_ID,
                            "client_secret": config.OIDC_CLIENT_SECRET,
                            "oauth2_server_uri": config.OIDC_TOKEN_URL,
                        },
                    ],
                    "attach": [
                        {
                            "path": config.LAKEKEEPER_WAREHOUSE,
                            "alias": "lake",
                            "options": {
                                "type": "iceberg",
                                "secret": "lakekeeper",
                                "endpoint": f"{config.LAKEKEEPER_URL}/catalog",
                            },
                        },
                    ],
                },
            },
        },
    }

    profiles_dir = os.path.join(tmpdir, "profiles")
    os.makedirs(profiles_dir, exist_ok=True)
    with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
        yaml.safe_dump(profiles, f, sort_keys=False)
    return profiles_dir


def _run_deps(
    runner: dbtRunner, project_dir: str, profiles_dir: str, token: str
) -> None:
    """Run ``dbt deps`` when the project declares packages."""
    has_packages = any(
        os.path.exists(os.path.join(project_dir, name))
        for name in ("packages.yml", "package-lock.yml")
    )
    if not has_packages:
        return

    res = runner.invoke(
        [
            "deps",
            "--project-dir",
            project_dir,
            "--profiles-dir",
            profiles_dir,
            "--target",
            "box",
        ]
    )
    if not res.success or res.exception is not None:
        msg = str(res.exception) if res.exception else "unknown error"
        raise RuntimeError(f"dbt deps failed: {_sanitize(msg, token)}")


def _node_results(res: Any) -> list[dict[str, Any]]:
    """Extract per-node results from a dbt invocation result.

    Some commands return no result object (``deps`` returns None), so
    guard for missing attributes.
    """
    result = getattr(res, "result", None)
    if result is None or not hasattr(result, "results"):
        return []

    nodes = []
    for r in result.results:
        message = str(r.message) if r.message else ""
        nodes.append(
            {
                "resource_type": str(r.node.resource_type),
                "name": r.node.name,
                "status": str(r.status),
                "execution_time": round(float(r.execution_time), 3),
                "message": message[:_MAX_MESSAGE_CHARS],
            }
        )
    return nodes


def _count_nodes(nodes: list[dict[str, Any]]) -> tuple[int, int, int, int]:
    """Count (models_total, models_failed, tests_total, tests_failed)."""
    models_total = models_failed = tests_total = tests_failed = 0
    for n in nodes:
        failed = n["status"] in _FAILED_STATUSES
        if n["resource_type"] in _MODEL_RESOURCE_TYPES:
            models_total += 1
            models_failed += failed
        elif n["resource_type"] in _TEST_RESOURCE_TYPES:
            tests_total += 1
            tests_failed += failed
    return models_total, models_failed, tests_total, tests_failed
