"""Making the worker's clone of a dbt repo ready for dbt-oss v2 on the box.

The worker builds from a fresh clone in a temp dir, so what it writes here
never reaches the customer's repository. Three edits:

- catalogs.yml: the `lake` Iceberg REST catalog, pointing at the box's
  Lakekeeper with vended credentials. The worker owns that one entry: a
  repo's own `lake` (a BYO setup pointing at the box from a laptop) is
  replaced, its other catalogs are kept.
- dbt_project.yml flags: `use_catalogs_v2` and a project-wide
  `+catalog_name: lake`, only when the repo doesn't set them, so a project
  written for dbt 1.x builds unchanged.
- on-run-start hooks, ahead of the repo's own: DuckDB settings that must be
  GLOBAL, and a check that fails the run if a memory bound did not take.
  dbt applies the profile's `settings:` as plain SETs on a connection it
  then closes; for an extension setting that SET is session-scoped, so it
  never reaches the statements that write. A hook's SET GLOBAL does.
"""

from __future__ import annotations

import os
import re
from typing import Any

import yaml

from dlt_worker import config

CATALOG_NAME = "lake"
SECRET_NAME = "lakekeeper"

_SIZE = re.compile(r"^\d+(\.\d+)?\s*[KMGT]?i?B$", re.IGNORECASE)


def _lake_catalog() -> dict[str, Any]:
    duckdb: dict[str, Any] = {
        "endpoint": f"{config.LAKEKEEPER_URL}/catalog",
        "warehouse": config.LAKEKEEPER_WAREHOUSE,
        "secret": SECRET_NAME,
        "authorization_type": "OAUTH2",
        "access_delegation_mode": "VENDED_CREDENTIALS",
    }
    if config.DBT_STAGE_CREATE_TABLES:
        duckdb["stage_create_tables"] = True
    return {
        "name": CATALOG_NAME,
        "type": "iceberg_rest",
        "table_format": "iceberg",
        "config": {"duckdb": duckdb},
    }


def _guard(name: str, expected: str) -> str:
    return (
        f"SELECT CASE WHEN current_setting('{name}') <> {expected} "
        f"THEN error('dlt-worker: DuckDB setting {name} did not take effect') END"
    )


def _run_start_hooks() -> list[str]:
    hooks: list[str] = []
    size = config.DBT_S3_UPLOADER_MAX_FILESIZE
    if size:
        if not _SIZE.match(size):
            raise ValueError(f"DBT_S3_UPLOADER_MAX_FILESIZE is not a size: {size!r}")
        hooks.append(f"SET GLOBAL s3_uploader_max_filesize = '{size}'")
        hooks.append(_guard("s3_uploader_max_filesize", f"'{size}'"))
    threads = config.DBT_DUCKDB_THREADS
    if threads:
        if not threads.isdigit():
            raise ValueError(f"DBT_DUCKDB_THREADS is not an integer: {threads!r}")
        hooks.append(_guard("threads", threads))
    return hooks


def _write_catalogs(project_dir: str) -> None:
    path = os.path.join(project_dir, "catalogs.yml")
    existing: dict[str, Any] = {}
    if os.path.exists(path):
        with open(path) as f:
            existing = yaml.safe_load(f) or {}
    others = [
        c
        for c in existing.get("catalogs") or []
        if not (isinstance(c, dict) and c.get("name") == CATALOG_NAME)
    ]
    existing["catalogs"] = [*others, _lake_catalog()]
    with open(path, "w") as f:
        yaml.safe_dump(existing, f, sort_keys=False)


def prepare_project(project_dir: str) -> None:
    """Edit the clone in project_dir in place for a v2 build on the box."""
    path = os.path.join(project_dir, "dbt_project.yml")
    if not os.path.exists(path):
        raise RuntimeError("dbt_project.yml not found at the repository root")
    with open(path) as f:
        project: dict[str, Any] = yaml.safe_load(f) or {}

    flags = project.get("flags") or {}
    flags.setdefault("use_catalogs_v2", True)
    project["flags"] = flags

    name = project.get("name")
    if name:
        models = project.get("models") or {}
        root = models.get(name) or {}
        if "+catalog_name" not in root and "catalog_name" not in root:
            root["+catalog_name"] = CATALOG_NAME
        models[name] = root
        project["models"] = models

    existing = project.get("on-run-start") or []
    if isinstance(existing, str):
        existing = [existing]
    project["on-run-start"] = [*_run_start_hooks(), *existing]

    with open(path, "w") as f:
        yaml.safe_dump(project, f, sort_keys=False)
    _write_catalogs(project_dir)

    stale = os.path.join(project_dir, "target", "run_results.json")
    if os.path.exists(stale):
        os.remove(stale)
