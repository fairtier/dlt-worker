"""Load pipeline definitions from a git checkout (files mode).

The FairTier Console renders every pipeline save to
``pipelines/<name>.yaml`` in a per-customer git repo; a sidecar keeps a
pull-only checkout of that repo mounted at ``PIPELINES_DIR``. In files
mode those files — not the API poll — are the source of truth for
definitions and schedules. ``id`` is the stable key; the filename is
cosmetic.

The loader must never take down the scheduler: one unparseable file is
logged and skipped while the rest keep loading.
"""

from __future__ import annotations

import glob
import logging
import os
from dataclasses import dataclass

import yaml

from dlt_worker.api_client import PipelineConfig

logger = logging.getLogger(__name__)


@dataclass
class PipelineFilesResult:
    configs: list[PipelineConfig]
    # True when any file failed to load (unparseable, missing required
    # fields, unreadable directory). The caller uses this to suppress
    # scheduler-state pruning: a transiently broken file must not lose its
    # last_run_at and re-fire on repair.
    had_errors: bool


# The Console guarantees these; a file missing any of them cannot be run
# and is treated as broken rather than defaulted.
_REQUIRED_FIELDS = ("id", "source_type", "dataset_name")


def load_pipeline_configs(pipelines_dir: str) -> PipelineFilesResult:
    """Read all pipeline definitions under ``<pipelines_dir>/pipelines/``."""
    pattern = os.path.join(pipelines_dir, "pipelines", "*.yaml")
    try:
        paths = sorted(glob.glob(pattern))
    except OSError:
        logger.error("Failed to list pipeline files at %s", pattern, exc_info=True)
        return PipelineFilesResult(configs=[], had_errors=True)

    if not os.path.isdir(os.path.join(pipelines_dir, "pipelines")):
        # Distinguish "no pipelines yet" (empty dir) from a missing/broken
        # checkout — the latter must not prune scheduler state.
        logger.error("Pipelines checkout missing at %s/pipelines", pipelines_dir)
        return PipelineFilesResult(configs=[], had_errors=True)

    configs: list[PipelineConfig] = []
    had_errors = False
    for path in paths:
        cfg = _load_one(path)
        if cfg is None:
            had_errors = True
            continue
        configs.append(cfg)
    return PipelineFilesResult(configs=configs, had_errors=had_errors)


def _load_one(path: str) -> PipelineConfig | None:
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except (OSError, yaml.YAMLError):
        logger.warning("Skipping unreadable pipeline file %s", path, exc_info=True)
        return None

    if not isinstance(data, dict):
        logger.warning("Skipping pipeline file %s: not a mapping", path)
        return None

    for required in _REQUIRED_FIELDS:
        if not data.get(required):
            logger.warning("Skipping pipeline file %s: missing %s", path, required)
            return None

    source_config = data.get("source_config") or {}
    if not isinstance(source_config, dict):
        logger.warning("Skipping pipeline file %s: source_config not a mapping", path)
        return None

    # Defaults mirror the API mapping in api_client.get_pipeline_configs so
    # a pipeline behaves identically whichever transport delivered it.
    # Credentials are never rendered to files (Phase 3 moves them into the
    # repo encrypted); triggers and last_run_at are joined in by the caller.
    name = str(data.get("name") or _file_stem(path))
    return PipelineConfig(
        id=str(data["id"]),
        name=name,
        source_type=str(data["source_type"]),
        source_config=source_config,
        source_credentials={},
        dataset_name=str(data["dataset_name"]),
        schedule=str(data["schedule"]) if data.get("schedule") else None,
        write_disposition=data.get("write_disposition") or "append",
        merge_strategy=str(data.get("merge_strategy") or ""),
        enabled=bool(data.get("enabled", True)),
    )


def _file_stem(path: str) -> str:
    return os.path.splitext(os.path.basename(path))[0]
