"""Load pipeline definitions from a git checkout (files mode).

The FairTier Console renders every pipeline save to
``pipelines/<name>.yaml`` in a per-customer git repo; a sidecar keeps a
pull-only checkout of that repo mounted at ``PIPELINES_DIR``. In files
mode those files — not the API poll — are the source of truth for
definitions and schedules. ``id`` is the stable key; the filename is
cosmetic.

Source credentials live beside each definition as
``pipelines/<name>.credentials.age`` — armored age ciphertext of the
credentials JSON, encrypted by the control plane to this box's public
key. When ``AGE_KEY_FILE`` points at the box identity, they are
decrypted here (in memory only, never logged) and take precedence over
anything the API poll delivers.

The loader must never take down the scheduler: one unparseable file is
logged and skipped while the rest keep loading, and a broken credential
file degrades that one pipeline to poll/cached credentials rather than
marking its definition broken.
"""

from __future__ import annotations

import base64
import binascii
import glob
import json
import logging
import os
from dataclasses import dataclass

import pyrage
import yaml

from dlt_worker.api_client import PipelineConfig

logger = logging.getLogger(__name__)

_ARMOR_BEGIN = "-----BEGIN AGE ENCRYPTED FILE-----"
_ARMOR_END = "-----END AGE ENCRYPTED FILE-----"

# Warn-once latch for identity-load failures: the load runs every tick,
# and a persistently unreadable key must not flood the log. Reset on
# success so a rotation-then-breakage warns again.
_identity_warned = False


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


def load_pipeline_configs(
    pipelines_dir: str, age_key_file: str = ""
) -> PipelineFilesResult:
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

    # Loaded once per tick (not per process) so a Secret rotation is picked
    # up without a restart.
    identity = _load_identity(age_key_file) if age_key_file else None

    configs: list[PipelineConfig] = []
    had_errors = False
    for path in paths:
        cfg = _load_one(path)
        if cfg is None:
            had_errors = True
            continue
        if identity is not None:
            _attach_file_credentials(cfg, path, identity)
        configs.append(cfg)
    return PipelineFilesResult(configs=configs, had_errors=had_errors)


def _load_identity(age_key_file: str) -> pyrage.x25519.Identity | None:
    """Read the box age identity, or None (warn-once) when unavailable."""
    global _identity_warned
    try:
        with open(age_key_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("AGE-SECRET-KEY-"):
                    identity = pyrage.x25519.Identity.from_str(line)
                    _identity_warned = False
                    return identity
        raise ValueError("no AGE-SECRET-KEY line found")
    except (OSError, ValueError, pyrage.IdentityError):
        if not _identity_warned:
            logger.warning(
                "Cannot load age identity from %s — credential files will "
                "be ignored, falling back to polled credentials",
                age_key_file,
                exc_info=True,
            )
            _identity_warned = True
        return None


def _attach_file_credentials(
    cfg: PipelineConfig, yaml_path: str, identity: pyrage.x25519.Identity
) -> None:
    """Decrypt ``<stem>.credentials.age`` beside yaml_path, if present.

    Any failure degrades to polled credentials for this pipeline only: it
    is logged (file name only — never ciphertext or plaintext) and must
    not mark the definition file broken (no ``had_errors``).
    """
    cred_path = os.path.splitext(yaml_path)[0] + ".credentials.age"
    try:
        with open(cred_path, "rb") as f:
            ciphertext = f.read()
    except FileNotFoundError:
        return
    except OSError:
        logger.warning("Cannot read %s", cred_path, exc_info=True)
        return

    try:
        if ciphertext.lstrip().startswith(_ARMOR_BEGIN.encode()):
            ciphertext = _dearmor(ciphertext)
        plaintext = pyrage.decrypt(ciphertext, [identity])
        credentials = json.loads(plaintext)
        if not isinstance(credentials, dict):
            raise ValueError("decrypted credentials are not a JSON object")
    except (
        pyrage.DecryptError,
        ValueError,
        binascii.Error,
        UnicodeDecodeError,
    ):
        # Deliberately no exc_info: decrypt errors can quote input bytes.
        logger.warning(
            "Cannot decrypt %s — falling back to polled credentials", cred_path
        )
        return

    cfg.source_credentials = credentials
    cfg.has_file_credentials = True


def _dearmor(data: bytes) -> bytes:
    """Strict ASCII-armor decode (age's PEM-like wrapping)."""
    lines = [line.strip() for line in data.decode("ascii").strip().splitlines()]
    if not lines or lines[0] != _ARMOR_BEGIN or lines[-1] != _ARMOR_END:
        raise ValueError("malformed age armor")
    return base64.b64decode("".join(lines[1:-1]), validate=True)


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
    # Credentials arrive separately (the sibling .credentials.age file,
    # attached by the caller, or the API poll); triggers and last_run_at
    # are joined in by the caller.
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
