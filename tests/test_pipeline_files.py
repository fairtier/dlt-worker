"""Tests for pipeline_files: loading definitions from the checkout."""

from __future__ import annotations

import base64
import json
import textwrap
from pathlib import Path
from typing import Any

import pyrage

from dlt_worker import pipeline_files
from dlt_worker.pipeline_files import load_pipeline_configs

_FULL_YAML = """\
# Rendered by the FairTier Console — a Console save overwrites this file.
id: p1
name: orders
source_type: sql_database
source_config:
  tables:
    - orders
dataset_name: raw
schedule: "*/5 * * * *"
write_disposition: merge
merge_strategy: upsert
enabled: true
"""

_MINIMAL_YAML = """\
id: p2
name: minimal
source_type: rest_api
dataset_name: raw
enabled: true
"""


def _checkout(tmp_path: Path) -> Path:
    """A fake pipelines checkout: files live under <root>/pipelines/."""
    d = tmp_path / "pipelines"
    d.mkdir()
    return d


def test_parses_full_file(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    (d / "orders.yaml").write_text(_FULL_YAML)

    result = load_pipeline_configs(str(tmp_path))

    assert result.had_errors is False
    assert len(result.configs) == 1
    cfg = result.configs[0]
    assert cfg.id == "p1"
    assert cfg.name == "orders"
    assert cfg.source_type == "sql_database"
    assert cfg.source_config == {"tables": ["orders"]}
    assert cfg.dataset_name == "raw"
    assert cfg.schedule == "*/5 * * * *"
    assert cfg.write_disposition == "merge"
    assert cfg.merge_strategy == "upsert"
    assert cfg.enabled is True
    # Never rendered to files; joined in from the poll by the caller.
    assert cfg.source_credentials == {}
    assert cfg.trigger_now is False
    assert cfg.pending_run_id == ""
    assert cfg.last_run_at is None


def test_defaults_mirror_api_mapping(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    (d / "minimal.yaml").write_text(_MINIMAL_YAML)

    result = load_pipeline_configs(str(tmp_path))

    cfg = result.configs[0]
    assert cfg.schedule is None
    assert cfg.write_disposition == "append"
    assert cfg.merge_strategy == ""
    assert cfg.source_config == {}


def test_enabled_defaults_true(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    (d / "p.yaml").write_text("id: p3\nsource_type: rest_api\ndataset_name: raw\n")

    result = load_pipeline_configs(str(tmp_path))

    assert result.configs[0].enabled is True


def test_non_yaml_files_ignored(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    (d / "README.md").write_text("# not a pipeline")
    (d / "orders.yaml").write_text(_FULL_YAML)

    result = load_pipeline_configs(str(tmp_path))

    assert result.had_errors is False
    assert [c.id for c in result.configs] == ["p1"]


def test_missing_id_skipped_with_errors_flag(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    (d / "broken.yaml").write_text("name: no-id\nsource_type: rest_api\n")
    (d / "orders.yaml").write_text(_FULL_YAML)

    result = load_pipeline_configs(str(tmp_path))

    assert result.had_errors is True
    assert [c.id for c in result.configs] == ["p1"]


def test_unparseable_yaml_skipped_others_load(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    (d / "bad.yaml").write_text("id: [unclosed\n")
    (d / "orders.yaml").write_text(_FULL_YAML)

    result = load_pipeline_configs(str(tmp_path))

    assert result.had_errors is True
    assert [c.id for c in result.configs] == ["p1"]


def test_non_mapping_yaml_skipped(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    (d / "list.yaml").write_text("- just\n- a list\n")

    result = load_pipeline_configs(str(tmp_path))

    assert result.had_errors is True
    assert result.configs == []


def test_empty_dir_is_not_an_error(tmp_path: Path) -> None:
    _checkout(tmp_path)

    result = load_pipeline_configs(str(tmp_path))

    assert result.had_errors is False
    assert result.configs == []


def test_missing_checkout_is_an_error(tmp_path: Path) -> None:
    result = load_pipeline_configs(str(tmp_path / "nonexistent"))

    assert result.had_errors is True
    assert result.configs == []


def test_name_falls_back_to_file_stem(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    (d / "fallback.yaml").write_text(
        "id: p4\nsource_type: rest_api\ndataset_name: raw\n"
    )

    result = load_pipeline_configs(str(tmp_path))

    assert result.configs[0].name == "fallback"


# --- age credential files ---


def _keypair(tmp_path: Path) -> tuple[Path, pyrage.x25519.Recipient]:
    """A box identity file (the dlt-age Secret mount) + its recipient."""
    identity = pyrage.x25519.Identity.generate()
    recipient = identity.to_public()
    key_file = tmp_path / "key.txt"
    key_file.write_text(
        f"# created: 2026-07-18T00:00:00+00:00\n# public key: {recipient}\n{identity}\n"
    )
    return key_file, recipient


def _armor(ciphertext: bytes) -> bytes:
    b64 = base64.b64encode(ciphertext).decode()
    return (
        "-----BEGIN AGE ENCRYPTED FILE-----\n"
        + "\n".join(textwrap.wrap(b64, 64))
        + "\n-----END AGE ENCRYPTED FILE-----\n"
    ).encode()


def _write_creds(
    d: Path, stem: str, recipient: pyrage.x25519.Recipient, creds: Any
) -> None:
    ciphertext = pyrage.encrypt(json.dumps(creds).encode(), [recipient])
    (d / f"{stem}.credentials.age").write_bytes(_armor(ciphertext))


def test_decrypts_armored_credential_file(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    key_file, recipient = _keypair(tmp_path)
    (d / "orders.yaml").write_text(_FULL_YAML)
    _write_creds(d, "orders", recipient, {"password": "s3cr3t"})

    result = load_pipeline_configs(str(tmp_path), str(key_file))

    assert result.had_errors is False
    cfg = result.configs[0]
    assert cfg.source_credentials == {"password": "s3cr3t"}
    assert cfg.has_file_credentials is True


def test_decrypts_binary_credential_file(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    key_file, recipient = _keypair(tmp_path)
    (d / "orders.yaml").write_text(_FULL_YAML)
    ciphertext = pyrage.encrypt(json.dumps({"k": "v"}).encode(), [recipient])
    (d / "orders.credentials.age").write_bytes(ciphertext)

    result = load_pipeline_configs(str(tmp_path), str(key_file))

    assert result.configs[0].source_credentials == {"k": "v"}
    assert result.configs[0].has_file_credentials is True


def test_missing_credential_file_leaves_poll_semantics(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    key_file, _ = _keypair(tmp_path)
    (d / "orders.yaml").write_text(_FULL_YAML)

    result = load_pipeline_configs(str(tmp_path), str(key_file))

    assert result.had_errors is False
    assert result.configs[0].source_credentials == {}
    assert result.configs[0].has_file_credentials is False


def test_wrong_key_falls_back_without_errors(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    key_file, _ = _keypair(tmp_path)
    other = pyrage.x25519.Identity.generate().to_public()
    (d / "orders.yaml").write_text(_FULL_YAML)
    _write_creds(d, "orders", other, {"password": "s3cr3t"})

    result = load_pipeline_configs(str(tmp_path), str(key_file))

    # Broken credential file must not mark the definition broken.
    assert result.had_errors is False
    assert result.configs[0].source_credentials == {}
    assert result.configs[0].has_file_credentials is False


def test_garbage_credential_file_falls_back(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    key_file, _ = _keypair(tmp_path)
    (d / "orders.yaml").write_text(_FULL_YAML)
    (d / "orders.credentials.age").write_bytes(b"not age data at all")

    result = load_pipeline_configs(str(tmp_path), str(key_file))

    assert result.had_errors is False
    assert result.configs[0].has_file_credentials is False


def test_truncated_armor_falls_back(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    key_file, recipient = _keypair(tmp_path)
    (d / "orders.yaml").write_text(_FULL_YAML)
    ciphertext = pyrage.encrypt(b"{}", [recipient])
    armored = _armor(ciphertext)
    (d / "orders.credentials.age").write_bytes(armored[: len(armored) // 2])

    result = load_pipeline_configs(str(tmp_path), str(key_file))

    assert result.had_errors is False
    assert result.configs[0].has_file_credentials is False


def test_non_object_credentials_fall_back(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    key_file, recipient = _keypair(tmp_path)
    (d / "orders.yaml").write_text(_FULL_YAML)
    _write_creds(d, "orders", recipient, ["not", "a", "dict"])

    result = load_pipeline_configs(str(tmp_path), str(key_file))

    assert result.had_errors is False
    assert result.configs[0].has_file_credentials is False


def test_no_key_file_ignores_credential_files(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    _, recipient = _keypair(tmp_path)
    (d / "orders.yaml").write_text(_FULL_YAML)
    _write_creds(d, "orders", recipient, {"password": "s3cr3t"})

    result = load_pipeline_configs(str(tmp_path))

    assert result.configs[0].source_credentials == {}
    assert result.configs[0].has_file_credentials is False


def test_unreadable_identity_degrades_and_warns_once(tmp_path: Path) -> None:
    d = _checkout(tmp_path)
    _, recipient = _keypair(tmp_path)
    (d / "orders.yaml").write_text(_FULL_YAML)
    _write_creds(d, "orders", recipient, {"password": "s3cr3t"})
    pipeline_files._identity_warned = False

    missing = str(tmp_path / "nope" / "key.txt")
    result = load_pipeline_configs(str(tmp_path), missing)

    assert result.had_errors is False
    assert result.configs[0].has_file_credentials is False
    assert pipeline_files._identity_warned is True
