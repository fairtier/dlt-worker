"""Tests for pipeline_files: loading definitions from the checkout."""

from __future__ import annotations

from pathlib import Path

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
