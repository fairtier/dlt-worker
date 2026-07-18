"""Tests for scheduler_state: worker-owned last_run_at persistence."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from dlt_worker.scheduler_state import SchedulerState

_T1 = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
_T2 = datetime(2026, 7, 2, 12, 0, 0, tzinfo=timezone.utc)


def test_record_and_load_roundtrip(tmp_path: Path) -> None:
    state = SchedulerState.load(str(tmp_path))
    state.record("p1", _T1)

    reloaded = SchedulerState.load(str(tmp_path))
    assert reloaded.get("p1") == _T1
    assert reloaded.get("p1").tzinfo is not None  # type: ignore[union-attr]


def test_missing_file_starts_empty(tmp_path: Path) -> None:
    state = SchedulerState.load(str(tmp_path))
    assert state.get("p1") is None
    assert "p1" not in state


def test_seed_is_set_if_absent(tmp_path: Path) -> None:
    state = SchedulerState.load(str(tmp_path))
    state.seed("p1", _T1)
    state.seed("p1", _T2)  # must not overwrite

    assert state.get("p1") == _T1
    assert SchedulerState.load(str(tmp_path)).get("p1") == _T1


def test_record_overwrites(tmp_path: Path) -> None:
    state = SchedulerState.load(str(tmp_path))
    state.record("p1", _T1)
    state.record("p1", _T2)

    assert SchedulerState.load(str(tmp_path)).get("p1") == _T2


def test_prune_removes_only_stale(tmp_path: Path) -> None:
    state = SchedulerState.load(str(tmp_path))
    state.record("p1", _T1)
    state.record("p2", _T2)

    state.prune({"p1"})

    reloaded = SchedulerState.load(str(tmp_path))
    assert reloaded.get("p1") == _T1
    assert reloaded.get("p2") is None


def test_corrupt_file_starts_empty_without_raising(tmp_path: Path) -> None:
    (tmp_path / "scheduler.json").write_text("{not json")

    state = SchedulerState.load(str(tmp_path))
    assert state.get("p1") is None

    # And it can save over the corrupt file.
    state.record("p1", _T1)
    assert SchedulerState.load(str(tmp_path)).get("p1") == _T1


def test_zulu_timestamps_accepted(tmp_path: Path) -> None:
    (tmp_path / "scheduler.json").write_text(json.dumps({"p1": "2026-07-01T12:00:00Z"}))

    assert SchedulerState.load(str(tmp_path)).get("p1") == _T1


def test_temp_file_is_gitignored_name(tmp_path: Path) -> None:
    # The state repo's .gitignore covers `..*` — the atomic-write temp file
    # must match it so a mid-write autosave commit can never pick it up.
    from dlt_worker import scheduler_state

    assert scheduler_state._TMP_FILENAME.startswith("..")


def test_no_temp_file_left_behind(tmp_path: Path) -> None:
    state = SchedulerState.load(str(tmp_path))
    state.record("p1", _T1)

    assert [p.name for p in tmp_path.iterdir()] == ["scheduler.json"]
