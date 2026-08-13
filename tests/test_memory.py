"""Tests for the three-holder memory release.

Reclaiming bytes is not directly assertable — RSS moves for reasons unrelated
to the call. What these pin is that all three holders are asked, and that a
missing one is degraded rather than fatal, because the failure mode being
guarded against is the call silently doing less than it looks like it does.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from dlt_worker import memory


def test_all_three_holders_are_asked(monkeypatch: Any) -> None:
    collect = MagicMock()
    trim = MagicMock()
    pool = MagicMock()
    monkeypatch.setattr(memory.gc, "collect", collect)
    monkeypatch.setattr(memory, "_malloc_trim", trim)

    import pyarrow

    monkeypatch.setattr(pyarrow, "default_memory_pool", lambda: pool)

    memory.release_memory()

    collect.assert_called_once()
    pool.release_unused.assert_called_once()
    # 0 means "trim as much as possible", not "trim nothing".
    trim.assert_called_once_with(0)


def test_missing_malloc_trim_is_not_fatal(monkeypatch: Any) -> None:
    """musl has no malloc_trim; the call is an optimization, never required."""
    monkeypatch.setattr(memory, "_malloc_trim", None)

    memory.release_memory()  # must not raise


def test_pyarrow_failure_does_not_stop_the_trim(monkeypatch: Any) -> None:
    """The holder most likely to be holding the bytes must still be asked."""
    trim = MagicMock()
    monkeypatch.setattr(memory, "_malloc_trim", trim)

    import pyarrow

    def boom() -> Any:
        raise RuntimeError("pool unavailable")

    monkeypatch.setattr(pyarrow, "default_memory_pool", boom)

    memory.release_memory()

    trim.assert_called_once_with(0)


def test_malloc_trim_resolves_on_this_platform() -> None:
    """Debian-based image: if this ever returns None the fix is inert.

    Not asserted as a hard requirement — the loader is deliberately tolerant —
    but a regression here would silently remove the only lever that reclaims
    glibc's arenas, so it is worth a visible check.
    """
    assert memory._load_malloc_trim() is not None
