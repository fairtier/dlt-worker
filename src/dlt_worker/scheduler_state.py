"""Worker-owned scheduling state for files mode.

In files mode the worker — not the central API — owns ``last_run_at``.
It lives in ``<state_dir>/scheduler.json`` as a flat
``{pipeline_id: ISO-8601 UTC}`` object, deliberately inside the
git-synced dlt state directory so it snapshots, restores, and (when
needed) gets break-glass-edited together with the rest of the machine
state.

Persistence is best-effort: a corrupt or unwritable file degrades to
in-memory state and a log line, never a crashed scheduler.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

_FILENAME = "scheduler.json"
# The state repo's .gitignore covers `..*` — this name keeps a mid-write
# temp file out of autosave commits.
_TMP_FILENAME = ".." + _FILENAME + ".tmp"


class SchedulerState:
    def __init__(self, state_dir: str, entries: dict[str, datetime]) -> None:
        self._state_dir = state_dir
        self._entries = entries

    @classmethod
    def load(cls, state_dir: str) -> SchedulerState:
        """Read scheduler.json; a missing or corrupt file starts empty.

        Callers load fresh every tick rather than caching, so an operator
        edit of scheduler.json pulled in by the state sidecar takes effect
        on the next evaluation.
        """
        path = os.path.join(state_dir, _FILENAME)
        entries: dict[str, datetime] = {}
        try:
            with open(path, encoding="utf-8") as f:
                raw = json.load(f)
            for pipeline_id, stamp in raw.items():
                entries[pipeline_id] = datetime.fromisoformat(
                    str(stamp).replace("Z", "+00:00")
                )
        except FileNotFoundError:
            pass
        except (OSError, ValueError, AttributeError):
            # Corrupt state re-seeds from the API's last_run_at on the same
            # tick when central is reachable — losing it is recoverable.
            logger.warning(
                "Corrupt %s — starting with empty scheduler state", path, exc_info=True
            )
        return cls(state_dir, entries)

    def get(self, pipeline_id: str) -> datetime | None:
        return self._entries.get(pipeline_id)

    def __contains__(self, pipeline_id: str) -> bool:
        return pipeline_id in self._entries

    def seed(self, pipeline_id: str, at: datetime) -> None:
        """One-time migration: adopt the API's last_run_at, never overwrite."""
        if pipeline_id in self._entries:
            return
        self._entries[pipeline_id] = at
        self._save()

    def record(self, pipeline_id: str, at: datetime) -> None:
        self._entries[pipeline_id] = at
        self._save()

    def prune(self, keep: set[str]) -> None:
        """Drop entries whose pipeline no longer has a definition file."""
        stale = [pid for pid in self._entries if pid not in keep]
        if not stale:
            return
        for pid in stale:
            del self._entries[pid]
        self._save()

    def _save(self) -> None:
        path = os.path.join(self._state_dir, _FILENAME)
        tmp_path = os.path.join(self._state_dir, _TMP_FILENAME)
        payload = {pid: at.isoformat() for pid, at in self._entries.items()}
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, sort_keys=True)
                f.write("\n")
            os.replace(tmp_path, path)
        except OSError:
            logger.error(
                "Failed to persist %s — scheduling state is in-memory only "
                "until the next successful save",
                path,
                exc_info=True,
            )
