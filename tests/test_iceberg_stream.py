"""Tests for the memory-bounded Iceberg load patch."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.dataset

from dlt.common.destination.exceptions import DestinationUndefinedEntity

from dlt_worker import iceberg_stream


def _arrow_dataset(num_rows: int) -> Any:
    tbl = pa.table(
        {
            "id": pa.array(range(num_rows), type=pa.int64()),
            "name": pa.array([f"row-{i}" for i in range(num_rows)]),
        }
    )
    return pyarrow.dataset.dataset(tbl)


class _FakeTxn:
    def __init__(self) -> None:
        self.appended: list[pa.Table] = []
        self.deletes: list[Any] = []
        self.committed = False

    def append(self, df: pa.Table) -> None:
        assert not self.committed
        self.appended.append(df)

    def delete(self, delete_filter: Any) -> None:
        assert not self.appended, "replace must truncate before appending"
        self.deletes.append(delete_filter)

    def commit_transaction(self) -> None:
        self.committed = True


class _FakeJob:
    """Just the attributes _streamed_run touches."""

    load_table_name = "yellow_trips"

    def __init__(self, disposition: str, num_rows: int) -> None:
        self._load_table = {"write_disposition": disposition}
        self._ds = _arrow_dataset(num_rows)
        self.txn = _FakeTxn()
        table = MagicMock()
        table.transaction.return_value = self.txn
        self._job_client = MagicMock()
        self._job_client.load_open_table.return_value = table

    @property
    def arrow_dataset(self) -> Any:
        return self._ds


def test_append_streams_in_chunks() -> None:
    job = _FakeJob("append", num_rows=2_500)
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000)

    original.assert_not_called()
    assert job.txn.committed
    assert not job.txn.deletes
    assert len(job.txn.appended) >= 3  # 2500 rows in <=1000-row chunks
    assert sum(t.num_rows for t in job.txn.appended) == 2_500
    assert all(t.num_rows <= 1_000 for t in job.txn.appended)


def test_replace_truncates_then_appends() -> None:
    job = _FakeJob("replace", num_rows=500)
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000)

    assert len(job.txn.deletes) == 1
    assert sum(t.num_rows for t in job.txn.appended) == 500
    assert job.txn.committed


def test_replace_of_empty_dataset_still_truncates() -> None:
    job = _FakeJob("replace", num_rows=0)
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000)

    assert len(job.txn.deletes) == 1
    assert not job.txn.appended
    assert job.txn.committed


def test_merge_delegates_to_original() -> None:
    job = _FakeJob("merge", num_rows=100)
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000)

    original.assert_called_once_with(job)
    assert not job.txn.committed


def test_missing_table_delegates_to_original() -> None:
    job = _FakeJob("append", num_rows=100)
    job._job_client.load_open_table.side_effect = DestinationUndefinedEntity("nope")
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000)

    original.assert_called_once_with(job)


def test_apply_patches_once() -> None:
    from dlt.destinations.impl.filesystem.filesystem import IcebergLoadFilesystemJob

    before = IcebergLoadFilesystemJob.run
    try:
        iceberg_stream.apply()
        patched = IcebergLoadFilesystemJob.run
        assert patched is not before
        assert iceberg_stream._original_run is before
        iceberg_stream.apply()  # idempotent
        assert IcebergLoadFilesystemJob.run is patched
    finally:
        IcebergLoadFilesystemJob.run = before
        iceberg_stream._original_run = None
