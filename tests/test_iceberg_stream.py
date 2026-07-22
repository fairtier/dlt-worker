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
        assert not self.committed, "append after commit — stale transaction reused"
        self.appended.append(df)

    def delete(self, delete_filter: Any) -> None:
        assert not self.appended, "replace must truncate before appending"
        self.deletes.append(delete_filter)

    def commit_transaction(self) -> None:
        assert not self.committed, "double commit"
        self.committed = True


class _FakeJob:
    """Just the attributes _streamed_run touches. A fresh transaction is handed
    out on every table.transaction() call (mirrors PyIceberg) so the periodic
    commit — which starts a new transaction after each interim commit — is
    exercised faithfully."""

    load_table_name = "yellow_trips"

    def __init__(self, disposition: str, num_rows: int) -> None:
        self._load_table = {"write_disposition": disposition}
        self._ds = _arrow_dataset(num_rows)
        self.txns: list[_FakeTxn] = []
        table = MagicMock()
        table.transaction.side_effect = self._new_txn
        self._job_client = MagicMock()
        self._job_client.load_open_table.return_value = table

    def _new_txn(self) -> _FakeTxn:
        txn = _FakeTxn()
        self.txns.append(txn)
        return txn

    @property
    def arrow_dataset(self) -> Any:
        return self._ds

    # --- aggregate helpers across all transactions -----------------------
    @property
    def appended(self) -> list[pa.Table]:
        return [t for txn in self.txns for t in txn.appended]

    @property
    def deletes(self) -> list[Any]:
        return [d for txn in self.txns for d in txn.deletes]

    @property
    def commit_count(self) -> int:
        return sum(1 for txn in self.txns if txn.committed)

    @property
    def all_committed(self) -> bool:
        # every transaction that received work must have been committed
        return all(
            txn.committed for txn in self.txns if txn.appended or txn.deletes
        )


def test_append_streams_in_chunks() -> None:
    job = _FakeJob("append", num_rows=2_500)
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=0)

    original.assert_not_called()
    assert job.all_committed
    assert not job.deletes
    assert len(job.appended) >= 3  # 2500 rows in <=1000-row chunks
    assert sum(t.num_rows for t in job.appended) == 2_500
    assert all(t.num_rows <= 1_000 for t in job.appended)


def test_periodic_commit_flushes_and_reopens_transactions() -> None:
    # 5 chunks of 1000 rows; commit every 2 -> interim commits after chunks
    # 2 and 4, plus a final commit for chunk 5. Three transactions, each
    # committed, no rows lost or duplicated.
    job = _FakeJob("append", num_rows=5_000)
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=2)

    assert len(job.txns) == 3
    assert job.commit_count == 3
    assert job.all_committed
    assert sum(t.num_rows for t in job.appended) == 5_000
    # nothing appended to a transaction after it was committed
    for txn in job.txns:
        assert txn.committed


def test_single_commit_when_periodic_disabled() -> None:
    job = _FakeJob("append", num_rows=5_000)
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=0)

    assert len(job.txns) == 1
    assert job.commit_count == 1
    assert sum(t.num_rows for t in job.appended) == 5_000


def test_replace_truncates_then_appends() -> None:
    job = _FakeJob("replace", num_rows=500)
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=20)

    assert len(job.deletes) == 1
    assert sum(t.num_rows for t in job.appended) == 500
    assert job.all_committed


def test_replace_of_empty_dataset_still_truncates() -> None:
    job = _FakeJob("replace", num_rows=0)
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=20)

    assert len(job.deletes) == 1
    assert not job.appended
    assert job.commit_count == 1


def test_merge_delegates_to_original() -> None:
    job = _FakeJob("merge", num_rows=100)
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=20)

    original.assert_called_once_with(job)
    assert job.commit_count == 0


def test_missing_table_delegates_to_original() -> None:
    job = _FakeJob("append", num_rows=100)
    job._job_client.load_open_table.side_effect = DestinationUndefinedEntity("nope")
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=20)

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
