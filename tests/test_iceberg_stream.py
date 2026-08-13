"""Tests for the memory-bounded Iceberg load patch."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.dataset
import pytest

from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationUndefinedEntity,
)

from dlt_worker import iceberg_stream


def _arrow_dataset(num_rows: int) -> Any:
    tbl = pa.table(
        {
            "id": pa.array(range(num_rows), type=pa.int64()),
            "name": pa.array([f"row-{i}" for i in range(num_rows)]),
        }
    )
    return pyarrow.dataset.dataset(tbl)


def _batched_dataset(batch_rows: list[int]) -> Any:
    """A dataset whose batches deliberately do NOT align with the chunk size.

    The scanner hands these through as stored, which is the shape that used to
    break the chunk bound: a batch tail leaves a partial buffer, and the next
    batch's slice was taken as a full chunk regardless.
    """
    # Explicit schema: a zero-row string column would otherwise infer as null
    # and refuse to concatenate with its non-empty siblings.
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batches = [
        pa.record_batch(
            {
                "id": pa.array(range(n), type=pa.int64()),
                "name": pa.array([f"row-{i}" for i in range(n)], type=pa.string()),
            },
            schema=schema,
        )
        for n in batch_rows
    ]
    return pyarrow.dataset.dataset(pa.Table.from_batches(batches, schema=schema))


class _FakeTxn:
    def __init__(self, append_error: Exception | None = None) -> None:
        self.appended: list[pa.Table] = []
        self.committed = False
        self._append_error = append_error

    def append(self, df: pa.Table) -> None:
        assert not self.committed, "append after commit — stale transaction reused"
        if self._append_error is not None:
            raise self._append_error
        self.appended.append(df)

    def commit_transaction(self) -> None:
        assert not self.committed, "double commit"
        self.committed = True


class _FakeUpdateSchema:
    def __init__(self, table: "_FakeTable") -> None:
        self._table = table

    def __enter__(self) -> "_FakeUpdateSchema":
        return self

    def __exit__(self, *exc: Any) -> None:
        pass

    def make_column_optional(self, name: str) -> None:
        field = next(f for f in self._table.fields if f.name == name)
        assert field.required, f"make_column_optional on already-optional {name}"
        field.required = False
        self._table.relaxed.append(name)


class _FakeTable:
    """A fresh transaction on every transaction() call (mirrors PyIceberg) so
    the periodic commit — which reopens a transaction after each interim
    commit — is exercised faithfully. `delete` is the standalone truncate the
    replace path commits before any appends. `fields` is the table schema as
    (name, required) pairs; defaults to the _arrow_dataset columns, optional."""

    def __init__(
        self,
        fields: list[tuple[str, bool]] | None = None,
        append_error: Exception | None = None,
    ) -> None:
        self.txns: list[_FakeTxn] = []
        self.deletes: list[Any] = []
        self.fields = [
            SimpleNamespace(name=n, required=r)
            for n, r in (fields or [("id", False), ("name", False)])
        ]
        self.relaxed: list[str] = []
        self._append_error = append_error

    def schema(self) -> Any:
        return SimpleNamespace(fields=self.fields)

    def update_schema(self) -> _FakeUpdateSchema:
        return _FakeUpdateSchema(self)

    def transaction(self) -> _FakeTxn:
        txn = _FakeTxn(self._append_error)
        self.txns.append(txn)
        return txn

    def delete(self, delete_filter: Any) -> None:
        # truncate must land before any append transaction is opened
        assert not self.txns, "replace truncate must precede the append stream"
        self.deletes.append(delete_filter)


class _FakeJob:
    """Just the attributes _streamed_run touches."""

    load_table_name = "yellow_trips"

    def __init__(
        self,
        disposition: str,
        num_rows: int,
        table_fields: list[tuple[str, bool]] | None = None,
        append_error: Exception | None = None,
        fresh_table_per_open: bool = False,
        dataset: Any | None = None,
    ) -> None:
        self._load_table = {"write_disposition": disposition}
        self._ds = dataset if dataset is not None else _arrow_dataset(num_rows)
        self._table_fields = table_fields
        self._append_error = append_error
        self._job_client = MagicMock()
        if fresh_table_per_open:
            # A vending catalog builds a new Table (and a new FileIO around
            # newly minted credentials) on every load_table, so a distinct
            # object per open is what makes a mid-load refresh observable.
            self.tables: list[_FakeTable] = []
            self._job_client.load_open_table.side_effect = lambda *a, **kw: (
                self._new_table()
            )
        else:
            self.table = _FakeTable(table_fields, append_error)
            self.tables = [self.table]
            self._job_client.load_open_table.return_value = self.table

    def _new_table(self) -> _FakeTable:
        table = _FakeTable(self._table_fields, self._append_error)
        self.tables.append(table)
        return table

    @property
    def arrow_dataset(self) -> Any:
        return self._ds

    # --- aggregate helpers ----------------------------------------------
    # Across every table the job opened, so the counts mean the same thing
    # whether or not the load re-opened for fresh credentials.
    @property
    def txns(self) -> list[_FakeTxn]:
        return [txn for table in self.tables for txn in table.txns]

    @property
    def appended(self) -> list[pa.Table]:
        return [t for txn in self.txns for t in txn.appended]

    @property
    def deletes(self) -> list[Any]:
        return [d for table in self.tables for d in table.deletes]

    @property
    def commit_count(self) -> int:
        return sum(1 for txn in self.txns if txn.committed)

    @property
    def all_committed(self) -> bool:
        # every transaction that received appends must have been committed
        return all(txn.committed for txn in self.txns if txn.appended)


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


def test_chunk_bound_holds_when_batches_do_not_align() -> None:
    """The bound the whole design rests on: peak memory is ONE chunk.

    It did not hold. A batch tail left a partial buffer, and the next batch's
    first slice was still taken as a full `chunk_rows`, so a chunk could reach
    `2 * chunk_rows - 1`. Measured on a real 421-chunk taxi load: 22 chunks
    over the nominal 200,000 rows, the largest 298,963 — and the run was
    OOM-killed on one of the oversized ones.

    700-row batches against a 1,000-row chunk reproduce it exactly: the old
    code emitted 1,400.
    """
    job = _FakeJob("append", num_rows=0, dataset=_batched_dataset([700] * 6))
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=0)

    assert sum(t.num_rows for t in job.appended) == 4_200
    assert max(t.num_rows for t in job.appended) <= 1_000
    # And it must still fill chunks rather than emitting one per batch —
    # bounding by flushing early would trade the OOM for tiny data files.
    assert [t.num_rows for t in job.appended] == [1_000, 1_000, 1_000, 1_000, 200]


def test_chunk_bound_holds_for_batches_larger_than_a_chunk() -> None:
    """The other direction: one batch must still be split into whole chunks."""
    job = _FakeJob("append", num_rows=0, dataset=_batched_dataset([2_500, 300]))
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=0)

    assert sum(t.num_rows for t in job.appended) == 2_800
    assert [t.num_rows for t in job.appended] == [1_000, 1_000, 800]


def test_empty_batches_do_not_stall_the_stream() -> None:
    """A zero-row batch must be skipped, not loop forever on no progress."""
    job = _FakeJob("append", num_rows=0, dataset=_batched_dataset([0, 500, 0, 500]))
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=0)

    assert sum(t.num_rows for t in job.appended) == 1_000


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

    # the standalone truncate empties the table; no append transaction is
    # committed because there is nothing to append.
    assert len(job.deletes) == 1
    assert not job.appended
    assert job.commit_count == 0


def test_missing_required_columns_relaxed_to_optional() -> None:
    # The live-box failure: yellow_trips was created by dlt's row-by-row
    # normalize with required _dlt_load_id/_dlt_id, but the Arrow-native read
    # path doesn't produce those columns — PyIceberg rejected every append.
    job = _FakeJob(
        "append",
        num_rows=500,
        table_fields=[
            ("id", False),
            ("name", False),
            ("_dlt_load_id", True),
            ("_dlt_id", True),
        ],
    )
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=20)

    assert sorted(job.table.relaxed) == ["_dlt_id", "_dlt_load_id"]
    assert all(not f.required for f in job.table.fields)
    assert sum(t.num_rows for t in job.appended) == 500
    assert job.all_committed


def test_required_columns_present_in_data_left_alone() -> None:
    # Required columns the data DOES provide must stay required.
    job = _FakeJob(
        "append",
        num_rows=500,
        table_fields=[("id", True), ("name", True)],
    )
    original = MagicMock()

    iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=20)

    assert job.table.relaxed == []
    assert all(f.required for f in job.table.fields)
    assert sum(t.num_rows for t in job.appended) == 500


def test_schema_rejected_append_is_terminal() -> None:
    # PyIceberg signals incompatible data with a bare ValueError; dlt would
    # retry that forever. The streamed path must convert it to a terminal
    # destination error so the run fails visibly.
    job = _FakeJob(
        "append", num_rows=500, append_error=ValueError("Mismatch in fields")
    )
    original = MagicMock()

    with pytest.raises(DestinationTerminalException, match="Mismatch in fields"):
        iceberg_stream._streamed_run(job, original, chunk_rows=1_000, commit_every=20)

    original.assert_not_called()


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


class _FakeClock:
    """monotonic() that jumps a fixed step per call, so 'how long have we held
    these credentials' is decided by the test rather than by how fast the
    machine runs."""

    def __init__(self, step: float = 60.0) -> None:
        self._now = 0.0
        self._step = step

    def monotonic(self) -> float:
        self._now += self._step
        return self._now


def test_credentials_refreshed_at_interim_commits(monkeypatch: Any) -> None:
    # Vended storage credentials expire mid-load and PyIceberg never renews
    # them, so a long load must re-open the table to get fresh ones. 5 chunks
    # committing every 2 gives interim commits after chunks 2 and 4; with the
    # refresh interval already exceeded, each must re-open.
    monkeypatch.setattr(iceberg_stream.time, "monotonic", _FakeClock().monotonic)
    job = _FakeJob("append", num_rows=5_000, fresh_table_per_open=True)

    iceberg_stream._streamed_run(
        job, MagicMock(), chunk_rows=1_000, commit_every=2, credential_refresh=1
    )

    assert len(job.tables) == 3  # initial open + one per interim commit
    # and the load is still correct across the swap: nothing lost, nothing
    # appended to a transaction belonging to a superseded table.
    assert sum(t.num_rows for t in job.appended) == 5_000
    assert job.all_committed


def test_credentials_not_refreshed_before_the_interval(monkeypatch: Any) -> None:
    monkeypatch.setattr(iceberg_stream.time, "monotonic", _FakeClock().monotonic)
    job = _FakeJob("append", num_rows=5_000, fresh_table_per_open=True)

    iceberg_stream._streamed_run(
        job, MagicMock(), chunk_rows=1_000, commit_every=2, credential_refresh=10**9
    )

    assert len(job.tables) == 1
    assert sum(t.num_rows for t in job.appended) == 5_000


def test_credential_refresh_disabled(monkeypatch: Any) -> None:
    monkeypatch.setattr(iceberg_stream.time, "monotonic", _FakeClock().monotonic)
    job = _FakeJob("append", num_rows=5_000, fresh_table_per_open=True)

    iceberg_stream._streamed_run(
        job, MagicMock(), chunk_rows=1_000, commit_every=2, credential_refresh=0
    )

    assert len(job.tables) == 1


def test_credential_refresh_warns_when_no_interim_commits(
    monkeypatch: Any, caplog: Any
) -> None:
    # Without interim commits there is no boundary to swap the table at, so
    # the refresh cannot run — say so rather than appear configured.
    monkeypatch.setattr(iceberg_stream.time, "monotonic", _FakeClock().monotonic)
    job = _FakeJob("append", num_rows=5_000, fresh_table_per_open=True)

    with caplog.at_level("WARNING", logger=iceberg_stream.logger.name):
        iceberg_stream._streamed_run(
            job, MagicMock(), chunk_rows=1_000, commit_every=0, credential_refresh=1
        )

    assert len(job.tables) == 1
    assert any("credential refresh is inert" in r.message for r in caplog.records)
