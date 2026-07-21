"""Memory-bounded Iceberg loads for dlt's filesystem destination.

dlt's ``IcebergLoadFilesystemJob.run`` materializes the WHOLE load package
into a single in-memory Arrow table (``self.arrow_dataset.to_table()``)
before handing it to PyIceberg — a few million rows is enough to blow a
small worker's container memory limit and get it OOM-killed mid-load. The
neighboring Delta job already streams a RecordBatchReader; the Iceberg job
doesn't only because PyIceberg's write API takes a ``pa.Table``.

``apply()`` replaces the append/replace write path with chunked
``Transaction.append()`` calls over the dataset's batch stream. PyIceberg
writes data files eagerly on every ``append`` and commits metadata once at
``commit_transaction()``, so peak memory is one chunk regardless of dataset
size while the load remains a single atomic commit. Table creation and the
merge/upsert path (which needs the full table for its join) are delegated
to the original implementation unchanged.
"""

from __future__ import annotations

import gc
import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Rows per Transaction.append() chunk. ~200k rows of a typical analytics
# table is tens of MB in Arrow, comfortably inside a 1Gi worker even with
# writer-side copies, while keeping the produced data files from getting
# pathologically small.
DEFAULT_CHUNK_ROWS = 200_000

_original_run: Callable[..., None] | None = None


def apply(chunk_rows: int = DEFAULT_CHUNK_ROWS) -> None:
    """Monkeypatch IcebergLoadFilesystemJob.run with the streamed version.

    Idempotent; safe to call before any pipeline runs.
    """
    global _original_run
    if _original_run is not None:
        return

    from dlt.destinations.impl.filesystem.filesystem import IcebergLoadFilesystemJob

    original_run = IcebergLoadFilesystemJob.run
    _original_run = original_run

    def run(self: Any) -> None:
        _streamed_run(self, original_run, chunk_rows)

    IcebergLoadFilesystemJob.run = run  # type: ignore[method-assign]
    logger.info(
        "Iceberg loads patched to streamed appends (chunk of %d rows)", chunk_rows
    )


def _streamed_run(job: Any, original_run: Callable[..., None], chunk_rows: int) -> None:
    from dlt.common.destination.exceptions import DestinationUndefinedEntity
    from dlt.common.libs.pyarrow import pyarrow as pa
    from dlt.common.libs.pyiceberg import ensure_iceberg_compatible_arrow_data
    from pyiceberg.expressions import AlwaysTrue

    disposition = job._load_table["write_disposition"]
    if disposition == "merge":
        # Upsert joins against the incoming data as a whole; keep upstream
        # behavior rather than pretend chunked upserts are equivalent.
        return original_run(job)

    ds = job.arrow_dataset
    try:
        table = job._job_client.load_open_table(
            "iceberg", job.load_table_name, schema=ds.schema
        )
    except DestinationUndefinedEntity:
        # Upstream's missing-table branch creates the table and re-enters
        # self.run() — i.e. this patched run — without materializing data.
        return original_run(job)

    txn = table.transaction()
    if disposition == "replace":
        txn.delete(delete_filter=AlwaysTrue())

    buf: list[Any] = []
    buf_rows = 0
    total_rows = 0
    chunks = 0

    def flush() -> None:
        nonlocal buf, buf_rows, total_rows, chunks
        if not buf:
            return
        chunk = pa.Table.from_batches(buf, schema=ds.schema)
        buf = []
        buf_rows = 0
        txn.append(ensure_iceberg_compatible_arrow_data(chunk))
        total_rows += chunk.num_rows
        chunks += 1
        rows = chunk.num_rows
        del chunk
        # Arrow's default pool retains freed buffers; on a memory-limited
        # worker the RSS would otherwise ratchet up across chunks.
        pa.default_memory_pool().release_unused()
        gc.collect()
        logger.info(
            "Iceberg streamed load: chunk %d (%d rows, %d total) appended to %s"
            " [arrow allocated: %d]",
            chunks,
            rows,
            total_rows,
            job.load_table_name,
            pa.total_allocated_bytes(),
        )

    # batch_size caps what the scanner materializes per batch; the explicit
    # slicing below guarantees the bound even for sources that emit their
    # batches as stored (in-memory datasets, oversized row groups). Readahead
    # MUST be off: the default scanner prefetches up to 16 batches (plus 4
    # fragments) in background threads, and local reads outpace the
    # append-to-object-storage writes by so much that the prefetch queue
    # quietly re-materializes most of the dataset in memory.
    scanner = ds.scanner(
        batch_size=chunk_rows,
        batch_readahead=0,
        fragment_readahead=0,
        use_threads=False,
    )
    for batch in scanner.to_batches():
        for start in range(0, batch.num_rows, chunk_rows):
            piece = batch.slice(start, chunk_rows)
            buf.append(piece)
            buf_rows += piece.num_rows
            if buf_rows >= chunk_rows:
                flush()
    flush()
    txn.commit_transaction()

    logger.info(
        "Iceberg streamed %s: %d rows in %d chunk(s) of <=%d rows to table %s",
        disposition,
        total_rows,
        chunks,
        chunk_rows,
        job.load_table_name,
    )
