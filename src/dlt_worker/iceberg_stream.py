"""Memory-bounded Iceberg loads for dlt's filesystem destination.

dlt's ``IcebergLoadFilesystemJob.run`` materializes the WHOLE load package
into a single in-memory Arrow table (``self.arrow_dataset.to_table()``)
before handing it to PyIceberg — a few million rows is enough to blow a
small worker's container memory limit and get it OOM-killed mid-load. The
neighboring Delta job already streams a RecordBatchReader; the Iceberg job
doesn't only because PyIceberg's write API takes a ``pa.Table``.

``apply()`` replaces the append/replace write path with chunked
``Transaction.append()`` calls over the dataset's batch stream. PyIceberg
writes data files eagerly on every ``append``; we commit every
``commit_every`` chunks (rather than once at the end) so the transaction's
accumulated snapshot/manifest state — which grows with the number of
appends, not the chunk size — can never itself outgrow the worker. Peak
memory is therefore one chunk plus at most ``commit_every`` chunks' worth of
file metadata, regardless of dataset size.

``replace`` truncates the table in its OWN committed transaction first, then
streams the new rows as plain appends. Truncating *inside* the append
transaction is what makes PyIceberg take its copy-on-write delete path,
loading the existing table into RAM to filter it — the OOM that a whole-table
metadata drop plus streamed appends avoids entirely. Table creation and the
merge/upsert path (which needs the full table for its join) are delegated
to the original implementation unchanged.

Before appending, any *required* table column the incoming data doesn't
provide is relaxed to optional (metadata-only, always forward-compatible):
tables created by dlt's row-by-row normalize carry required
``_dlt_load_id``/``_dlt_id`` columns that the Arrow-native read path never
populates, and PyIceberg rejects such appends outright — an error dlt then
retries as if transient. Any schema rejection that still occurs is raised
as a terminal destination error rather than left to dlt's retry loop.

Every append is bracketed by an RSS + wall-clock log line. A single 200k-row
append taking minutes or the RSS climbing across chunks are the two ways this
load can still OOM a small box; the trace makes which one it is unambiguous
in the box logs instead of a silent 13-minute gap before the OOM-kill.

Long loads also have to survive their own storage credentials expiring.
With credential vending (Lakekeeper's default for R2), the catalog's
``load_table`` response carries temporary S3 credentials, and PyIceberg
builds the table's ``FileIO`` from them exactly once — there is no refresh,
and the response advertises no expiry to refresh against. A load that runs
longer than the credential's lifetime therefore fails on whatever it touches
last, usually a manifest or snapshot write, with ``ACCESS_DENIED`` or
``SIGNATURE_DOES_NOT_MATCH``. dlt reads that as transient and retries the
whole load package, which for ``replace`` truncates and starts over — so the
load does not merely fail, it loops forever, and a table that takes longer
than one credential lifetime can never load at all. ``credential_refresh``
re-opens the table at interim commit boundaries, which re-issues
``load_table`` and returns freshly vended credentials.
"""

from __future__ import annotations

import gc
import logging
import os
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Rows per Transaction.append() chunk. ~200k rows of a typical analytics
# table is tens of MB in Arrow, comfortably inside a 1Gi worker even with
# writer-side copies, while keeping the produced data files from getting
# pathologically small.
DEFAULT_CHUNK_ROWS = 200_000

# Commit (and start a fresh transaction) every N appends. PyIceberg holds
# every appended data file's metadata in the open transaction until commit;
# across hundreds of chunks that alone can outgrow a small worker, so we flush
# it to a real snapshot periodically. This trades one atomic commit for
# several — acceptable here (snapshot expiry in the maintenance CronJob reaps
# the extra snapshots), and far better than an OOM-killed load. 0 disables
# periodic commits (single atomic commit at the end, the pre-0.2.5 behavior).
DEFAULT_COMMIT_EVERY = 20

# Re-open the table (and so re-vend its storage credentials) once a load has
# held the same ones for this long. Vended credentials carry no advertised
# expiry — the catalog response has an access key, a secret and a session
# token, and nothing that says when they die — so this cannot be derived and
# has to be a conservative interval. 15 minutes is well inside the hour that
# Lakekeeper's R2 vending has been observed to grant, leaves room for a
# deployment configured shorter, and costs one extra catalog round-trip per
# interval. Refresh happens only at an interim commit boundary, so it is
# inert when commit_every is 0. 0 disables it (pre-0.6.1 behavior).
DEFAULT_CREDENTIAL_REFRESH = 15 * 60

_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096

_original_run: Callable[..., None] | None = None


def _rss_mb() -> int:
    """Current resident set size in MiB from /proc; 0 where unavailable."""
    try:
        with open("/proc/self/statm") as f:
            resident_pages = int(f.read().split()[1])
        return resident_pages * _PAGE_SIZE // (1024 * 1024)
    except Exception:
        return 0


def apply(
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    commit_every: int = DEFAULT_COMMIT_EVERY,
    credential_refresh: int = DEFAULT_CREDENTIAL_REFRESH,
) -> None:
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
        _streamed_run(self, original_run, chunk_rows, commit_every, credential_refresh)

    IcebergLoadFilesystemJob.run = run  # type: ignore[method-assign]
    logger.info(
        "Iceberg loads patched to streamed appends (chunk of %d rows, commit every"
        " %d, credential refresh every %ds)",
        chunk_rows,
        commit_every,
        credential_refresh,
    )


def _streamed_run(
    job: Any,
    original_run: Callable[..., None],
    chunk_rows: int,
    commit_every: int,
    credential_refresh: int = DEFAULT_CREDENTIAL_REFRESH,
) -> None:
    from dlt.common.destination.exceptions import (
        DestinationTerminalException,
        DestinationUndefinedEntity,
    )
    from dlt.common.libs.pyarrow import pyarrow as pa
    from dlt.common.libs.pyiceberg import ensure_iceberg_compatible_arrow_data
    from pyiceberg.expressions import AlwaysTrue

    disposition = job._load_table["write_disposition"]
    if disposition == "merge":
        # Upsert joins against the incoming data as a whole; keep upstream
        # behavior rather than pretend chunked upserts are equivalent. This
        # path materializes the load package — loud so an OOM here is not a
        # mystery.
        logger.warning(
            "Iceberg streamed load: merge disposition for %s falls back to dlt's"
            " materializing load path (not memory-bounded)",
            job.load_table_name,
        )
        return original_run(job)

    ds = job.arrow_dataset
    n_files = len(getattr(job, "file_paths", None) or [])
    logger.info(
        "Iceberg streamed load: start %s -> %s (%d file(s)) [rss=%dMB]",
        disposition,
        job.load_table_name,
        n_files,
        _rss_mb(),
    )

    def open_table() -> Any:
        # Every call re-issues the catalog's load_table, so a vending catalog
        # hands back freshly minted storage credentials and PyIceberg builds a
        # new FileIO around them. That re-vending is the whole point of
        # re-opening mid-load; the schema union is a no-op by then.
        return job._job_client.load_open_table(
            "iceberg", job.load_table_name, schema=ds.schema
        )

    open_started = time.monotonic()
    try:
        table = open_table()
    except DestinationUndefinedEntity:
        # Upstream's missing-table branch creates the table and re-enters
        # self.run() — i.e. this patched run — without materializing data.
        return original_run(job)
    opened_at = time.monotonic()
    logger.info(
        "Iceberg streamed load: table opened/evolved in %.1fs [rss=%dMB]",
        opened_at - open_started,
        _rss_mb(),
    )

    if disposition == "replace":
        # Truncate in its OWN committed transaction, never mixed with the
        # appends below. A delete-all combined with appends in one transaction
        # drives PyIceberg down its copy-on-write rewrite path — it loads the
        # *existing* table's data files into memory to filter them (see
        # Table.delete: `rewrites_needed` -> ArrowScan.to_table per file),
        # which OOM-kills a small worker on a non-trivial table. On its own,
        # a whole-table delete drops files by metadata. Truncate-then-append
        # also makes the append path here identical to the plain-append case.
        trunc_started = time.monotonic()
        logger.info(
            "Iceberg streamed load: truncating %s for replace [rss=%dMB]",
            job.load_table_name,
            _rss_mb(),
        )
        table.delete(delete_filter=AlwaysTrue())
        logger.info(
            "Iceberg streamed load: truncated in %.1fs [rss=%dMB]",
            time.monotonic() - trunc_started,
            _rss_mb(),
        )
        # Re-open on the now-empty table so the appends build on a clean base.
        table = open_table()
        opened_at = time.monotonic()

    # Required table columns the incoming data doesn't provide can never be
    # satisfied by an append — PyIceberg rejects every write outright, and dlt
    # retries the rejection as if it were transient. It happens for real: a
    # table created by dlt's row-by-row normalize has required _dlt_load_id /
    # _dlt_id columns, but the Arrow-native read path (v0.2.1) doesn't add
    # them, so every later load of that table is rejected. Relaxing the
    # columns to optional is metadata-only and always forward-compatible.
    data_names = set(ds.schema.names)
    missing_required = [
        f.name for f in table.schema().fields if f.required and f.name not in data_names
    ]
    if missing_required:
        logger.warning(
            "Iceberg streamed load: required column(s) %s of %s are missing from"
            " the incoming data; making them optional so the load can proceed",
            ", ".join(missing_required),
            job.load_table_name,
        )
        with table.update_schema() as update:
            for name in missing_required:
                update.make_column_optional(name)

    if credential_refresh and not commit_every:
        # Without interim commits there is no boundary to swap the table at,
        # so a load in this mode still dies when its credentials expire. Say
        # so once, rather than let the refresh look active in the config line.
        logger.warning(
            "Iceberg streamed load: credential refresh is inert with"
            " commit_every=0 — a load outlasting its vended credentials will"
            " fail and be retried from the start"
        )

    txn = table.transaction()
    pending = False  # the current txn holds uncommitted appends

    buf: list[Any] = []
    buf_rows = 0
    total_rows = 0
    chunks = 0
    committed_chunks = 0

    def flush() -> None:
        nonlocal buf, buf_rows, total_rows, chunks, committed_chunks, txn, pending
        nonlocal table, opened_at
        if not buf:
            return
        chunk = pa.Table.from_batches(buf, schema=ds.schema)
        buf = []
        buf_rows = 0
        rows = chunk.num_rows
        chunks += 1
        # Log *before* the append: a stall inside PyIceberg's write (e.g. a
        # data-file PUT to object storage) would otherwise be an invisible gap.
        logger.info(
            "Iceberg streamed load: appending chunk %d (%d rows) [rss=%dMB]",
            chunks,
            rows,
            _rss_mb(),
        )
        started = time.monotonic()
        try:
            txn.append(ensure_iceberg_compatible_arrow_data(chunk))
        except ValueError as exc:
            # PyIceberg rejects schema-incompatible data with a bare
            # ValueError, which dlt's load treats as transient — the job is
            # retried with backoff and an unfixable mismatch turns into an
            # hours-long silent "hang". Deterministic rejection is terminal.
            raise DestinationTerminalException(
                f"Iceberg streamed load: schema-incompatible append to"
                f" {job.load_table_name}: {exc}"
            ) from exc
        total_rows += rows
        pending = True
        del chunk
        # Arrow's default pool retains freed buffers; on a memory-limited
        # worker the RSS would otherwise ratchet up across chunks.
        pa.default_memory_pool().release_unused()
        gc.collect()
        logger.info(
            "Iceberg streamed load: chunk %d appended in %.1fs (%d rows, %d total)"
            " [rss=%dMB, arrow=%dMB]",
            chunks,
            time.monotonic() - started,
            rows,
            total_rows,
            _rss_mb(),
            pa.total_allocated_bytes() // (1024 * 1024),
        )
        # Periodically commit so the open transaction's accumulated data-file
        # metadata is flushed to a snapshot and released, bounding memory to
        # ~commit_every chunks regardless of dataset size.
        if commit_every and (chunks - committed_chunks) >= commit_every:
            commit_started = time.monotonic()
            txn.commit_transaction()
            committed_chunks = chunks
            pending = False
            logger.info(
                "Iceberg streamed load: interim commit at chunk %d (%d rows) in"
                " %.1fs [rss=%dMB]",
                chunks,
                total_rows,
                time.monotonic() - commit_started,
                _rss_mb(),
            )
            # Between commits is the only safe moment to swap the table: an
            # open transaction belongs to the object that created it, so a
            # refresh has to land where nothing is pending.
            held_for = time.monotonic() - opened_at
            if credential_refresh and held_for >= credential_refresh:
                refresh_started = time.monotonic()
                table = open_table()
                opened_at = time.monotonic()
                logger.info(
                    "Iceberg streamed load: re-opened %s for fresh storage"
                    " credentials after %.0fm in %.1fs [rss=%dMB]",
                    job.load_table_name,
                    held_for / 60,
                    opened_at - refresh_started,
                    _rss_mb(),
                )
            txn = table.transaction()

    # batch_size caps what the scanner materializes per batch; the explicit
    # slicing below guarantees the bound even for sources that emit their
    # batches as stored (in-memory datasets, oversized row groups). Readahead
    # MUST be off: the default scanner prefetches up to 16 batches (plus 4
    # fragments) in background threads, and local reads outpace the
    # append-to-object-storage writes by so much that the prefetch queue
    # quietly re-materializes most of the dataset in memory. pre_buffer MUST
    # also be off: the parquet default coalesces and CACHES column-chunk
    # ranges as the scan advances through a fragment and holds them until the
    # fragment is exhausted, so the Arrow pool grows monotonically toward the
    # load-package file's size no matter how small the batches are (measured:
    # ~4MB retained per 200k-row chunk of taxi data; killed the 41M-row
    # Minimal-tier run at chunk 100 with 487MB of pool). With it off the pool
    # stays flat at ~one batch. Non-parquet datasets ignore the option.
    scanner_kwargs: dict[str, Any] = dict(
        batch_size=chunk_rows,
        batch_readahead=0,
        fragment_readahead=0,
        use_threads=False,
    )
    try:
        import pyarrow.dataset as pads

        scanner = ds.scanner(
            fragment_scan_options=pads.ParquetFragmentScanOptions(pre_buffer=False),
            **scanner_kwargs,
        )
    except (TypeError, ValueError):
        # Exotic dataset/format that rejects parquet scan options — the
        # memory-growth fix doesn't apply there, streaming still does.
        scanner = ds.scanner(**scanner_kwargs)
    for batch in scanner.to_batches():
        for start in range(0, batch.num_rows, chunk_rows):
            piece = batch.slice(start, chunk_rows)
            buf.append(piece)
            buf_rows += piece.num_rows
            if buf_rows >= chunk_rows:
                flush()
    flush()
    if pending:
        txn.commit_transaction()

    logger.info(
        "Iceberg streamed %s: %d rows in %d chunk(s) of <=%d rows to table %s"
        " [rss=%dMB]",
        disposition,
        total_rows,
        chunks,
        chunk_rows,
        job.load_table_name,
        _rss_mb(),
    )
