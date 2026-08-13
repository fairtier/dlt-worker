"""Returning memory to the OS, which Python does not do on its own.

Three separate things hold a finished run's bytes, and releasing one does not
release the others:

1. **Python objects** still referenced by a cycle — ``gc.collect()``.
2. **Arrow's memory pool**, which parks freed buffers for reuse rather than
   handing them back — ``release_unused()``.
3. **glibc's heap**, which keeps freed chunks in its arenas' free lists. This
   is the one that is easy to miss, because from Python nothing is leaking:
   the objects are gone, the Arrow pool is empty, and RSS does not move.
   ``malloc_trim(0)`` is what actually returns those pages.

The third is not hypothetical. On 2026-08-12 a child that had run
extract + normalize before its load started the load phase at **492MB** and
was OOM-killed at 529MB, while a child that resumed the same load package —
skipping extract and normalize — ran the identical chunks at **324–407MB**.
The load loop was already calling (1) and (2) after every chunk, and they did
not reclaim the ~170MB difference, which is what points at (3).
"""

from __future__ import annotations

import ctypes
import ctypes.util
import gc
import logging

logger = logging.getLogger(__name__)


def _load_malloc_trim():
    """Resolve glibc's ``malloc_trim``, or ``None`` where there isn't one.

    musl (Alpine) has no ``malloc_trim`` at all, and it is not an error to run
    there — the call is an optimization, so its absence must not be fatal. The
    image is Debian-based, so in practice this resolves.
    """
    try:
        libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
        trim = libc.malloc_trim
    except (OSError, AttributeError):
        logger.info("malloc_trim unavailable; freed heap pages stay with the process")
        return None
    trim.argtypes = [ctypes.c_size_t]
    trim.restype = ctypes.c_int
    return trim


_malloc_trim = _load_malloc_trim()


def release_memory() -> None:
    """Give back everything the three holders above are sitting on.

    Cheap enough to call between chunks: ``malloc_trim`` walks the arena free
    lists, which costs microseconds against a chunk append that costs seconds.
    Safe to call when pyarrow is not imported — the load path always has it,
    other callers may not.
    """
    gc.collect()
    try:
        import pyarrow

        pyarrow.default_memory_pool().release_unused()
    except Exception:
        logger.debug("pyarrow pool release skipped", exc_info=True)
    if _malloc_trim is not None:
        _malloc_trim(0)
