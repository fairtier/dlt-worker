"""Entry point for ``python -m dlt_worker``."""

from dlt_worker.main import run

# The guard matters now that pipeline runs use multiprocessing spawn: a
# child must never fall into the worker loop if this module is ever its
# __main__ (spawn skips re-importing package __main__ modules, but the
# guard is the documented belt-and-braces for any entry-point shape).
if __name__ == "__main__":
    run()
