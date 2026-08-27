"""PostgreSQL URL normalization, shared by the run path and the probe path.

Its own module for the same reason scrub.py is: the source-test probe needs
it, and the probe child has no reason to import dlt (~115 MB) to rewrite a
URL scheme.
"""

from __future__ import annotations


def normalize_pg_connection_string(connection_string: str) -> str:
    """Map the common PostgreSQL URL schemes onto the installed driver.

    This image deliberately ships psycopg (v3) as its only database driver,
    but SQLAlchemy resolves a plain ``postgresql://`` to psycopg2 and knows
    no ``postgres`` dialect at all — so the natural forms users paste would
    crash at engine creation. Anything that is not a PostgreSQL scheme is
    left untouched (the FairTier API rejects those at save time; a
    self-hoster who installed extra drivers keeps them working).
    """
    scheme, sep, rest = connection_string.partition("://")
    if sep and scheme.lower() in ("postgres", "postgresql"):
        return "postgresql+psycopg://" + rest
    return connection_string
