"""Keeping credential values out of the text a failure produces.

Second line of defense behind "never log the config": source exceptions
routinely echo it back — SQLAlchemy errors include the connection URL,
requests errors the full request URL, a DuckDB ATTACH error the rendered
connection string. That text becomes an error_message persisted to the
workspace database, sent to the central API, shown in the Console and
written to a log the box ships to central Loki. Nothing credential-shaped
may reach any of them.

It lives in its own module because more than one caller needs it and only
one of them can afford to import dlt: the source-test probe runs in a child
that has no reason to pull in ~115 MB of dlt to scrub a string.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

# Credential values shorter than this are not scrubbed: replacing every
# occurrence of a 1-3 char string would mangle unrelated message text.
MIN_SCRUB_LENGTH = 4


def credential_values(obj: Any) -> list[str]:
    """Collect every string value nested anywhere in a credentials structure."""
    values: list[str] = []
    if isinstance(obj, dict):
        for item in obj.values():
            values.extend(credential_values(item))
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            values.extend(credential_values(item))
    elif isinstance(obj, str) and len(obj) >= MIN_SCRUB_LENGTH:
        values.append(obj)
    return values


def scrub_credentials(text: str, credentials: dict[str, Any]) -> str:
    """Replace credential values (and their URL-encoded forms) with ***.

    Longest values first so a value that contains another is scrubbed whole
    before its substring punches a hole in it.
    """
    values = sorted(
        set(credential_values(credentials)), key=lambda v: len(v), reverse=True
    )
    for value in values:
        for variant in (value, quote(value, safe="")):
            text = text.replace(variant, "***")
    return text
