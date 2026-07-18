"""Generate the box age keypair (``python -m dlt_worker.agekey <outdir>``).

Runs as an init container of the dlt seed Job on the box. Writes an age
identity file to ``<outdir>/key.txt`` (mode 0600) and the matching
recipient to ``<outdir>/key.pub``. The seed Job turns them into the
``dlt-age`` Kubernetes Secret and deposits the public key with the
FairTier control plane, which then encrypts pipeline source credentials
to it (``pipelines/<name>.credentials.age`` in the pipelines repo).

The private key must never leave the box: this tool prints no key
material and refuses to overwrite an existing ``key.txt`` (the Secret is
the durable home; a rerun against a non-empty dir is a bug, not a
rotation mechanism).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from pyrage import x25519


def generate(outdir: str) -> int:
    key_path = os.path.join(outdir, "key.txt")
    pub_path = os.path.join(outdir, "key.pub")

    if os.path.exists(key_path):
        print(f"refusing to overwrite existing {key_path}", file=sys.stderr)
        return 1

    identity = x25519.Identity.generate()
    recipient = str(identity.to_public())
    created = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Standard age identity-file layout (age-keygen compatible).
    key_body = f"# created: {created}\n# public key: {recipient}\n{identity}\n"

    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w") as f:
            f.write(key_body)
    except BaseException:
        os.unlink(key_path)
        raise

    with open(pub_path, "w", encoding="utf-8") as f:
        f.write(recipient + "\n")

    print(f"wrote age keypair to {outdir} (public key: {recipient})")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("usage: python -m dlt_worker.agekey <outdir>", file=sys.stderr)
        return 2
    try:
        return generate(argv[0])
    except OSError as exc:
        # Never echo key material — OSError carries only paths.
        print(f"agekey failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
