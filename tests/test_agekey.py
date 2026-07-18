"""Tests for the agekey generator CLI."""

from __future__ import annotations

import stat
from pathlib import Path

import pyrage
import pytest

from dlt_worker import agekey


def test_generates_matching_keypair(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert agekey.main([str(tmp_path)]) == 0

    key_text = (tmp_path / "key.txt").read_text()
    recipient_str = (tmp_path / "key.pub").read_text().strip()
    assert recipient_str.startswith("age1")
    assert f"# public key: {recipient_str}" in key_text

    secret_line = next(
        line for line in key_text.splitlines() if line.startswith("AGE-SECRET-KEY-")
    )
    identity = pyrage.x25519.Identity.from_str(secret_line)
    recipient = pyrage.x25519.Recipient.from_str(recipient_str)
    ciphertext = pyrage.encrypt(b"roundtrip", [recipient])
    assert pyrage.decrypt(ciphertext, [identity]) == b"roundtrip"

    # The secret must never reach stdout/stderr.
    out = capsys.readouterr()
    assert "AGE-SECRET-KEY-" not in out.out + out.err


def test_key_file_mode_0600(tmp_path: Path) -> None:
    assert agekey.main([str(tmp_path)]) == 0

    mode = stat.S_IMODE((tmp_path / "key.txt").stat().st_mode)
    assert mode == 0o600


def test_refuses_to_overwrite(tmp_path: Path) -> None:
    (tmp_path / "key.txt").write_text("existing\n")

    assert agekey.main([str(tmp_path)]) == 1
    assert (tmp_path / "key.txt").read_text() == "existing\n"


def test_usage_error(tmp_path: Path) -> None:
    assert agekey.main([]) == 2
    assert agekey.main([str(tmp_path), "extra"]) == 2


def test_missing_outdir_fails_cleanly(tmp_path: Path) -> None:
    assert agekey.main([str(tmp_path / "does-not-exist")]) == 1
