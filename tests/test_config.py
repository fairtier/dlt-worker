"""Tests for env-var configuration, focused on the memory bounds it injects."""

from __future__ import annotations

import glob
import os
import tempfile
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dlt_worker import config

# Minimal set of required env vars so config.load() doesn't sys.exit.
_REQUIRED = {
    "CUSTOMER_SLUG": "acme",
    "FAIRTIER_API_URL": "https://api.example",
    "LAKEKEEPER_URL": "https://lk.example",
    "AWS_ACCESS_KEY_ID": "key",
    "AWS_SECRET_ACCESS_KEY": "secret",
    "AWS_ENDPOINT_URL": "https://acc.r2.cloudflarestorage.com",
    "AWS_REGION": "auto",
    "S3_BUCKET": "ft-acme",
}

_ROW_GROUP_KEY = "DATA_WRITER__ROW_GROUP_SIZE"


@pytest.fixture
def clean_env() -> Iterator[None]:
    """Isolate os.environ AND config's module globals: config.load() injects
    env keys via setdefault (bypassing monkeypatch) and mutates config's
    UPPER_CASE module attrs — both leak into later tests if not restored."""
    saved_env = dict(os.environ)
    saved_cfg = {k: getattr(config, k) for k in dir(config) if k.isupper()}
    for key in list(os.environ):
        if "DATA_WRITER" in key or key == "DATA_WRITER_CHUNK_ROWS":
            del os.environ[key]
    os.environ.update(_REQUIRED)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved_env)
        for key, value in saved_cfg.items():
            setattr(config, key, value)


def test_default_sets_global_row_group_cap(clean_env: None) -> None:
    config.load()

    assert config.DATA_WRITER_CHUNK_ROWS == 100_000
    # Deliberately the GLOBAL data_writer section — the extract writer resolves
    # its parquet config under sources.* and ignores stage-scoped prefixes.
    assert os.environ[_ROW_GROUP_KEY] == "100000"


def test_zero_disables_the_bound(clean_env: None) -> None:
    os.environ["DATA_WRITER_CHUNK_ROWS"] = "0"

    config.load()

    assert _ROW_GROUP_KEY not in os.environ


def test_explicit_dlt_override_wins(clean_env: None) -> None:
    os.environ[_ROW_GROUP_KEY] = "12345"

    config.load()

    # setdefault must not clobber an operator-provided value.
    assert os.environ[_ROW_GROUP_KEY] == "12345"


def test_real_pipeline_row_groups_are_bounded(clean_env: None) -> None:
    """The guarantee, exercised against real dlt: even a source that yields one
    huge Arrow table in a single item is written and re-normalized in bounded
    row groups, so normalize never reads more than the cap into RAM.

    This is an end-to-end check on purpose — it is what caught that the
    stage-scoped `EXTRACT__`/`NORMALIZE__` prefixes were silently ignored.
    """
    import dlt

    cap = 1_000
    os.environ["DATA_WRITER_CHUNK_ROWS"] = str(cap)
    config.load()

    n = 250_000
    big = pa.table(
        {
            "id": pa.array(range(n), type=pa.int64()),
            "v": pa.array([f"x{i}" for i in range(n)]),
        }
    )

    @dlt.resource(name="big")
    def one_big_table() -> Any:
        yield big

    tmp = tempfile.mkdtemp()
    pipe = dlt.pipeline(
        pipeline_name="memcheck",
        destination=dlt.destinations.filesystem(bucket_url="file://" + tmp + "/out"),
        dataset_name="ds",
        pipelines_dir=tmp,
    )
    # extract → normalize; inspect the intermediate parquet before it is cleaned
    pipe.extract(one_big_table())
    pipe.normalize()

    max_rg = 0
    files = 0
    for f in glob.glob(tmp + "/**/*.parquet", recursive=True):
        md = pq.ParquetFile(f).metadata
        files += 1
        for i in range(md.num_row_groups):
            max_rg = max(max_rg, md.row_group(i).num_rows)

    assert files > 0, "no parquet produced — test wiring broken"
    assert max_rg <= cap, f"row group {max_rg} exceeds cap {cap}; normalize would OOM"
