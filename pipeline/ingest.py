"""Stage 1 — Raw ingestion.

Loads every CSV discovered in data/ (see config.discover_raw_files), keyed by
filename stem. Every column ending in "_id" — declared or not — is forced to
string dtype: an identifier read as int64 risks silent precision loss on a
blank value, and worse, breaks membership checks against a sibling table where
the same column parsed as a different dtype (e.g. "999" vs 999). A declared
table (config.REQUIRED_COLUMNS) additionally gets a required-column check.
"""

from pathlib import Path

import pandas as pd

from . import config


class SchemaError(Exception):
    pass


def load_raw(data_dir: Path = config.DATA_DIR) -> dict[str, pd.DataFrame]:
    files = config.discover_raw_files(data_dir)
    if not files:
        raise FileNotFoundError(f"no CSV files found in {data_dir}")

    raw = {}
    for name, path in files.items():
        header = pd.read_csv(path, nrows=0)
        dtype = {col: str for col in header.columns if col.endswith("_id")}
        dtype.update(config.DTYPES.get(name, {}))

        df = pd.read_csv(path, dtype=dtype)

        if name in config.REQUIRED_COLUMNS:
            missing = set(config.REQUIRED_COLUMNS[name]) - set(df.columns)
            if missing:
                raise SchemaError(f"{name}: missing required column(s) {sorted(missing)}")

        raw[name] = df

    return raw


def row_counts(tables: dict[str, pd.DataFrame]) -> dict[str, int]:
    return {name: len(df) for name, df in tables.items()}
