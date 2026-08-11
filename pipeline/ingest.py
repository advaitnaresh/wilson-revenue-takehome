"""Stage 1 — Raw ingestion.

Reads each source file verbatim. The only liberty taken is forcing ID columns
to string dtype (see config.DTYPES) — everything else is read exactly as it
appears on disk, uncoerced, so QC in the next stage sees the data as it truly is.
"""

import pandas as pd

from . import config


class SchemaError(Exception):
    pass


def load_raw() -> dict[str, pd.DataFrame]:
    raw = {}
    for name, path in config.RAW_FILES.items():
        if not path.exists():
            raise FileNotFoundError(f"{name}: expected raw file at {path}")

        df = pd.read_csv(path, dtype=config.DTYPES.get(name, {}))

        missing = set(config.REQUIRED_COLUMNS[name]) - set(df.columns)
        if missing:
            raise SchemaError(f"{name}: missing required column(s) {sorted(missing)}")

        raw[name] = df

    return raw


def row_counts(tables: dict[str, pd.DataFrame]) -> dict[str, int]:
    return {name: len(df) for name, df in tables.items()}
