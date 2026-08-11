"""Generic inference for CSVs that aren't in the known schema (pipeline/config.py).

A CSV dropped into data/ with a declared schema (customers/orders/promotions
today) gets exact, deterministic rules. A CSV with no declaration gets these
best-effort heuristics instead, so the pipeline never just skips a file it
doesn't recognize — it profiles and cleans it generically, and says plainly
that the finding is inferred rather than authoritative.
"""

import pandas as pd

NUMERIC_PARSE_THRESHOLD = 0.9
DATE_PARSE_THRESHOLD = 0.9
KEY_UNIQUENESS_THRESHOLD = 0.95


def infer_numeric_columns(df: pd.DataFrame, threshold: float = NUMERIC_PARSE_THRESHOLD) -> list[str]:
    """Object columns where most non-null values parse as numbers."""
    inferred = []
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
        non_null = df[col].dropna().astype(str)
        if non_null.empty:
            continue
        parsed = pd.to_numeric(non_null, errors="coerce")
        if parsed.notna().mean() >= threshold:
            inferred.append(col)
    return inferred


def infer_date_columns(df: pd.DataFrame, threshold: float = DATE_PARSE_THRESHOLD) -> list[str]:
    """Object columns where most non-null values parse as dates."""
    inferred = []
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        non_null = df[col].dropna().astype(str)
        if non_null.empty:
            continue
        parsed = pd.to_datetime(non_null, errors="coerce", format="mixed")
        if parsed.notna().mean() >= threshold:
            inferred.append(col)
    return inferred


def infer_primary_key(df: pd.DataFrame, threshold: float = KEY_UNIQUENESS_THRESHOLD) -> str | None:
    """Best guess at a table's key: the most-unique column whose name ends in
    "_id". Returns None if nothing clears the uniqueness bar."""
    best_col, best_ratio = None, 0.0
    for col in df.columns:
        if not col.endswith("_id"):
            continue
        non_null = df[col].dropna()
        if non_null.empty:
            continue
        ratio = non_null.nunique() / len(non_null)
        if ratio > best_ratio:
            best_col, best_ratio = col, ratio
    return best_col if best_ratio >= threshold else None


def detect_foreign_keys(
    tables: dict[str, pd.DataFrame], exclude_tables: set[str] = frozenset()
) -> list[tuple[str, str, str, str]]:
    """Best-effort FK detection for tables *outside* `exclude_tables` (the
    known/declared schema, which uses config.FOREIGN_KEYS instead).

    A column ending in "_id" is a candidate FK into any other table where that
    same column name is highly unique — i.e. looks like that table's key.
    Symmetric key-like columns (unique on both sides) produce a check in both
    directions; that's harmless duplication, not a correctness problem, since
    each direction is just an independent orphan check.
    """
    key_owners: dict[str, list[str]] = {}
    for name, df in tables.items():
        for col in df.columns:
            if not col.endswith("_id"):
                continue
            non_null = df[col].dropna()
            if len(non_null) and non_null.nunique() / len(non_null) >= KEY_UNIQUENESS_THRESHOLD:
                key_owners.setdefault(col, []).append(name)

    fks = []
    for child, df in tables.items():
        if child in exclude_tables:
            continue
        for col in df.columns:
            for parent in key_owners.get(col, []):
                if parent != child:
                    fks.append((child, col, parent, col))
    return fks
