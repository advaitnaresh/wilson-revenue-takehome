"""Stage 3 — Filtering.

Drops rows only where there is no defensible way to keep them: exact duplicate
rows, values that don't parse, non-positive amounts. Every drop is logged with
a reason and a count, so the row-count ledger in Stage 6 can reconcile exactly.

Ambiguous cases are deliberately NOT dropped here — an order from an unknown
customer, a refunded order, a discount bigger than the order — because dropping
them would be a business decision disguised as data cleaning. Those are handled
downstream in clean.py / transform.py, where the choice and its effect are visible.

A table outside the known schema (config.KNOWN_TABLES) has no business-specific
rules to apply, so it only gets the one filter that's safe regardless of what
the CSV actually contains: dropping exact duplicate rows.
"""

import pandas as pd

from . import config


def _log(filter_log: list[dict], table: str, rule: str, before: int, after: int) -> None:
    dropped = before - after
    filter_log.append({"table": table, "rule": rule, "rows_before": before, "rows_after": after, "rows_dropped": dropped})


def filter_orders(orders: pd.DataFrame, filter_log: list[dict]) -> pd.DataFrame:
    df = orders

    before = len(df)
    df = df.drop_duplicates()
    _log(filter_log, "orders", "exact_duplicate_rows", before, len(df))

    before = len(df)
    parsed_amount = pd.to_numeric(df.amount, errors="coerce")
    parsed_date = pd.to_datetime(df.order_date, errors="coerce")
    df = df[parsed_amount.notna() & parsed_date.notna()]
    _log(filter_log, "orders", "unparseable_amount_or_date", before, len(df))

    before = len(df)
    df = df[pd.to_numeric(df.amount) > 0]
    _log(filter_log, "orders", "non_positive_amount", before, len(df))

    return df.reset_index(drop=True)


def filter_promotions(promotions: pd.DataFrame, filter_log: list[dict]) -> pd.DataFrame:
    df = promotions

    before = len(df)
    parsed = pd.to_numeric(df.discount_amount, errors="coerce")
    df = df[parsed.notna() & (parsed >= 0)]
    _log(filter_log, "promotions", "unparseable_or_negative_discount", before, len(df))

    return df.reset_index(drop=True)


def filter_customers(customers: pd.DataFrame, filter_log: list[dict]) -> pd.DataFrame:
    df = customers

    before = len(df)
    df = df.drop_duplicates(subset=[config.PRIMARY_KEY["customers"]])
    _log(filter_log, "customers", "duplicate_customer_id", before, len(df))

    return df.reset_index(drop=True)


def filter_generic(df: pd.DataFrame, table: str, filter_log: list[dict]) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    _log(filter_log, table, "exact_duplicate_rows", before, len(df))
    return df.reset_index(drop=True)


KNOWN_FILTERS = {
    "customers": filter_customers,
    "orders": filter_orders,
    "promotions": filter_promotions,
}


def apply_filters(tables: dict[str, pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], list[dict]]:
    filter_log: list[dict] = []
    filtered = {}
    for name, df in tables.items():
        fn = KNOWN_FILTERS.get(name)
        filtered[name] = fn(df, filter_log) if fn else filter_generic(df, name, filter_log)
    return filtered, filter_log
