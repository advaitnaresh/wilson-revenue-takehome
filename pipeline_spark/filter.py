"""Stage 3 — Filtering (Spark).

Same rule as pipeline.filter: drop only rows with no defensible way to keep
them — exact duplicates, unparseable values, non-positive amounts. Ambiguous
cases (unknown customer, refunded order, oversized discount) are deliberately
NOT dropped here, for the same reason as the pandas version: dropping them
would be a business decision disguised as data cleaning.

The Spark-specific detail: every rule below is logged with a before/after
count, and each count is a full pass over the table. Without caching, Spark
would re-read (or, worse, re-derive through everything upstream) the table
from scratch for every single one of those counts. Each table is cached once
on entry so the three-or-so rules that follow share one materialization
instead of paying for it three times.
"""

from pyspark.sql import DataFrame, functions as F


def _log(filter_log: list[dict], table: str, rule: str, before: int, after: int) -> None:
    filter_log.append({"table": table, "rule": rule, "rows_before": before, "rows_after": after, "rows_dropped": before - after})


def filter_orders(df: DataFrame, filter_log: list[dict]) -> DataFrame:
    before = df.count()
    df = df.dropDuplicates()
    after = df.count()
    _log(filter_log, "orders", "exact_duplicate_rows", before, after)

    before = after
    df = df.filter(F.col("amount").cast("double").isNotNull() & F.to_date("order_date").isNotNull())
    after = df.count()
    _log(filter_log, "orders", "unparseable_amount_or_date", before, after)

    before = after
    df = df.filter(F.col("amount").cast("double") > 0)
    after = df.count()
    _log(filter_log, "orders", "non_positive_amount", before, after)

    return df


def filter_promotions(df: DataFrame, filter_log: list[dict]) -> DataFrame:
    before = df.count()
    parsed = F.col("discount_amount").cast("double")
    df = df.filter(parsed.isNotNull() & (parsed >= 0))
    after = df.count()
    _log(filter_log, "promotions", "unparseable_or_negative_discount", before, after)
    return df


def filter_customers(df: DataFrame, filter_log: list[dict]) -> DataFrame:
    before = df.count()
    df = df.dropDuplicates(["customer_id"])
    after = df.count()
    _log(filter_log, "customers", "duplicate_customer_id", before, after)
    return df


def filter_generic(df: DataFrame, table: str, filter_log: list[dict]) -> DataFrame:
    before = df.count()
    df = df.dropDuplicates()
    after = df.count()
    _log(filter_log, table, "exact_duplicate_rows", before, after)
    return df


KNOWN_FILTERS = {
    "customers": filter_customers,
    "orders": filter_orders,
    "promotions": filter_promotions,
}


def apply_filters(tables: dict[str, DataFrame]) -> tuple[dict[str, DataFrame], list[dict]]:
    filter_log: list[dict] = []
    filtered = {}
    for name, df in tables.items():
        df = df.cache()
        fn = KNOWN_FILTERS.get(name)
        result = fn(df, filter_log) if fn else filter_generic(df, name, filter_log)
        filtered[name] = result.cache()
        df.unpersist()
    return filtered, filter_log
