"""Stage 4 — Cleaning (Spark).

Same responsibilities as pipeline.clean: coerce types, collapse promotions to
one row per order *before* any join, trim whitespace. Casts and trims are
column-level Spark expressions — lazy, so nothing here is actually computed
until something downstream forces it. Row count in equals row count out for
every table except promotions, which intentionally collapses to order-grain,
exactly like the pandas version.

An undeclared table gets clean_generic instead: the same trim-and-coerce
treatment, but which columns to coerce is decided from a bounded sample
(see qc.py's docstring) rather than known ahead of time.
"""

from pyspark.sql import DataFrame, functions as F

from pipeline import introspect


def _trim_all_strings(df: DataFrame) -> DataFrame:
    for c, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
    return df


def clean_customers(df: DataFrame) -> DataFrame:
    return _trim_all_strings(df)


def clean_orders(df: DataFrame) -> DataFrame:
    df = _trim_all_strings(df)
    return df.withColumn("amount", F.col("amount").cast("double")).withColumn("order_date", F.to_date("order_date"))


def clean_promotions(df: DataFrame) -> DataFrame:
    """Aggregate to one row per order_id: sum stacked discounts, keep every
    promo code for reference. Same reasoning as pipeline.clean.clean_promotions
    — two distinct promotion_ids on one order are two concessions granted, not
    the same discount recorded twice."""
    df = _trim_all_strings(df).withColumn("discount_amount", F.col("discount_amount").cast("double"))
    return df.groupBy("order_id").agg(
        F.sum("discount_amount").alias("discount_raw"),
        F.concat_ws(" + ", F.sort_array(F.collect_set("promo_code"))).alias("promo_codes"),
        F.count("promotion_id").alias("n_promos"),
    )


def clean_generic(df: DataFrame, sample) -> DataFrame:
    df = _trim_all_strings(df)
    if sample is None:
        return df
    for c in introspect.infer_numeric_columns(sample):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))
    for c in introspect.infer_date_columns(sample):
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))
    return df


KNOWN_CLEANERS = {
    "customers": clean_customers,
    "orders": clean_orders,
    "promotions": clean_promotions,
}


def clean_all(tables: dict[str, DataFrame], samples: dict | None = None) -> dict[str, DataFrame]:
    samples = samples or {}
    cleaned = {}
    for name, df in tables.items():
        fn = KNOWN_CLEANERS.get(name)
        cleaned[name] = fn(df) if fn else clean_generic(df, samples.get(name))
    return cleaned
