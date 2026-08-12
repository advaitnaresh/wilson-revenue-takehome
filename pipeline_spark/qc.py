"""QC checks (Spark), run twice exactly like pipeline.qc: once on raw input
(Stage 2), once on the final transformed table (Stage 6).

The severity model is reused unchanged (QCIssue/QCReport, imported from
pipeline.qc — fail/warn/info mean the same thing here as there). What's
rewritten is HOW each check is computed: every check here is a distributed
aggregation (.count(), .agg(), a left_anti join), never a full collect() of
the underlying table. The only data that ever reaches the driver is:

  (a) small aggregate results — a handful of counts, at most one row per
      distinct category (status, segment) — and
  (b) a bounded sample (config.SCHEMA_INFERENCE_SAMPLE_SIZE rows) used to
      decide the *type* of an undeclared table's columns, reusing the exact
      same pandas-based inference in pipeline.introspect that the small-data
      pipeline uses on the whole table. At real scale, scanning every value
      of every column just to guess whether it's "numeric" isn't a check
      worth the cost — the sample-based decision is a deliberate trade-off,
      and every finding built on it is tagged 'info', never 'warn'/'fail',
      to make clear it's inferred from a sample, not verified against the
      whole table.

One simplification versus the pandas version, made explicit rather than
silently dropped: the "how many duplicate-key groups have conflicting field
values" refinement is skipped here. In pandas that's `groupby(key).nunique()`
across every column, which is cheap when the table fits in memory; at scale
it means shuffling every column for every duplicate key just to answer a
question the reader can just as easily be told to go check by hand.
"""

from pyspark.sql import DataFrame, functions as F

from pipeline import introspect
from pipeline.qc import QCIssue, QCReport  # reuse the severity model as-is
from pipeline_spark import config

__all__ = ["QCIssue", "QCReport", "run_initial_qc", "run_final_qc"]


def _null_check(report: QCReport, stage: str, table: str, df: DataFrame) -> None:
    agg = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).first()
    for col in df.columns:
        n = agg[col]
        if n:
            report.add(stage, table, f"null:{col}", "warn", int(n), f"{n} null value(s) in {table}.{col}")


def _duplicate_row_check(report: QCReport, stage: str, table: str, df: DataFrame, total: int) -> None:
    distinct = df.distinct().count()
    dup = total - distinct
    if dup:
        report.add(stage, table, "duplicate_rows", "warn", dup, f"{dup} fully duplicated row(s) in {table}")


def _duplicate_key_check(report: QCReport, stage: str, table: str, df: DataFrame, key: str | None, inferred: bool) -> None:
    if key is None or key not in df.columns:
        return
    dup_groups = df.groupBy(key).count().filter(F.col("count") > 1)
    n = dup_groups.count()
    if n == 0:
        return
    label = f"(inferred key) {key}" if inferred else key
    report.add(
        stage, table, f"duplicate_key:{key}", "info" if inferred else "warn", n,
        f"{n} duplicated {label}(s) — conflicting-field detection across duplicate groups is "
        f"skipped at this scale; treat any duplicate key as needing review",
    )


def _numeric_check(report: QCReport, stage: str, table: str, df: DataFrame, columns: list[str], inferred: bool) -> None:
    for col in columns:
        if col not in df.columns:
            continue
        casted = F.col(col).cast("double")
        stats = df.select(
            F.count(F.when(F.col(col).isNotNull() & casted.isNull(), col)).alias("unparseable"),
            F.count(F.when(casted < 0, col)).alias("negative"),
            F.count(F.when(casted == 0, col)).alias("zero"),
        ).first()
        prefix = "(inferred numeric column) " if inferred else ""
        if stats["unparseable"]:
            report.add(stage, table, f"unparseable:{col}", "info" if inferred else "fail", int(stats["unparseable"]), f"{prefix}{stats['unparseable']} value(s) in {table}.{col} do not parse as numeric")
        if not inferred and stats["negative"]:
            report.add(stage, table, f"negative:{col}", "warn", int(stats["negative"]), f"{stats['negative']} negative value(s) in {table}.{col}")
        if not inferred and stats["zero"]:
            report.add(stage, table, f"zero:{col}", "info", int(stats["zero"]), f"{stats['zero']} zero value(s) in {table}.{col}")


def _date_check(report: QCReport, stage: str, table: str, df: DataFrame, columns: list[str], inferred: bool) -> None:
    for col in columns:
        if col not in df.columns:
            continue
        casted = F.to_timestamp(F.col(col))
        n = df.filter(F.col(col).isNotNull() & casted.isNull()).count()
        if n:
            prefix = "(inferred date column) " if inferred else ""
            report.add(stage, table, f"unparseable:{col}", "info" if inferred else "fail", n, f"{prefix}{n} value(s) in {table}.{col} do not parse as a date")


def _categorical_check(report: QCReport, stage: str, table: str, df: DataFrame) -> None:
    if table != "orders" or "status" not in df.columns:
        return
    observed = {row["status"] for row in df.select("status").distinct().collect() if row["status"] is not None}
    unexpected = observed - config.EXPECTED_STATUSES
    if unexpected:
        n = df.filter(F.col("status").isin(*unexpected)).count()
        report.add(stage, table, "unexpected_status", "warn", n, f"{n} row(s) with status outside {sorted(config.EXPECTED_STATUSES)}: {sorted(unexpected)}")


def _referential_integrity_check(report: QCReport, stage: str, tables: dict[str, DataFrame]) -> None:
    for child, child_col, parent, parent_col in config.FOREIGN_KEYS:
        if child not in tables or parent not in tables:
            continue
        child_df, parent_df = tables[child], tables[parent]
        orphans = child_df.join(parent_df.select(parent_col).distinct(), child_df[child_col] == parent_df[parent_col], "left_anti")
        n = orphans.count()
        if n:
            distinct_missing = orphans.select(child_col).distinct().count()
            report.add(
                stage, child, f"orphan_fk:{child_col}->{parent}.{parent_col}", "warn", n,
                f"{n} {child} row(s) reference a {parent_col} not present in {parent} ({distinct_missing} distinct missing value(s))",
            )


def _inferred_referential_integrity_check(report: QCReport, stage: str, tables: dict[str, DataFrame], samples: dict) -> None:
    """Same idea, for an undeclared table's columns. Which columns look like
    keys is decided cheaply: a known table's key is already exact (its
    declared config.PRIMARY_KEY, no sampling needed); an undeclared table's
    key is guessed from its sample (introspect.infer_primary_key). Together
    these form the candidate parent-key registry that any *_id column in an
    undeclared table gets checked against. Once a candidate pair is picked,
    the actual orphan count is still computed exactly, via a full left_anti
    join — only the *decision to check* is sample-based, not the check itself.
    """
    declared = {(c, cc) for c, cc, *_ in config.FOREIGN_KEYS}

    key_owners: dict[str, list[str]] = {}
    for name in tables:
        if name in config.KNOWN_TABLES:
            key = config.PRIMARY_KEY.get(name)
        else:
            sample = samples.get(name)
            key = introspect.infer_primary_key(sample) if sample is not None else None
        if key:
            key_owners.setdefault(key, []).append(name)

    for child, child_df in tables.items():
        if child in config.KNOWN_TABLES:
            continue  # known tables use explicit config.FOREIGN_KEYS instead
        for col in child_df.columns:
            if not col.endswith("_id"):
                continue
            for parent in key_owners.get(col, []):
                if parent == child or (child, col) in declared:
                    continue
                parent_df = tables[parent]
                orphans = child_df.join(parent_df.select(col).distinct(), child_df[col] == parent_df[col], "left_anti")
                n = orphans.count()
                if n:
                    distinct_missing = orphans.select(col).distinct().count()
                    report.add(
                        stage, child, f"orphan_fk:{col}->{parent}.{col}", "info", n,
                        f"(inferred FK — declare it in config.py to make this authoritative) "
                        f"{n} {child} row(s) reference a {col} not present in {parent} ({distinct_missing} distinct missing value(s))",
                    )


def _promo_cardinality_check(report: QCReport, stage: str, tables: dict[str, DataFrame]) -> None:
    if "promotions" not in tables:
        return
    stacked = tables["promotions"].groupBy("order_id").count().filter(F.col("count") > 1).count()
    if stacked:
        report.add(
            stage, "promotions", "stacked_promotions", "info", stacked,
            f"{stacked} order(s) carry more than one promotion row — must aggregate to one row "
            f"per order before joining, or the join will fan out",
        )


def _discount_exceeds_amount_check(report: QCReport, stage: str, tables: dict[str, DataFrame]) -> None:
    if "orders" not in tables or "promotions" not in tables:
        return
    orders = tables["orders"].dropDuplicates().select("order_id", F.col("amount").cast("double").alias("amount"))
    discount_sum = (
        tables["promotions"]
        .withColumn("discount_amount", F.col("discount_amount").cast("double"))
        .groupBy("order_id")
        .agg(F.sum("discount_amount").alias("discount_sum"))
    )
    n = discount_sum.join(orders, "order_id", "inner").filter(F.col("discount_sum") > F.col("amount")).count()
    if n:
        report.add(
            stage, "promotions", "discount_exceeds_amount", "warn", n,
            f"{n} order(s) have total discount greater than the order amount — "
            f"would produce negative net revenue unless capped downstream",
        )


def run_initial_qc(tables: dict[str, DataFrame], samples: dict | None = None) -> QCReport:
    """Stage 2 — profile the raw tables. Observational only; nothing is fixed here.

    `samples` is a dict of {table_name: pandas.DataFrame} for tables OUTSIDE
    the known schema (see run.py) — a bounded sample used only to decide
    column types/keys for those tables, per the module docstring above.
    """
    samples = samples or {}
    report = QCReport()
    stage = "initial_qc"

    for name, df in tables.items():
        total = df.count()
        _null_check(report, stage, name, df)
        _duplicate_row_check(report, stage, name, df, total)

        if name in config.KNOWN_TABLES:
            _duplicate_key_check(report, stage, name, df, config.PRIMARY_KEY.get(name), inferred=False)
            _numeric_check(report, stage, name, df, config.NUMERIC_COLUMNS.get(name, []), inferred=False)
            _date_check(report, stage, name, df, config.DATE_COLUMNS.get(name, []), inferred=False)
            _categorical_check(report, stage, name, df)
        elif name in samples:
            sample = samples[name]
            _duplicate_key_check(report, stage, name, df, introspect.infer_primary_key(sample), inferred=True)
            _numeric_check(report, stage, name, df, introspect.infer_numeric_columns(sample), inferred=True)
            _date_check(report, stage, name, df, introspect.infer_date_columns(sample), inferred=True)

    _referential_integrity_check(report, stage, tables)
    _inferred_referential_integrity_check(report, stage, tables, samples)
    _promo_cardinality_check(report, stage, tables)
    _discount_exceeds_amount_check(report, stage, tables)

    return report


def run_final_qc(
    final_df: DataFrame,
    *,
    orders_in: int,
    row_ledger: list[dict],
    segment_revenue_total: float,
    reported_total_revenue: float,
) -> QCReport:
    """Stage 6 — validate the pipeline's own output before it's trusted. Same
    checks as pipeline.qc.run_final_qc, computed as distributed aggregations."""
    report = QCReport()
    stage = "final_qc"
    table = "analysis_table"

    total = final_df.count()
    distinct_orders = final_df.select("order_id").distinct().count()
    if distinct_orders != total:
        n = total - distinct_orders
        report.add(stage, table, "duplicate_order_id", "fail", n, f"{n} duplicate order_id(s) survived into the final table — a join fanned out")

    if total != orders_in:
        report.add(
            stage, table, "row_count_mismatch", "fail", abs(total - orders_in),
            f"final table has {total:,} rows but cleaned orders had {orders_in:,} — join must be exactly one row per order",
        )

    stats = final_df.select(
        F.count(F.when(F.col("discount") > F.col("amount"), 1)).alias("over_capped"),
        F.count(F.when((F.col("discount") < 0) | (F.col("amount") < 0), 1)).alias("negative"),
    ).first()
    if stats["over_capped"]:
        report.add(stage, table, "discount_exceeds_amount", "fail", int(stats["over_capped"]), f"{stats['over_capped']} row(s) still have discount > amount after capping")
    if stats["negative"]:
        report.add(stage, table, "negative_value_survived", "fail", int(stats["negative"]), "negative amount or discount survived into the final table")

    checksum_diff = round(abs(segment_revenue_total - reported_total_revenue), 2)
    if checksum_diff > 0.01:
        report.add(
            stage, table, "segment_checksum", "fail", 1,
            f"revenue-by-segment sums to {segment_revenue_total:,.2f} but total revenue is "
            f"{reported_total_revenue:,.2f} (diff {checksum_diff:,.2f}) — the parts don't add up to the whole",
        )

    counts_by_stage = {row["stage"]: row["count"] for row in row_ledger if row["table"] == "orders"}
    report.add(stage, table, "row_count_ledger", "info", len(row_ledger), str(counts_by_stage))

    return report
