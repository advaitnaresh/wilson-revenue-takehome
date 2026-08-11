"""QC checks, run twice: once on raw input (Stage 2) and once on the final
transformed table (Stage 6).

Stage 2 profiles the data *before* anything is touched, so every cleaning
decision made downstream is grounded in an observed count rather than a guess.
Stage 6 checks the pipeline's own arithmetic — row counts reconcile, joins
didn't fan out, checksums tie out — so a bug in the pipeline itself is caught
before it reaches the final numbers.

Nothing in this module mutates data. It only observes and records; every issue
is a `QCIssue` with a severity, so a report can be inspected or asserted on
without re-deriving the checks by hand.
"""

from dataclasses import dataclass, field

import pandas as pd

from . import config, introspect

# "fail" = pipeline should not be trusted downstream of this until resolved.
# "warn" = a real data-quality issue, but one with a defensible default handling.
# "info" = observation, not a defect (e.g. a categorical's value distribution).
Severity = str


@dataclass
class QCIssue:
    stage: str
    table: str
    check: str
    severity: Severity
    count: int
    detail: str


@dataclass
class QCReport:
    issues: list[QCIssue] = field(default_factory=list)

    def add(self, stage: str, table: str, check: str, severity: Severity, count: int, detail: str) -> None:
        self.issues.append(QCIssue(stage, table, check, severity, count, detail))

    def by_severity(self, severity: Severity) -> list[QCIssue]:
        return [i for i in self.issues if i.severity == severity]

    def failures(self) -> list[QCIssue]:
        return self.by_severity("fail")

    def raise_if_failed(self) -> None:
        failures = self.failures()
        if failures:
            lines = "\n".join(f"  - [{i.table}] {i.check}: {i.detail}" for i in failures)
            raise AssertionError(f"QC failed with {len(failures)} hard failure(s):\n{lines}")

    def to_records(self) -> list[dict]:
        return [vars(i) for i in self.issues]


def _null_check(report: QCReport, stage: str, table: str, df: pd.DataFrame) -> None:
    nulls = df.isna().sum()
    for col, n in nulls.items():
        if n > 0:
            report.add(stage, table, f"null:{col}", "warn", int(n), f"{n} null value(s) in {table}.{col}")


def _whitespace_check(report: QCReport, stage: str, table: str, df: pd.DataFrame) -> None:
    for col in df.select_dtypes(include=["object", "str"]).columns:
        s = df[col].dropna().astype(str)
        n = int((s != s.str.strip()).sum())
        if n:
            report.add(stage, table, f"whitespace:{col}", "warn", n, f"{n} value(s) with leading/trailing whitespace in {table}.{col}")


def _duplicate_row_check(report: QCReport, stage: str, table: str, df: pd.DataFrame) -> None:
    n = int(df.duplicated().sum())
    if n:
        report.add(stage, table, "duplicate_rows", "warn", n, f"{n} fully duplicated row(s) in {table}")


def _duplicate_key_check(report: QCReport, stage: str, table: str, df: pd.DataFrame) -> None:
    key = config.PRIMARY_KEY.get(table)
    inferred = key is None
    if key is None:
        key = introspect.infer_primary_key(df)
    if key is None or key not in df.columns:
        return

    dup_ids = df[key][df[key].duplicated()].unique()
    if len(dup_ids) == 0:
        return
    dup_rows = df[df[key].isin(dup_ids)]
    conflicting = int(dup_rows.groupby(key).nunique().gt(1).any(axis=1).sum())
    label = f"(inferred key) {key}" if inferred else key
    report.add(
        stage, table, f"duplicate_key:{key}", "warn" if not inferred else "info", len(dup_ids),
        f"{len(dup_ids)} duplicated {label}(s), {conflicting} group(s) with conflicting field values "
        f"(non-conflicting duplicates are exact row repeats; conflicting ones need a human decision)",
    )


def _numeric_check(report: QCReport, stage: str, table: str, df: pd.DataFrame) -> None:
    for col in config.NUMERIC_COLUMNS.get(table, []):
        if col not in df.columns:
            continue
        parsed = pd.to_numeric(df[col], errors="coerce")
        unparseable = int(parsed.isna().sum() - df[col].isna().sum())
        if unparseable:
            report.add(stage, table, f"unparseable:{col}", "fail", unparseable, f"{unparseable} value(s) in {table}.{col} do not parse as numeric")
        negative = int((parsed < 0).sum())
        if negative:
            report.add(stage, table, f"negative:{col}", "warn", negative, f"{negative} negative value(s) in {table}.{col}")
        zero = int((parsed == 0).sum())
        if zero:
            report.add(stage, table, f"zero:{col}", "info", zero, f"{zero} zero value(s) in {table}.{col}")


def _generic_numeric_date_check(report: QCReport, stage: str, table: str, df: pd.DataFrame) -> None:
    """For tables with no declared schema: sniff columns that look numeric or
    date-like, and flag the few values within them that don't fit — at 'info'
    severity, since the type itself is a guess, not a contract."""
    for col in introspect.infer_numeric_columns(df):
        parsed = pd.to_numeric(df[col], errors="coerce")
        unparseable = int(parsed.isna().sum() - df[col].isna().sum())
        if unparseable:
            report.add(
                stage, table, f"unparseable:{col}", "info", unparseable,
                f"(inferred numeric column) {unparseable} value(s) in {table}.{col} don't parse as numeric",
            )
    for col in introspect.infer_date_columns(df):
        parsed = pd.to_datetime(df[col], errors="coerce", format="mixed")
        unparseable = int(parsed.isna().sum() - df[col].isna().sum())
        if unparseable:
            report.add(
                stage, table, f"unparseable:{col}", "info", unparseable,
                f"(inferred date column) {unparseable} value(s) in {table}.{col} don't parse as a date",
            )


def _date_check(report: QCReport, stage: str, table: str, df: pd.DataFrame) -> None:
    for col in config.DATE_COLUMNS.get(table, []):
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        unparseable = int(parsed.isna().sum() - df[col].isna().sum())
        if unparseable:
            report.add(stage, table, f"unparseable:{col}", "fail", unparseable, f"{unparseable} value(s) in {table}.{col} do not parse as a date")
        future = int((parsed > pd.Timestamp.now()).sum())
        if future:
            report.add(stage, table, f"future_date:{col}", "warn", future, f"{future} row(s) in {table}.{col} are dated in the future")


def _categorical_check(report: QCReport, stage: str, table: str, df: pd.DataFrame) -> None:
    if table == "orders" and "status" in df.columns:
        observed = set(df.status.dropna().unique())
        unexpected = observed - config.EXPECTED_STATUSES
        if unexpected:
            n = int(df.status.isin(unexpected).sum())
            report.add(stage, table, "unexpected_status", "warn", n, f"{n} row(s) with status outside {sorted(config.EXPECTED_STATUSES)}: {sorted(unexpected)}")


def _referential_integrity_check(report: QCReport, stage: str, tables: dict[str, pd.DataFrame]) -> None:
    for child, child_col, parent, parent_col in config.FOREIGN_KEYS:
        if child not in tables or parent not in tables:
            continue
        child_df, parent_df = tables[child], tables[parent]
        orphans = child_df[~child_df[child_col].isin(parent_df[parent_col])]
        if len(orphans):
            report.add(
                stage, child, f"orphan_fk:{child_col}->{parent}.{parent_col}", "warn", len(orphans),
                f"{len(orphans)} {child} row(s) reference a {parent_col} not present in {parent} "
                f"({orphans[child_col].nunique()} distinct missing value(s))",
            )


def _inferred_referential_integrity_check(report: QCReport, stage: str, tables: dict[str, pd.DataFrame]) -> None:
    """Same idea as _referential_integrity_check, but for relationships that
    aren't declared in config.FOREIGN_KEYS — i.e. tables outside the known
    schema. Detected by naming convention + uniqueness, not a contract, so
    these are always 'info', never 'fail'."""
    declared = {(c, cc) for c, cc, *_ in config.FOREIGN_KEYS}
    for child, child_col, parent, parent_col in introspect.detect_foreign_keys(tables, exclude_tables=config.KNOWN_TABLES):
        if (child, child_col) in declared:
            continue
        child_df, parent_df = tables[child], tables[parent]
        orphans = child_df[~child_df[child_col].isin(parent_df[parent_col])]
        if len(orphans):
            report.add(
                stage, child, f"orphan_fk:{child_col}->{parent}.{parent_col}", "info", len(orphans),
                f"(inferred FK — declare it in config.py to make this authoritative) "
                f"{len(orphans)} {child} row(s) reference a {parent_col} not present in {parent} "
                f"({orphans[child_col].nunique()} distinct missing value(s))",
            )


def _promo_cardinality_check(report: QCReport, stage: str, tables: dict[str, pd.DataFrame]) -> None:
    if "promotions" not in tables:
        return
    per_order = tables["promotions"].order_id.value_counts()
    stacked = int((per_order > 1).sum())
    if stacked:
        report.add(
            stage, "promotions", "stacked_promotions", "info", stacked,
            f"{stacked} order(s) carry more than one promotion row — must aggregate to one row "
            f"per order before joining, or the join will fan out",
        )


def _discount_exceeds_amount_check(report: QCReport, stage: str, tables: dict[str, pd.DataFrame]) -> None:
    if "orders" not in tables or "promotions" not in tables:
        return
    orders = tables["orders"].drop_duplicates()
    amounts = pd.to_numeric(orders.set_index("order_id").amount, errors="coerce")
    discount_sum = (
        tables["promotions"]
        .assign(discount_amount=pd.to_numeric(tables["promotions"].discount_amount, errors="coerce"))
        .groupby("order_id")
        .discount_amount.sum()
    )
    merged = discount_sum.to_frame("discount_sum").join(amounts, how="inner")
    over = merged[merged.discount_sum > merged.amount]
    if len(over):
        report.add(
            stage, "promotions", "discount_exceeds_amount", "warn", len(over),
            f"{len(over)} order(s) have total discount greater than the order amount — "
            f"would produce negative net revenue unless capped downstream",
        )


def run_initial_qc(tables: dict[str, pd.DataFrame]) -> QCReport:
    """Stage 2 — profile the raw tables. Observational only; nothing is fixed here."""
    report = QCReport()
    stage = "initial_qc"

    for name, df in tables.items():
        _null_check(report, stage, name, df)
        _whitespace_check(report, stage, name, df)
        _duplicate_row_check(report, stage, name, df)
        _duplicate_key_check(report, stage, name, df)
        _numeric_check(report, stage, name, df)
        _date_check(report, stage, name, df)
        _categorical_check(report, stage, name, df)
        if name not in config.KNOWN_TABLES:
            _generic_numeric_date_check(report, stage, name, df)

    _referential_integrity_check(report, stage, tables)
    _inferred_referential_integrity_check(report, stage, tables)
    _promo_cardinality_check(report, stage, tables)
    _discount_exceeds_amount_check(report, stage, tables)

    return report


def run_final_qc(
    final_df: pd.DataFrame,
    *,
    orders_in: int,
    row_ledger: list[dict],
    segment_revenue_total: float,
    reported_total_revenue: float,
) -> QCReport:
    """Stage 6 — validate the pipeline's own output before it's trusted.

    This is not re-checking the raw data (that's Stage 2); it's checking that
    the transform stage did what it claims: no row got duplicated or dropped
    without being accounted for, no join fanned out, and the headline numbers
    reconcile with the tables they were computed from.
    """
    report = QCReport()
    stage = "final_qc"
    table = "analysis_table"

    if final_df.order_id.duplicated().any():
        n = int(final_df.order_id.duplicated().sum())
        report.add(stage, table, "duplicate_order_id", "fail", n, f"{n} duplicate order_id(s) survived into the final table — a join fanned out")

    if len(final_df) != orders_in:
        report.add(
            stage, table, "row_count_mismatch", "fail", abs(len(final_df) - orders_in),
            f"final table has {len(final_df):,} rows but cleaned orders had {orders_in:,} — "
            f"join must be exactly one row per order",
        )

    over_capped = final_df.discount > final_df.amount
    if over_capped.any():
        n = int(over_capped.sum())
        report.add(stage, table, "discount_exceeds_amount", "fail", n, f"{n} row(s) still have discount > amount after capping")

    if final_df.discount.lt(0).any() or final_df.amount.lt(0).any():
        report.add(stage, table, "negative_value_survived", "fail", 1, "negative amount or discount survived into the final table")

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
