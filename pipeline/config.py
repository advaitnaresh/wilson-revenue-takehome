"""Schema and rule declarations for the pipeline.

Kept separate from the stage logic so pointing this pipeline at a different
extract of the same shape (new month, different source system) is a config
edit, not a code change.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

RAW_FILES = {
    "customers": REPO_ROOT / "customers.csv",
    "orders": REPO_ROOT / "orders.csv",
    "promotions": REPO_ROOT / "promotions.csv",
}

OUTPUT_DIR = REPO_ROOT / "pipeline_output"

# customer_id / order_id / promotion_id are identifiers, not quantities — read as
# strings so a blank value can't get silently coerced into a float and lose precision.
DTYPES = {
    "customers": {"customer_id": str},
    "orders": {"order_id": str, "customer_id": str},
    "promotions": {"promotion_id": str, "order_id": str},
}

REQUIRED_COLUMNS = {
    "customers": ["customer_id", "customer_name", "segment", "region"],
    "orders": ["order_id", "customer_id", "order_date", "amount", "status"],
    "promotions": ["promotion_id", "order_id", "promo_code", "discount_amount"],
}

PRIMARY_KEY = {
    "customers": "customer_id",
    "orders": "order_id",
    "promotions": "promotion_id",
}

NUMERIC_COLUMNS = {
    "orders": ["amount"],
    "promotions": ["discount_amount"],
}

DATE_COLUMNS = {
    "orders": ["order_date"],
}

FOREIGN_KEYS = [
    # (child table, child column, parent table, parent column)
    ("orders", "customer_id", "customers", "customer_id"),
    ("promotions", "order_id", "orders", "order_id"),
]

# Values observed in a clean extract. Anything outside this set is not dropped
# automatically — it's surfaced in QC so a human decides — because a new status
# value (e.g. "chargeback") is a business question, not a formatting error.
EXPECTED_STATUSES = {"completed", "refunded", "cancelled"}
REVENUE_STATUSES = {"completed"}

# Rows this dirty force a hard drop in the filter stage — kept to unambiguous,
# unrecoverable cases. Anything ambiguous (unknown customer, non-completed status,
# discount too large) is a business judgment call and is handled downstream,
# in clean/transform, where it can be labeled and reported rather than deleted.
HARD_DROP_RULES = {
    "exact_duplicate_rows": True,
    "unparseable_amount_or_date": True,
    "non_positive_amount": True,
}
