"""Schema and rule declarations for the pipeline.

Two layers, deliberately separate:

1. Raw files are discovered automatically from `data/` — drop a new CSV in
   there and it is picked up on the next run, named after its filename stem.
   No code change needed to have it flow through ingest -> QC -> filter -> clean.

2. A table declared below (customers/orders/promotions today) gets exact,
   deterministic rules: typed columns, required-column checks, the revenue
   transform. A table that shows up in data/ but isn't declared here still
   gets profiled and cleaned — using the generic, type-sniffed rules in
   introspect.py — it just doesn't feed the revenue calculation, since that
   calculation is specific to this business schema, not a generic CSV concept.

To onboard a new *known* table (e.g. a `returns.csv` you want capped-discount-style
business rules for), add entries to REQUIRED_COLUMNS / PRIMARY_KEY / etc. below.
To onboard a CSV you just want profiled and cleaned generically, do nothing —
dropping the file into data/ is enough.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
OUTPUT_DIR = REPO_ROOT / "pipeline_output"


def discover_raw_files(data_dir: Path = DATA_DIR) -> dict[str, Path]:
    """Every *.csv in data_dir becomes a table, named after its filename stem."""
    return {p.stem: p for p in sorted(Path(data_dir).glob("*.csv"))}


# --- Known-schema business rules --------------------------------------------

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

# The transform stage (revenue calc) only runs if every one of these is present
# among the discovered tables. Extra, undeclared CSVs never block it.
REVENUE_TABLES = {"customers", "orders", "promotions"}

KNOWN_TABLES = set(REQUIRED_COLUMNS)
