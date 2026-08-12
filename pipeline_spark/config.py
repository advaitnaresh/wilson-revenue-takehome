"""Spark-specific configuration.

Business rules — which columns are required, which statuses are expected,
which foreign keys exist — are declared exactly once, in pipeline.config, and
imported here unchanged. Duplicating them would let the two pipelines drift:
someone adds a new expected status for the pandas path and the Spark path
silently keeps flagging it as unexpected. This module only adds what's
actually specific to running those same rules at scale.
"""

from pathlib import Path

from pyspark.sql.types import StringType, StructField, StructType

from pipeline import config as base_config

REPO_ROOT = base_config.REPO_ROOT
DATA_DIR = base_config.DATA_DIR
OUTPUT_DIR = REPO_ROOT / "pipeline_output_spark"

REQUIRED_COLUMNS = base_config.REQUIRED_COLUMNS
PRIMARY_KEY = base_config.PRIMARY_KEY
NUMERIC_COLUMNS = base_config.NUMERIC_COLUMNS
DATE_COLUMNS = base_config.DATE_COLUMNS
FOREIGN_KEYS = base_config.FOREIGN_KEYS
EXPECTED_STATUSES = base_config.EXPECTED_STATUSES
REVENUE_STATUSES = base_config.REVENUE_STATUSES
REVENUE_TABLES = base_config.REVENUE_TABLES
KNOWN_TABLES = base_config.KNOWN_TABLES


def _all_string_schema(columns: list[str]) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in columns])


# Explicit, all-string schemas for the declared tables. Two reasons this
# beats inferSchema=True at scale:
#   1. inferSchema forces Spark to read the entire file once just to guess
#      types, then read it again to actually load it — double the I/O.
#   2. If we let Spark guess "amount" is a double, a value that fails to
#      parse becomes a silent null with no record that it ever failed to
#      parse — the same "was it null in the source, or did it fail to
#      convert" ambiguity the pandas pipeline avoids by reading raw and
#      coercing explicitly. Reading everything as string and casting in
#      clean.py, with QC comparing null counts before/after, preserves that.
SCHEMAS = {name: _all_string_schema(cols) for name, cols in REQUIRED_COLUMNS.items()}

# For an undeclared table: how many rows to sample to the driver to decide
# column types via pipeline.introspect (the exact same pandas-based inference
# the small-data pipeline uses). Scanning every value of every column across
# the full table just to guess its *type* doesn't scale — this bounds that
# cost to one fixed-size sample per table, regardless of table size.
SCHEMA_INFERENCE_SAMPLE_SIZE = 10_000

# order_date is used to lay out the final table's Parquet output
# (partitionBy year/month) and as the watermark column for incremental runs
# (pipeline_spark/incremental.py).
BATCH_WATERMARK_COLUMN = "order_date"
