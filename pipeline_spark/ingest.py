"""Stage 1 — Raw ingestion (Spark).

Same contract as pipeline.ingest.load_raw: every source in data_dir becomes a
table keyed by its stem, read as-is. Two things differ because the data might
not fit on one machine:

- a declared table (config.SCHEMAS) is read with an explicit schema, so Spark
  skips inferSchema's extra full pass over the file just to guess types
- a source can be a *directory* of many part-files (data/orders/part-0001.csv,
  part-0002.csv, ...) instead of a single file — that's how data actually
  lands at scale, arriving in batches rather than as one giant file written in
  one shot. spark.read.csv reads a whole directory as one logical table with
  no code change; a single CSV file still works exactly as before.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from pipeline_spark import config


class SchemaError(Exception):
    pass


def discover_raw_sources(data_dir: Path = config.DATA_DIR) -> dict[str, Path]:
    """A *.csv file becomes a table named after its stem; a directory
    containing *.csv part-files becomes a table named after the directory."""
    sources = {}
    for p in sorted(Path(data_dir).iterdir()):
        if p.is_file() and p.suffix == ".csv":
            sources[p.stem] = p
        elif p.is_dir() and any(p.glob("*.csv")):
            sources[p.name] = p
    return sources


def load_raw(spark: SparkSession, data_dir: Path = config.DATA_DIR) -> dict[str, DataFrame]:
    sources = discover_raw_sources(data_dir)
    if not sources:
        raise FileNotFoundError(f"no CSV files (or part-file directories) found in {data_dir}")

    raw = {}
    for name, path in sources.items():
        reader = spark.read.option("header", True)
        schema = config.SCHEMAS.get(name)
        df = reader.schema(schema).csv(str(path)) if schema is not None else reader.csv(str(path))

        if name in config.REQUIRED_COLUMNS:
            missing = set(config.REQUIRED_COLUMNS[name]) - set(df.columns)
            if missing:
                raise SchemaError(f"{name}: missing required column(s) {sorted(missing)}")

        raw[name] = df

    return raw
