"""Optional batch/incremental mode: process only orders newer than the last
run, instead of reprocessing the whole table every time.

This is the standard pattern for a genuinely large, continuously-arriving
dataset — rerunning Stages 1-6 over the entire multi-year history every night
doesn't scale even once the cluster does; you want just the new slice,
appended to what's already there.

Two things make this correct rather than merely fast:

1. **Idempotent appends.** `order_date` is a day, not a timestamp, so a
   `>= watermark` filter alone can't distinguish "new since last run" from
   "already ran the day this row belongs to" when a day's orders span two
   runs. Before appending, the batch is anti-joined against the order_ids
   already sitting in the Parquet output, so re-running the same batch twice
   — or a batch's window overlapping the previous one — never double-counts.
   This also makes it safe to run *after* a full pipeline.run() over the same
   data/: everything already written just gets filtered back out.
2. **Same pipeline, smaller input.** This reuses filter/clean/transform
   unchanged. An incremental run isn't a different pipeline; it's the same
   pipeline given a smaller slice of `orders` — the only table large enough
   at real scale to need slicing in the first place. customers/promotions are
   dimension-sized regardless of how big orders gets, so they're re-read in
   full each batch rather than sliced.

What's deliberately NOT handled here: the returned revenue summary describes
*this batch only*, not a running total across all batches. Turning that into
a cumulative figure needs a separate aggregation pass over the accumulated
Parquet output — a small, cheap query against already-reduced data, and a
natural next stage, just not one implemented here.
"""

import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from pipeline_spark import clean, config, filter as filter_stage, ingest, transform


def _read_watermark(path: Path) -> str | None:
    if not path.exists():
        return None
    return json.loads(path.read_text())["watermark"]


def _write_watermark(path: Path, watermark: str) -> None:
    path.write_text(json.dumps({"watermark": watermark}))


def run_batch(
    spark: SparkSession,
    data_dir: Path = config.DATA_DIR,
    output_dir: Path = config.OUTPUT_DIR,
    watermark_path: Path | None = None,
) -> dict:
    # Defensive, and worth explaining: Spark can reuse cached blocks across
    # DIFFERENT DataFrames within the same session when their query plans are
    # structurally identical (same path, same filter literals) — which two
    # separate run_batch calls produce exactly when the watermark hasn't moved
    # between them. Without clearing it, a later call can silently be served
    # a previous call's cached (now-stale) result instead of rescanning the
    # source, which is the opposite of what a fresh batch needs. In production
    # each batch is normally its own spark-submit job, so this never comes up
    # — the JVM restarts every time. It matters here because pytest (and any
    # long-running orchestrator that loops calling run_batch on one SparkSession
    # instead of relaunching Spark per batch) hits exactly that shared-session case.
    spark.catalog.clearCache()

    watermark_path = watermark_path or (output_dir / "_watermark.json")
    watermark = _read_watermark(watermark_path)

    raw = ingest.load_raw(spark, data_dir)
    orders = raw["orders"]
    if watermark is not None:
        orders = orders.filter(F.to_date("order_date") >= F.to_date(F.lit(watermark)))
    orders = orders.cache()

    source_slice_count = orders.count()
    if source_slice_count == 0:
        return {"status": "no_new_rows", "watermark": watermark, "rows_in_source_slice": 0}

    # promotions/customers are dimension-sized regardless of how big orders
    # gets -- re-reading them whole each batch is the right trade-off here.
    promotions = raw["promotions"].join(orders.select("order_id"), "order_id", "inner")
    customers = raw["customers"]

    filtered, filter_log = filter_stage.apply_filters({"customers": customers, "orders": orders, "promotions": promotions})
    cleaned = clean.clean_all(filtered)
    cleaned["orders"].cache()
    rows_after_filter_clean = cleaned["orders"].count()

    analysis_table = transform.build_analysis_table(cleaned["customers"], cleaned["orders"], cleaned["promotions"])
    analysis_table = analysis_table.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))

    parquet_path = output_dir / "analysis_table"
    if parquet_path.exists():
        already_written = spark.read.parquet(str(parquet_path)).select("order_id").distinct()
        analysis_table = analysis_table.join(already_written, "order_id", "left_anti")

    analysis_table = analysis_table.cache()
    rows_newly_written = analysis_table.count()

    # Every aggregate we still need from analysis_table has to be pulled
    # BEFORE the write below. analysis_table's lineage reads `already_written`
    # from parquet_path; writing to that same path invalidates the cache for
    # anything derived from it, so an action taken *after* the write silently
    # recomputes against the now-updated target — re-running the left_anti
    # join post-write would wipe out the very rows it's trying to summarize.
    batch_revenue = transform.compute_total_revenue(analysis_table) if rows_newly_written else None
    new_watermark = cleaned["orders"].agg(F.max("order_date")).first()[0]

    if rows_newly_written:
        output_dir.mkdir(exist_ok=True)
        analysis_table.write.mode("append").partitionBy("order_month").parquet(str(parquet_path))

    if new_watermark:
        output_dir.mkdir(exist_ok=True)
        _write_watermark(watermark_path, str(new_watermark))

    return {
        "status": "ok",
        "previous_watermark": watermark,
        "new_watermark": str(new_watermark) if new_watermark else watermark,
        "rows_in_source_slice": source_slice_count,
        "rows_after_filter_clean": rows_after_filter_clean,
        "rows_newly_written": rows_newly_written,
        "rows_already_present": rows_after_filter_clean - rows_newly_written,
        "batch_revenue_summary": batch_revenue,
        "filter_log": filter_log,
    }
