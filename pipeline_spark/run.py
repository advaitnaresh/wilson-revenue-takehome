"""Orchestrator (Spark) — runs every stage in order and writes the final
artifacts. Same seven stages, same contract as pipeline.run.run():

    1. ingest     every CSV (or directory of part-files) in data/
    2. qc (raw)   profile the raw data; nothing is fixed yet
    3. filter     drop only the unambiguous, unrecoverable rows
    4. clean      fix types, collapse promotions to order-grain
    5. transform  broadcast-join + derive the four requested metrics (only if
                  customers/orders/promotions are all present)
    6. qc (final) validate row counts, joins, and checksums on the pipeline's
                  own output
    7. results    write partitioned Parquet for the (potentially huge)
                  analysis table, small JSON/CSV for the aggregated metrics

Run with: python -m pipeline_spark.run
"""

import json
import sys

from pyspark.sql import functions as F

from pipeline_spark import clean, config, filter as filter_stage, ingest, qc, session, transform


def _record_counts(ledger: list[dict], stage: str, tables: dict) -> None:
    for name, df in tables.items():
        ledger.append({"stage": stage, "table": name, "count": df.count()})


def run(spark, data_dir=config.DATA_DIR) -> dict:
    ledger: list[dict] = []

    # 1. Raw ingestion
    raw = ingest.load_raw(spark, data_dir)
    for df in raw.values():
        df.cache()
    _record_counts(ledger, "1_raw_ingestion", raw)

    # A bounded sample per undeclared table — see qc.py's module docstring for why.
    samples = {
        name: df.limit(config.SCHEMA_INFERENCE_SAMPLE_SIZE).toPandas()
        for name, df in raw.items()
        if name not in config.KNOWN_TABLES
    }

    # 2. First QC — profile raw data, fix nothing
    initial_qc = qc.run_initial_qc(raw, samples)

    # 3. Filtering
    filtered, filter_log = filter_stage.apply_filters(raw)
    _record_counts(ledger, "3_filtering", filtered)

    # 4. Cleaning
    cleaned = clean.clean_all(filtered, samples)
    for df in cleaned.values():
        df.cache()
    _record_counts(ledger, "4_cleaning", cleaned)

    result = {
        "tables": cleaned,
        "row_count_ledger": ledger,
        "filter_log": filter_log,
        "initial_qc": initial_qc,
        "revenue_summary": None,
        "by_segment": None,
        "top5": None,
        "final_qc": None,
        "analysis_table": None,
    }

    if not config.REVENUE_TABLES.issubset(cleaned):
        missing = config.REVENUE_TABLES - set(cleaned)
        initial_qc.add(
            "initial_qc", "pipeline", "revenue_tables_missing", "info", len(missing),
            f"skipping the revenue transform — data/ is missing {sorted(missing)}",
        )
        _write_outputs(result)
        return result

    # 5. Transformations
    analysis_table = transform.build_analysis_table(cleaned["customers"], cleaned["orders"], cleaned["promotions"]).cache()
    orders_in = cleaned["orders"].count()
    _record_counts(ledger, "5_transform", {"orders": analysis_table})

    all_segments = sorted({r["segment"] for r in raw["customers"].select("segment").distinct().collect() if r["segment"]} | {"Unknown"})
    revenue_summary = transform.compute_total_revenue(analysis_table)
    by_segment = transform.compute_revenue_by_segment(analysis_table, all_segments)
    top5 = transform.compute_top_customers(analysis_table, n=5)

    # 6. Second QC
    final_qc = qc.run_final_qc(
        analysis_table,
        orders_in=orders_in,
        row_ledger=ledger,
        segment_revenue_total=float(by_segment.revenue.sum()),
        reported_total_revenue=revenue_summary["total_revenue"],
    )
    capped = analysis_table.filter(F.col("discount_raw") > F.col("amount")).count()
    if capped:
        reduction = analysis_table.select(F.sum(F.col("discount_raw") - F.col("discount")).alias("r")).first()["r"]
        final_qc.add(
            "final_qc", "analysis_table", "discounts_capped", "info", capped,
            f"{capped} order(s) had discount capped at the order amount (reduced total discount by {reduction:,.2f})",
        )
    final_qc.raise_if_failed()

    result.update(
        revenue_summary=revenue_summary,
        by_segment=by_segment,
        top5=top5,
        final_qc=final_qc,
        analysis_table=analysis_table,
    )
    _write_outputs(result)
    return result


def _write_outputs(result: dict) -> None:
    config.OUTPUT_DIR.mkdir(exist_ok=True)

    if result["analysis_table"] is not None:
        # Potentially huge — written as partitioned Parquet, never collected to
        # the driver. Partitioning by year-month gives downstream readers
        # partition pruning on the most common query pattern (a date range)
        # without the excessive small-file count that partitioning by exact
        # date would produce.
        (
            result["analysis_table"]
            .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
            .write.mode("overwrite")
            .partitionBy("order_month")
            .parquet(str(config.OUTPUT_DIR / "analysis_table"))
        )
        with open(config.OUTPUT_DIR / "revenue_summary.json", "w") as f:
            json.dump(result["revenue_summary"], f, indent=2)
        result["by_segment"].to_csv(config.OUTPUT_DIR / "revenue_by_segment.csv")
        result["top5"].to_csv(config.OUTPUT_DIR / "top5_customers.csv", index_label="rank")

    final_qc = result["final_qc"]
    qc_report = {
        "row_count_ledger": result["row_count_ledger"],
        "filter_log": result["filter_log"],
        "initial_qc_issues": result["initial_qc"].to_records(),
        "final_qc_issues": final_qc.to_records() if final_qc else [],
    }
    with open(config.OUTPUT_DIR / "qc_report.json", "w") as f:
        json.dump(qc_report, f, indent=2, default=str)


def _print_summary(result: dict) -> None:
    print("=== Discovered tables ===")
    print(f"  {', '.join(sorted(result['tables']))}")

    print("\n=== Row count ledger ===")
    for row in result["row_count_ledger"]:
        print(f"  {row['stage']:<16} {row['table']:<12} {row['count']:>10,}")

    print("\n=== Filter log ===")
    for row in result["filter_log"]:
        print(f"  {row['table']:<12} {row['rule']:<32} {row['rows_before']:>9,} -> {row['rows_after']:>9,}  (-{row['rows_dropped']})")

    print(f"\n=== Initial QC: {len(result['initial_qc'].issues)} issue(s) "
          f"({len(result['initial_qc'].failures())} fail, "
          f"{len(result['initial_qc'].by_severity('warn'))} warn, "
          f"{len(result['initial_qc'].by_severity('info'))} info) ===")
    for issue in result["initial_qc"].issues:
        print(f"  [{issue.severity:<4}] {issue.table:<12} {issue.check:<40} {issue.detail}")

    if result["final_qc"] is None:
        print("\n=== Final QC: skipped (revenue tables not all present) ===")
        return

    print(f"\n=== Final QC: {len(result['final_qc'].issues)} issue(s) "
          f"({len(result['final_qc'].failures())} fail) ===")
    for issue in result["final_qc"].issues:
        print(f"  [{issue.severity:<4}] {issue.table:<16} {issue.check:<24} {issue.detail}")

    rs = result["revenue_summary"]
    print("\n=== Results ===")
    print(f"1. Total revenue:  {rs['total_revenue']:>18,.2f}  ({rs['completed_orders']:,} completed orders)")
    print(f"4. Net revenue:    {rs['net_revenue']:>18,.2f}  (discounts {-rs['total_discount']:,.2f})")
    print("\n2. Revenue by segment:")
    print(result["by_segment"].to_string())
    print("\n3. Top 5 customers:")
    print(result["top5"].to_string())
    print(f"\nArtifacts written to {config.OUTPUT_DIR}/")


if __name__ == "__main__":
    spark = session.get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        try:
            result = run(spark)
        except AssertionError as e:
            print(f"PIPELINE FAILED QC: {e}", file=sys.stderr)
            sys.exit(1)
        _print_summary(result)
    finally:
        spark.stop()
