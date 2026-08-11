"""Orchestrator — runs every stage in order and writes the final artifacts.

    1. ingest     every CSV discovered in data/, as-is
    2. qc (raw)   profile the raw data; nothing is fixed yet
    3. filter     drop only the unambiguous, unrecoverable rows
    4. clean      fix types, collapse promotions to order-grain
    5. transform  join + derive the four requested metrics (only if
                  customers/orders/promotions are all present — see
                  config.REVENUE_TABLES)
    6. qc (final) validate row counts, joins, and checksums on the pipeline's own output
    7. results    write the final artifacts to pipeline_output/

Any CSV in data/ beyond the known schema still flows through stages 1-4 using
the generic rules in introspect.py — it's ingested, profiled, filtered, and
cleaned — it just doesn't feed stage 5, since the revenue calculation is
specific to this business schema.

Run with: python -m pipeline.run
"""

import json
import sys
from pathlib import Path

import pandas as pd

from . import clean, config, filter as filter_stage, ingest, qc, transform


def _record_counts(ledger: list[dict], stage: str, tables: dict[str, pd.DataFrame]) -> None:
    for name, df in tables.items():
        ledger.append({"stage": stage, "table": name, "count": len(df)})


def run(data_dir: Path = config.DATA_DIR) -> dict:
    ledger: list[dict] = []

    # 1. Raw ingestion — every CSV in data_dir, known schema or not
    raw = ingest.load_raw(data_dir)
    _record_counts(ledger, "1_raw_ingestion", raw)

    # 2. First QC — profile raw data, fix nothing
    initial_qc = qc.run_initial_qc(raw)

    # 3. Filtering — drop only unambiguous, unrecoverable rows
    filtered, filter_log = filter_stage.apply_filters(raw)
    _record_counts(ledger, "3_filtering", filtered)

    # 4. Cleaning — types, shape, promo aggregation to order-grain
    cleaned = clean.clean_all(filtered)
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
    }

    if not config.REVENUE_TABLES.issubset(cleaned):
        missing = config.REVENUE_TABLES - set(cleaned)
        initial_qc.add(
            "initial_qc", "pipeline", "revenue_tables_missing", "info", len(missing),
            f"skipping the revenue transform — data/ is missing {sorted(missing)}",
        )
        _write_outputs(result, {})
        return result

    # 5. Transformations — join + derive metrics
    analysis_table = transform.build_analysis_table(cleaned["customers"], cleaned["orders"], cleaned["promotions"])
    _record_counts(ledger, "5_transform", {"orders": analysis_table})

    all_segments = sorted(set(raw["customers"].segment.dropna().unique()) | {"Unknown"})
    revenue_summary = transform.compute_total_revenue(analysis_table)
    by_segment = transform.compute_revenue_by_segment(analysis_table, all_segments)
    top5 = transform.compute_top_customers(analysis_table, n=5)

    # 6. Second QC — validate the pipeline's own arithmetic before trusting it
    final_qc = qc.run_final_qc(
        analysis_table,
        orders_in=len(cleaned["orders"]),
        row_ledger=ledger,
        segment_revenue_total=float(by_segment.revenue.sum()),
        reported_total_revenue=revenue_summary["total_revenue"],
    )
    capped = int((analysis_table.discount_raw > analysis_table.amount).sum())
    if capped:
        final_qc.add(
            "final_qc", "analysis_table", "discounts_capped", "info", capped,
            f"{capped} order(s) had discount capped at the order amount "
            f"(reduced total discount by {(analysis_table.discount_raw - analysis_table.discount).sum():,.2f})",
        )

    final_qc.raise_if_failed()

    result.update(
        revenue_summary=revenue_summary,
        by_segment=by_segment,
        top5=top5,
        final_qc=final_qc,
    )
    _write_outputs(result, {"revenue_summary": revenue_summary, "by_segment": by_segment, "top5": top5})
    return result


def _write_outputs(result: dict, artifacts: dict) -> None:
    config.OUTPUT_DIR.mkdir(exist_ok=True)

    if artifacts:
        with open(config.OUTPUT_DIR / "revenue_summary.json", "w") as f:
            json.dump(artifacts["revenue_summary"], f, indent=2)
        artifacts["by_segment"].to_csv(config.OUTPUT_DIR / "revenue_by_segment.csv")
        artifacts["top5"].to_csv(config.OUTPUT_DIR / "top5_customers.csv", index_label="rank")

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
        print(f"  {row['stage']:<16} {row['table']:<12} {row['count']:>8,}")

    print("\n=== Filter log ===")
    for row in result["filter_log"]:
        print(f"  {row['table']:<12} {row['rule']:<32} {row['rows_before']:>7,} -> {row['rows_after']:>7,}  (-{row['rows_dropped']})")

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
    print(f"\n=== Results ===")
    print(f"1. Total revenue:  {rs['total_revenue']:>18,.2f}  ({rs['completed_orders']:,} completed orders)")
    print(f"4. Net revenue:    {rs['net_revenue']:>18,.2f}  (discounts {-rs['total_discount']:,.2f})")
    print("\n2. Revenue by segment:")
    print(result["by_segment"].to_string())
    print("\n3. Top 5 customers:")
    print(result["top5"].to_string())
    print(f"\nArtifacts written to {config.OUTPUT_DIR}/")


if __name__ == "__main__":
    try:
        result = run()
    except AssertionError as e:
        print(f"PIPELINE FAILED QC: {e}", file=sys.stderr)
        sys.exit(1)
    _print_summary(result)
