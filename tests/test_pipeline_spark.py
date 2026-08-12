"""Tests for pipeline_spark/. Mirrors tests/test_pipeline.py's approach —
small, hand-built tables with injected errors, not the real CSVs — so each
check is proven to catch a *class* of problem rather than just today's rows.
What's specific to this suite: every table is built as an all-string Spark
DataFrame (StructType of StringType columns), matching exactly what
pipeline_spark.ingest actually produces from a raw CSV — so a numeric/date
"doesn't parse" check here exercises the same cast-and-compare path
production data goes through, not a shortcut using Python's native types.
"""

import pandas as pd
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline_spark import clean, config, filter as filter_stage, incremental, qc, run, session, transform


@pytest.fixture(scope="module")
def spark():
    s = session.get_spark_session("pytest-pipeline-spark")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _string_df(spark, rows, columns):
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.createDataFrame(rows, schema)


def make_customers(spark):
    return _string_df(
        spark,
        [("1", "Alice Co", "Enterprise", "West"), ("2", "Bob LLC", "SMB", "East"), ("3", "Carol Inc", "SMB", "East")],
        ["customer_id", "customer_name", "segment", "region"],
    )


def make_dirty_orders(spark):
    return _string_df(
        spark,
        [
            ("o1", "1", "2025-01-01", "100.0", "completed"),
            ("o1", "1", "2025-01-01", "100.0", "completed"),  # exact duplicate of o1
            ("o2", "2", "2025-01-02", "50.0", "completed"),
            ("o3", "3", "not-a-date", "75.0", "completed"),  # bad date
            ("o4", "999", "2025-01-03", "30.0", "completed"),  # unknown customer
            ("o5", "1", "2025-01-04", "-20.0", "completed"),  # negative amount
            ("o6", "2", "2025-01-05", "0.0", "completed"),  # zero amount
        ],
        ["order_id", "customer_id", "order_date", "amount", "status"],
    )


def make_dirty_promotions(spark):
    return _string_df(
        spark,
        [("p1", "o2", "SAVE10", "10.0"), ("p2", "o2", "SAVE50", "60.0")],  # stacked; 70 > o2's amount of 50
        ["promotion_id", "order_id", "promo_code", "discount_amount"],
    )


class TestInitialQC:
    def test_catches_exact_duplicate_rows(self, spark):
        report = qc.run_initial_qc({"customers": make_customers(spark), "orders": make_dirty_orders(spark), "promotions": make_dirty_promotions(spark)})
        hit = [i for i in report.issues if i.check == "duplicate_rows" and i.table == "orders"]
        assert hit and hit[0].count == 1

    def test_catches_unparseable_date(self, spark):
        report = qc.run_initial_qc({"customers": make_customers(spark), "orders": make_dirty_orders(spark), "promotions": make_dirty_promotions(spark)})
        hit = [i for i in report.issues if i.check == "unparseable:order_date"]
        assert hit and hit[0].count == 1
        assert hit[0].severity == "fail"

    def test_catches_negative_and_zero_amount(self, spark):
        report = qc.run_initial_qc({"customers": make_customers(spark), "orders": make_dirty_orders(spark), "promotions": make_dirty_promotions(spark)})
        negative = [i for i in report.issues if i.check == "negative:amount"]
        zero = [i for i in report.issues if i.check == "zero:amount"]
        assert negative and negative[0].count == 1
        assert zero and zero[0].count == 1

    def test_catches_orphan_foreign_key(self, spark):
        report = qc.run_initial_qc({"customers": make_customers(spark), "orders": make_dirty_orders(spark), "promotions": make_dirty_promotions(spark)})
        hit = [i for i in report.issues if i.check.startswith("orphan_fk")]
        assert hit and hit[0].count == 1  # the o4 / customer_id "999" row

    def test_catches_discount_exceeding_amount(self, spark):
        report = qc.run_initial_qc({"customers": make_customers(spark), "orders": make_dirty_orders(spark), "promotions": make_dirty_promotions(spark)})
        hit = [i for i in report.issues if i.check == "discount_exceeds_amount"]
        assert hit and hit[0].count == 1  # order o2: discount 70 > amount 50

    def test_catches_unexpected_status(self, spark):
        orders = make_dirty_orders(spark).withColumn(
            "status", F.when(F.col("order_id") == "o1", "chargeback").otherwise(F.col("status"))
        )
        report = qc.run_initial_qc({"customers": make_customers(spark), "orders": orders, "promotions": make_dirty_promotions(spark)})
        hit = [i for i in report.issues if i.check == "unexpected_status"]
        assert hit and "chargeback" in hit[0].detail


class TestFilterStage:
    def test_drops_exact_duplicates_and_bad_rows_only(self, spark):
        filtered, log = filter_stage.apply_filters(
            {"customers": make_customers(spark), "orders": make_dirty_orders(spark), "promotions": make_dirty_promotions(spark)}
        )
        orders = filtered["orders"]
        order_ids = {r.order_id for r in orders.select("order_id").collect()}

        assert orders.filter(F.col("order_id") == "o1").count() == 1  # duplicate collapsed, o1 itself survives
        assert "o3" not in order_ids  # bad date
        assert "o5" not in order_ids  # negative amount
        assert "o6" not in order_ids  # zero amount
        assert "o4" in order_ids  # unknown customer -- filtering isn't the place to drop it

        dropped = {row["rule"]: row["rows_dropped"] for row in log if row["table"] == "orders"}
        assert dropped["exact_duplicate_rows"] == 1
        assert dropped["unparseable_amount_or_date"] == 1
        assert dropped["non_positive_amount"] == 2


class TestCleanAndTransform:
    def test_promotions_collapse_to_order_grain(self, spark):
        cleaned = clean.clean_promotions(make_dirty_promotions(spark))
        assert cleaned.select("order_id").distinct().count() == cleaned.count()
        row = cleaned.filter(F.col("order_id") == "o2").first()
        assert row["discount_raw"] == 70.0

    def test_join_does_not_fan_out_and_caps_discount(self, spark):
        filtered, _ = filter_stage.apply_filters(
            {"customers": make_customers(spark), "orders": make_dirty_orders(spark), "promotions": make_dirty_promotions(spark)}
        )
        cleaned = clean.clean_all(filtered)
        analysis = transform.build_analysis_table(cleaned["customers"], cleaned["orders"], cleaned["promotions"])

        assert analysis.count() == cleaned["orders"].count()
        assert analysis.select("order_id").distinct().count() == analysis.count()

        o2 = analysis.filter(F.col("order_id") == "o2").first()
        assert o2["discount_raw"] == 70.0
        assert o2["discount"] == 50.0  # capped at the order's own amount

        o4 = analysis.filter(F.col("order_id") == "o4").first()
        assert o4["segment"] == "Unknown"
        assert o4["is_known_customer"] is False

    def test_final_qc_fails_when_a_fanout_slips_through(self, spark):
        # Simulate the exact bug the pipeline is designed to prevent: joining
        # against un-aggregated promotions, which fans out order o2 into two rows.
        filtered, _ = filter_stage.apply_filters(
            {"customers": make_customers(spark), "orders": make_dirty_orders(spark), "promotions": make_dirty_promotions(spark)}
        )
        orders = clean.clean_orders(filtered["orders"])
        naive = orders.join(make_dirty_promotions(spark), "order_id", "left").withColumn(
            "discount", F.col("discount_amount").cast("double")
        ).withColumn("amount", F.col("amount").cast("double"))

        report = qc.run_final_qc(naive, orders_in=orders.count(), row_ledger=[], segment_revenue_total=0.0, reported_total_revenue=0.0)
        with pytest.raises(AssertionError):
            report.raise_if_failed()


class TestGenericTableHandling:
    """An undeclared table (nothing in pipeline_spark.config) still gets
    profiled/filtered/cleaned via sample-based inference — same story as
    tests/test_generalization.py, but exercised through the Spark path."""

    def make_support_tickets(self, spark):
        return _string_df(
            spark,
            [
                ("t1", "1", "2025-01-05", "high"),
                ("t1", "1", "2025-01-05", "high"),  # exact duplicate
                ("t2", "2", "2025-01-06", "low"),
                ("t3", "999", "2025-01-07", "low"),  # orphan customer_id
            ],
            ["ticket_id", "customer_id", "opened_at", "priority"],
        )

    def test_infers_foreign_key_and_flags_orphan(self, spark):
        tables = {"customers": make_customers(spark), "support_tickets": self.make_support_tickets(spark)}
        samples = {"support_tickets": tables["support_tickets"].toPandas()}
        report = qc.run_initial_qc(tables, samples)

        dup = [i for i in report.issues if i.table == "support_tickets" and i.check == "duplicate_rows"]
        assert dup and dup[0].count == 1

        orphan = [i for i in report.issues if i.table == "support_tickets" and i.check.startswith("orphan_fk")]
        assert orphan and orphan[0].count == 1 and orphan[0].severity == "info"

    def test_generic_filter_and_clean(self, spark):
        tickets = self.make_support_tickets(spark)
        filtered, log = filter_stage.apply_filters({"support_tickets": tickets})
        assert filtered["support_tickets"].count() == 3
        assert {row["rule"]: row["rows_dropped"] for row in log}["exact_duplicate_rows"] == 1

        sample = filtered["support_tickets"].toPandas()
        cleaned = clean.clean_generic(filtered["support_tickets"], sample)
        assert dict(cleaned.dtypes)["opened_at"] in ("timestamp", "date")


class TestEndToEnd:
    def test_matches_pandas_pipeline_on_real_data(self, spark, tmp_path):
        result = run.run(spark, config.DATA_DIR)
        assert result["revenue_summary"]["total_revenue"] == pytest.approx(16_153_512.87, abs=0.01)
        assert result["revenue_summary"]["net_revenue"] == pytest.approx(15_879_675.40, abs=0.01)
        assert result["revenue_summary"]["completed_orders"] == 25_575
        assert len(result["final_qc"].failures()) == 0

    def test_skips_transform_when_a_revenue_table_is_missing(self, spark, tmp_path):
        pd.read_csv(config.DATA_DIR / "customers.csv").to_csv(tmp_path / "customers.csv", index=False)
        pd.read_csv(config.DATA_DIR / "orders.csv").to_csv(tmp_path / "orders.csv", index=False)
        # promotions.csv intentionally omitted
        result = run.run(spark, tmp_path)
        assert result["revenue_summary"] is None
        assert result["final_qc"] is None
        skip_note = [i for i in result["initial_qc"].issues if i.check == "revenue_tables_missing"]
        assert skip_note and "promotions" in skip_note[0].detail


class TestIncremental:
    def test_backfill_then_idempotent_rerun_then_new_batch(self, spark, tmp_path):
        data_dir = tmp_path / "data"
        out_dir = tmp_path / "out"
        (data_dir / "orders").mkdir(parents=True)
        pd.DataFrame(
            {"customer_id": ["1", "2"], "customer_name": ["Alice Co", "Bob LLC"], "segment": ["Enterprise", "SMB"], "region": ["West", "East"]}
        ).to_csv(data_dir / "customers.csv", index=False)
        pd.DataFrame({"promotion_id": [], "order_id": [], "promo_code": [], "discount_amount": []}).to_csv(data_dir / "promotions.csv", index=False)
        pd.DataFrame(
            {"order_id": ["o1", "o2"], "customer_id": ["1", "2"], "order_date": ["2025-01-01", "2025-01-02"], "amount": [100.0, 50.0], "status": ["completed", "completed"]}
        ).to_csv(data_dir / "orders" / "part-0000.csv", index=False)

        backfill = incremental.run_batch(spark, data_dir=data_dir, output_dir=out_dir)
        assert backfill["rows_newly_written"] == 2
        assert backfill["batch_revenue_summary"]["total_revenue"] == 150.0

        rerun = incremental.run_batch(spark, data_dir=data_dir, output_dir=out_dir)
        assert rerun["rows_newly_written"] == 0
        # Only o2 gets rescanned: the watermark is o2's date (2025-01-02), and
        # the source-slice filter is `order_date >= watermark`, so o1 (dated
        # 2025-01-01, strictly before it) is never revisited again — that's
        # the point of a watermark. The anti-join then no-ops on o2 too.
        assert rerun["rows_already_present"] == 1

        pd.DataFrame(
            {"order_id": ["o3"], "customer_id": ["1"], "order_date": ["2025-01-10"], "amount": [75.0], "status": ["completed"]}
        ).to_csv(data_dir / "orders" / "part-0001.csv", index=False)

        new_batch = incremental.run_batch(spark, data_dir=data_dir, output_dir=out_dir)
        assert new_batch["rows_newly_written"] == 1
        assert new_batch["batch_revenue_summary"]["total_revenue"] == 75.0

        total = spark.read.parquet(str(out_dir / "analysis_table")).select("order_id").distinct().count()
        assert total == 3
