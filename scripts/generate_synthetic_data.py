"""Generate a synthetic multi-million-row dataset, entirely inside Spark —
never materialized on the driver — to exercise pipeline_spark at a scale
data/ (a few thousand rows) can't. This is what produced the scale numbers in
README_BIGDATA.md.

Usage (run from the repo root, so pipeline_spark is importable):
    python scripts/generate_synthetic_data.py /path/to/output/dir [n_orders] [n_customers]

Writes customers/, orders/, promotions/ as directories of CSV part-files
(the same shape ingest.py expects for a batch-arrival source), so the output
can be pointed at directly:

    python -c "from pipeline_spark import session, run; run.run(session.get_spark_session(), '/path/to/output/dir')"
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F

from pipeline_spark import session


def generate(out: Path, n_orders: int = 5_000_000, n_customers: int = 50_000) -> None:
    out.mkdir(parents=True, exist_ok=True)
    spark = session.get_spark_session("gen-synthetic-bigdata")
    spark.sparkContext.setLogLevel("ERROR")

    customers = (
        spark.range(n_customers)
        .withColumn("customer_id", F.concat(F.lit("C"), F.col("id").cast("string")))
        .withColumn("customer_name", F.concat(F.lit("Customer "), F.col("id").cast("string")))
        .withColumn("segment", F.when(F.col("id") % 10 < 4, "SMB").when(F.col("id") % 10 < 8, "Enterprise").otherwise("Mid-Market"))
        .withColumn("region", F.element_at(F.array(F.lit("Northeast"), F.lit("South"), F.lit("Midwest"), F.lit("West")), (F.col("id") % 4 + 1).cast("int")))
        .select("customer_id", "customer_name", "segment", "region")
    )
    customers.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out / "customers"))

    orders = (
        spark.range(n_orders)
        .withColumn("order_id", F.concat(F.lit("O"), F.col("id").cast("string")))
        # ~2% reference a customer_id outside the known range -- same shape orphan FK as the real dataset
        .withColumn("customer_ref", F.when(F.col("id") % 50 == 0, (F.col("id") % 1000) + n_customers).otherwise(F.col("id") % n_customers))
        .withColumn("customer_id", F.concat(F.lit("C"), F.col("customer_ref").cast("string")))
        .withColumn("order_date", F.date_add(F.lit("2024-01-01"), (F.col("id") % 700).cast("int")))
        .withColumn("amount", F.round(F.rand(seed=42) * 2000 + 10, 2))
        .withColumn("status", F.when(F.col("id") % 10 < 8, "completed").when(F.col("id") % 10 < 9, "refunded").otherwise("cancelled"))
        .select("order_id", "customer_id", "order_date", "amount", "status")
    )
    # 8 part-files, the way batches actually land -- not one giant file.
    orders.repartition(8).write.mode("overwrite").option("header", True).csv(str(out / "orders"))

    promotions = (
        spark.range(n_orders)
        .filter(F.col("id") % 8 == 0)  # ~12.5% of orders get a promo
        .withColumn("promotion_id", F.concat(F.lit("P"), F.col("id").cast("string")))
        .withColumn("order_id", F.concat(F.lit("O"), F.col("id").cast("string")))
        .withColumn("promo_code", F.element_at(F.array(F.lit("SAVE10"), F.lit("SAVE20"), F.lit("WELCOME")), (F.col("id") % 3 + 1).cast("int")))
        .withColumn("discount_amount", F.round(F.rand(seed=7) * 100, 2))
        .select("promotion_id", "order_id", "promo_code", "discount_amount")
    )
    promotions.coalesce(2).write.mode("overwrite").option("header", True).csv(str(out / "promotions"))

    print(f"generated {n_customers:,} customers, {n_orders:,} orders at {out}")
    spark.stop()


if __name__ == "__main__":
    out_dir = Path(sys.argv[1])
    n_orders = int(sys.argv[2]) if len(sys.argv) > 2 else 5_000_000
    n_customers = int(sys.argv[3]) if len(sys.argv) > 3 else 50_000
    generate(out_dir, n_orders, n_customers)
