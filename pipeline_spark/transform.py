"""Stage 5 — Transformations (Spark).

Same two judgment calls as pipeline.transform: an order from an unknown
customer is kept and labeled "Unknown" rather than dropped, and a discount
larger than its order is capped at the order amount. What's Spark-specific is
the join strategy and where the boundary to pandas is drawn:

- customers is the dimension table — orders of magnitude smaller than orders/
  promotions even at real scale — so it's broadcast: every executor gets a
  full local copy and the join becomes a map-side operation with no shuffle,
  instead of moving the (potentially huge) orders table across the cluster
  just to look up a segment.
- the *analysis table* (order-grain, potentially billions of rows) stays a
  Spark DataFrame throughout — it's never collected.
- the three *reported* metrics (total revenue, revenue-by-segment,
  top-5-customers) are, by construction, tiny — a handful of numbers, one row
  per segment, five rows. Once Spark's distributed aggregation has reduced the
  data down to that, finishing the shaping (reindexing to show an explicit
  zero segment, computing derived ratios) in pandas is both simpler and free —
  the expensive part already happened in the cluster.
"""

import pandas as pd
from pyspark.sql import DataFrame, functions as F

from pipeline_spark import config


def build_analysis_table(customers: DataFrame, orders: DataFrame, promotions: DataFrame) -> DataFrame:
    df = orders.join(promotions, "order_id", "left").join(F.broadcast(customers), "customer_id", "left")
    return (
        df.withColumn("discount_raw", F.coalesce(F.col("discount_raw"), F.lit(0.0)))
        .withColumn("discount", F.least(F.col("discount_raw"), F.col("amount")))
        .withColumn("is_known_customer", F.col("customer_name").isNotNull())
        .withColumn("segment", F.coalesce(F.col("segment"), F.lit("Unknown")))
    )


def compute_total_revenue(df: DataFrame) -> dict:
    completed = df.filter(F.col("status").isin(*config.REVENUE_STATUSES))
    totals = completed.agg(
        F.sum("amount").alias("total_revenue"),
        F.sum("discount").alias("total_discount"),
        F.count(F.lit(1)).alias("completed_orders"),
    ).first()

    by_status_rows = df.groupBy("status").agg(F.count(F.lit(1)).alias("orders"), F.sum("amount").alias("amount")).collect()

    total_revenue = float(totals["total_revenue"] or 0.0)
    total_discount = float(totals["total_discount"] or 0.0)
    return {
        "total_revenue": total_revenue,
        "total_discount": total_discount,
        "net_revenue": total_revenue - total_discount,
        "completed_orders": int(totals["completed_orders"]),
        "by_status": {row["status"]: {"orders": int(row["orders"]), "amount": float(row["amount"])} for row in by_status_rows},
    }


def compute_revenue_by_segment(df: DataFrame, all_segments: list[str]) -> pd.DataFrame:
    completed = df.filter(F.col("status").isin(*config.REVENUE_STATUSES))
    rows = (
        completed.groupBy("segment")
        .agg(F.count(F.lit(1)).alias("orders"), F.sum("amount").alias("revenue"), F.sum("discount").alias("discounts"))
        .collect()
    )
    pdf = pd.DataFrame([r.asDict() for r in rows]).set_index("segment") if rows else pd.DataFrame(columns=["orders", "revenue", "discounts"])
    pdf = pdf.reindex(all_segments).fillna(0)
    pdf["orders"] = pdf.orders.astype(int)
    pdf["net_revenue"] = pdf.revenue - pdf.discounts
    total = pdf.revenue.sum()
    pdf["pct_of_revenue"] = (pdf.revenue / total * 100) if total else 0.0
    pdf["avg_order_value"] = (pdf.revenue / pdf.orders).fillna(0)
    return pdf.sort_values("revenue", ascending=False)


def compute_top_customers(df: DataFrame, n: int = 5) -> pd.DataFrame:
    completed = df.filter(F.col("status").isin(*config.REVENUE_STATUSES) & F.col("is_known_customer"))
    rows = (
        completed.groupBy("customer_id", "customer_name", "segment", "region")
        .agg(F.count(F.lit(1)).alias("orders"), F.sum("amount").alias("revenue"), F.sum("discount").alias("discounts"))
        .orderBy(F.col("revenue").desc())
        .limit(n)
        .collect()
    )
    pdf = pd.DataFrame([r.asDict() for r in rows])
    if pdf.empty:
        return pdf
    pdf["net_revenue"] = pdf.revenue - pdf.discounts
    pdf.index = range(1, len(pdf) + 1)
    return pdf
