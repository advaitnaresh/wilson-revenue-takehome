"""Stage 5 — Transformations.

Joins the cleaned tables into one order-grain analysis table, then derives the
four requested metrics from it. Two judgment calls live here rather than in
clean.py, because they need the joined view to make sense:

- an order whose customer_id has no match in customers.csv is kept (the
  transaction is real) but labeled segment="Unknown" instead of dropped —
  deleting it would understate revenue for a reason unrelated to the order
- a discount larger than its order's amount is capped at the order amount,
  flooring net revenue at zero rather than letting it go negative
"""

import pandas as pd

from . import config


def build_analysis_table(customers: pd.DataFrame, orders: pd.DataFrame, promotions: pd.DataFrame) -> pd.DataFrame:
    df = (
        orders.merge(promotions, on="order_id", how="left", validate="one_to_one")
        .merge(customers, on="customer_id", how="left", validate="many_to_one")
    )
    assert len(df) == len(orders), "join changed the row count — a table was not order-grain going in"

    df["discount_raw"] = df.discount_raw.fillna(0.0)
    df["discount"] = df[["discount_raw", "amount"]].min(axis=1)
    df["is_known_customer"] = df.customer_name.notna()
    df["segment"] = df.segment.fillna("Unknown")

    return df


def compute_total_revenue(df: pd.DataFrame) -> dict:
    completed = df[df.status.isin(config.REVENUE_STATUSES)]
    total_revenue = float(completed.amount.sum())
    total_discount = float(completed.discount.sum())
    return {
        "total_revenue": total_revenue,
        "total_discount": total_discount,
        "net_revenue": total_revenue - total_discount,
        "completed_orders": int(len(completed)),
        "by_status": {
            status: {"orders": int(n), "amount": float(v)}
            for status, (n, v) in df.groupby("status").amount.agg(["size", "sum"]).iterrows()
        },
    }


def compute_revenue_by_segment(df: pd.DataFrame, all_segments: list[str]) -> pd.DataFrame:
    completed = df[df.status.isin(config.REVENUE_STATUSES)]
    by_segment = (
        completed.groupby("segment")
        .agg(orders=("order_id", "size"), revenue=("amount", "sum"), discounts=("discount", "sum"))
        .reindex(all_segments)
        .fillna(0)
        .assign(
            net_revenue=lambda d: d.revenue - d.discounts,
            pct_of_revenue=lambda d: d.revenue / d.revenue.sum() * 100,
            avg_order_value=lambda d: (d.revenue / d.orders).fillna(0),
        )
        .astype({"orders": int})
        .sort_values("revenue", ascending=False)
    )
    return by_segment


def compute_top_customers(df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    completed = df[df.status.isin(config.REVENUE_STATUSES) & df.is_known_customer]
    top = (
        completed.groupby(["customer_id", "customer_name", "segment", "region"])
        .agg(orders=("order_id", "size"), revenue=("amount", "sum"), discounts=("discount", "sum"))
        .assign(net_revenue=lambda d: d.revenue - d.discounts)
        .sort_values("revenue", ascending=False)
        .head(n)
        .reset_index()
    )
    top.index = range(1, len(top) + 1)
    return top
