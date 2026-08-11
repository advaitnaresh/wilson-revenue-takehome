"""Stage 4 — Cleaning.

Operates on tables that have already passed the filter stage. Fixes types and
shape so the transform stage can join without surprises:

- coerce amount/discount to numeric and order_date to datetime (filter already
  guaranteed these parse; this makes the dtype match the guarantee)
- collapse promotions to one row per order *before* any join, so a 1:many
  relationship can't silently fan out a later merge
- trim stray whitespace on text columns

Nothing here drops rows — row count in should equal row count out for every
table except promotions, which intentionally collapses to order-grain.

A table outside the known schema gets the generic treatment instead: trim
whitespace, coerce whatever introspect.py sniffed as numeric/date-like — no
business-specific reshaping, since that requires knowing what the table means.
"""

import pandas as pd

from . import introspect


def clean_customers(customers: pd.DataFrame) -> pd.DataFrame:
    df = customers.copy()
    for col in df.select_dtypes(include=["object", "str"]).columns:
        df[col] = df[col].str.strip()
    return df


def clean_orders(orders: pd.DataFrame) -> pd.DataFrame:
    df = orders.copy()
    df["amount"] = pd.to_numeric(df.amount)
    df["order_date"] = pd.to_datetime(df.order_date, format="%Y-%m-%d")
    df["status"] = df.status.str.strip()
    df["customer_id"] = df.customer_id.str.strip()
    assert df.order_id.is_unique, "order_id not unique after filter stage — dedup rule did not converge"
    return df


def clean_promotions(promotions: pd.DataFrame) -> pd.DataFrame:
    """Aggregate to one row per order_id: sum stacked discounts, keep every promo
    code for reference. Two distinct promotion_ids on one order are two
    concessions granted (stacking), not the same discount recorded twice —
    if they were duplicate records I'd expect the *same* code repeated, which
    is not what a stacked-promo order looks like.
    """
    df = promotions.copy()
    df["discount_amount"] = pd.to_numeric(df.discount_amount)
    df["promo_code"] = df.promo_code.str.strip()

    agg = (
        df.groupby("order_id")
        .agg(
            discount_raw=("discount_amount", "sum"),
            promo_codes=("promo_code", lambda s: " + ".join(sorted(s))),
            n_promos=("promotion_id", "size"),
        )
        .reset_index()
    )
    assert agg.order_id.is_unique
    return agg


def clean_generic(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include=["object", "str"]).columns:
        df[col] = df[col].str.strip()
    for col in introspect.infer_numeric_columns(df):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in introspect.infer_date_columns(df):
        df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed")
    return df


KNOWN_CLEANERS = {
    "customers": clean_customers,
    "orders": clean_orders,
    "promotions": clean_promotions,
}


def clean_all(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    cleaned = {}
    for name, df in tables.items():
        fn = KNOWN_CLEANERS.get(name)
        cleaned[name] = fn(df) if fn else clean_generic(df)
    return cleaned
