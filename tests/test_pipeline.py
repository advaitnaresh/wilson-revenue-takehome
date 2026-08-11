"""Tests use small, hand-built DataFrames with deliberately injected errors —
not the real CSVs — to prove each stage catches a class of problem generically,
not just the specific rows present in this one dataset.
"""

import pandas as pd
import pytest

from pipeline import clean, filter as filter_stage, qc, transform


def make_customers():
    return pd.DataFrame(
        {
            "customer_id": ["1", "2", "3"],
            "customer_name": ["Alice Co", "Bob LLC", "Carol Inc"],
            "segment": ["Enterprise", "SMB", "SMB"],
            "region": ["West", "East", "East"],
        }
    )


def make_dirty_orders():
    return pd.DataFrame(
        {
            "order_id": ["o1", "o1", "o2", "o3", "o4", "o5", "o6"],
            "customer_id": ["1", "1", "2", "3", "999", "1", "2"],
            "order_date": ["2025-01-01", "2025-01-01", "2025-01-02", "not-a-date", "2025-01-03", "2025-01-04", "2025-01-05"],
            "amount": [100.0, 100.0, 50.0, 75.0, 30.0, -20.0, 0.0],
            "status": ["completed", "completed", "completed", "completed", "completed", "completed", "completed"],
        }
    )


def make_dirty_promotions():
    return pd.DataFrame(
        {
            "promotion_id": ["p1", "p2"],
            "order_id": ["o2", "o2"],
            "promo_code": ["SAVE10", "SAVE50"],
            "discount_amount": [10.0, 60.0],  # stacked total (70) > order o2's amount (50)
        }
    )


class TestInitialQC:
    def test_catches_exact_duplicate_rows(self):
        report = qc.run_initial_qc({"customers": make_customers(), "orders": make_dirty_orders(), "promotions": make_dirty_promotions()})
        hit = [i for i in report.issues if i.check == "duplicate_rows" and i.table == "orders"]
        assert hit and hit[0].count == 1  # one duplicated pair (o1 x2)

    def test_catches_unparseable_date(self):
        report = qc.run_initial_qc({"customers": make_customers(), "orders": make_dirty_orders(), "promotions": make_dirty_promotions()})
        hit = [i for i in report.issues if i.check == "unparseable:order_date"]
        assert hit and hit[0].count == 1
        assert hit[0].severity == "fail"

    def test_catches_negative_and_zero_amount(self):
        report = qc.run_initial_qc({"customers": make_customers(), "orders": make_dirty_orders(), "promotions": make_dirty_promotions()})
        negative = [i for i in report.issues if i.check == "negative:amount"]
        zero = [i for i in report.issues if i.check == "zero:amount"]
        assert negative and negative[0].count == 1
        assert zero and zero[0].count == 1

    def test_catches_orphan_foreign_key(self):
        report = qc.run_initial_qc({"customers": make_customers(), "orders": make_dirty_orders(), "promotions": make_dirty_promotions()})
        hit = [i for i in report.issues if i.check.startswith("orphan_fk")]
        assert hit and hit[0].count == 1  # the o4 / customer_id "999" row

    def test_catches_discount_exceeding_amount(self):
        report = qc.run_initial_qc({"customers": make_customers(), "orders": make_dirty_orders(), "promotions": make_dirty_promotions()})
        hit = [i for i in report.issues if i.check == "discount_exceeds_amount"]
        assert hit and hit[0].count == 1  # order o2: discount 70 > amount 50

    def test_catches_unexpected_status(self):
        orders = make_dirty_orders()
        orders.loc[0, "status"] = "chargeback"
        report = qc.run_initial_qc({"customers": make_customers(), "orders": orders, "promotions": make_dirty_promotions()})
        hit = [i for i in report.issues if i.check == "unexpected_status"]
        assert hit and "chargeback" in hit[0].detail


class TestFilterStage:
    def test_drops_exact_duplicates_and_bad_rows_only(self):
        filtered, log = filter_stage.apply_filters(
            {"customers": make_customers(), "orders": make_dirty_orders(), "promotions": make_dirty_promotions()}
        )
        orders = filtered["orders"]
        # o1's duplicate is gone, but o1 itself survives
        assert (orders.order_id == "o1").sum() == 1
        # o3 (bad date) and o5/o6 (non-positive amount) are dropped
        assert "o3" not in orders.order_id.values
        assert "o5" not in orders.order_id.values
        assert "o6" not in orders.order_id.values
        # o4 (unknown customer) survives filtering — that's a labeling decision, not a filter
        assert "o4" in orders.order_id.values

        dropped_by_rule = {row["rule"]: row["rows_dropped"] for row in log if row["table"] == "orders"}
        assert dropped_by_rule["exact_duplicate_rows"] == 1
        assert dropped_by_rule["unparseable_amount_or_date"] == 1
        assert dropped_by_rule["non_positive_amount"] == 2


class TestCleanAndTransform:
    def test_promotions_collapse_to_order_grain(self):
        cleaned = clean.clean_promotions(make_dirty_promotions())
        assert cleaned.order_id.is_unique
        assert cleaned.loc[cleaned.order_id == "o2", "discount_raw"].item() == 70.0

    def test_join_does_not_fan_out_and_caps_discount(self):
        filtered, _ = filter_stage.apply_filters(
            {"customers": make_customers(), "orders": make_dirty_orders(), "promotions": make_dirty_promotions()}
        )
        cleaned = clean.clean_all(filtered)
        analysis = transform.build_analysis_table(cleaned["customers"], cleaned["orders"], cleaned["promotions"])

        assert len(analysis) == len(cleaned["orders"])
        assert analysis.order_id.is_unique

        o2 = analysis.loc[analysis.order_id == "o2"].iloc[0]
        assert o2.discount_raw == 70.0
        assert o2.discount == 50.0  # capped at the order's own amount

        o4 = analysis.loc[analysis.order_id == "o4"].iloc[0]
        assert o4.segment == "Unknown"
        assert o4.is_known_customer == False

    def test_final_qc_fails_when_a_fanout_slips_through(self):
        # Simulate the exact bug the pipeline is designed to prevent: joining
        # against un-aggregated promotions, which fans out order o2 into two rows.
        filtered, _ = filter_stage.apply_filters(
            {"customers": make_customers(), "orders": make_dirty_orders(), "promotions": make_dirty_promotions()}
        )
        orders = clean.clean_orders(filtered["orders"])
        naive = orders.merge(make_dirty_promotions(), on="order_id", how="left")
        naive["discount"] = naive.get("discount_amount", 0)
        naive["discount_raw"] = naive["discount"]

        report = qc.run_final_qc(
            naive,
            orders_in=len(orders),
            row_ledger=[],
            segment_revenue_total=0.0,
            reported_total_revenue=0.0,
        )
        with pytest.raises(AssertionError):
            report.raise_if_failed()
