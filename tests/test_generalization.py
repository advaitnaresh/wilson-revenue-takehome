"""Proves the pipeline generalizes to CSVs it has no schema declaration for.

Writes a temp data/ directory with the three known tables PLUS a brand-new,
undeclared CSV (support_tickets.csv), then runs the exact same stage functions
(and the same end-to-end run.run()) used for the known tables. Nothing here is
schema-specific setup — dropping a real new CSV into data/ should behave the
same way these fixtures do.
"""

import pandas as pd
import pytest

from pipeline import clean, config, filter as filter_stage, ingest, qc, run


@pytest.fixture
def data_dir(tmp_path):
    pd.DataFrame(
        {
            "customer_id": ["1", "2"],
            "customer_name": ["Alice Co", "Bob LLC"],
            "segment": ["Enterprise", "SMB"],
            "region": ["West", "East"],
        }
    ).to_csv(tmp_path / "customers.csv", index=False)

    pd.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "customer_id": ["1", "2"],
            "order_date": ["2025-01-01", "2025-01-02"],
            "amount": [100.0, 50.0],
            "status": ["completed", "completed"],
        }
    ).to_csv(tmp_path / "orders.csv", index=False)

    pd.DataFrame(
        {
            "promotion_id": ["p1"],
            "order_id": ["o1"],
            "promo_code": ["SAVE10"],
            "discount_amount": [10.0],
        }
    ).to_csv(tmp_path / "promotions.csv", index=False)

    # A CSV with no entry anywhere in pipeline/config.py — this is the new,
    # never-seen-before file a future data source might add.
    pd.DataFrame(
        {
            "ticket_id": ["t1", "t1", "t2", "t3"],
            "customer_id": ["1", "1", "2", "999"],  # t1 duplicated row; "999" is an orphan
            "opened_at": ["2025-01-05", "2025-01-05", "2025-01-06", "2025-01-07"],
            "priority": ["high", "high", "low", "low"],
        }
    ).to_csv(tmp_path / "support_tickets.csv", index=False)

    return tmp_path


class TestDiscoveryAndIngestion:
    def test_discovers_every_csv_including_the_unknown_one(self, data_dir):
        files = config.discover_raw_files(data_dir)
        assert set(files) == {"customers", "orders", "promotions", "support_tickets"}

    def test_ingest_loads_the_unknown_table_untouched(self, data_dir):
        raw = ingest.load_raw(data_dir)
        assert "support_tickets" in raw
        assert len(raw["support_tickets"]) == 4


class TestGenericQC:
    def test_flags_duplicate_row_in_unknown_table(self, data_dir):
        raw = ingest.load_raw(data_dir)
        report = qc.run_initial_qc(raw)
        hit = [i for i in report.issues if i.table == "support_tickets" and i.check == "duplicate_rows"]
        assert hit and hit[0].count == 1

    def test_infers_foreign_key_into_customers_and_flags_orphan(self, data_dir):
        raw = ingest.load_raw(data_dir)
        report = qc.run_initial_qc(raw)
        hit = [i for i in report.issues if i.table == "support_tickets" and i.check.startswith("orphan_fk")]
        assert hit, "expected support_tickets.customer_id -> customers.customer_id to be inferred"
        assert hit[0].count == 1  # the "999" row
        assert hit[0].severity == "info"  # inferred, not declared, so never a hard fail
        assert "inferred FK" in hit[0].detail

    def test_infers_date_column_and_would_flag_bad_dates(self):
        # Enough good dates to clear the 90% sniffing threshold, so the column
        # is still classified as date-like despite the one bad value.
        df = pd.DataFrame(
            {
                "ticket_id": [f"t{i}" for i in range(10)],
                "opened_at": ["not-a-date"] + [f"2025-01-{i:02d}" for i in range(1, 10)],
            }
        )
        report = qc.run_initial_qc({"support_tickets": df})
        hit = [i for i in report.issues if i.table == "support_tickets" and i.check == "unparseable:opened_at"]
        assert hit and hit[0].count == 1


class TestGenericFilterAndClean:
    def test_filter_drops_only_exact_duplicates_from_unknown_table(self, data_dir):
        raw = ingest.load_raw(data_dir)
        filtered, log = filter_stage.apply_filters(raw)
        tickets = filtered["support_tickets"]
        assert len(tickets) == 3  # one exact duplicate dropped
        assert "999" in tickets.customer_id.values  # orphan kept — filtering isn't the place to drop it
        dropped = {row["rule"]: row["rows_dropped"] for row in log if row["table"] == "support_tickets"}
        assert dropped["exact_duplicate_rows"] == 1

    def test_clean_coerces_inferred_date_column_on_unknown_table(self, data_dir):
        raw = ingest.load_raw(data_dir)
        filtered, _ = filter_stage.apply_filters(raw)
        cleaned = clean.clean_all(filtered)
        assert pd.api.types.is_datetime64_any_dtype(cleaned["support_tickets"].opened_at)


class TestEndToEnd:
    def test_run_succeeds_with_extra_csv_present_and_revenue_unaffected(self, data_dir):
        result = run.run(data_dir)

        assert "support_tickets" in result["tables"]
        assert result["revenue_summary"]["total_revenue"] == 150.0  # o1 + o2, unaffected by the extra file

        support_ticket_issues = [i for i in result["initial_qc"].issues if i.table == "support_tickets"]
        assert support_ticket_issues, "the new table should still be profiled even though it's undeclared"

    def test_run_still_works_if_a_revenue_table_is_missing(self, data_dir, tmp_path):
        (data_dir / "promotions.csv").unlink()
        result = run.run(data_dir)
        assert result["revenue_summary"] is None
        assert result["final_qc"] is None
        skip_note = [i for i in result["initial_qc"].issues if i.check == "revenue_tables_missing"]
        assert skip_note and "promotions" in skip_note[0].detail
