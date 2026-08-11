# Revenue data-quality pipeline — how to use it

A staged pipeline that turns raw CSVs in `data/` into a validated revenue report,
catching common data-quality problems (duplicates, orphaned foreign keys, bad
types, fan-out joins) along the way. For the design rationale — why the stages
are split the way they are, why some findings are `fail` and others `info` —
see [README_PIPELINE.md](README_PIPELINE.md). This doc is just the "how do I
run it" reference.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Requires Python 3.10+ (uses `dict[str, X]` and `X | None` type hints) and pandas.

## Run the pipeline

```bash
python -m pipeline.run
```

This reads every `*.csv` in `data/`, runs it through all seven stages, prints a
summary to the terminal, and writes artifacts to `pipeline_output/`:

| File | Contents |
|---|---|
| `revenue_summary.json` | total revenue, net revenue, discounts, status breakdown |
| `revenue_by_segment.csv` | revenue/discounts/net revenue per customer segment |
| `top5_customers.csv` | top 5 customers by revenue |
| `qc_report.json` | every QC finding from both passes, the row-count ledger, and the filter log |

Exit code is non-zero if a hard (`fail`-severity) QC issue is found — e.g. a
join fanned out, or a value that should be numeric didn't parse. Check
`qc_report.json` or the printed `Final QC` section for what tripped it.

## Add a new data file

Drop a `.csv` into `data/` and re-run `python -m pipeline.run` — nothing else
needed. What happens next depends on whether the file matches the known
schema:

- **It's a new extract of `customers.csv` / `orders.csv` / `promotions.csv`**
  (same name, replaced or updated) — it's ingested with the exact rules those
  tables already have (typed ID columns, required-column checks, the revenue
  transform) and the report picks up whatever changed.
- **It's a CSV with no schema declared anywhere** (e.g. `returns.csv`,
  `support_tickets.csv`) — it still flows through ingest → QC → filter → clean
  automatically, using best-effort rules: column types, primary keys, and
  foreign keys into other tables are all *inferred* rather than declared. Its
  findings show up in `qc_report.json` tagged `info` (never `warn`/`fail`,
  since it's a guess). It does **not** feed the revenue calculation — that
  needs `customers`, `orders`, and `promotions` specifically — but its presence
  never blocks or changes that calculation either.

To give a new table the same exact treatment the known tables get (typed
columns, required-column checks, and — if it's meant to join into the revenue
calc — foreign keys), declare it in `pipeline/config.py`: add entries to
`REQUIRED_COLUMNS`, `PRIMARY_KEY`, `DTYPES`, `NUMERIC_COLUMNS`/`DATE_COLUMNS`,
and `FOREIGN_KEYS` as needed. No other file needs to change.

## Run the tests

```bash
python -m pytest tests/
```

- `tests/test_pipeline.py` — checks against the known customers/orders/promotions
  schema, using small hand-built DataFrames with injected errors (duplicate
  rows, bad dates, negative amounts, orphaned IDs, a discount bigger than its
  order, an unrecognized status, a naive join that fans out).
- `tests/test_generalization.py` — checks that a CSV with *no* schema
  declaration still gets profiled, filtered, and cleaned correctly, and that
  adding one never affects the revenue numbers.

## Using it programmatically

```python
from pipeline import run

result = run.run()                    # or run.run("/path/to/other/data/dir")
result["revenue_summary"]             # dict: total_revenue, net_revenue, ...
result["by_segment"]                  # DataFrame
result["top5"]                        # DataFrame
result["initial_qc"].issues           # list[QCIssue] from the raw-data pass
result["final_qc"].issues             # list[QCIssue] from the output-validation pass (None if
                                       # customers/orders/promotions weren't all present)
```

`run.run(data_dir)` takes any directory of CSVs, not just `data/` — useful for
pointing the pipeline at a different extract without moving files around.
