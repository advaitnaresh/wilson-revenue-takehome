# Revenue Data Pipeline — Project Overview

This repo started as a data-analyst take-home exercise and grew, in three
deliberate phases, into a small data-engineering project: a one-off notebook
analysis, then a reusable pipeline that generalizes the same checks to any
similarly-shaped CSV, then (on another branch) a distributed version of that
pipeline for datasets too large to fit on one machine. This document covers
all three phases, in order — the first two in full, since this branch has
everything for them; the third as an overview, since that code lives on a
separate branch.

## Branch map

| Branch | What it adds | Contains |
|---|---|---|
| `main` | Phase 1 only | Notebook, raw data, instructions |
| `gen-data-quality-pipeline` ← **you are here** | Phase 1 + 2 | + the pandas pipeline |
| `gen-data-quality-pipeline-bigdata` | Phase 1 + 2 + 3 | + the PySpark port |

---

## Phase 1 — The basic challenge

**The task** (`instructions.md`): given `customers.csv`, `orders.csv`, and
`promotions.csv`, compute four numbers — total revenue, revenue by customer
segment, top 5 customers by revenue, and net revenue after promotional
discounts — while documenting every cleaning decision and judgment call the
data forced.

**The work**: `revenue_analysis.ipynb`. It audits each file before joining
anything, and every nonobvious decision is written down in a markdown cell
next to the code that implements it. The headline findings:

| # | Issue found | Decision made |
|---|---|---|
| 1 | 300 exact duplicate order rows | Dropped — byte-identical rows sharing an `order_id` |
| 2 | 90 orders whose `customer_id` isn't in `customers.csv` (a `199xxx` ID block, disjoint from the known `100001`-`103000` range) | Kept in company totals; bucketed as `Unknown` in the segment view; excluded from top-customer ranking |
| 3 | Three order statuses: `completed`, `refunded`, `cancelled` | Revenue = `completed` only; the other two reported separately, not silently dropped |
| 4 | 383 orders carry two stacked promotions | Aggregated to one row per order *before* joining, so the join can't fan out |
| 5 | 73 orders where total discount exceeds the order amount | Capped at the order amount (floors net revenue at zero); uncapped figure reported alongside |
| 6 | `Mid-Market` has 612 customers and zero orders | Reported as an explicit `0` row, flagged as a likely pipeline gap — not imputed |

**The headline numbers**:

- **Total revenue** (completed orders): **$16,153,512.87** across 25,575 orders
- **Net revenue** (after discounts): **$15,879,675.40**
- **Revenue by segment**: SMB $11,211,218.26 · Enterprise $4,895,261.50 · Unknown $47,033.11 · Mid-Market $0 (explicit gap, see #6 above)
- **Top 5 customers**: led by Summit Ventures ($18,192.94), Solstice Labs LLC, Amber Consulting, Cobalt Group, Lakeside Foods LLC

The point of this phase wasn't just the numbers — it was making every
ambiguous call visible and reversible, since "revenue" has no single correct
definition once refunds, unknown customers, and oversized discounts are in
the data. See the notebook for the full reasoning and a sensitivity table
showing how each number moves under the alternative choice.

---

## Phase 2 — A generalized data-quality pipeline

The notebook's decisions are correct for *this* extract, but they're
one-off — re-run manually, by a human, on data someone has already looked at.
Phase 2 turns that same reasoning into a pipeline that runs the same checks
automatically on any new extract, and is explicit about which parts of the
reasoning generalize (mechanical checks: nulls, duplicates, referential
integrity, join cardinality) and which don't (business judgment calls, which
still need a human to encode them once per new pattern).

### Architecture

```
data/*.csv (any number of files)
   │
   ▼
1. ingest      every CSV in data/, discovered automatically, read as-is
   ▼
2. QC (raw)    profile the data — nulls, dupes, referential integrity, value
   │           ranges, unexpected categories. Observes only; fixes nothing yet.
   ▼
3. filter      drop only rows with no defensible way to keep them: exact
   │           duplicates, unparseable values, non-positive amounts
   ▼
4. clean       fix types, collapse promotions to one row per order *before*
   │           any join (so a 1:many relationship can't silently fan it out)
   ▼
5. transform   join, then derive the four requested metrics. Ambiguous cases
   │           (unknown customer, oversized discount) are labeled here, not dropped
   ▼
6. QC (final)  validate the pipeline's own output: row counts reconcile, the
   │           join didn't fan out, checksums tie out. Hard failures halt the run.
   ▼
7. results     pipeline_output/revenue_summary.json, revenue_by_segment.csv,
               top5_customers.csv, qc_report.json
```

Every QC finding carries a severity — `fail` (don't trust the output past
this point), `warn` (a real issue with a defensible default handling), or
`info` (worth surfacing, not a defect) — so the pipeline can run unattended
without a human re-deriving which findings actually matter.

### Generalizing beyond this dataset's exact schema

A CSV whose schema is declared in `pipeline/config.py`
(`customers`/`orders`/`promotions` today) gets exact, deterministic rules:
typed columns, required-column checks, explicit foreign keys, the revenue
transform itself. A CSV that isn't declared — drop a new file into `data/`
and it's picked up on the next run, no code change — still flows through
every stage automatically, using best-effort rules in `pipeline/introspect.py`:
column types and primary/foreign keys are *inferred* rather than declared,
and every finding built on an inference is tagged `info`, never `warn`/`fail`,
because a guess about schema shouldn't be able to halt a pipeline the way an
actual violation can.

### What's verified

- Reproduces the notebook's numbers exactly (`$16,153,512.87` / `$15,879,675.40`).
- `tests/test_pipeline.py` (10 tests) — the known schema's checks, against
  small hand-built DataFrames with injected errors, proving each check catches
  a *class* of problem, not just this dataset's specific rows.
- `tests/test_generalization.py` (9 tests) — an undeclared CSV
  (`support_tickets.csv`, with an injected duplicate row and an orphaned
  `customer_id`) flowing through every stage correctly with zero config
  changes, and never affecting the revenue numbers.

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Requires Python 3.10+ (uses `dict[str, X]` and `X | None` type hints) and pandas.

### Run the pipeline

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

### Add a new data file

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

### Run the tests

```bash
python -m pytest tests/
```

### Using it programmatically

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

**Deep dive**: [README_PIPELINE.md](README_PIPELINE.md) — the full design
rationale: why filtering and cleaning are a deliberate line rather than a
convenience split, why QC runs twice for different reasons, and the two
levels of "generalizing beyond this dataset."

---

## Phase 3 — A scalable big-data system

*(Overview only — the code for this phase lives on the
`gen-data-quality-pipeline-bigdata` branch, not this one. `git checkout
gen-data-quality-pipeline-bigdata` for the full README, `pipeline_spark/`,
and `README_BIGDATA.md`.)*

`pipeline/` loads every table into one process's memory — the right call for
~30K rows, and the wrong one once `orders.csv` is millions or billions of
rows and doesn't fit on one machine. Phase 3 re-expresses the *same seven
stages* in PySpark, so the pipeline scales without becoming a different
pipeline:

- The business rules and QC severity model (`pipeline.config`,
  `pipeline.introspect`, `pipeline.qc.QCReport`) are imported unchanged — one
  source of truth for what counts as a violation, for both engines.
- Explicit read schemas instead of `inferSchema`; a "table" can be a directory
  of CSV part-files, matching how batches actually arrive at scale.
- Every QC/filter check is a distributed aggregation or anti-join — the
  driver only ever collects small aggregates or a bounded sample, never the
  full table. `customers` is broadcast into the join (no shuffle); the
  analysis table is written as partitioned Parquet, never collected.
- An optional incremental/batch mode tracks a watermark and anti-joins
  against already-written output for idempotent re-runs, instead of
  reprocessing the full history every time.
- Verified against a synthetic 5-million-order dataset (generated entirely
  inside Spark), completing end-to-end in under a minute on a laptop with
  zero hard QC failures.

Two genuine Spark correctness gotchas were found and fixed building this —
Spark 4's ANSI mode raising on invalid casts instead of coercing to null, and
a long-lived SparkSession serving a stale cached result across structurally
identical queries — both documented in detail in that branch's
`README_BIGDATA.md`.

---

## Repository layout (this branch)

```
instructions.md                 the original take-home prompt
revenue_analysis.ipynb           Phase 1 — the notebook analysis
data/                            raw CSVs (customers, orders, promotions)

pipeline/                        Phase 2 — the pandas pipeline
  config.py                      business rules: schema, statuses, foreign keys
  introspect.py                  generic type/key/FK inference for undeclared tables
  ingest.py, qc.py, filter.py, clean.py, transform.py, run.py
README_PIPELINE.md                Phase 2 design rationale

tests/
  test_pipeline.py, test_generalization.py   Phase 2 tests (19)

requirements.txt
```
