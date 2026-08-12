# Revenue Data Pipeline — Project Overview

This repo started as a data-analyst take-home exercise and grew, in three
deliberate phases, into a small data-engineering project: a one-off notebook
analysis, then a reusable pipeline that generalizes the same checks to any
similarly-shaped CSV, then a distributed version of that pipeline for
datasets too large to fit on one machine. This document walks through all
three, in order, and points to the code and deep-dive docs for each.

## Branch map

| Branch | What it adds | Contains |
|---|---|---|
| `main` | Phase 1 only | Notebook, raw data, instructions |
| `gen-data-quality-pipeline` | Phase 1 + 2 | + the pandas pipeline |
| `gen-data-quality-pipeline-bigdata` ← **you are here** | Phase 1 + 2 + 3 | + the PySpark port |

This branch has everything: the original notebook, the generalized pandas
pipeline, and the PySpark scale-out. Nothing below is a forward reference to
another branch — every file mentioned exists right here.

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

### Running it

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m pipeline.run          # runs against data/, writes pipeline_output/
python -m pytest tests/test_pipeline.py tests/test_generalization.py
```

```python
from pipeline import run
result = run.run()                    # or run.run("/path/to/other/data/dir")
result["revenue_summary"], result["by_segment"], result["top5"]
result["initial_qc"].issues, result["final_qc"].issues
```

To onboard a new *known* table (typed columns, required-column checks, and —
if it should join into the revenue calc — foreign keys), add entries to
`REQUIRED_COLUMNS` / `PRIMARY_KEY` / `DTYPES` / `NUMERIC_COLUMNS` /
`DATE_COLUMNS` / `FOREIGN_KEYS` in `pipeline/config.py`. Nothing else needs
to change.

**Deep dive**: [README_PIPELINE.md](README_PIPELINE.md) — the full design
rationale: why filtering and cleaning are a deliberate line rather than a
convenience split, why QC runs twice for different reasons, and the two
levels of "generalizing beyond this dataset."

---

## Phase 3 — A scalable big-data system

`pipeline/` loads every table into one process's memory — the right call for
~30K rows, and the wrong one once `orders.csv` is millions or billions of
rows and doesn't fit on one machine. Phase 3 re-expresses the *same seven
stages* in PySpark, in `pipeline_spark/`, so the pipeline scales without
becoming a different pipeline.

### What's reused, not rewritten

The business rules and the QC severity model have nothing to do with pandas
vs. Spark, so they're imported unchanged: `pipeline_spark.config` imports
`pipeline.config` directly (one place a business rule gets declared, for both
engines), `pipeline_spark.qc` reuses `pipeline.introspect`'s type/key
inference and `pipeline.qc`'s `QCIssue`/`QCReport` dataclasses outright. What's
rewritten is only what genuinely has to change when data no longer fits on one
machine — every operation in `pipeline_spark/` is a distributed DataFrame
transformation, and the driver only ever collects small aggregates or a
bounded sample, never the full table.

### What's different from Phase 2's architecture

- **Explicit read schemas** instead of `inferSchema=True` — avoids a second
  full pass over each file just to guess types.
- **A "table" can be a directory of CSV part-files**, not just one file — how
  batches actually arrive at scale, landing as new files rather than one
  ever-growing one.
- **Every QC/filter check is a distributed aggregation or `left_anti` join.**
  An undeclared table's column types/keys are still inferred, but from a
  bounded sample (10,000 rows) rather than the whole table — scanning every
  value of every column across a huge table just to guess its type isn't a
  check worth the cost.
- **`customers` is broadcast into the join** — a map-side operation with no
  shuffle, since it's a dimension table orders of magnitude smaller than
  `orders`/`promotions` even at real scale.
- **The analysis table is written as partitioned Parquet**, partitioned by
  `order_month`, and never collected to the driver. The three *reported*
  metrics (total revenue, revenue-by-segment, top-5) are, by construction,
  tiny, so finishing their shaping in pandas after Spark's aggregation is
  simpler and free.
- **An optional incremental/batch mode** (`pipeline_spark/incremental.py`):
  tracks a watermark (the max `order_date` processed) and only pulls orders
  from that point forward, anti-joining against already-written output so
  re-running the same batch — or running right after a full run — never
  double-counts. For a dataset large or continuous enough that reprocessing
  the entire history every run stops making sense.

### Two real correctness lessons, not just design choices

- **Spark defaults to ANSI SQL mode** (as of Spark 4): an invalid cast
  *raises* instead of returning null. Every cast in this pipeline is written
  the way pandas' `errors="coerce"` works, so ANSI mode is turned off
  (`spark.sql.ansi.enabled=false` in `pipeline_spark/session.py`) to match.
- **A long-lived SparkSession can serve a stale cached result** across two
  structurally-identical queries (same path, same filter literal — which two
  consecutive incremental runs with an unchanged watermark produce exactly).
  `run_batch()` defensively calls `spark.catalog.clearCache()` on entry.
  Doesn't come up in production (each batch is normally its own
  `spark-submit` job, so the JVM restarts every time) — it surfaced from,
  and is guarded against, exactly the shared-session case this repo's own
  test suite exercises.

### What's verified

- Matches the pandas pipeline to the cent on the real `data/`.
- `scripts/generate_synthetic_data.py` builds a dataset entirely inside Spark
  (`spark.range`, never touching the driver). A 5-million-order run completed
  end-to-end — all 7 stages, partitioned Parquet write included — in under a
  minute on a single laptop, with zero hard QC failures and correct detection
  of injected orphan foreign keys and oversized discounts at that scale.
- `tests/test_pipeline_spark.py` (15 tests) mirrors Phase 2's dirty-data
  testing approach through the Spark code path, plus incremental-mode
  idempotency checks (backfill → no-op rerun → correctly-sized new batch).

### Running it

```bash
pip install -r requirements-bigdata.txt   # adds pyspark + pyarrow on top of requirements.txt
python -m pipeline_spark.run              # full run over data/
python -m pytest tests/test_pipeline_spark.py

# generate a synthetic multi-million-row dataset and run against it:
python scripts/generate_synthetic_data.py /tmp/bigdata 5000000 50000
python -c "from pipeline_spark import session, run; run.run(session.get_spark_session(), '/tmp/bigdata')"
```

Defaults to `local[*]`; point `SPARK_MASTER` at `yarn`, `k8s://...`, or
`spark://...` to run the same code against a real cluster.

**Deep dive**: [README_BIGDATA.md](README_BIGDATA.md) — full architecture
writeup, every stage's scale-specific behavior, both correctness gotchas in
detail, and what's deliberately simplified (and why) relative to Phase 2.

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

pipeline_spark/                  Phase 3 — the PySpark port
  config.py, session.py           reuses pipeline.config; Spark schemas/session setup
  ingest.py, qc.py, filter.py, clean.py, transform.py, run.py
  incremental.py                  watermark-based batch/incremental mode
scripts/generate_synthetic_data.py  multi-million-row synthetic data generator
README_BIGDATA.md                 Phase 3 design rationale

tests/
  test_pipeline.py, test_generalization.py   Phase 2 tests (19)
  test_pipeline_spark.py                     Phase 3 tests (15)

requirements.txt, requirements-bigdata.txt
```
