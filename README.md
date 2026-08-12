# Revenue Data Pipeline — Project Overview

This repo started as a data-analyst take-home exercise and grew, in three
deliberate phases, into a small data-engineering project: a one-off notebook
analysis (this branch), then a reusable pipeline that generalizes the same
checks to any similarly-shaped CSV, then a distributed version of that
pipeline for datasets too large to fit on one machine (both on other
branches). This document covers all three phases, in order — the first in
full, since this branch has everything for it; the other two as overviews,
since that code lives elsewhere.

## Branch map

| Branch | What it adds | Contains |
|---|---|---|
| `main` ← **you are here** | Phase 1 only | Notebook, raw data, instructions |
| `gen-data-quality-pipeline` | Phase 1 + 2 | + the pandas pipeline |
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
the data. Open `revenue_analysis.ipynb` for the full reasoning behind each
call and a sensitivity table showing how each number moves under the
alternative choice.

**Running it**: open the notebook and run all cells — it reads `customers.csv`,
`orders.csv`, `promotions.csv` from the same directory (`pandas` is the only
dependency).

---

## Phase 2 — A generalized data-quality pipeline

*(Overview only — the code for this phase lives on the
`gen-data-quality-pipeline` branch, not this one. `git checkout
gen-data-quality-pipeline` for the full README, `pipeline/`, and
`README_PIPELINE.md`.)*

The notebook's decisions above are correct for *this* extract, but they're
one-off — re-run manually, by a human, on data someone has already looked at.
Phase 2 turns that same reasoning into a pipeline that runs the same checks
automatically on any new extract:

```
data/*.csv (any number of files)
   │
   ▼
1. ingest → 2. QC (raw) → 3. filter → 4. clean → 5. transform → 6. QC (final) → 7. results
```

Every stage is explicit about which parts of the reasoning generalize
(mechanical checks: nulls, duplicates, referential integrity, join
cardinality — these run the same way regardless of what the data actually
means) and which don't (business judgment calls like "refunds don't count as
revenue," which still need a human to encode once per new pattern). A CSV
whose schema is declared gets exact rules; drop an undeclared CSV into `data/`
and it still gets profiled, filtered, and cleaned automatically using
best-effort, sample-inferred rules — every finding built on an inference is
tagged `info`, never `warn`/`fail`, since a guess about schema shouldn't be
able to halt a pipeline the way an actual violation can.

Reproduces this branch's notebook numbers exactly, and is covered by 19 tests
against both the known schema and an undeclared-table scenario.

---

## Phase 3 — A scalable big-data system

*(Overview only — the code for this phase lives on the
`gen-data-quality-pipeline-bigdata` branch, not this one. `git checkout
gen-data-quality-pipeline-bigdata` for the full README, `pipeline_spark/`,
and `README_BIGDATA.md`.)*

Phase 2's pipeline loads every table into one process's memory — the right
call for ~30K rows, and the wrong one once `orders.csv` is millions or
billions of rows and doesn't fit on one machine. Phase 3 re-expresses the
*same seven stages* in PySpark, so the pipeline scales without becoming a
different pipeline:

- The business rules and QC severity model are imported unchanged from
  Phase 2's code — one source of truth for what counts as a violation, for
  both engines.
- Every check is a distributed aggregation or anti-join; the driver only ever
  collects small aggregates or a bounded sample, never the full table.
  `customers` is broadcast into the join; the analysis table is written as
  partitioned Parquet.
- An optional incremental/batch mode tracks a watermark and anti-joins
  against already-written output for idempotent re-runs, instead of
  reprocessing the full history every time.
- Verified against a synthetic 5-million-order dataset (generated entirely
  inside Spark), completing end-to-end in under a minute on a laptop with
  zero hard QC failures.

Two genuine Spark correctness gotchas were found and fixed building this —
Spark 4's ANSI mode raising on invalid casts instead of coercing to null, and
a long-lived SparkSession serving a stale cached result across structurally
identical queries — both documented in detail on that branch.

---

## Repository layout (this branch)

```
instructions.md            the original take-home prompt
revenue_analysis.ipynb      Phase 1 — the notebook analysis
customers.csv, orders.csv, promotions.csv   the raw data
```
