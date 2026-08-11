# Revenue data-quality pipeline

Productionized version of the judgment calls made in `revenue_analysis.ipynb`, structured as
a pipeline so the next dirty extract — a different month, a different source system — gets the
same checks automatically instead of relying on someone re-noticing the same issues by eye.

## Why a pipeline, not just a cleaner notebook

The notebook's value was the *reasoning*: here's what's wrong, here's why, here's the call I
made. That reasoning doesn't disappear here — it's encoded as code, so it runs on every load
instead of once. The pipeline's job is narrower than the notebook's: catch and report the
*shape* of problem (duplicate rows, orphaned foreign keys, out-of-range values, fan-out joins),
not necessarily re-derive every judgment call from scratch for data it's never seen.

## Repository layout

```
data/                      raw CSVs — the data source. Drop a new file in here
  customers.csv            and it's picked up on the next run, no code change.
  orders.csv
  promotions.csv
pipeline/
  config.py                schema declarations for known tables + auto-discovery
  introspect.py             generic type/key/FK inference for undeclared tables
  ingest.py … run.py        the seven stages
pipeline_output/           generated artifacts (gitignored)
tests/
  test_pipeline.py          checks against the known customers/orders/promotions schema
  test_generalization.py    checks that an undeclared CSV still flows through cleanly
```

## Stages

```
data/*.csv (however many)
   │
   ▼
1. ingest      — every CSV in data/ is discovered and read, keyed by filename stem;
   │              "_id" columns are forced to string; a declared table also gets a
   │              required-column check
   ▼
2. QC (raw)    — profile each table: nulls, dupes, referential integrity, value ranges,
   │              unexpected categories. Observes only — nothing is fixed yet. A table
   │              outside the known schema gets the same profile via type-sniffed,
   │              best-effort rules instead of exact ones (see Generalizing, below)
   ▼
3. filter      — drop rows only where there's no defensible way to keep them:
   │              exact duplicate rows, unparseable values, non-positive amounts
   │              (an undeclared table only gets the always-safe one: exact dupes)
   ▼
4. clean       — fix types, collapse promotions to one row per order *before* any join
   │              (so a 1:many relationship can't silently fan out a merge)
   ▼
5. transform   — join, then derive the four requested metrics. Ambiguous cases
   │              (unknown customer, discount > amount) are labeled here, not dropped.
   │              Runs only if customers/orders/promotions are all present in data/ —
   │              an extra, unrelated CSV never blocks it.
   ▼
6. QC (final)  — validate the pipeline's own output: row counts reconcile, the join
   │              didn't fan out, discount capping actually held, segment revenue
   │              sums to total revenue. Hard failures raise and stop the run.
   ▼
7. results     — pipeline_output/revenue_summary.json, revenue_by_segment.csv,
                  top5_customers.csv, qc_report.json
```

Run it: `python -m pipeline.run`
Test it: `python -m pytest tests/`

## Design choices

**Filter vs. clean/transform is a deliberate line, not a convenience split.** Stage 3 only
drops rows that are *unambiguously* unusable — an exact duplicate, a value that doesn't parse,
a non-positive amount. Everything the notebook flagged as a judgment call (an order from an
unknown customer, a refunded order, a discount bigger than its order) is deliberately **not**
dropped in filtering. Those get handled downstream where the choice and its effect are visible
in the output, rather than silently vanishing as a "filtered" row. A pipeline that quietly drops
ambiguous rows during filtering would hide the exact decisions this exercise was designed to
surface.

**QC runs twice for different reasons.** Stage 2 profiles the *input* — it's the audit a human
would do before deciding anything, run automatically instead. Stage 6 checks the *pipeline's own
arithmetic* — did the join fan out, do the segment totals reconcile to the grand total, did
capping actually floor every row at zero. These catch different failure modes: Stage 2 catches
a bad extract; Stage 6 catches a bug in this code. Conflating them would mean a clean input
with a broken transform looks the same as a dirty input that was handled correctly.

**Every QC finding has a severity, not just a print statement.** `fail` means the pipeline
should not be trusted past this point (a value that doesn't parse, a join that fanned out) and
`raise_if_failed()` stops the run. `warn` is a real issue with a defensible default handling
(orphaned foreign key → label `Unknown`, not drop). `info` is a value worth surfacing but not
a defect (order-level promo stacking). This distinction is what lets the pipeline run unattended
on new data: the things that must stop a human short don't get lost in the same bucket as the
things that don't.

**The row-count ledger is the audit trail.** Every stage logs its row count per table, and
Stage 6 checks that `filtering → cleaning → transform` conserves count exactly (aside from
promotions collapsing to order-grain, which is expected and named). That reconciliation, not a
spot-check, is what proves nothing was silently dropped between stages.

## Generalizing beyond this dataset

There are two levels of generalization here, and they're deliberately different in how much
they trust the result:

**A *declared* table** (customers/orders/promotions, in `pipeline/config.py`) gets exact,
deterministic rules: typed columns, a required-column check, explicit foreign keys, the revenue
transform. Pointing this at a similarly-shaped extract — new month, different source system — is
a config edit: add the file's schema to `REQUIRED_COLUMNS` / `PRIMARY_KEY` / `NUMERIC_COLUMNS` /
etc., and it gets the same treatment. `tests/test_pipeline.py` proves this against small,
hand-built DataFrames with injected errors (duplicate rows, a bad date, a negative amount, an
orphaned customer_id, a discount exceeding its order, an unrecognized status, a naive join that
fans out), to show each check catches a *class* of problem, not just the specific rows this one
dataset happens to have.

**An *undeclared* table** — any CSV dropped into `data/` that isn't in config.py — still flows
through ingest → QC → filter → clean automatically, using best-effort rules in
`pipeline/introspect.py` instead of exact ones:

- column types are *sniffed*: an object column where ≥90% of values parse as a number or date is
  treated as numeric/date-like for validation purposes
- a primary key is *guessed*: the most-unique `*_id` column, if one clears a 95% uniqueness bar
- foreign keys are *inferred*: a `*_id` column is treated as a candidate reference into any other
  table where that same column name looks like a key there too

Every finding from an inferred rule is tagged `info` and its detail says "(inferred...)" —
never `warn` or `fail` — because a guess about what a column *means* shouldn't be able to stop a
pipeline run the way an actual schema violation can. `tests/test_generalization.py` proves this
end-to-end: it builds a temp `data/` directory with the three known tables *plus* a new,
never-declared `support_tickets.csv` (containing a duplicate row and a customer_id that doesn't
exist in `customers.csv`), and asserts the pipeline profiles, filters, and cleans it correctly —
without needing a single line added to `config.py` — while the revenue numbers stay exactly
what they'd be without that file. Drop a real new CSV into `data/` and you get the same behavior
these tests describe.

## What this pipeline does not decide for you

It formalizes the mechanical checks (parses, ranges, uniqueness, referential integrity, join
cardinality) but the substantive judgment calls from the notebook — refunded orders excluded
from revenue, unknown customers kept in the total but bucketed as `Unknown`, discounts capped
rather than left negative, Mid-Market reported as an explicit zero rather than imputed — are
encoded as the current default behavior in `transform.py` and `config.py`, not re-derived. If a
new extract's ambiguity doesn't match one of these patterns (e.g., a genuinely new `status`
value), Stage 2 QC will flag it as `unexpected_status` rather than silently applying a rule that
was never designed for it — that's a signal for a human to make the same kind of call the
notebook did, not something the pipeline resolves on its own.
