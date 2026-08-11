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

## Stages

```
raw CSVs
   │
   ▼
1. ingest      — read files as-is, enforce only that required columns exist
   │
   ▼
2. QC (raw)    — profile the data: nulls, dupes, referential integrity, value ranges,
   │              unexpected categories. Observes only — nothing is fixed yet.
   ▼
3. filter      — drop rows only where there's no defensible way to keep them:
   │              exact duplicate rows, unparseable values, non-positive amounts
   ▼
4. clean       — fix types, collapse promotions to one row per order *before* any join
   │              (so a 1:many relationship can't silently fan out a merge)
   ▼
5. transform   — join, then derive the four requested metrics. Ambiguous cases
   │              (unknown customer, discount > amount) are labeled here, not dropped
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

The QC/filter rules are declared in `pipeline/config.py` (required columns, expected `status`
values, numeric/date columns, foreign-key relationships) rather than hardcoded inline, so
pointing this at a similarly-shaped extract — new month, different source system — is a config
edit. `tests/test_pipeline.py` proves this: it runs the pipeline against small, hand-built
DataFrames with injected errors (duplicate rows, a bad date, a negative amount, an orphaned
customer_id, a discount exceeding its order, an unrecognized status, a naive join that fans out)
rather than the real CSVs, to show each check catches a *class* of problem, not just the
specific rows this one dataset happens to have.

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
