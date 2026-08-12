# Scaling the pipeline to datasets that don't fit on one machine

`pipeline/` (pandas) loads every table into one process's memory. That's the
right choice for the ~30K-row dataset this exercise ships with — it's simple,
fast to iterate on, and easy to read. It stops being the right choice once
`orders.csv` is millions or billions of rows: pandas has nowhere to put data
that doesn't fit in one machine's RAM, and nothing to parallelize the work
across more than one core's worth of I/O.

`pipeline_spark/` is the same pipeline re-expressed in PySpark, for exactly
that situation. Same seven stages, same severity model, same final numbers —
what changes is that every operation is a distributed DataFrame transformation
instead of an in-memory pandas one, and nothing but small aggregates or a
bounded sample ever comes back to the driver.

## What's reused, not rewritten

The business rules and the severity model are the parts of this pipeline that
have nothing to do with pandas vs. Spark, so they're imported unchanged:

- `pipeline.config` — required columns, expected statuses, foreign keys,
  revenue statuses. `pipeline_spark.config` imports these directly; there is
  exactly one place a new business rule gets declared, for both engines.
- `pipeline.introspect` — the pandas-based type/key inference used for
  undeclared tables. Reused for exactly what it's good at: making a schema
  guess from a small, already-in-memory table (a sample, at Spark scale).
- `pipeline.qc.QCIssue` / `QCReport` — the fail/warn/info model, unchanged.
  A `fail` means the same thing whether it came from a pandas `.isna().sum()`
  or a Spark `.count()`.

What's rewritten is ingest/qc/filter/clean/transform/run themselves — every
function in `pipeline_spark/` takes and returns Spark DataFrames, and is
written so the driver never collects the (potentially huge) table itself.

## Stages, and what's different about each one at scale

```
data/*.csv or data/<table>/part-*.csv
   │
   ▼
1. ingest      Explicit all-string schemas (pipeline_spark.config.SCHEMAS) instead
   │           of inferSchema=True — inferSchema means Spark reads the whole file
   │           once just to guess types, then again to load it. A *source* can now
   │           be a directory of part-files, not just one CSV: that's how batches
   │           actually arrive at scale (each load/batch is a new file), not as one
   │           file that grows forever.
   ▼
2. QC (raw)    Every check is a distributed aggregation (.count(), .agg(), a
   │           left_anti join) — never a full collect(). The one exception: an
   │           UNDECLARED table's column types/keys are decided from a bounded
   │           sample (config.SCHEMA_INFERENCE_SAMPLE_SIZE rows), reusing
   │           pipeline.introspect on that sample — scanning every value of every
   │           column across a huge table just to guess its type isn't a check
   │           worth the cost. Every finding built on a sample is tagged 'info',
   │           never 'warn'/'fail'.
   ▼
3. filter      Same rule as pandas: only drop the unambiguous, unrecoverable rows.
   │           Each table is cached on entry so the ~3 rules that follow share one
   │           materialization instead of re-deriving it per .count().
   ▼
4. clean       Same responsibilities (coerce types, collapse promotions to
   │           order-grain). Casts use Spark's coercing (non-ANSI) cast, matching
   │           pandas' errors="coerce" — see "ANSI mode", below.
   ▼
5. transform   customers is broadcast — map-side join, no shuffle, since it's a
   │           dimension table orders of magnitude smaller than orders/promotions
   │           even at real scale. The analysis table itself is never collected;
   │           the three *reported* metrics are, by construction, tiny (a handful
   │           of numbers, one row per segment, five rows) — once Spark's
   │           distributed aggregation reduces the data to that, finishing the
   │           shaping in pandas is simpler and free.
   ▼
6. QC (final)  Same checks as pandas (row counts reconcile, join didn't fan out,
   │           checksums tie out), computed as distributed aggregations.
   ▼
7. results     The (potentially huge) analysis table is written as Parquet,
               partitioned by order_month — never collected. The tiny aggregated
               metrics are written as JSON/CSV, same as the pandas pipeline.
```

## Two ways to run it

**Full run** — `python -m pipeline_spark.run`. Reprocesses every row in
`data/` every time. Correct default for a dataset that fits a nightly batch
window, which "millions of rows" usually still does on a real cluster.

**Incremental / batch mode** — `pipeline_spark.incremental.run_batch(spark)`.
For a dataset large enough, or arriving continuously enough, that reprocessing
the entire history every run stops making sense. Tracks a watermark
(`pipeline_output_spark/_watermark.json`, the max `order_date` processed so
far) and only pulls orders from that point forward. Two things make this
*correct*, not just fast:

1. **Idempotent appends.** `order_date` is a day, not a timestamp, so
   `>= watermark` alone can't tell "new since last run" from "already ran the
   day this belongs to." Before appending, the batch is anti-joined against
   the order_ids already in the Parquet output — safe to re-run the same batch
   twice, safe to run right after a full `run()` over the same data.
2. **Same filter/clean/transform, smaller input.** An incremental run isn't a
   different pipeline; it's the same one given a smaller slice of `orders` —
   the only table large enough at real scale to need slicing in the first
   place. `customers`/`promotions` are re-read in full each batch.

What it deliberately doesn't do: the returned revenue summary describes *that
batch*, not a running total. Cumulative figures need a separate aggregation
pass over the accumulated Parquet output — a small, cheap query against
already-reduced data, and a natural next stage, just not one implemented here.

## Two real correctness lessons from building this

**Spark defaults to ANSI SQL mode as of Spark 4** — an invalid cast *raises*
instead of returning null. Every cast in this pipeline is written the way
pandas' `errors="coerce"` works (parse failure → null, which QC then counts
and reports), so ANSI's raise-on-invalid-input would crash the job on exactly
the malformed values this pipeline exists to catch. `pipeline_spark.session`
turns it off (`spark.sql.ansi.enabled=false`) for that reason — see the
comment there before turning it back on for a different project.

**A long-lived SparkSession can reuse cached results across unrelated calls.**
Two separate `run_batch()` calls whose query plans happen to be structurally
identical (same path, same filter literal — which two consecutive calls with
an unchanged watermark produce exactly) can have the second one served the
first one's cached, now-stale result instead of rescanning. In production this
never comes up, because each batch is normally its own `spark-submit`
job — the JVM restarts every time, so there's no session to share stale state
across. It matters for anything that loops calling `run_batch` on one
long-running SparkSession instead of relaunching Spark per batch (a scheduler
service, or a test suite), so `run_batch` defensively calls
`spark.catalog.clearCache()` on entry. `tests/test_pipeline_spark.py`'s
`TestIncremental` test is what surfaced this.

## What's simplified relative to the pandas pipeline, on purpose

- **Duplicate-key conflict detection is dropped.** Pandas checks not just "is
  this key duplicated" but "do the duplicate rows actually disagree on any
  field" (`groupby(key).nunique()`). At scale that's a shuffle across every
  column for every duplicate key, to answer a question a human can just as
  easily check by hand once flagged. Spark reports the duplicate count only.
- **Schema inference for undeclared tables is sample-based, not exact.** A
  column's type, and whether it looks like a primary/foreign key, is decided
  from `SCHEMA_INFERENCE_SAMPLE_SIZE` (10,000) rows, not the whole table. The
  actual orphan-count check that follows is still exact (a real `left_anti`
  join) — only the decision to check, and what column to check, is
  sample-based. Every finding built on that decision is tagged `info`.
- **Top-5-by-revenue is an exact global sort**, not an approximation. That's
  fine here — it runs once, on the already-aggregated (thousands-of-rows,
  not millions) per-customer table — but it's worth naming as the kind of
  operation that would need `approx` handling if run per-row at real scale.
- **Output partitioning is tuned for a laptop, not a cluster.** The Parquet
  write partitions by `order_month`; with `spark.sql.shuffle.partitions=8`
  (this repo's local-mode default), that's 8 files per month. At real data
  volume, tune shuffle partitions (or `.coalesce()` before writing) so
  partitions land in the 128MB-1GB range instead of either thousands of tiny
  files or a handful of huge ones.

## Running it

```bash
pip install -r requirements-bigdata.txt   # adds pyspark + pyarrow on top of requirements.txt
python -m pipeline_spark.run              # full run over data/
python -m pytest tests/test_pipeline_spark.py
```

Defaults to `local[*]` so the exact same code runs against the sample `data/`
directory on a laptop and against a real cluster — point `SPARK_MASTER` at
`yarn`, `k8s://...`, or `spark://...` and nothing else changes. On a genuinely
large job, also raise driver/executor memory beyond Spark's 1GB local default
(e.g. `--driver-memory 8g` via `spark-submit`, or `SPARK_MASTER`/config
overrides in `pipeline_spark/session.py`).

**Verified, not just written**: numbers match the pandas pipeline to the cent
against the real `data/` (total revenue $16,153,512.87, net $15,879,675.40).
`scripts/generate_synthetic_data.py` builds a synthetic dataset entirely
inside Spark via `spark.range` (never touching the driver) to prove the
pipeline at a scale `data/`'s ~30K rows can't:

```bash
python scripts/generate_synthetic_data.py /tmp/bigdata 5000000 50000   # 5M orders, 50K customers
python -c "from pipeline_spark import session, run; run.run(session.get_spark_session(), '/tmp/bigdata')"
```

Run this way, 5 million orders processed end-to-end (all 7 stages, partitioned
Parquet write included) in under a minute on a single laptop, with zero hard
QC failures and correct detection of the injected orphan foreign keys and
oversized discounts at that scale.

## What's not handled here

Real deployment would also want: a real object store or HDFS/lake path
instead of local disk, a metastore-registered table (Hive/Iceberg/Delta)
instead of a bare Parquet directory so downstream consumers get schema
evolution and time travel for free, cluster-sized `spark.sql.shuffle.partitions`
tuning, and an actual orchestrator (Airflow/Dagster) triggering `run_batch` on
a schedule rather than a human running `python -m pipeline_spark.run`. None of
that changes the pipeline's logic — it's deployment plumbing around the same
seven stages.
