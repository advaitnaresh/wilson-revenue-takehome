# Pipeline Inspector — a visual front end for the data-quality pipeline

**This branch (`gh-pages`) holds only the deployed static site** —
`index.html` plus its sample data — so GitHub Pages can serve it directly
from the branch root. For the actual project this is a front end for (the
Python pipeline, the PySpark port, the notebook, tests, all the source code
this page's JS mirrors), see the [`pipeline-visualizer-webapp`](../../tree/pipeline-visualizer-webapp)
branch or [`main`](../../tree/main).

A single self-contained web page (`index.html`) that runs the same seven-stage
pipeline as the `pipeline/` package on those other branches — ingest → QC
(raw) → filter → clean → transform → QC (final) → results — live, in the
browser, on whatever CSVs you upload. Nothing is sent anywhere: parsing,
validation, and the revenue calculation all happen client-side in
JavaScript. There is no server and no build step — open the file and it
works.

## Using it

1. Open the [hosted site](https://advaitnaresh.github.io/wilson-revenue-takehome/), or open `index.html` in a browser locally (double-click it, or `open index.html`).
2. Provide a dataset, either:
   - drag/drop or browse to `.csv` files, or
   - click one of the four **sample** buttons for an instant demo
     (see "Samples", below).
3. Click **Run pipeline**. The diagram animates stage by stage; a manifest
   card unfolds under each stage as it completes. Below that, the Report
   section always shows three things regardless of what you uploaded — a
   **data health** summary, a **before/after QC comparison**, and a plain-
   language account of **what filtering, cleaning, and the join actually did**
   — and then either the revenue dashboard, a note that revenue needs all
   three known tables, or a **blocked** notice (see below).
4. **Download the QC report** as **.md** (readable), **.json** (parseable),
   or **.pdf** (an actual generated PDF file, no print dialog involved — see
   "About the PDF download," below) — all three carry the same
   health/comparison/change-summary/results content. Every download also
   opens an on-page panel with the report's text and a copy button — if a
   save dialog doesn't appear for you, that panel
   is the fallback (see "If a download button doesn't seem to do anything,"
   further down).

Tables are matched by filename: `customers.csv` / `orders.csv` /
`promotions.csv` get the exact, declared-schema treatment (typed columns,
required-column checks, the revenue join); anything else is still ingested,
profiled, filtered, and cleaned, using the same sample-free-because-it's-all-
in-memory version of the inference `pipeline/introspect.py` uses for
undeclared tables — see "How this maps to the Python pipeline," below.

## Samples

Four quick-demo buttons load datasets **embedded directly in the page** (no
download needed):

| Button | What it shows |
|---|---|
| Clean batch | A well-formed dataset — every stage completes with no findings. |
| Dirty batch | A handful of common issues (duplicate row, unexpected status) that the pipeline handles and still produces a report for. |
| Unknown table | `customers`/`orders`/`promotions` plus an undeclared CSV, to show generic profiling in action. |
| Instruction batch | The *actual* take-home dataset — the same `customers.csv`/`orders.csv`/`promotions.csv` at `data/` on the [`main`](../../tree/main/data) branch, byte-for-byte, ~30K orders — embedded so it loads in one click instead of a manual download+upload. Runs the full pipeline at real scale and should reproduce `revenue_analysis.ipynb`'s numbers exactly: total revenue **$16,153,512.87**, net revenue **$15,879,675.40**. This is the one to use to confirm the page isn't just working on toy data. |

The first three are small by design, so their findings are easy to read at a
glance. Larger, **downloadable** versions of those same three scenarios are
in [`sample_data/`](sample_data/), for testing the actual upload flow (not
just the one-click embedded demo):

| Folder | Rows | Designed to trigger |
|---|---|---|
| `clean_batch/` | 8 customers, 42 orders, 9 promotions | Every stage passes clean — a baseline for "what does a good run look like." |
| `dirty_batch/` | 6 customers, 35 orders, 4 promotions | Duplicate rows, a **conflicting** duplicate `order_id` (same id, different values — deliberately *not* an exact duplicate), an orphaned `customer_id`, a malformed date, a negative amount, a zero amount, an unrecognized status, and stacked promotions that exceed their order's amount. The conflicting duplicate survives filtering (only *exact* duplicates get auto-dropped — see `README_PIPELINE.md` on the `main` branch) and reaches final QC as a hard failure, so **this run halts and withholds the report** — see "What 'blocked' means," below. This is the one sample designed to demonstrate that, not just to show warnings. |
| `generalization_batch/` | 8 customers, 24 orders, 2 promotions, plus `returns.csv` | An undeclared table (`returns.csv`, referencing `orders.order_id`) with an exact-duplicate row and one orphaned `order_id` — profiled and cleaned generically, at `info` severity, without affecting the revenue numbers at all. |

There's no separate `sample_data/instruction_batch/` folder — the Instruction
batch button embeds the `data/` folder from `main` directly, so that folder
already *is* its downloadable form.

## What's in the Report section

Every run — clean, dirty, blocked, or missing a table — shows all three of
these, in order, before anything specific to whether there's a revenue
number to report:

**Data health.** A single status pill — `Healthy`, `Clean, with notes`,
`Needs review`, or `Blocked` — plus three concrete numbers: the fail/warn/info
issue count across *both* QC passes combined, what percentage of the primary
table's rows (`orders`, if present) survived from ingestion through to the
final table, and how many rows filtering dropped. The status is derived
directly from issue severity (any `fail` → Blocked; any `warn` → Needs review;
only `info` → Clean, with notes; nothing → Healthy) — it's not a fabricated
score, just a direct readout of what the two QC passes already found.

**QC comparison — before vs after.** A small counts table (fail/warn/info,
initial vs final) followed by the two full issue lists side by side: what
Stage 2 found in the raw data, and what Stage 6 found in the pipeline's own
output afterward. Reading them together shows what filtering/cleaning/
transform actually resolved (e.g. exact duplicates — gone by Stage 6) versus
what only got labeled and passed through (e.g. an unknown customer — still
counted, just tagged `Unknown`, forever).

**What the pipeline changed.** Three short, data-derived lists — Filtering,
Cleaning, Transform — written in plain language from the actual run, not
templated text: which filter rules dropped rows and how many, what got
type-cast or collapsed during cleaning (including exactly how many
promotion rows collapsed into how many order-level rows), and — if the
revenue transform ran — how many orders referenced an unknown customer and
how many discounts got capped, with the dollar amount.

These three are why the QC report download (below) is worth opening even on
a run that produced a full, boring, healthy report: it's the same "what
actually happened to my data" narrative, saved to a file.

## What "blocked" means

The Python pipeline's `run.py` calls `final_qc.raise_if_failed()` before
writing any output — a hard (`fail`-severity) finding in the final QC stage
means the run stops and nothing gets written, on the theory that a report
built on data that failed its own validation isn't worth trusting just
because it happens to compute. This page mirrors that: if the final QC stage
finds a hard failure, Stage 7 shows **BLOCKED**, and the revenue dashboard —
stat tiles, segment chart, top-5 table — is replaced with a note explaining
why, instead of numbers computed from an unvalidated table. The data health,
QC comparison, and change summary above it are unaffected and still fully
populated (and still downloadable) — they describe what happened during the
run, which is true and useful information regardless of whether the final
number is trustworthy. The `dirty_batch/` sample above is built specifically
to demonstrate this path — everything else in the repo's sample data produces
a full report.

## About the PDF download

The first version of this button used `window.print()`: clone the rendered
report into a hidden container, open a new tab, print it. That depends on
permissions a page can't grant itself — a sandboxed embed without
`allow-popups`/`allow-modals` swallows both `window.open()` and
`window.print()` silently, no error, the click just does nothing. That's
exactly the failure this replaced.

**Download QC report (.pdf)** now hand-generates an actual PDF file — real
bytes, built by string concatenation in `buildPdf()`, no library, no CDN
(both would need a server or violate the page's content-security policy).
It uses one of PDF's 14 standard fonts (Courier — built into every PDF
reader, needs no embedding) and lays out the same markdown report text used
for the `.md` download as wrapped, paginated monospace lines. The resulting
bytes go through the *exact* same `downloadBlob()` path already used for
`.md`/`.json` — no `window.print`, no `window.open`, nothing that needs a
permission a sandboxed context might withhold.

The one real constraint this creates: PDF's standard fonts don't cover
arbitrary Unicode — no curly quotes, no checkmarks — and `Blob([string])`
encodes as UTF-8, which only equals the intended ASCII bytes for code points
under 0x80. `sanitizeAscii()` maps this report's actual Unicode characters
(em dashes, curly quotes, the checkmark, a handful of others — see
`PDF_ASCII_MAP` in `index.html`) to plain ASCII, and falls back to `?` for
anything unexpected, so the file is always well-formed even if some future
report text uses a character nobody thought to map. Verified with a real
PDF parser (`pdfjs-dist` — Firefox's own PDF engine), not just "it looks like
a PDF": generated one from the actual ~30K-row dataset's report, parsed it
back, and diffed the extracted text against the source — exact match,
correct 2-page pagination, zero non-ASCII bytes in the file. Same check
against a **blocked** run's report (health/comparison/change-summary intact,
the BLOCKED explanation in place of numbers) and a couple of pathological
inputs (empty text, a single 500-character unbroken "word") — all produced
valid, parseable PDFs.

## If a download button doesn't seem to do anything

Some contexts a browser can run this page in — a sandboxed iframe without
download permission, certain restricted embeds — can still silently swallow
a triggered `<a download>` click: it fires, nothing throws, no file appears.
There's no reliable way for the page to detect that happened, so every one
of the three download buttons pairs its attempt with an always-shown
fallback panel containing the report's text and a **Copy to clipboard**
button. If the save dialog you expected doesn't show up, that panel is where
the report still is — copy it out, or open the
[hosted site](https://advaitnaresh.github.io/wilson-revenue-takehome/) (or
`index.html` locally) directly in an ordinary, non-embedded browser tab,
where this is far less likely to come
up at all.

## How this maps to the Python pipeline

Same seven stages, same severity model (`fail` / `warn` / `info`, plus an
implicit "pass" when a stage finds nothing). The table below is the map
between this file's JS functions and the Python modules they mirror — keep
both sides in sync if you change a rule in one place.

| Stage | Python | JS (in `index.html`, first `<script>` block) |
|---|---|---|
| 1. Ingest | `pipeline/ingest.py: load_raw` | `ingest()` |
| 2. QC (raw) | `pipeline/qc.py: run_initial_qc` | `runInitialQC()` |
| 3. Filter | `pipeline/filter.py: apply_filters` | `applyFilters()` |
| 4. Clean | `pipeline/clean.py: clean_all` | `cleanAll()` |
| 5. Transform | `pipeline/transform.py` | `buildAnalysisTable()`, `computeTotalRevenue()`, `computeRevenueBySegment()`, `computeTopCustomers()` |
| 6. QC (final) | `pipeline/qc.py: run_final_qc` | `runFinalQC()` |
| generic-table inference | `pipeline/introspect.py` | `inferNumericColumns()`, `inferDateColumns()`, `inferPrimaryKey()` |

**Deliberate simplifications**, made because this is a demo tool for
datasets that fit comfortably in a browser tab, not a scale target:

- No sampling for schema inference on undeclared tables — the JS engine just
  looks at every row, because in-browser datasets are small enough that doing
  so costs nothing. (`pipeline_spark/` samples specifically because *its*
  datasets might not be.)
- The "conflicting duplicate key" detail pandas surfaces
  (`groupby(key).nunique()` — do the duplicate rows actually disagree on any
  field) isn't separately reported here; a duplicate key is just a duplicate
  key. It still reaches final QC as a hard failure if it survives filtering,
  same as the Python version.
- No file-size guard beyond what a browser tab can hold. `data/`'s ~30K-row
  dataset runs in well under a second; a truly huge dataset belongs in
  `pipeline_spark/`, not a browser tab.

## Design notes

Palette and type choices are documented as CSS custom properties at the top
of `index.html` (`:root`, plus the two dark-mode blocks) — an "inspection
line" visual language (manifest cards, severity badges, a schematic of the
pipeline as literal stages-in-sequence) rather than a generic dashboard
template. The categorical chart palette (revenue-by-segment) and the four
status colors (fail/warn/info/pass) were chosen and checked for
colorblind-safe separation using the validator in Anthropic's `dataviz`
skill; segment colors are assigned by name, not by sort order, so a
segment's color never changes depending on which one happens to lead that run.

## Files

```
index.html            the entire app — HTML, CSS, and JS, self-contained
sample_data/
  clean_batch/         customers.csv, orders.csv, promotions.csv
  dirty_batch/         same three, deliberately dirty (see table above)
  generalization_batch/  clean data + returns.csv, an undeclared table
README.md              this file
```

`index.html` is about 1.5MB — most of that is the Instruction batch sample
(the full ~30K-row dataset, embedded verbatim so it loads with one click).
Nothing else in the file is large; the app itself is a few hundred KB of
HTML/CSS/JS.
