# Data Analyst / SWE Co-op — Take-Home Exercise

**Time budget:** ~60-90 minutes as a rough guide — we're more interested in how you got to your answer than in hitting a specific number, so don't overthink getting the 'right numbers'. Please submit by Wed 8/12, if you need more time send me an email.

**AI use:** Allowed, Optional. Use whatever AI tools you'd normally use (ChatGPT, Claude, Cursor, etc.). Please submit your full chat log (a share-able chat log link or claude code `/export`) alongside your notebook if applicable.

---

## The Data

Three files are attached:

- `customers.csv` — customer_id, customer_name, segment, region
- `orders.csv` — order_id, customer_id, order_date, amount, status
- `promotions.csv` — promotion_id, order_id, promo_code, discount_amount

Join keys: `customer_id` links `customers` and `orders`. `order_id` links `orders` and `promotions`.

## What We Need

A Python or SQL notebook or script. Clean and inspect the data as you see fit (data libraries, e.g. pandas encouraged), then calculate:

1. **Total revenue**
2. **Revenue by customer segment**
3. **Top 5 customers by revenue**
4. **Net revenue** (after promotional discounts)

## How to Work
This is messy data, you will be required to make judgment calls about cleaning. As you go, note in markdown cells:

- Any cleaning or transformations you made, and why
- Any judgment calls where the data didn't have an obvious right answer, and how you decided to handle them

We're grading your reasoning at least as much as the final numbers. There isn't one "correct" way to handle every ambiguity in this data — we want to see how you think it through.

## Submission

Send back:
- Your notebook / script (.ipynb, .py, .sql) — code, outputs, and notes all in place
- Your full AI chat log
