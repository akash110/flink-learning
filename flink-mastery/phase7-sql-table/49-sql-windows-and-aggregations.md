# 49. SQL Windows and Aggregations

Phase 2 taught you `TumblingEventTimeWindows.of(Time.minutes(10))`. This chapter is the SQL equivalent — plus two window types the DataStream API doesn't give you for free, and the aggregation patterns (Top-N, dedup, `OVER`) that are far easier in SQL than in Java.

---

## Window TVFs — the modern syntax

A **window table-valued function** takes a table in and returns a table out, with three extra columns bolted on.

```sql
SELECT *
FROM TABLE(
    TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '10' MINUTES));
```

Anatomy:

```
FROM TABLE( TUMBLE( TABLE transactions , DESCRIPTOR(ts) , INTERVAL '10' MINUTES ) )
     │       │       │                    │                │
     │       │       │                    │                └─ window size
     │       │       │                    └─ which column is the time attribute
     │       │       └─ the input, wrapped in the TABLE() keyword
     │       └─ the window function
     └─ the outer TABLE() that makes the whole thing a table expression
```

- The outer `TABLE(...)` is required SQL-standard syntax for calling a TVF in a `FROM` clause.
- `TABLE transactions` — the inner one marks the argument as a *table*, not a scalar. Both are needed. Forgetting one is the most common syntax error here.
- `DESCRIPTOR(ts)` — names a column. `ts` must be a **time attribute** (chapter 48) or you get *"The window function requires the timecol is a time attribute type."*
- Intervals: `INTERVAL '10' MINUTES`, `INTERVAL '1' HOUR`, `INTERVAL '30' SECOND`, `INTERVAL '1' DAY`.

The three appended columns:

| Column | Type | Meaning |
|---|---|---|
| `window_start` | `TIMESTAMP(3)` / `TIMESTAMP_LTZ(3)` | Inclusive start |
| `window_end` | same | **Exclusive** end |
| `window_time` | same, **and a time attribute** | `window_end - 1ms`. Usable as the rowtime for a *downstream* window. |

> **Key idea:** `window_time` is what makes windows composable. A windowed aggregation destroys nothing — it hands you a *new* time attribute so you can window the window results. `window_end` itself is a plain timestamp and cannot be used that way.

The TVF alone just tags rows. The useful form adds the aggregation:

```sql
SELECT
    userId,
    window_start,
    window_end,
    SUM(amount)   AS total,
    COUNT(*)      AS cnt,
    MAX(amount)   AS biggest
FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '10' MINUTES))
GROUP BY userId, window_start, window_end;
```

- `GROUP BY userId, window_start, window_end` — **both** window columns must be in the `GROUP BY`. This is the shape the planner pattern-matches to produce a `WindowAggregate` operator. Group by only `window_start` and you get an ordinary retracting `GROUP BY` with unbounded state instead.
- Result is **append-only** (chapter 47): each window fires once when the watermark passes `window_end`, and never again.

---

## TUMBLE — fixed, non-overlapping

```sql
TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '10' MINUTES))
```

```
time ──────────────────────────────────────────────────────►
      10:00      10:10      10:20      10:30      10:40
        │          │          │          │          │
        ├──────────┤          │          │          │
        │ window 1 │          │          │          │
        │          ├──────────┤          │          │
        │          │ window 2 │          │          │
        │          │          ├──────────┤          │
        │          │          │ window 3 │          │
        └──────────┴──────────┴──────────┴──────────┘

  every record belongs to EXACTLY ONE window
  boundaries are epoch-aligned: [10:00, 10:10)
```

Optional fourth argument, the offset:

```sql
TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '1' DAY, INTERVAL '8' HOUR))
```

Daily windows starting at 08:00 instead of midnight. Use for business days that don't align to UTC midnight, though setting `table.local-time-zone` is usually the cleaner fix.

You may also write the arguments by name, which is clearer for the four-argument form:

```sql
TABLE(TUMBLE(DATA => TABLE transactions,
             TIMECOL => DESCRIPTOR(ts),
             SIZE => INTERVAL '1' DAY,
             OFFSET => INTERVAL '8' HOUR))
```

---

## HOP — sliding / overlapping

```sql
SELECT userId, window_start, window_end, SUM(amount) AS total
FROM TABLE(HOP(TABLE transactions, DESCRIPTOR(ts),
               INTERVAL '5' MINUTES,      -- SLIDE: how often a new window starts
               INTERVAL '15' MINUTES))    -- SIZE:  how long each window is
GROUP BY userId, window_start, window_end;
```

**Argument order is slide first, then size.** Getting them backwards is a silent logic bug — you'd get 15-minute-apart 5-minute windows, which is a gap, not an overlap, and Flink will accept it.

```
SLIDE = 5 min, SIZE = 15 min
time ────────────────────────────────────────────────────────►
      10:00  10:05  10:10  10:15  10:20  10:25  10:30

  W1  ├────────────────────┤                 [10:00, 10:15)
  W2         ├────────────────────┤          [10:05, 10:20)
  W3                ├────────────────────┤   [10:10, 10:25)
  W4                       ├────────────────────┤

  a record at 10:12 lands in W1, W2, AND W3
  duplication factor = SIZE / SLIDE = 3
```

The cost is the duplication factor. `SIZE / SLIDE` copies of every record are held in state and every record is aggregated that many times. A 1-hour window sliding every 10 seconds is 360× — that job will not survive.

Use for: smoothed moving averages, "in any 15-minute period" alert rules.

---

## CUMULATE — the one people don't know

Windows that all share a start and grow to a maximum. This is *the* answer to "running total so far today, updated every hour."

```sql
SELECT window_start, window_end, SUM(amount) AS running_total
FROM TABLE(CUMULATE(TABLE transactions, DESCRIPTOR(ts),
                    INTERVAL '1' HOUR,     -- STEP: how often to emit
                    INTERVAL '1' DAY))     -- MAX SIZE: total window length
GROUP BY window_start, window_end;
```

**Step first, then max size.** `MAX SIZE` must be an exact multiple of `STEP`, or you get *"Cumulative table function requires maxSize must be an integral multiple of step."*

```
STEP = 1 hour, MAX SIZE = 1 day
time ───────────────────────────────────────────────────────────►
     00:00                                                  24:00

  W1  ├──┤                                    [00:00, 01:00)
  W2  ├─────┤                                 [00:00, 02:00)
  W3  ├────────┤                              [00:00, 03:00)
  W4  ├───────────┤                           [00:00, 04:00)
   ...
  W24 ├──────────────────────────────────────┤ [00:00, 24:00)

  then it resets and a new day begins at 00:00
  every window has the SAME start; only the end grows
```

Output, one row per hour:

```
window_start          window_end            running_total
2026-08-29 00:00:00   2026-08-29 01:00:00   1240.50
2026-08-29 00:00:00   2026-08-29 02:00:00   3100.75
2026-08-29 00:00:00   2026-08-29 03:00:00   4890.00
...
2026-08-29 00:00:00   2026-08-30 00:00:00   58210.25
2026-08-30 00:00:00   2026-08-30 01:00:00   980.00     ← new day, reset
```

Why this matters: the alternative is a non-windowed `GROUP BY DATE(ts)`, which is retract-mode with state that never expires and emits on *every* record. `CUMULATE` is append-only, emits on a fixed schedule, and cleans itself up at the end of each day. For every "so far today / so far this hour" dashboard, `CUMULATE` is the correct tool.

Note that unlike `HOP`, `CUMULATE` does **not** duplicate records across windows in state — it keeps one accumulator and emits a snapshot at each step. It is genuinely cheap.

---

## SESSION — activity bursts (Flink 1.19+ as a TVF)

Windows defined by a gap of inactivity, not by the clock.

```sql
SELECT userId, window_start, window_end, COUNT(*) AS events
FROM TABLE(SESSION(TABLE transactions PARTITION BY userId,
                   DESCRIPTOR(ts),
                   INTERVAL '10' MINUTES))       -- GAP
GROUP BY userId, window_start, window_end;
```

- `PARTITION BY userId` — **required**, and it goes inside the inner `TABLE` argument, not as a separate parameter. Sessions are inherently per-key; there is no global session TVF.
- `INTERVAL '10' MINUTES` is the **gap**: 10 minutes with no event for this user closes the session.

```
GAP = 10 min, one user
time ────────────────────────────────────────────────────────────►
   e  e   e        e                              e  e     e
   │  │   │        │                              │  │     │
   └──┴───┴────────┘   ← gap > 10 min →           └──┴─────┘
     session 1                                      session 2

  windows have variable length and variable start
```

Sessions **merge**: an event arriving between two existing sessions can join them into one, so the operator holds and merges partial windows. More state churn than tumbling.

**Version note:** the `SESSION` window TVF arrived in **Flink 1.19**. On 1.18 you must use the legacy grouped form below.

---

## The legacy syntax (deprecated — you will still see it)

Before window TVFs, windows were grouped-window *functions* used directly in `GROUP BY`:

```sql
-- OLD. Deprecated. Do not write new queries this way.
SELECT userId,
       TUMBLE_START(ts, INTERVAL '10' MINUTES) AS wstart,
       TUMBLE_END(ts, INTERVAL '10' MINUTES)   AS wend,
       SUM(amount)
FROM transactions
GROUP BY userId, TUMBLE(ts, INTERVAL '10' MINUTES);
```

The family: `TUMBLE` / `HOP` / `SESSION` in `GROUP BY`, with `TUMBLE_START`, `TUMBLE_END`, `TUMBLE_ROWTIME`, `TUMBLE_PROCTIME` (and `HOP_*`, `SESSION_*`) in the `SELECT`.

Why the TVF form replaced it:

| | Legacy grouped window | Window TVF |
|---|---|---|
| `CUMULATE` | ✗ not available | ✓ |
| Window Top-N | ✗ | ✓ |
| Window join | ✗ | ✓ |
| Window deduplication | ✗ | ✓ |
| Access to window columns | via `TUMBLE_START(...)` helpers | plain columns `window_start` / `window_end` |
| Batch/stream unified | partially | fully |
| Local-global two-phase agg | limited | ✓ (`table.optimizer.agg-phase-strategy`) |

You will meet the legacy form constantly in blog posts and Stack Overflow answers written before Flink 1.13. **Recognize it, translate it, don't write it.** It still runs in 1.20 but is documented as deprecated.

---

## Windows on processing time

Swap the descriptor column for the `PROCTIME()` one. Nothing else changes:

```sql
SELECT window_start, window_end, COUNT(*)
FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(proc), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;
```

Fires on the system clock. No watermark needed, no late data, no reproducibility.

---

## Window Top-N and window deduplication

Because a window TVF produces a normal table with `window_start`/`window_end`, you can rank *within* each window:

```sql
-- top 3 spenders per 10-minute window
SELECT userId, window_start, window_end, total
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end
                            ORDER BY total DESC) AS rn
  FROM (
    SELECT userId, window_start, window_end, SUM(amount) AS total
    FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '10' MINUTES))
    GROUP BY userId, window_start, window_end
  )
)
WHERE rn <= 3;
```

Read it inside-out:

1. Innermost: per-user totals per window. Append-only.
2. Middle: rank users within each window by total, descending.
3. Outer: keep the top 3.

**This is append-only**, because `PARTITION BY` includes the window columns — the ranking within a completed window is final. That is the crucial difference from a global Top-N (below), which is not.

---

## Non-windowed GROUP BY — and why it's dangerous

```sql
SELECT userId, SUM(amount) AS total, COUNT(*) AS cnt
FROM transactions
GROUP BY userId;
```

Simple, and a production hazard:

- **Retract mode.** Two output rows per update (chapter 47). Needs an upsert-capable sink.
- **Emits on every input record.** A million records/sec in becomes two million rows/sec out. There is no batching or throttling by default.
- **Unbounded state.** One accumulator per distinct `userId`, retained forever, because a record for any past user could arrive tomorrow.

```
state size = (distinct users) × (accumulator size)
             ↑
             grows monotonically. Nothing ever removes an entry.
```

The mitigation, from chapter 47:

```sql
SET 'table.exec.state.ttl' = '36 h';
```

State for a key untouched for 36 hours is dropped. If that user returns on day 3, the sum restarts from zero and you get a `+I` where semantics demanded `+U`. **This is a deliberate correctness trade.** Choose the TTL from your real key-recurrence distribution.

Two more knobs worth knowing for this operator:

```sql
-- batch updates: hold results in a buffer and emit at most every 1 second per key
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1 s';
SET 'table.exec.mini-batch.size' = '5000';

-- split a skewed aggregation into two phases (helps a hot key)
SET 'table.optimizer.distinct-agg.split.enabled' = 'true';
```

- **Mini-batch** buffers records and applies them to state in groups. Massively reduces state access and output volume for hot keys, at the cost of up to `allow-latency` extra delay. All three settings are required together — `size` is the buffer size per operator.
- **`distinct-agg.split`** rewrites `COUNT(DISTINCT x)` into a two-phase aggregation, which is the SQL analogue of the salting you learned for skew.

> **Key idea:** If a windowed formulation expresses your requirement, use it. Windowed = append + bounded state + one emission per window. Non-windowed = retract + unbounded state + emission per record. The difference is not stylistic.

---

## OVER window aggregations — running totals per row

An `OVER` window emits **one output row per input row**, with an aggregate computed over a range of preceding rows. Append-only, no retractions.

```sql
SELECT
    userId,
    ts,
    amount,
    SUM(amount) OVER w  AS running_total,
    AVG(amount) OVER w  AS running_avg,
    COUNT(*)    OVER w  AS n
FROM transactions
WINDOW w AS (
    PARTITION BY userId
    ORDER BY ts
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
);
```

- `WINDOW w AS (...)` — a named window definition, so three aggregates share one spec. Inlining `OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ...)` on each aggregate works too but repeats itself.
- `PARTITION BY userId` — separate running total per user. Omit it and you get one global partition (parallelism 1 — avoid).
- **`ORDER BY ts` must be on a time attribute, ascending.** Flink cannot buffer and re-sort an infinite stream. `ORDER BY amount` gives *"Over Agg: The window rank function without order by is not supported"* / an ordering validation error.
- `ROWS BETWEEN 9 PRECEDING AND CURRENT ROW` — a **count-based** range: the last 10 rows for this user.

The two range types:

```sql
ROWS  BETWEEN 9 PRECEDING AND CURRENT ROW               -- last 10 ROWS
RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW  -- last 5 MINUTES of rows
ROWS  BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW       -- everything so far (unbounded state!)
```

```
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
──────────────────────────────────────────────
rows:   r1   r2   r3   r4   r5   r6
                        └────┬────┘
                     when r5 is processed,
                     the frame is {r3, r4, r5}
        exactly 3 rows regardless of their timestamps


RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
──────────────────────────────────────────────
time:  10:00  10:01  10:02          10:09  10:10
rows:   r1     r2     r3             r4     r5
                                     └───┬──┘
                      when r5 is processed at 10:10,
                      the frame is {r4, r5} — r1..r3 are
                      older than 10:05 and have fallen out
        variable row count, fixed time span
```

- `CURRENT ROW` must be the upper bound. Flink does not support `FOLLOWING` in streaming — it would require seeing the future.
- `UNBOUNDED PRECEDING` gives a true running total from the beginning of time, with state that never shrinks. Governed by `table.exec.state.ttl`. Use `RANGE ... PRECEDING` instead when you can — its state is self-cleaning via the watermark.

Very common practical use — the fraud rule from phase 4, in three lines:

```sql
SELECT userId, ts, amount, cnt_5min
FROM (
  SELECT userId, ts, amount,
         COUNT(*) OVER (PARTITION BY userId ORDER BY ts
                        RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW) AS cnt_5min
  FROM transactions
  WHERE type = 'purchase'
)
WHERE cnt_5min >= 5;
```

"Alert whenever a user makes their 5th purchase within any 5-minute span." In the DataStream API this was a `KeyedProcessFunction` with a `ListState` and timers.

---

## Deduplication with ROW_NUMBER

The canonical pattern for "one row per key":

```sql
-- KEEP FIRST: the earliest record per userId
SELECT userId, type, amount, ts
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts ASC) AS rn
  FROM transactions
)
WHERE rn = 1;
```

The planner recognizes this exact shape — `ROW_NUMBER()` partitioned by a key, ordered by a **time attribute**, filtered to `rn = 1` — and compiles it to a dedicated `Deduplicate` operator, not a general ranking operator.

> **Key idea:** `ORDER BY <time attribute> ASC` + `rn = 1` = **deduplicate, keep first**. It is **append-only**: once the first row for a key is emitted, nothing can beat it, so nothing is ever retracted. State is one boolean-ish marker per key.
>
> `ORDER BY <time attribute> DESC` + `rn = 1` = **deduplicate, keep last**. This is **upsert** mode: every later row replaces the previous one, so the operator emits `+U` (and stores the current winner).

```
KEEP FIRST (ASC)                    KEEP LAST (DESC)
────────────────────────            ────────────────────────
u-001 @10:00  → +I (10:00)          u-001 @10:00  → +I (10:00)
u-001 @10:05  → (suppressed)        u-001 @10:05  → +U (10:05)
u-001 @10:09  → (suppressed)        u-001 @10:09  → +U (10:09)

append-only, tiny state             upsert, stores current row
```

Keep-first is what you want for **idempotent ingestion** — a Kafka topic with at-least-once producers that occasionally duplicates a message. Keep-last is what you want for **CDC-style latest-state** — the current row per primary key.

`ORDER BY` on a non-time column here is legal but turns the query into a general Top-N (retract mode, more state). The planner only produces the cheap deduplicate operator for time-ordered ranking.

---

## Global Top-N

Same construct, `rn <= N`, no window in the partition:

```sql
SELECT userId, total, rn
FROM (
  SELECT userId, total,
         ROW_NUMBER() OVER (ORDER BY total DESC) AS rn
  FROM (
    SELECT userId, SUM(amount) AS total FROM transactions GROUP BY userId
  )
)
WHERE rn <= 10;
```

"The 10 biggest spenders, right now, continuously."

Facts about this query:

- **Update mode.** The ranking shifts constantly; a user entering the top 10 pushes another out, so rows are retracted or upserted.
- **State: N rows**, plus the upstream `GROUP BY` state (one accumulator per user, unbounded — that's the expensive half, not the Top-N).
- **The rank column changes.** If `rn` is in the `SELECT` list, moving from rank 4 to rank 3 emits a change for *both* rows. Omit `rn` from the output when you don't need it — Flink then only emits the affected row, roughly halving the output. This is a real, documented optimization and worth knowing.
- Only `ROW_NUMBER()` is supported for streaming Top-N. `RANK()` and `DENSE_RANK()` are not.
- `WHERE rn <= 10` must be a constant comparison on the rank column, in exactly this position, or the planner won't recognize the pattern and will complain that ranking without a limit is unbounded.

Sink for this must be upsert-capable with `PRIMARY KEY (userId)`.

For a *bounded* alternative, prefer the **window Top-N** shown earlier — per-window rankings are append-only and their state expires with the window.

---

## The window state knob

Windowed aggregations clean their state when the watermark passes `window_end`. But they can hold a little longer:

```sql
SET 'table.exec.window-agg.buffer-size-limit' = '100';   -- rows buffered before flushing to state
```

Small tuning knob; the default of 100 is fine almost always. The real lever on window state is the window size and, for `HOP`, the `SIZE/SLIDE` duplication factor.

---

## Cheat sheet

| Need | Construct | Mode | State |
|---|---|---|---|
| Fixed non-overlapping buckets | `TUMBLE` TVF | append | bounded by window |
| Overlapping / moving average | `HOP` TVF | append | bounded × SIZE/SLIDE |
| "So far today / this hour" | `CUMULATE` TVF | append | bounded, one accumulator |
| Activity bursts, variable length | `SESSION` TVF (1.19+) | append | bounded by gap, merging |
| Running total per row, last N rows | `OVER ... ROWS BETWEEN` | append | N rows per key |
| Running total per row, last N minutes | `OVER ... RANGE BETWEEN` | append | self-cleaning by watermark |
| Current total per key, forever | `GROUP BY key` | **retract** | **unbounded — needs TTL** |
| One row per key, earliest wins | `ROW_NUMBER() ORDER BY ts ASC, rn=1` | append | tiny |
| One row per key, latest wins | `ROW_NUMBER() ORDER BY ts DESC, rn=1` | upsert | one row per key |
| Top N per window | `ROW_NUMBER() PARTITION BY window_start,...` | append | N per window |
| Top N overall, continuous | `ROW_NUMBER() ORDER BY metric DESC, rn<=N` | upsert | N + upstream agg state |

---

## Remember

- Window TVF syntax: `FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '10' MINUTES))`. Two `TABLE` keywords — the outer one and the argument one.
- `DESCRIPTOR(ts)` must name a **time attribute**.
- Always `GROUP BY window_start, window_end` (plus your keys). Both columns, or you silently get a plain retracting `GROUP BY`.
- `window_time` = `window_end - 1ms` and is itself a time attribute — that's how you chain windows.
- `HOP(t, d, SLIDE, SIZE)` — **slide first**. Duplication factor = SIZE/SLIDE; that's your state multiplier.
- `CUMULATE(t, d, STEP, MAXSIZE)` — **step first**, and MAXSIZE must be an integral multiple of STEP. The right tool for "running total so far today".
- `SESSION` needs `PARTITION BY key` inside the `TABLE(...)` argument, and is a TVF only from Flink 1.19.
- Legacy `GROUP BY TUMBLE(ts, INTERVAL ...)` with `TUMBLE_START()` is deprecated — recognize it, translate it, don't write it. It cannot express `CUMULATE`, window Top-N, or window joins.
- Non-windowed `GROUP BY`: retract mode, emits per record, state grows forever. Always pair with `table.exec.state.ttl`, and consider mini-batch.
- `OVER` windows emit one row per input row, append-only. `ORDER BY` must be an ascending time attribute; upper bound must be `CURRENT ROW`.
- `ROWS BETWEEN n PRECEDING` = count-based. `RANGE BETWEEN INTERVAL ... PRECEDING` = time-based and self-cleaning. `UNBOUNDED PRECEDING` = unbounded state.
- Dedup: `ROW_NUMBER() ... ORDER BY ts ASC` + `rn = 1` = keep-first, **append-only**. `DESC` = keep-last, **upsert**.
- Streaming Top-N supports `ROW_NUMBER()` only, needs a constant `rn <= N` filter, and is cheaper if you omit `rn` from the output.

**Interview one-liners**

- *"What's the modern window syntax in Flink SQL?"* → Window TVFs: `TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '10' MINUTES))` with `GROUP BY window_start, window_end`. The old `GROUP BY TUMBLE(...)` grouped-window form is deprecated.
- *"What is CUMULATE for?"* → Running totals within a bounded period — "so far today", emitted every hour. Same start, growing end, resets at the max size. Append-only, unlike the `GROUP BY DATE(ts)` alternative.
- *"HOP argument order?"* → Slide, then size. Reversing them is accepted and gives you gaps instead of overlaps.
- *"Why is a windowed aggregation cheaper than a plain GROUP BY?"* → The watermark tells the window it's complete, so it fires once, appends, and frees its state. A plain GROUP BY has no completion signal: retract mode, unbounded state.
- *"How do you deduplicate in Flink SQL?"* → `ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts ASC)` filtered to `rn = 1`. ASC keeps the first and is append-only; DESC keeps the latest and is upsert.
- *"Difference between ROWS and RANGE in an OVER window?"* → ROWS counts rows; RANGE spans time. RANGE cleans its own state as the watermark advances.
- *"How do you do a continuous Top 10?"* → `ROW_NUMBER() OVER (ORDER BY metric DESC)` with `WHERE rn <= 10`. Upsert mode, needs an upsert sink; leave `rn` out of the output to halve the emitted changes.
