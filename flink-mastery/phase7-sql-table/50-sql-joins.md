# 50. Joins in Streaming SQL

Batch SQL has one `JOIN`. Streaming SQL has **four**, they look almost identical, and they have wildly different state behaviour. Picking the wrong one is how a Flink SQL job runs beautifully for three weeks and then dies.

```
                 does the join have a TIME bound?
                              │
              ┌───────────────┴───────────────┐
             no                              yes
              │                               │
      REGULAR JOIN                  ┌─────────┴─────────┐
   both sides in state         time RANGE between    point-in-time
        forever                  the two streams        LOOKUP
     ⚠ unbounded                       │                  │
                              INTERVAL JOIN      ┌─────────┴─────────┐
                              bounded state   event time        processing time
                                                 │                  │
                                          TEMPORAL JOIN       LOOKUP JOIN
                                        (versioned table)   (external system)
```

> **Key idea:** In streaming, a join is a stateful operator that must remember rows from both sides so a future row on either side can find its match. The only question that matters is **when is Flink allowed to forget a row?** Each join type answers it differently.

---

## 1. Regular join — correct, and the most dangerous

```sql
SELECT o.orderId, o.amount, c.name, c.country
FROM orders o
JOIN customers c ON o.customerId = c.id;
```

Ordinary SQL. Flink runs it as a symmetric hash join.

**Mechanics.** Two keyed state maps, one per side:

```
        left state                       right state
   { customerId → [orders] }        { customerId → [customers] }

  order arrives  ──► store in left state
                 ──► probe right state, emit a joined row per match

  customer row arrives ──► store in right state
                       ──► probe LEFT state, emit joined rows for
                           every order already seen for that customer
```

That last step is the point: an order that arrived at 09:00 must still be in state at 17:00 in case its customer row shows up then.

**Consequences:**

- **Both sides are retained indefinitely.** State grows with the total volume of both streams, forever.
- **Retract mode** (chapter 47). With an outer join it is worse: `LEFT JOIN` emits `(order, NULL)` immediately if no match exists, then **retracts** it and emits `(order, customer)` when the match arrives. Two extra rows.
- **The time attribute is destroyed** on the output (chapter 48). You cannot window the result.

**The only mitigation:**

```sql
SET 'table.exec.state.ttl' = '24 h';
```

or, better, per-side hints (Flink 1.18+):

```sql
SELECT /*+ STATE_TTL('o' = '6h', 'c' = '30d') */
       o.orderId, o.amount, c.name
FROM orders o
JOIN customers c ON o.customerId = c.id;
```

Orders expire after 6 hours; the customer dimension is kept for 30 days. Asymmetric TTL is nearly always what you want, because the two sides have completely different retention requirements. The hint's aliases must match the table aliases in the query.

**When to use it:** when you genuinely need to join two unbounded streams with no time relationship, and you have accepted a TTL. If you're joining a stream to a slowly-changing dimension, you almost certainly want a temporal or lookup join instead — read on.

---

## 2. Interval join — bounded state, self-cleaning

Add a time predicate relating the two rowtimes, and the planner switches operators entirely.

```sql
SELECT o.orderId, o.amount, s.shipmentId, s.carrier
FROM orders o, shipments s
WHERE o.orderId = s.orderId
  AND s.ship_ts BETWEEN o.order_ts AND o.order_ts + INTERVAL '4' HOUR;
```

- `o.orderId = s.orderId` — the equi-join key. Required; the interval alone isn't enough.
- `s.ship_ts BETWEEN o.order_ts AND o.order_ts + INTERVAL '4' HOUR` — the **interval condition**. Both columns must be event-time attributes on their tables.
- The comma-join + `WHERE` form and the explicit `JOIN ... ON` form are equivalent; put the interval in `ON` if you prefer:

```sql
FROM orders o JOIN shipments s
  ON o.orderId = s.orderId
 AND s.ship_ts BETWEEN o.order_ts AND o.order_ts + INTERVAL '4' HOUR
```

**Why state is bounded:**

```
watermark ──────────────────────────────────────►  now = 14:00
                                                          │
  an order at 09:00 can only match shipments in           │
  [09:00, 13:00].  Watermark is past 13:00, so:           │
  ─────────────────────────────────────────────           │
  order @ 09:00  →  DELETE from state                     │
  order @ 12:00  →  keep (window runs to 16:00)           │
```

Once the watermark passes `order_ts + upper bound`, that order can never match again and Flink drops it. State is proportional to *the join interval × the arrival rate*, not to total history.

**Output is append-only.** A match is final. The time attribute survives, so you can window the result.

Other valid interval shapes:

```sql
-- symmetric: within 10 minutes either way
AND a.ts BETWEEN b.ts - INTERVAL '10' MINUTE AND b.ts + INTERVAL '10' MINUTE

-- comparison form, equivalent to a BETWEEN
AND a.ts >= b.ts - INTERVAL '5' MINUTE AND a.ts <= b.ts

-- one-sided using timestamp arithmetic
AND b.ts >= a.ts AND b.ts < a.ts + INTERVAL '1' HOUR
```

**When to use it:** correlating two event streams that are causally related within a known time budget — order→shipment, click→conversion, request→response, login→purchase. This is the SQL equivalent of the DataStream `intervalJoin` from phase 6.

**Watch out:** if your interval predicate references a column that is *not* a time attribute, the planner silently produces a **regular join** with an ordinary filter. Same results, unbounded state. Check with `EXPLAIN` — you want `IntervalJoin` in the plan, not `Join(joinType=[InnerJoin])`.

---

## 3. Temporal join — the correct way to enrich with a changing dimension

The classic problem: convert order amounts to USD using **the exchange rate that was in effect when the order happened**, not the rate now.

A regular join gets this wrong. Rates change; a regular join gives you whatever rate row is currently in state, which is non-deterministic on reprocessing.

### Step 1: a versioned table

A **versioned table** is a dynamic table that tracks the history of a key. Flink builds one automatically from any table that has:

1. a **PRIMARY KEY**, and
2. an **event-time attribute with a watermark**.

```sql
CREATE TABLE currency_rates (
  currency    STRING,
  rate        DECIMAL(10, 4),
  update_ts   TIMESTAMP(3),
  WATERMARK FOR update_ts AS update_ts - INTERVAL '10' SECOND,
  PRIMARY KEY (currency) NOT ENFORCED
) WITH (
  'connector'    = 'upsert-kafka',
  'topic'        = 'currency-rates',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format'   = 'json',
  'value.format' = 'json'
);
```

- `PRIMARY KEY (currency) NOT ENFORCED` — the version key. **Without this the temporal join will not plan.**
- `WATERMARK FOR update_ts` — the version time. Without it, likewise.
- `upsert-kafka` because the rate table is a changelog: each new message replaces the previous rate for that currency.

Flink now internally maintains, per currency, a history:

```
currency = EUR
  ┌────────────────────┬────────┐
  │ valid from         │ rate   │
  ├────────────────────┼────────┤
  │ 09:00:00           │ 1.0800 │
  │ 11:30:00           │ 1.0850 │
  │ 14:15:00           │ 1.0790 │   ← current version
  └────────────────────┴────────┘
```

### Step 2: the join

```sql
SELECT
    o.orderId,
    o.amount,
    o.currency,
    r.rate,
    o.amount * r.rate AS amount_usd,
    o.order_ts
FROM orders AS o
LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF o.order_ts AS r
       ON o.currency = r.currency;
```

- `FOR SYSTEM_TIME AS OF o.order_ts` — "give me the version of `currency_rates` that was current at `o.order_ts`". `o.order_ts` must be the **event-time attribute of the probe side**.
- `ON o.currency = r.currency` — must be an equality on the versioned table's **primary key**. Not a subset, not an extra predicate on the key.
- `LEFT JOIN` is recommended: an order for a currency with no rate yet still comes through, with `NULL` rate, instead of vanishing.

```
orders (probe)                 currency_rates (versioned)
──────────────────────         ───────────────────────────
o1  EUR  10:00  ────────────►  looks up EUR @ 10:00 → 1.0800  ✓
o2  EUR  12:00  ────────────►  looks up EUR @ 12:00 → 1.0850  ✓
o3  EUR  15:00  ────────────►  looks up EUR @ 15:00 → 1.0790  ✓

  deterministic: re-run the job tomorrow, get the same numbers
```

**Properties:**

- **Append-only output.** The version is fixed by event time, so the result is never revised.
- **Bounded state.** Flink keeps versions of the dimension only back to the current watermark — older versions can no longer be needed and are pruned. It also buffers probe rows whose event time is ahead of the dimension's watermark, until the dimension catches up.
- **Deterministic and reprocessable.** This is the whole point.
- **The probe side's time attribute survives.**

**The failure mode:** the dimension side's watermark must advance. If `currency_rates` is a low-traffic topic that gets one message a day, its watermark barely moves, the join buffers orders waiting for it, and output stalls. Fix with `table.exec.source.idle-timeout` (chapter 48), or use a processing-time lookup join instead.

**Processing-time temporal join** is also possible against a versioned table, using `FOR SYSTEM_TIME AS OF <proctime>` — but then you always get the *latest* version and lose determinism. If you want that, a lookup join is usually the better tool.

---

## 4. Lookup join — enrich from an external system

Sometimes the dimension isn't a stream at all. It's a table in MySQL, or HBase, or Redis. A lookup join queries it **per record**.

```sql
CREATE TABLE customers (
  id      INT,
  name    STRING,
  country STRING,
  tier    STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url'       = 'jdbc:mysql://localhost:3306/crm',
  'table-name'= 'customers',
  'username'  = 'flink',
  'password'  = 'secret',

  -- lookup cache
  'lookup.cache'                    = 'PARTIAL',
  'lookup.partial-cache.max-rows'   = '50000',
  'lookup.partial-cache.expire-after-write'  = '10 min',
  'lookup.partial-cache.expire-after-access' = '5 min',
  'lookup.partial-cache.cache-missing-key'   = 'true',
  'lookup.max-retries'              = '3'
);
```

```sql
SELECT o.orderId, o.amount, c.name, c.country, c.tier
FROM orders AS o
LEFT JOIN customers FOR SYSTEM_TIME AS OF o.proc AS c
       ON o.customerId = c.id;
```

- `FOR SYSTEM_TIME AS OF o.proc` — `o.proc` is a **processing-time** attribute (`proc AS PROCTIME()`). This is what distinguishes a lookup join from a temporal join syntactically.
- Flink issues a `SELECT ... FROM customers WHERE id = ?` per incoming order (subject to the cache).

The cache options, which you must understand or you will hammer your database:

| Option | Meaning |
|---|---|
| `lookup.cache` | `NONE` (default) or `PARTIAL` or `FULL` |
| `lookup.partial-cache.max-rows` | Cache capacity per subtask |
| `lookup.partial-cache.expire-after-write` | TTL from insertion — bounds staleness |
| `lookup.partial-cache.expire-after-access` | TTL from last read — evicts cold keys |
| `lookup.partial-cache.cache-missing-key` | Cache "not found" too. **Turn this on** or every miss re-queries the DB forever. |
| `lookup.max-retries` | Retries on a failed lookup |

`lookup.cache = 'FULL'` loads the whole dimension into memory per subtask and refreshes it periodically (`lookup.full-cache.reload-strategy`, `lookup.full-cache.periodic-reload.interval`). Good for small, stable dimensions — country codes, product categories. Bad for anything that doesn't comfortably fit in a TaskManager's heap, multiplied by parallelism.

**Properties:**

- **Append-only output.**
- **No Flink state at all** — the "state" is the external database plus the cache.
- **Non-deterministic.** Re-running the job tomorrow reads today's dimension, not the historical one. If reproducibility matters, you need a temporal join.
- **Throughput is bounded by the external system.** Cache hit ratio is the metric to watch. A cold cache at parallelism 8 with 50k records/sec is 50k queries/sec at your MySQL.

**Async lookup:** some connectors support asynchronous lookups, configured on the connector (`lookup.async` where supported) or hinted:

```sql
SELECT /*+ LOOKUP('table'='c', 'async'='true', 'output-mode'='allow_unordered',
                  'capacity'='100', 'timeout'='180s') */
       o.orderId, c.name
FROM orders AS o
LEFT JOIN customers FOR SYSTEM_TIME AS OF o.proc AS c ON o.customerId = c.id;
```

This is the SQL equivalent of `AsyncDataStream.unorderedWait` from phase 6. `allow_unordered` lets results emit as they complete rather than in input order — much higher throughput, and safe when downstream doesn't depend on order.

The same hint carries retry configuration for the common "the dimension row hasn't been written yet" race:

```sql
/*+ LOOKUP('table'='c', 'retry-predicate'='lookup_miss',
           'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */
```

---

## Window join (bonus, Flink 1.15+)

Because window TVFs produce tables, you can join two *windowed* streams on their shared window:

```sql
SELECT L.userId, L.window_start, L.window_end, L.cnt, R.cnt
FROM (
  SELECT * FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(ts), INTERVAL '10' MINUTES))
) L
FULL JOIN (
  SELECT * FROM TABLE(TUMBLE(TABLE purchases, DESCRIPTOR(ts), INTERVAL '10' MINUTES))
) R
ON L.userId = R.userId
AND L.window_start = R.window_start
AND L.window_end = R.window_end;
```

The `window_start` and `window_end` equalities are **mandatory** — that's what makes it a window join rather than a regular join. Append-only, state bounded by the window. Useful for "clicks vs purchases in the same 10 minutes", including the `FULL JOIN` case where one side is empty.

---

## Comparison table

| | Regular join | Interval join | Temporal join | Lookup join |
|---|---|---|---|---|
| **Syntax marker** | plain `JOIN ... ON` | time `BETWEEN` predicate | `FOR SYSTEM_TIME AS OF <rowtime>` | `FOR SYSTEM_TIME AS OF <proctime>` |
| **Right side** | a stream | a stream | a versioned table (PK + watermark) | an external lookup source |
| **State** | ⚠ **both sides, forever** | bounded by the interval | dimension versions back to watermark | none (external + cache) |
| **Cleanup** | only `table.exec.state.ttl` | automatic, by watermark | automatic, by watermark | cache TTL |
| **Output mode** | retract | append | append | append |
| **Time attribute preserved** | ✗ | ✓ | ✓ (probe side) | ✓ (probe side) |
| **Deterministic on replay** | ✗ | ✓ | ✓ | ✗ |
| **Needs a PK on the right** | no | no | **yes** | yes (lookup key) |
| **Needs watermarks** | no | on both sides | on both sides | no |
| **Typical use** | last resort | order↔shipment correlation | currency rates, SCD enrichment | CRM/product dimension in MySQL |

**Decision procedure:**

1. Is the right side an external database? → **lookup join**.
2. Is the right side a slowly-changing dimension where the *historical* value matters? → **temporal join**.
3. Are both sides event streams that correlate within a known time budget? → **interval join**.
4. Otherwise → **regular join**, and set a TTL today, not after the incident.

---

## The classic currency-conversion example, end to end

```sql
-- ============ 1. the fact stream ============
CREATE TABLE orders (
  orderId    STRING,
  userId     STRING,
  amount     DECIMAL(12, 2),
  currency   STRING,
  `timestamp` BIGINT,
  order_ts   AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
  WATERMARK FOR order_ts AS order_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic'     = 'orders',
  'properties.bootstrap.servers' = 'localhost:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format'    = 'json'
);

-- ============ 2. the versioned dimension ============
CREATE TABLE currency_rates (
  currency   STRING,
  rate       DECIMAL(10, 4),
  update_ts  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR update_ts AS update_ts - INTERVAL '10' SECOND,
  PRIMARY KEY (currency) NOT ENFORCED
) WITH (
  'connector'    = 'upsert-kafka',
  'topic'        = 'currency-rates',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format'   = 'json',
  'value.format' = 'json'
);

-- ============ 3. the temporal join ============
CREATE VIEW orders_usd AS
SELECT
    o.orderId,
    o.userId,
    o.amount,
    o.currency,
    r.rate,
    o.amount * COALESCE(r.rate, CAST(1 AS DECIMAL(10,4))) AS amount_usd,
    o.order_ts
FROM orders AS o
LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF o.order_ts AS r
       ON o.currency = r.currency;

-- ============ 4. window the enriched, append-only result ============
SELECT
    window_start,
    window_end,
    currency,
    SUM(amount_usd) AS total_usd,
    COUNT(*)        AS orders
FROM TABLE(TUMBLE(TABLE orders_usd, DESCRIPTOR(order_ts), INTERVAL '1' HOUR))
GROUP BY window_start, window_end, currency;
```

Step 4 only works because the temporal join **preserved** `order_ts` as a time attribute. Swap step 3 for a regular join and step 4 fails with *"The window function requires the timecol is a time attribute type"* (chapter 48). That single fact is the strongest practical argument for temporal joins over regular ones.

`COALESCE(r.rate, 1)` handles the `LEFT JOIN`'s NULL for an unknown currency — decide explicitly rather than letting `NULL` poison the `SUM`.

---

## Checking which join you actually got

Never assume. Ask the planner:

```sql
EXPLAIN
SELECT o.orderId, s.carrier
FROM orders o, shipments s
WHERE o.orderId = s.orderId
  AND s.ship_ts BETWEEN o.order_ts AND o.order_ts + INTERVAL '4' HOUR;
```

In the physical plan you are looking for the operator name:

```
IntervalJoin(joinType=[InnerJoin], windowBounds=[isRowTime=true,
             leftLowerBound=-14400000, leftUpperBound=0, ...])
```

`IntervalJoin` with `isRowTime=true` — correct. If you instead see:

```
Join(joinType=[InnerJoin], where=[...], select=[...], leftInputSpec=[NoUniqueKey], ...)
```

you got a **regular join** and your state is unbounded. Most often the cause is that one of the timestamp columns lost its time-attribute marker somewhere upstream.

Similarly, a temporal join shows `TemporalJoin`, and a lookup join shows `LookupJoin` / `AsyncCalc` + `LookupJoin`.

---

## Remember

- Four join types; the syntax differences are tiny and the state differences are enormous.
- **Regular join**: both sides in state forever, retract output, destroys the time attribute. Only mitigation is `table.exec.state.ttl` or per-side `STATE_TTL` hints.
- **Interval join**: add a `BETWEEN` predicate on two event-time attributes. State bounded by the interval, cleaned by the watermark, append-only, time attribute preserved.
- **Temporal join**: `FOR SYSTEM_TIME AS OF <probe rowtime>` against a versioned table. Requires **PRIMARY KEY NOT ENFORCED + a watermark** on the dimension, and equality on that PK. Deterministic, append-only, replayable.
- **Lookup join**: `FOR SYSTEM_TIME AS OF <proctime>` against JDBC/HBase/etc. No Flink state; throughput bounded by the external system. Configure `lookup.cache = 'PARTIAL'` and turn on `cache-missing-key`.
- The `LOOKUP` hint enables async lookups and retry-on-miss — the SQL form of async I/O.
- **Window join**: join two window TVF outputs on `window_start` *and* `window_end` as well as the key.
- A temporal or interval join preserves the probe side's time attribute, so you can window afterwards. A regular join does not.
- `EXPLAIN` and read the operator name. `IntervalJoin` vs plain `Join` is the difference between a job that runs for a year and one that OOMs.

**Interview one-liners**

- *"What join types does Flink SQL have?"* → Regular, interval, temporal, and lookup — plus window joins on window TVF output.
- *"What's wrong with a regular join in streaming?"* → It keeps both sides in state indefinitely, because either side can still receive a matching row at any time. State grows forever; the only fix is a TTL that trades correctness for bounded state.
- *"How do you do currency conversion correctly?"* → A temporal join with `FOR SYSTEM_TIME AS OF order_ts` against a versioned rate table keyed by currency with a watermark. You get the rate as of the order's event time, deterministically.
- *"What makes a table 'versioned'?"* → A primary key plus an event-time attribute with a watermark. Flink then tracks the history of each key.
- *"Temporal join vs lookup join?"* → Temporal is event time against a versioned Flink table — deterministic and replayable. Lookup is processing time against an external database — always the current value, not reproducible.
- *"How do you bound state in a two-stream join?"* → Give it a time bound and make it an interval join. If you can't, set `table.exec.state.ttl`, ideally per side with `STATE_TTL` hints.
- *"How do you know which join the planner chose?"* → `EXPLAIN` and read the physical operator: `IntervalJoin`, `TemporalJoin`, `LookupJoin`, or plain `Join`.
