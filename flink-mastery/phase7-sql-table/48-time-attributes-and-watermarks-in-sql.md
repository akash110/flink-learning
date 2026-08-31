# 48. Time Attributes and Watermarks in SQL

In the DataStream API you attached a `WatermarkStrategy` to a source (chapter 10). In SQL you do the same thing — but you declare it in the `CREATE TABLE` DDL, and the result is a **time attribute**: a column that the planner treats as special.

> **Key idea:** A time attribute is a regular column *plus* a promise to the planner: "this column advances monotonically with the watermark, so you may use it to decide when state can be cleaned up." Windows, interval joins, temporal joins, and `OVER` windows all refuse to plan without one.

---

## Declaring event time in DDL

```sql
CREATE TABLE transactions (
  userId  STRING,
  type    STRING,
  amount  DOUBLE,
  ts      TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic'     = 'transactions',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format'    = 'json'
);
```

Breaking down the watermark line:

```
WATERMARK  FOR  ts   AS   ts - INTERVAL '5' SECOND
    │           │          └── the watermark expression
    │           └───────────── which column becomes the time attribute
    └───────────────────────── keyword
```

- `WATERMARK FOR <col>` — `ts` is now the **rowtime attribute**. A table may have exactly one.
- `AS ts - INTERVAL '5' SECOND` — the watermark emitted is *(max ts seen so far) − 5 seconds*. Identical to `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))`.
- The expression must reference the rowtime column and must not *increase* it. `ts - INTERVAL '0' SECOND` is legal (strictly ascending watermarks); `ts + INTERVAL '5' SECOND` is not.

Special case, perfectly ordered data:

```sql
WATERMARK FOR ts AS ts               -- ascending timestamps, zero lateness tolerance
```

Every record whose timestamp is not strictly greater than the previous one is late and dropped. Only use this if you are certain.

### The `TIMESTAMP(3)` requirement

The rowtime column must be `TIMESTAMP(p)` or `TIMESTAMP_LTZ(p)` with **p between 0 and 3**.

```sql
ts TIMESTAMP(3)        -- ✓ millisecond precision, the normal choice
ts TIMESTAMP(9)        -- ✗ nanoseconds — rejected
ts BIGINT              -- ✗ cannot be a time attribute directly
ts TIMESTAMP_LTZ(3)    -- ✓ and usually the more correct type
```

The error for the wrong precision:

```
Invalid data type of time field for watermark definition. The field must be of type
TIMESTAMP(p) or TIMESTAMP_LTZ(p), the supported precision 'p' is from 0 to 3, but the time field type is BIGINT
```

Flink's watermarks are `long` epoch milliseconds internally. Precision above 3 has nowhere to go.

### `TIMESTAMP` vs `TIMESTAMP_LTZ` — which to use

| Type | Semantics |
|---|---|
| `TIMESTAMP(3)` | A wall-clock reading with **no timezone**. `2026-08-29 14:00:00` — 14:00 *somewhere*. |
| `TIMESTAMP_LTZ(3)` | An **instant** — epoch millis. Rendered in the session timezone on display. |

For event time from an epoch or from Kafka, `TIMESTAMP_LTZ(3)` is the honest type: the event happened at a specific instant regardless of who reads it. Use `TIMESTAMP(3)` when your source genuinely stores a local naive timestamp string.

The session timezone controls how `TIMESTAMP_LTZ` prints and how `TUMBLE` on a day boundary is aligned:

```sql
SET 'table.local-time-zone' = 'America/Los_Angeles';
```

Default is the JVM's zone — which is often UTC on a cluster and your laptop's zone locally, so daily windows silently shift when you deploy. **Set it explicitly.**

---

## The bigint epoch column — the most common real case

Kafka JSON almost never contains an ISO timestamp. It contains this, matching your `Event` POJO:

```json
{"userId":"u-001","type":"purchase","amount":42.5,"timestamp":1756483200000}
```

`timestamp` is epoch millis in a `BIGINT`. You cannot put `WATERMARK FOR timestamp` on it. You derive a **computed column**:

```sql
CREATE TABLE transactions (
  userId    STRING,
  type      STRING,
  amount    DOUBLE,
  `timestamp` BIGINT,                                     -- the raw field, as it is in JSON
  ts AS TO_TIMESTAMP_LTZ(`timestamp`, 3),                 -- computed column
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND            -- watermark on the computed column
) WITH (
  'connector' = 'kafka',
  'topic'     = 'transactions',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format'    = 'json'
);
```

- `` `timestamp` `` — backticks because `TIMESTAMP` is a reserved word. Without them you get a parse error. (This is a strong argument for naming the POJO field `eventTime` instead, but you inherit the schema you inherit.)
- `ts AS <expression>` — the `AS` form (no type given) declares a **computed column**. It is not read from the source; it is evaluated per record.
- `TO_TIMESTAMP_LTZ(epoch, 3)` — second argument is the **precision of the input number**: `3` means the bigint is in *milliseconds*, `0` means *seconds*.

```
TO_TIMESTAMP_LTZ(1756483200000, 3)  → 2026-08-29 16:00:00.000  ✓ millis
TO_TIMESTAMP_LTZ(1756483200,    0)  → 2026-08-29 16:00:00.000  ✓ seconds
TO_TIMESTAMP_LTZ(1756483200000, 0)  → year 57641               ✗ wrong unit
```

The classic bug: seconds treated as millis gives you 1970, millis treated as seconds gives you the year 57000. Windows "work" and produce nothing sensible, with no error. Always `SELECT ts FROM transactions LIMIT 5;` after writing the DDL.

Parsing a string timestamp instead:

```sql
ts AS TO_TIMESTAMP(ts_string, 'yyyy-MM-dd HH:mm:ss')   -- → TIMESTAMP(3), no zone
```

`TO_TIMESTAMP` returns `TIMESTAMP(3)`, `TO_TIMESTAMP_LTZ` returns `TIMESTAMP_LTZ(3)`. Both are legal rowtime columns.

---

## Metadata columns — Kafka record time and friends

Kafka records carry their own timestamp in the record header. You expose it as a **metadata column**:

```sql
CREATE TABLE transactions (
  userId  STRING,
  amount  DOUBLE,
  ts      TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic'     = 'transactions',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format'    = 'json'
);
```

- `METADATA FROM 'timestamp'` — read the connector's metadata key `timestamp`, not a JSON field. The **column name** (`ts`) is yours; the **quoted string** is the connector's key.
- If your column is already named exactly like the metadata key, `METADATA` alone works: `` `timestamp` TIMESTAMP_LTZ(3) METADATA ``.

Kafka connector metadata keys available on read:

| Key | Type | Meaning |
|---|---|---|
| `topic` | `STRING` | Topic name |
| `partition` | `INT` | Partition |
| `offset` | `BIGINT` | Offset |
| `timestamp` | `TIMESTAMP_LTZ(3)` | Record timestamp |
| `timestamp-type` | `STRING` | `CreateTime` / `LogAppendTime` / `NoTimestampType` |
| `headers` | `MAP<STRING, BYTES>` | Kafka headers |
| `leader-epoch` | `INT` | Leader epoch |

Metadata columns are read-only by default. To make one writable on a sink, drop `VIRTUAL`; to make it read-only on a table you also write to, add it:

```sql
offset BIGINT METADATA VIRTUAL   -- readable, and excluded from INSERT INTO column list
```

Without `VIRTUAL`, an `INSERT INTO` on that table would need to supply a value for `offset`, which is nonsense.

> **Caution:** Kafka's record timestamp is the time the **producer** created (or the broker appended) the record — not necessarily when the business event happened. If a mobile client buffers events offline for two hours, record time and event time differ by two hours. Use the payload timestamp for correctness; use the metadata timestamp only when you know they're equivalent.

---

## Processing time

```sql
CREATE TABLE transactions (
  userId STRING,
  amount DOUBLE,
  proc AS PROCTIME()                -- computed column, always TIMESTAMP_LTZ(3) NOT NULL
) WITH ( ... );
```

- `PROCTIME()` is a special function; the column it defines is a **processing-time attribute**.
- No watermark declaration is needed or allowed — processing time always advances with the system clock.
- The value is not stored anywhere; it is generated when the operator reads the row. Two operators reading the same record see different `PROCTIME()` values.

Use it for: lookup joins (chapter 50), latency-sensitive windows where correctness on reprocessing doesn't matter, and quick experiments. Never for anything you'd need to reproduce.

A table can have both:

```sql
CREATE TABLE transactions (
  userId STRING,
  amount DOUBLE,
  `timestamp` BIGINT,
  ts   AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
  proc AS PROCTIME(),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH ( ... );
```

Event time for windows, processing time for lookup joins. Very common.

---

## Computed columns in general

`col AS expression` is a general mechanism, not just for timestamps.

```sql
CREATE TABLE transactions (
  userId    STRING,
  amount    DOUBLE,
  currency  STRING,
  `timestamp` BIGINT,

  ts          AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
  amount_cents AS CAST(amount * 100 AS BIGINT),
  is_large     AS amount > 1000,
  day          AS DATE_FORMAT(TO_TIMESTAMP_LTZ(`timestamp`, 3), 'yyyy-MM-dd'),

  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH ( ... );
```

Rules:

- Evaluated **on read**, per record, in the source operator. Not materialized, not stored.
- May reference other physical columns and metadata columns of the same table. Referencing another *computed* column is allowed in recent versions but avoid it — it's fragile.
- Excluded from `INSERT INTO` column lists. You cannot write to a computed column.
- May call any built-in function or registered UDF.

Verify with `DESCRIBE`:

```sql
Flink SQL> DESCRIBE transactions;
+-------------+-----------------------------+-------+-----+------------------------------------+-----------------------------+
|        name |                        type |  null | key |                             extras |                   watermark |
+-------------+-----------------------------+-------+-----+------------------------------------+-----------------------------+
|      userId |                      STRING |  TRUE |     |                                    |                             |
|      amount |                      DOUBLE |  TRUE |     |                                    |                             |
|   timestamp |                      BIGINT |  TRUE |     |                                    |                             |
|          ts | TIMESTAMP_LTZ(3) *ROWTIME* | FALSE |     | AS TO_TIMESTAMP_LTZ(`timestamp`,3) | `ts` - INTERVAL '5' SECOND  |
|        proc | TIMESTAMP_LTZ(3) *PROCTIME*| FALSE |     |                     AS PROCTIME()  |                             |
+-------------+-----------------------------+-------+-----+------------------------------------+-----------------------------+
```

`*ROWTIME*` and `*PROCTIME*` in the type are the markers. **If they are missing, the column is an ordinary timestamp and no window will accept it.** `DESCRIBE` is your first debugging step.

---

## Why a time attribute is required

Three families of operators need one:

**1. Windows.** A window needs a completion signal. Without a watermark there is no answer to "has this window received all its data?", so the window can never fire.

**2. Interval joins.** `a.ts BETWEEN b.ts - INTERVAL '5' MINUTE AND b.ts` lets Flink prove that a `b` row older than watermark − 5 minutes can never match again, and delete it. Without a time attribute the same predicate is just an ordinary filter over a regular join — correct, but with unbounded state.

**3. Temporal joins.** `FOR SYSTEM_TIME AS OF a.ts` needs `a.ts` to be a time attribute so Flink knows which version of the dimension table was current.

Same principle every time: **the time attribute is what lets Flink expire state.**

---

## The failure: losing the time attribute

This is the single most confusing Flink SQL error, and it is very easy to cause.

```sql
CREATE VIEW enriched AS
SELECT userId, amount, ts
FROM transactions;

SELECT userId, SUM(amount)
FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(ts), INTERVAL '10' MINUTES))
GROUP BY userId, window_start, window_end;
```

That works — a plain projection preserves the attribute. But:

```sql
CREATE VIEW enriched AS
SELECT userId, MAX(ts) AS ts, SUM(amount) AS amount
FROM transactions
GROUP BY userId;                              -- ← non-windowed aggregation

SELECT ... FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(ts), ...))
```

```
org.apache.flink.table.api.ValidationException: The window function requires the timecol
is a time attribute type, but is TIMESTAMP(3).
```

Or in older phrasing you'll also see:

```
Window aggregate can only be defined over a time attribute column, but TIMESTAMP(3) encountered.
```

The output of a non-windowed `GROUP BY` is a **retract** stream. Rows come out in arbitrary order relative to `ts`, and can be revised. `ts` is no longer monotonic, so it cannot be a time attribute. The planner downgrades it to plain `TIMESTAMP(3)`.

**Operations that destroy a time attribute:**

| Operation | Effect |
|---|---|
| `SELECT col` (projection) | ✓ preserved |
| `WHERE` | ✓ preserved |
| `UNION ALL` of two tables with rowtime | ✓ preserved |
| `CAST(ts AS TIMESTAMP(3))` | ✗ **destroyed** — an explicit cast strips the marker |
| `MAX(ts)` / any aggregate over it | ✗ destroyed |
| non-windowed `GROUP BY` output | ✗ destroyed |
| regular (unbounded) join output | ✗ destroyed |
| `ORDER BY` on a non-time column | ✗ destroyed |
| windowed aggregation output | ✓ `window_time` is a *new* time attribute (see ch. 49) |
| interval join output | ✓ preserved |
| temporal / lookup join output | ✓ preserved on the probe side |

The fix is architectural: **do the time-based operation first**, before anything that erases the attribute. Window, then aggregate the window results — not aggregate, then window.

If you genuinely must re-establish event time after a destructive step, you have to go through a sink and back, or drop to DataStream (`toChangelogStream`, re-apply a `WatermarkStrategy`, `fromDataStream` with `SOURCE_WATERMARK()`). There is no `RE-WATERMARK` statement.

---

## Idle sources

Watermark = min across all parallel subtasks. One Kafka partition with no traffic pins the watermark at its last value, and every window in the job stops firing. Exactly the DataStream problem from chapter 10, with a SQL-level knob:

```sql
SET 'table.exec.source.idle-timeout' = '30 s';
```

- After 30 s with no records from a source subtask, that subtask is marked **idle** and excluded from the watermark minimum.
- The moment a record arrives it becomes active again.
- Default is `0`, meaning the feature is off.

Set this whenever partition count exceeds actual traffic diversity — over-partitioned topics, low-volume tenants, night-time troughs. Symptom without it: the job is healthy, records are flowing, and windowed output is simply absent.

Related knobs worth knowing:

```sql
SET 'table.exec.source.cdc-events-duplicate' = 'true';   -- dedup CDC sources missing UPDATE_BEFORE
SET 'pipeline.auto-watermark-interval' = '200 ms';       -- how often watermarks are emitted
```

`pipeline.auto-watermark-interval` defaults to 200 ms. Lowering it reduces latency slightly at the cost of more watermark records; raising it does the reverse. Rarely worth touching.

Diagnosing a stuck watermark from the Flink UI: open the job, click the windowed operator, and read the **Watermarks** tab. Each subtask shows its current watermark. A subtask showing `-9223372036854775808` (`Long.MIN_VALUE`) has never received a watermark at all — that's an idle or misconfigured partition.

---

## Late data in SQL

There is no `allowedLateness` in Flink SQL and no side output for late records. A record whose timestamp is below the current watermark when it reaches a window operator is **silently dropped**.

The controls you have:

1. **Widen the out-of-orderness bound** — `INTERVAL '5' SECOND` → `INTERVAL '1' MINUTE`. Costs latency and window state.
2. **Measure the loss.** The window operator exposes a `numLateRecordsDropped` metric. Alert on it.
3. **Compute the correction elsewhere** — a batch reconciliation job, or a second Flink job with a much larger bound.

```
       watermark = 12:00:00
                      │
   ──────────────────►│
 record ts = 11:59:58  │  arrives now  → LATE → dropped, counted in
                       │                        numLateRecordsDropped
```

Being deliberate about the bound is the whole game. Look at your real distribution of `(processing_time − event_time)`, take a high percentile, use that.

---

## Full worked DDL

Everything in one table, the shape you will actually deploy:

```sql
CREATE TABLE transactions (
  -- physical columns, matching the JSON payload / the Event POJO
  userId       STRING,
  type         STRING,
  amount       DOUBLE,
  `timestamp`  BIGINT,

  -- metadata from the Kafka record itself
  kafka_ts     TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  part         INT              METADATA FROM 'partition' VIRTUAL,

  -- computed columns
  ts           AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
  proc         AS PROCTIME(),

  -- event-time attribute + watermark
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'transactions',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id'          = 'flink-sql-tx',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);
```

Then always, before writing any query:

```sql
DESCRIBE transactions;                         -- confirm *ROWTIME* appears
SELECT userId, `timestamp`, ts FROM transactions LIMIT 5;   -- confirm ts is a sane date
```

Those two commands catch the majority of time-related bugs before they become mysteries.

---

## Remember

- `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND` is the SQL form of `forBoundedOutOfOrderness(5s)`.
- The rowtime column must be `TIMESTAMP(p)` or `TIMESTAMP_LTZ(p)` with p ≤ 3. One rowtime attribute per table.
- Epoch bigint → `ts AS TO_TIMESTAMP_LTZ(epoch_millis, 3)`. The second arg is the precision of the *input*: `3` = millis, `0` = seconds. Getting it wrong yields 1970 or year 57000, silently.
- Backtick a column named `` `timestamp` `` — it's a reserved word.
- `ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'` reads the **Kafka record timestamp**, which is producer/broker time, not necessarily business event time.
- `proc AS PROCTIME()` for processing time. No watermark. A table can carry both attributes.
- `col AS expr` is a computed column: evaluated per record on read, never written to, excluded from `INSERT INTO`.
- `DESCRIBE table;` — look for `*ROWTIME*` / `*PROCTIME*` in the type. That's the proof the attribute exists.
- A time attribute is **required** for windows, interval joins, and temporal joins — it's what lets Flink expire state.
- Non-windowed `GROUP BY`, regular joins, and explicit `CAST` **destroy** the time attribute. Do time-based operations first.
- `SET 'table.exec.source.idle-timeout' = '30 s';` or one quiet partition freezes every window in the job.
- `SET 'table.local-time-zone' = 'UTC';` explicitly, or daily windows shift between laptop and cluster.
- Late records are dropped silently. Watch `numLateRecordsDropped`.

**Interview one-liners**

- *"How do you declare event time in Flink SQL?"* → A `WATERMARK FOR <col> AS <expr>` clause in the DDL, on a `TIMESTAMP(3)`/`TIMESTAMP_LTZ(3)` column.
- *"Kafka gives me epoch millis in a bigint. Now what?"* → Computed column `ts AS TO_TIMESTAMP_LTZ(millis, 3)` and put the watermark on `ts`.
- *"What's the difference between the payload timestamp and `METADATA FROM 'timestamp'`?"* → The metadata one is Kafka's record time (producer or broker), which can differ from when the business event occurred.
- *"Why does my window query say 'requires the timecol is a time attribute'?"* → Something upstream — a non-windowed aggregation, a regular join, or a `CAST` — stripped the rowtime marker.
- *"Windows produce nothing and the job looks healthy."* → The watermark isn't advancing. Usually an idle partition; set `table.exec.source.idle-timeout`.
- *"How do you handle late data in SQL?"* → You mostly don't — widen the out-of-orderness bound, monitor `numLateRecordsDropped`, and reconcile out of band. There's no `allowedLateness` in SQL.
- *"TIMESTAMP vs TIMESTAMP_LTZ?"* → `TIMESTAMP` is a naive wall-clock value; `TIMESTAMP_LTZ` is an instant rendered in the session timezone. Event time from an epoch should be `TIMESTAMP_LTZ`.
