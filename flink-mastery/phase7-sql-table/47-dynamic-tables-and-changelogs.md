# 47. Dynamic Tables and Changelogs

This is **the** chapter of Phase 7. Every confusing Flink SQL error you will ever hit traces back to something in here. Read it twice.

---

## The duality

A batch table is a fixed set of rows. A stream is an infinite sequence of records. Flink's claim is that these are **the same thing viewed differently**.

```
STREAM                                    TABLE
──────────────────────────────────────────────────────────────
u-001, 100  ──┐
u-002,  50  ──┤   "materialize"          userId | total
u-001, 200  ──┼─────────────────────►    -------|------
u-003,  10  ──┤                          u-001  |  300
u-001,  ??  ──┘                          u-002  |   50
                                         u-003  |   10
              ◄─────────────────────
                  "changelog"
```

- **Stream → Table:** apply every record to an evolving table. The table is the *current state*.
- **Table → Stream:** emit one record for every change to the table. The stream is the *log of changes*.

> **Key idea:** A **dynamic table** is a table whose contents change over time. A **continuous query** runs over a dynamic table, never terminates, and produces another dynamic table. The result of a Flink SQL query is not an answer — it's a table that keeps being corrected.

Here is the loop, which is worth memorizing:

```
   ┌──────────────────┐   continuous   ┌──────────────────┐
   │  dynamic table   │──── query ────►│  dynamic table   │
   │   (input)        │                │   (result)       │
   └────────▲─────────┘                └─────────┬────────┘
            │                                    │
     materialize                            changelog
            │                                    ▼
   ┌────────┴─────────┐                ┌──────────────────┐
   │  input stream    │                │ output stream    │
   │  (from Kafka)    │                │ (to Kafka/JDBC)  │
   └──────────────────┘                └──────────────────┘
```

Flink never actually materializes the whole table. It keeps *just enough state* to know how each new input record changes the result, and emits the change. Which changes it can emit is the **changelog mode**.

---

## The four row kinds

Every row moving through Flink SQL carries a `RowKind` flag. In the SQL Client's `changelog` result mode you see it in the `op` column.

| Flag | `RowKind` | Meaning |
|---|---|---|
| `+I` | `INSERT` | A brand-new row. Nothing before it. |
| `-U` | `UPDATE_BEFORE` | "Forget this row — I told you this before and it's now wrong." |
| `+U` | `UPDATE_AFTER` | "Here is the corrected row." |
| `-D` | `DELETE` | "This row is gone. There is no replacement." |

`-U` is sometimes called the **retraction**. `-U` and `+U` always travel as a pair, in that order, for the same key.

Concretely, watch `SELECT userId, SUM(amount) FROM t GROUP BY userId` as three records arrive:

```
INPUT                            OUTPUT (changelog)
────────────────────────────────────────────────────────────
u-001, 100  ────────────────►    +I (u-001, 100)
                                     the first time we see u-001,
                                     there is nothing to retract

u-001, 200  ────────────────►    -U (u-001, 100)   "300 is wrong, it was 100"
                                 +U (u-001, 300)   "the new value is 300"

u-002,  50  ────────────────►    +I (u-002,  50)
```

Three input records, four output records. **A retract stream can emit more rows than it consumes.** Budget for that when sizing a Kafka sink.

---

## The three changelog modes

### 1. Append-only

Only `+I`. Rows are never corrected or removed.

```
+I +I +I +I +I +I ...
```

Produced by: `SELECT`/`WHERE`/projections, windowed aggregations, interval joins, `UNION ALL`, temporal joins.

Cheap. Any sink accepts it. This is what you want.

### 2. Retract

`+I`, `-U`, `+U`, `-D`. The full vocabulary. Downstream must be able to *undo* a previously emitted row, which means it must have received the identical row before.

```
+I(a,1)  -U(a,1) +U(a,2)  -U(a,2) +U(a,5)  +I(b,1)  -D(b,1)
```

Produced by: non-windowed `GROUP BY`, regular (unbounded) joins, `ORDER BY ... LIMIT` without a window.

Expensive: two rows per update, and the sink must handle the retraction. To retract, the operator must remember the previous emitted value — so **retract implies state**.

### 3. Upsert

`+U` (treated as "upsert this key") and `-D`. **No `-U`.** Requires the result to have a **primary key** so the downstream can say "replace whatever is stored under this key."

```
+I(a,1)  +U(a,2)  +U(a,5)  +I(b,1)  -D(b,1)
```

Half the rows of a retract stream, because the "forget the old value" step is implicit in the key.

Produced by: deduplication and Top-N with `ROW_NUMBER()`, and by any retract query written into an **upsert sink** (`upsert-kafka`, JDBC with a PK, Elasticsearch) — the planner converts retract to upsert when the sink declares a key and the query has a matching unique key.

```
COMPARISON, same logical update "a goes 1 → 2"
──────────────────────────────────────────────────────────
append   : impossible (cannot express an update at all)
retract  : -U(a,1)  then  +U(a,2)      2 rows, no key needed
upsert   : +U(a,2)                     1 row, needs PK = a
```

---

## Which query produces which mode

Memorize this table. It answers 80% of "why does my SQL not work."

| Query | Mode | Why |
|---|---|---|
| `SELECT a, b FROM t` | **append** | Projection cannot change a past row |
| `SELECT * FROM t WHERE amount > 100` | **append** | A filter either passes a row or not, once |
| `SELECT a, b*2 FROM t WHERE ...` | **append** | Same |
| `t1 UNION ALL t2` | **append** | Concatenation |
| `t1 UNION t2` (distinct) | **retract** | Needs to remember what it emitted |
| `GROUP BY userId` (no window) | **retract** | Every new record for a key corrects the previous answer |
| `GROUP BY window_start, window_end` (window TVF) | **append** | The window fires **once**, when the watermark passes its end. There is nothing to correct. |
| `TUMBLE(...)` legacy grouped window | **append** | Same reason |
| interval join (`a.ts BETWEEN b.ts - ... AND b.ts`) | **append** | Time-bounded; a match is final |
| regular join `a JOIN b ON a.id = b.id` | **retract** | A late row on either side changes past output |
| temporal join `FOR SYSTEM_TIME AS OF a.ts` | **append** | The version chosen is fixed by event time |
| lookup join `FOR SYSTEM_TIME AS OF a.proc` | **append** | Point-in-time lookup, never revisited |
| dedup: `ROW_NUMBER() ... WHERE rn = 1` | **upsert** (append if ordering by time ASC) | One winner per key, replaced as better rows arrive |
| Top-N: `ROW_NUMBER() ... WHERE rn <= 10` | **upsert / retract** | Ranking shifts as data arrives |
| `SELECT DISTINCT a FROM t` | **retract** | Same as `GROUP BY a` |
| `OVER` window aggregation | **append** | Emits one row per input row, never revised |

> **Key idea:** The decisive question is always *"can a future record make an already-emitted row wrong?"* If no → append. If yes and there's a key → upsert. If yes and there isn't → retract.

Windowed aggregation is append **because the watermark gives it a completion signal**. That is the deepest practical reason to use windows instead of plain `GROUP BY`.

---

## The error you will hit

You write this:

```sql
CREATE TABLE user_totals (
  userId STRING,
  total  DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic'     = 'user-totals',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format'    = 'json'
);

INSERT INTO user_totals
SELECT userId, SUM(amount) FROM transactions GROUP BY userId;
```

And Flink refuses at **plan time**, before a single record moves:

```
org.apache.flink.table.api.TableException: Table sink
'default_catalog.default_database.user_totals' doesn't support consuming
update changes which is produced by node
GroupAggregate(groupBy=[userId], select=[userId, SUM(amount) AS EXPR$1])
```

Read it literally: the **sink** only understands `+I`. The **query** produces `-U`/`+U`. The `kafka` connector appends to a topic — there is no way to un-append a message.

```
    GroupAggregate                    kafka sink
   ┌──────────────┐                  ┌──────────────┐
   │ emits        │   +I -U +U -D    │ accepts      │
   │ +I -U +U -D  │ ───────────────► │ +I only      │  ✗ MISMATCH
   └──────────────┘                  └──────────────┘
```

### Four ways to fix it

**Fix 1 — use an upsert sink.** Almost always the right answer.

```sql
CREATE TABLE user_totals (
  userId STRING,
  total  DOUBLE,
  PRIMARY KEY (userId) NOT ENFORCED     -- required by upsert-kafka
) WITH (
  'connector' = 'upsert-kafka',
  'topic'     = 'user-totals',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format'   = 'json',
  'value.format' = 'json'
);
```

- `PRIMARY KEY (userId) NOT ENFORCED` — the key. `NOT ENFORCED` means Flink trusts you rather than validating uniqueness (it cannot, on a stream).
- `upsert-kafka` writes the key into the Kafka message key and the row into the value. A `-D` becomes a **tombstone**: a message with that key and a `null` value. Pair with a log-compacted topic and the topic *is* the current table.
- `key.format` and `value.format` are separate options here; the plain `kafka` connector's single `format` option is not used.

**Fix 2 — make the query append-only by windowing it.**

```sql
INSERT INTO user_totals_windowed
SELECT userId, window_start, window_end, SUM(amount) AS total
FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '10' MINUTES))
GROUP BY userId, window_start, window_end;
```

Different semantics — per-window totals, not a running total — but it appends, and the state is bounded. Usually this is what the business actually wanted.

**Fix 3 — a JDBC sink with a primary key.** The `jdbc` connector generates `INSERT ... ON DUPLICATE KEY UPDATE` / `MERGE` when the table declares a PK, so it consumes updates natively.

**Fix 4 — take the changelog to Java** with `toChangelogStream` (chapter 46) and decide yourself what to do with `-U`.

---

## Watching it happen

The single most useful exercise in this whole phase:

```bash
./bin/sql-client.sh
```

```sql
SET 'sql-client.execution.result-mode' = 'changelog';

CREATE TABLE src (
  userId STRING,
  amount DOUBLE
) WITH (
  'connector'       = 'datagen',
  'rows-per-second' = '1',
  'fields.userId.length' = '1',
  'fields.amount.min'    = '1',
  'fields.amount.max'    = '10'
);

-- append-only: nothing but +I
SELECT userId, amount FROM src;

-- retract: watch -U / +U pairs appear
SELECT userId, SUM(amount) AS total FROM src GROUP BY userId;
```

The first query prints only `+I`. The second prints `+I` once per new user, then `-U`/`+U` pairs forever. Seeing this once is worth more than reading the theory three times.

---

## State: the practical consequence

A retract operator must remember its last emitted value per key to be able to retract it.

```
GROUP BY userId, no window
──────────────────────────────────────────────
state = { userId → accumulator }

100 users     →   tiny
100M users    →   100M entries, growing forever, never cleaned up
```

Nothing ever removes a key, because a record for `u-000001` might arrive tomorrow and the sum must still be correct. This is the **unbounded state** problem, and it is the number-one way a Flink SQL job dies in production.

The mitigation:

```sql
SET 'table.exec.state.ttl' = '36 h';
```

- Applies to *all* state in stateful SQL operators in the job: aggregations, joins, dedup.
- Semantics: state for a key not accessed within the TTL is dropped. If that key reappears afterwards, the aggregation restarts from zero and emits a `+I` where it should have emitted `+U`. **You are trading correctness for bounded state, deliberately.**
- Set it based on your real key access pattern, not a guess. If users are active daily, `36 h` is safe. If they're active monthly, it is not.
- There is no default (state is kept forever). Setting this in any long-running SQL job with a non-windowed aggregation or a regular join is not optional.

In Java:

```java
tEnv.getConfig().set("table.exec.state.ttl", "36 h");
```

Flink 1.18+ also allows per-operator TTL via `STATE_TTL` query hints on joins:

```sql
SELECT /*+ STATE_TTL('o' = '1d', 'c' = '30d') */ *
FROM orders o JOIN customers c ON o.customerId = c.id;
```

Different TTL per join side — orders expire in a day, the customer dimension lives for 30. That's usually what you actually want, and it is much better than one global number.

---

## Mode inference is a plan property, not a table property

The same table can be read as append and produce retract downstream. The mode is decided **per operator** by the optimizer, propagated bottom-up, and the sink is checked at the very end.

```
kafka source           GroupAggregate            Sink
[+I]          ──►      [+I,-U,+U,-D]      ──►   requires?
append                 retract                   append  → ✗ fail
                                                 upsert  → ✓ planner inserts
                                                            a ChangelogNormalize /
                                                            drops -U
```

When the planner *can* convert retract to upsert, it does, silently. That is why `upsert-kafka` "just works" as a sink for `GROUP BY`: the query has a unique key (`userId`) matching the sink PK, so the `-U` rows are dropped.

If your PK does **not** match the query's unique key, you get:

```
org.apache.flink.table.api.TableException: UpsertStreamTableSink requires
that Table has a full primary keys if it is updated.
```

Meaning: the sink says "key on `userId`" but the query's grouping produces one row per `(userId, day)`. Make the PK match the grouping.

---

## Reading a changelog on the wire

For `upsert-kafka`, the topic contains:

```
key: {"userId":"u-001"}   value: {"userId":"u-001","total":100.0}    <- +I
key: {"userId":"u-001"}   value: {"userId":"u-001","total":300.0}    <- +U
key: {"userId":"u-001"}   value: null                                <- -D (tombstone)
```

For the `kafka` connector with `'format' = 'debezium-json'` (chapter 51), the change kind is encoded *inside the value* instead:

```json
{"before": {"userId":"u-001","total":100.0},
 "after":  {"userId":"u-001","total":300.0},
 "op": "u"}
```

Two ways to carry the same information: in the message key (upsert) or in the payload (debezium). Both are readable back into Flink as a changelog source.

---

## Remember

- A dynamic table changes over time; a continuous query over it produces another dynamic table.
- Four row kinds: `+I` insert, `-U` retract the old, `+U` the corrected value, `-D` delete.
- `-U` and `+U` always come as an ordered pair.
- Three modes: **append** (`+I` only), **retract** (all four), **upsert** (`+U`/`-D`, needs a PK).
- The test: *can a future record invalidate a row I already emitted?* No → append. Yes + key → upsert. Yes, no key → retract.
- `GROUP BY` **without** a window → retract + unbounded state. `GROUP BY window_start, window_end` → append + bounded state. Prefer the window.
- `"doesn't support consuming update changes"` = retract query into an append-only sink. Fix with `upsert-kafka` (+ `PRIMARY KEY ... NOT ENFORCED`), a JDBC sink with a PK, or by windowing the query.
- A retract stream emits **more** rows than it consumes — up to 2× per update.
- `SET 'table.exec.state.ttl' = '36 h';` in any job with a non-windowed aggregation or a regular join. Or per-side `STATE_TTL` hints on joins.
- TTL trades correctness for bounded state, on purpose. Understand which keys you're dropping.
- `SET 'sql-client.execution.result-mode' = 'changelog';` to see the `op` column and learn all of this by watching.

**Interview one-liners**

- *"What is a dynamic table?"* → A table whose content changes as a stream feeds it; querying it continuously yields another dynamic table.
- *"What are the changelog modes?"* → Append-only, retract (+I/-U/+U/-D), and upsert (+U/-D with a primary key).
- *"Why does a streaming GROUP BY emit two rows per update?"* → It must retract the previously emitted value before emitting the corrected one, since a plain append sink has no notion of a key.
- *"Why is a windowed aggregation append-only but a plain GROUP BY isn't?"* → The watermark tells the window it is complete, so it fires once and never revises. A plain GROUP BY has no completion signal.
- *"'Table sink doesn't support consuming update changes' — what do you do?"* → Either switch to an upsert-capable sink with a primary key, or restructure the query (usually add a window) so it becomes append-only.
- *"What's the danger of a non-windowed GROUP BY in production?"* → State grows with the cardinality of the key, forever. Fix with `table.exec.state.ttl`, accepting that expired keys restart from zero.
- *"Upsert vs retract — which is cheaper?"* → Upsert: one row per change instead of two, but it requires a primary key on the result.
