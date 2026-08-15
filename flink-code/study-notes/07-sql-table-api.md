# Phase 7 — Flink SQL & Table API

Everything you built with `DataStream` can often be expressed in **SQL** — and for a large class of jobs (ETL, aggregations, joins) SQL is dramatically less code. This phase is about knowing the SQL/Table world and, crucially, **when to use it vs the DataStream API.**

### Dependency
```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-api-java-bridge</artifactId>
    <version>${flink.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner-loader</artifactId>
    <version>${flink.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-runtime</artifactId>
    <version>${flink.version}</version>
</dependency>
```

---

## 1. The big idea: streams as dynamic tables

Flink SQL treats an unbounded stream as a **dynamic table** — a table that's continuously appended to (or updated). A query over it produces another dynamic table, which is emitted as a **changelog stream** (inserts, and for aggregations, updates/retractions). This "stream ⇄ table duality" is the whole mental model.

- **Append-only** result → INSERTs only (e.g., filtering, windowed aggregation that fires once).
- **Updating** result → INSERT/UPDATE/DELETE rows (e.g., non-windowed `GROUP BY` where a group's total keeps changing). Downstream sinks must support updates (upsert sink) for these.

---

## 2. Flink SQL basics

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// simplest: run SQL that defines a source, transforms, and prints
tEnv.executeSql(
    "CREATE TABLE txns (" +
    "  user STRING," +
    "  amount INT," +
    "  ts TIMESTAMP(3)," +
    "  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND" +   // event time + watermark, in SQL!
    ") WITH (" +
    "  'connector' = 'datagen'," +                        // built-in random data generator
    "  'rows-per-second' = '5'" +
    ")");

Table result = tEnv.sqlQuery(
    "SELECT user, SUM(amount) AS total FROM txns GROUP BY user");

result.execute().print();   // prints a changelog (note the +I / -U / +U op column)
```

Notice: **watermarks and event time are declared in DDL** (`WATERMARK FOR ...`). No `assignTimestampsAndWatermarks` boilerplate.

---

## 3. Tables & the two APIs

There are two equivalent front-ends over the same engine:

**SQL strings** (most common):
```java
tEnv.sqlQuery("SELECT user, SUM(amount) FROM txns GROUP BY user");
```

**Table API** (fluent Java, type-checked-ish):
```java
import static org.apache.flink.table.api.Expressions.*;
tEnv.from("txns")
    .groupBy($("user"))
    .select($("user"), $("amount").sum().as("total"));
```

They compile to the same plan. SQL is more portable/readable; Table API composes better inside Java programs. Use whichever fits.

### Bridging to/from DataStream
You can mix APIs — do the SQL-friendly parts in SQL, drop to DataStream for custom logic:
```java
DataStream<Row> ds = tEnv.toChangelogStream(result);        // table -> stream
Table t = tEnv.fromDataStream(someDataStream);              // stream -> table
```

---

## 4. SQL windows (TVF syntax in 1.18)

Flink 1.18 uses **windowing table-valued functions (TVFs)** — `TUMBLE`, `HOP` (sliding), `CUMULATE`, `SESSION`.

```sql
-- Tumbling: 10-second buckets
SELECT window_start, window_end, user, SUM(amount) AS total
FROM TABLE(
  TUMBLE(TABLE txns, DESCRIPTOR(ts), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, user;

-- Hopping (sliding): size 10s, slide 5s
SELECT window_start, window_end, user, SUM(amount)
FROM TABLE(
  HOP(TABLE txns, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
GROUP BY window_start, window_end, user;

-- Cumulate: growing windows (e.g., running total every 1 min up to 1 hour)
SELECT window_start, window_end, SUM(amount)
FROM TABLE(
  CUMULATE(TABLE txns, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '1' HOUR))
GROUP BY window_start, window_end;
```

These map directly onto the Phase 2 window concepts — same semantics, SQL surface.

---

## 5. Aggregations

```sql
-- windowed (append-only output, fires when window closes)
SELECT window_start, user, COUNT(*), SUM(amount), AVG(amount)
FROM TABLE(TUMBLE(TABLE txns, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY window_start, user;

-- non-windowed group by (UPDATING output — total changes forever)
SELECT user, SUM(amount) AS lifetime_total
FROM txns
GROUP BY user;
```
The non-windowed one keeps state **per group forever** — same unbounded-state concern as Phase 3. Use TTL (`table.exec.state.ttl` config) or prefer windowed aggregations for bounded state.

---

## 6. Joins (know the flavors — this is where streaming SQL gets subtle)

Joining two **unbounded** streams is not like batch SQL. Flavors:

| Join | Semantics | State cost |
|------|-----------|-----------|
| **Regular join** (`a JOIN b ON ...`) | both sides kept in state indefinitely; any update re-emits | ⚠️ unbounded — needs state TTL |
| **Interval join** | join only if timestamps within a time bound (`b.ts BETWEEN a.ts - '5' MIN AND a.ts`) | bounded — state auto-expires |
| **Temporal join** (§7) | join a stream against a *versioned* table "as of" event time | bounded to versions |
| **Lookup join** | enrich a stream from an external table (JDBC/HBase) `FOR SYSTEM_TIME AS OF` | no stream state; external lookups |

```sql
-- interval join: match orders to shipments within 24h
SELECT o.id, s.carrier
FROM orders o, shipments s
WHERE o.id = s.order_id
  AND s.ts BETWEEN o.ts AND o.ts + INTERVAL '24' HOUR;
```

**Rule:** prefer interval / temporal / lookup joins over regular joins on streams — regular joins accumulate unbounded state.

---

## 7. Temporal joins (as-of joins)

The classic use case: enrich a stream of events with a value that **changes over time**, using the version that was current **at the event's time**. E.g., convert transaction amounts using the exchange rate *as of when the transaction happened* — not the latest rate.

```sql
-- rates is a versioned table (has a primary key + event-time attribute)
SELECT
  t.user, t.amount,
  t.amount * r.rate AS amount_usd
FROM txns t
JOIN rates FOR SYSTEM_TIME AS OF t.ts AS r
  ON t.currency = r.currency;
```

`FOR SYSTEM_TIME AS OF t.ts` = "use the rate row that was valid at `t.ts`." This is deterministic and replayable — exactly what event-time processing promises.

---

## 8. Kafka + Flink SQL

The payoff: an **entire streaming ETL job as pure SQL**, no Java.

```sql
CREATE TABLE transactions (
  user STRING, amount INT, ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'flink-sql',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE alerts (
  user STRING, total INT, window_end TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'alerts',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

INSERT INTO alerts
SELECT user, SUM(amount), window_end
FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY user, window_end
HAVING SUM(amount) > 10000;
```

(Needs `flink-sql-connector-kafka` on the classpath.) You can run this in the **SQL Client** (`sql-client.sh`) interactively — great for exploration.

---

## 9. When to use SQL vs DataStream API

| Use **SQL / Table API** when… | Use **DataStream API** when… |
|---|---|
| Standard ETL: filter, project, join, aggregate | Custom per-record logic / complex state machines |
| Windowed analytics & reporting | Fine-grained control over state, timers, side outputs |
| You want less code / analysts can maintain it | CEP-style logic that doesn't fit SQL cleanly |
| Rapid prototyping in SQL Client | You need exact control over operator UIDs, chaining |
| Enrichment via temporal/lookup joins | Non-relational transformations (e.g., custom serialization) |

**They mix.** A very common production shape: ingest + parse + window-aggregate in SQL, then `toDataStream` and run a custom `KeyedProcessFunction` for the gnarly business logic. Don't treat it as either/or.

---

### ✅ Phase 7 checklist

- [ ] Stream ⇄ dynamic table duality; append vs updating results
- [ ] `TableEnvironment`, DDL with `WATERMARK`
- [ ] SQL strings vs Table API; bridging to/from DataStream
- [ ] Window TVFs: `TUMBLE` / `HOP` / `CUMULATE` / `SESSION`
- [ ] Windowed vs non-windowed aggregation (state implications)
- [ ] Join flavors: regular / interval / temporal / lookup
- [ ] Temporal (as-of) joins
- [ ] Kafka source & sink tables; end-to-end SQL job
- [ ] Decision rule: SQL vs DataStream (and mixing)

⬅️ [Phase 6](06-advanced-event-processing.md)  ·  ➡️ [Phase 8 — Production architecture](08-production.md)
