# Phase 7 — Flink SQL and the Table API

The declarative half of Flink. Phases 1–6 taught you the DataStream API — operators, watermarks, windows, keyed state, timers, Kafka, checkpoints, CEP, joins, async I/O. This phase covers the same capabilities expressed as SQL, which compiles down to **exactly those operators**.

All SQL here is **Flink 1.18–1.20** era, using the modern **window TVF** syntax (`TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '10' MINUTES))`). The legacy grouped-window form (`GROUP BY TUMBLE(ts, ...)`) is covered only so you can recognize it in old documentation.

## How to use this

- **New to Flink SQL?** Read straight through, 46 → 53. Chapter 47 is the load-bearing one.
- **Hit an error and need the fix?** 47 for `"doesn't support consuming update changes"`, 48 for `"requires the timecol is a time attribute"`, 51 for `"Could not find any factory for identifier"`.
- **Deciding whether to use SQL at all?** Read 52 first, then 47 to understand the state implications.
- **Want to experiment right now?** Chapter 53 Part 1 runs on `datagen` with no Kafka and no jars.

## Table of contents

| # | Chapter | Key idea |
|---|---|---|
| 46 | [Table API and SQL basics](46-table-api-and-sql-basics.md) | SQL is a planner in front of the same DataStream runtime; `executeSql` runs now, `sqlQuery` is lazy; `toDataStream` vs `toChangelogStream` |
| 47 | [**Dynamic tables and changelogs**](47-dynamic-tables-and-changelogs.md) | **The central concept. Append / retract / upsert, and why your sink rejects the query.** Read twice. |
| 48 | [Time attributes and watermarks in SQL](48-time-attributes-and-watermarks-in-sql.md) | `WATERMARK FOR ts AS ...`, epoch bigint → `TO_TIMESTAMP_LTZ`, and how a query silently destroys the time attribute |
| 49 | [SQL windows and aggregations](49-sql-windows-and-aggregations.md) | Window TVFs: TUMBLE, HOP, CUMULATE, SESSION; `OVER` windows; dedup and Top-N with `ROW_NUMBER()` |
| 50 | [SQL joins](50-sql-joins.md) | Four join types, four completely different state behaviours. Temporal join is how you do SCD enrichment correctly. |
| 51 | [Connectors and formats in SQL](51-connectors-and-formats-in-sql.md) | Kafka, upsert-kafka, JSON/Avro/Confluent/Debezium, `datagen` + `print` for testing, and the jar problem |
| 52 | [SQL vs DataStream](52-sql-vs-datastream.md) | The decision framework, mixing both in one job, and the savepoint-compatibility trap that bites production SQL |
| 53 | [**SQL capstone**](53-sql-capstone.md) | **The whole pipeline end to end** — Kafka + watermark + temporal join + TUMBLE + Top-N + `STATEMENT SET`, in SQL and in Java |

## The eight things that matter most

1. **Every Flink SQL problem is a changelog-mode problem.** Ask "can a future record invalidate a row I already emitted?" No → append. Yes + key → upsert. Yes, no key → retract.
2. **Windowed `GROUP BY` is append-only with bounded state. Non-windowed `GROUP BY` is retract with unbounded state.** This is not a stylistic difference.
3. **`SET 'table.exec.state.ttl' = '...'`** in any job with a non-windowed aggregation or a regular join. Or per-side `STATE_TTL` hints on joins. It trades correctness for survival, deliberately.
4. **A time attribute is what lets Flink expire state.** Windows, interval joins, and temporal joins all require one. Non-windowed aggregations, regular joins, and explicit `CAST`s destroy it.
5. **`upsert-kafka` + `PRIMARY KEY ... NOT ENFORCED`** is the answer to `"Table sink doesn't support consuming update changes"`.
6. **`EXPLAIN` before you deploy.** `WindowAggregate` good, `GroupAggregate` unbounded. `IntervalJoin` good, plain `Join` unbounded. `EXPLAIN CHANGELOG_MODE` tells you whether the sink will take it.
7. **SQL jobs cannot set operator `uid`s.** Changing a query can change the plan and make the savepoint unrestorable. Use `COMPILE PLAN` / `EXECUTE PLAN`, or run the new job in parallel and cut over.
8. **`datagen` + `print` is the fastest learning loop in all of Flink.** No Kafka, no jars, no compile. Use `fields.<ts>.max-past` so windows actually fire.

## The error → chapter map

| Error | Cause | Chapter |
|---|---|---|
| `Table sink ... doesn't support consuming update changes` | Retract query into an append-only sink | [47](47-dynamic-tables-and-changelogs.md) |
| `The window function requires the timecol is a time attribute type` | Something upstream stripped the rowtime marker | [48](48-time-attributes-and-watermarks-in-sql.md) |
| `Invalid data type of time field for watermark definition` | Rowtime column isn't `TIMESTAMP(p≤3)` / `TIMESTAMP_LTZ(p≤3)` | [48](48-time-attributes-and-watermarks-in-sql.md) |
| Job healthy, no window output | Watermark frozen by an idle partition | [48](48-time-attributes-and-watermarks-in-sql.md) |
| `Cumulative table function requires maxSize must be an integral multiple of step` | `CUMULATE` argument order or ratio | [49](49-sql-windows-and-aggregations.md) |
| State grows forever, checkpoints balloon | Non-windowed `GROUP BY` or a regular join with no TTL | [47](47-dynamic-tables-and-changelogs.md), [50](50-sql-joins.md) |
| `UpsertStreamTableSink requires that Table has a full primary key` | Sink PK doesn't match the query's unique key | [47](47-dynamic-tables-and-changelogs.md) |
| `Could not find any factory for identifier 'kafka'` | Connector jar not in `lib/` | [51](51-connectors-and-formats-in-sql.md) |
| Job crash-loops on one Kafka message | `json.ignore-parse-errors` not set | [51](51-connectors-and-formats-in-sql.md) |
| `Cannot map checkpoint/savepoint state ... to the new program` | The query plan changed; operator IDs changed | [52](52-sql-vs-datastream.md) |
| `No operators defined in streaming topology` | `env.execute()` after `executeSql("INSERT INTO ...")` | [46](46-table-api-and-sql-basics.md) |

## Quick reference — the config keys you will actually set

```sql
SET 'table.local-time-zone'             = 'UTC';      -- or windows shift on deploy
SET 'table.exec.source.idle-timeout'    = '30 s';     -- or idle partitions freeze watermarks
SET 'table.exec.state.ttl'              = '24 h';     -- or unbounded state
SET 'table.exec.mini-batch.enabled'     = 'true';     -- hot-key aggregation throughput
SET 'table.exec.mini-batch.allow-latency' = '1 s';
SET 'table.exec.mini-batch.size'        = '5000';
SET 'table.optimizer.distinct-agg.split.enabled' = 'true';   -- skew in COUNT(DISTINCT)
SET 'execution.checkpointing.interval'  = '60 s';
SET 'sql-client.execution.result-mode'  = 'changelog'; -- the best way to learn chapter 47
```

## Where this fits

- **Phase 2** (windows) is the DataStream view of what chapter 49 does declaratively.
- **Phase 3** (state) explains what `table.exec.state.ttl` is actually controlling.
- **Phase 5** (checkpoints, exactly-once) is what `sink.delivery-guarantee = 'exactly-once'` configures.
- **Phase 6** (CEP, joins, async I/O) maps to chapter 50's joins and the `LOOKUP` async hint.
- **Phase 8** (production) applies to SQL jobs unchanged — the runtime is the same.

PyFlink note: the **Table API / SQL is where PyFlink is genuinely first-class**. No records cross the Python boundary, so there is no performance penalty. If your team is Python-first, write Flink SQL rather than PyFlink DataStream. See chapter 46.
