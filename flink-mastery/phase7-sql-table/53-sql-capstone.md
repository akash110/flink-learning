# 53. SQL Capstone — A Complete Pipeline

Everything from chapters 46–52 in one working pipeline. Two versions: a **local** one that runs on `datagen` with no Kafka at all, and the **production** one on Kafka. Then the same thing embedded in Java.

## What we're building

```
                          ┌──────────────────────────────────────┐
  transactions (Kafka)    │                                      │
  JSON, epoch ts     ─────┤                                      │
                          │  ① TUMBLE 10 min, per-user SUM       │
  currency_rates          │        (append-only, bounded state)  │
  (upsert-kafka)     ─────┤                                      │
  versioned dim           │  ② temporal join → USD amounts       │
                          │        FOR SYSTEM_TIME AS OF ts      │
                          │                                      │
                          │  ③ window Top-N: biggest 3 spenders  │
                          │        per window (append-only)      │
                          │                                      │
                          └──────┬──────────────────┬────────────┘
                                 │                  │
                        ④ INSERT INTO        ⑤ INSERT INTO
                        user_window_totals    top_spenders
                        (upsert-kafka)        (upsert-kafka)
                                 └────── one STATEMENT SET ──────┘
                                        = ONE Flink job
```

---

## Part 1 — The local version (`datagen`, no Kafka)

Save as `capstone-local.sql`. This runs on a bare Flink distribution with no extra jars.

```sql
-- ============================================================
-- SESSION CONFIGURATION
-- ============================================================
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';

-- Explicit timezone: without this, daily windows shift between
-- your laptop and the cluster (chapter 48).
SET 'table.local-time-zone' = 'UTC';

-- One quiet source subtask would otherwise freeze every watermark
-- in the job and no window would ever fire (chapter 48).
SET 'table.exec.source.idle-timeout' = '30 s';

-- Bound the state of any non-windowed operator (chapter 47).
SET 'table.exec.state.ttl' = '24 h';

-- Checkpointing: required for exactly-once sinks, and the thing
-- that makes the job recoverable at all.
SET 'execution.checkpointing.interval' = '30 s';

SET 'parallelism.default' = '2';


-- ============================================================
-- ① SOURCE: fake transactions
-- ============================================================
CREATE TEMPORARY TABLE transactions (
  -- generated numerics, kept low-cardinality so aggregations
  -- actually have something to group
  user_num   INT,
  type_num   INT,
  curr_num   INT,
  amount     DECIMAL(12, 2),
  ts         TIMESTAMP_LTZ(3),

  -- computed columns turn the ints into realistic values
  userId     AS CONCAT('u-', LPAD(CAST(user_num AS STRING), 3, '0')),
  type       AS CASE type_num WHEN 0 THEN 'purchase'
                              WHEN 1 THEN 'refund'
                              ELSE 'view' END,
  currency   AS CASE curr_num WHEN 0 THEN 'USD'
                              WHEN 1 THEN 'EUR'
                              ELSE 'GBP' END,

  -- the event-time attribute (chapter 48)
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'            = 'datagen',
  'rows-per-second'      = '50',
  'fields.user_num.min'  = '1',
  'fields.user_num.max'  = '20',      -- 20 distinct users
  'fields.type_num.min'  = '0',
  'fields.type_num.max'  = '2',
  'fields.curr_num.min'  = '0',
  'fields.curr_num.max'  = '2',
  'fields.amount.min'    = '1',
  'fields.amount.max'    = '500',
  -- THE critical option: timestamps within the last 5s of now,
  -- so the watermark advances and windows fire immediately.
  'fields.ts.max-past'   = '5000'
);


-- ============================================================
-- ② VERSIONED DIMENSION: currency rates
--    PRIMARY KEY + WATERMARK = a versioned table (chapter 50)
-- ============================================================
CREATE TEMPORARY TABLE currency_rates (
  curr_num   INT,
  rate       DECIMAL(10, 4),
  update_ts  TIMESTAMP_LTZ(3),
  currency   AS CASE curr_num WHEN 0 THEN 'USD'
                              WHEN 1 THEN 'EUR'
                              ELSE 'GBP' END,
  WATERMARK FOR update_ts AS update_ts - INTERVAL '5' SECOND
) WITH (
  'connector'            = 'datagen',
  'rows-per-second'      = '1',
  'fields.curr_num.min'  = '0',
  'fields.curr_num.max'  = '2',
  'fields.rate.min'      = '1',
  'fields.rate.max'      = '2',
  'fields.update_ts.max-past' = '5000'
);
-- NOTE: datagen cannot declare a PRIMARY KEY, so this local table is
-- append-only and the temporal join below would be rejected. For the
-- local run we substitute a simpler enrichment; the Kafka version in
-- Part 2 has the real versioned table. See "Local substitute" below.


-- ============================================================
-- SINKS: print, so everything is visible with no infrastructure
-- ============================================================
CREATE TEMPORARY TABLE user_window_totals (
  userId       STRING,
  window_start TIMESTAMP_LTZ(3),
  window_end   TIMESTAMP_LTZ(3),
  total        DECIMAL(20, 2),
  cnt          BIGINT
) WITH (
  'connector'        = 'print',
  'print-identifier' = 'TOTALS'
);

CREATE TEMPORARY TABLE top_spenders (
  window_start TIMESTAMP_LTZ(3),
  window_end   TIMESTAMP_LTZ(3),
  userId       STRING,
  total        DECIMAL(20, 2),
  rnk          BIGINT
) WITH (
  'connector'        = 'print',
  'print-identifier' = 'TOPN'
);


-- ============================================================
-- ③ THE WINDOWED AGGREGATION (chapter 49)
--    Window TVF + GROUP BY window_start, window_end
--    → APPEND-ONLY, bounded state
-- ============================================================
CREATE TEMPORARY VIEW per_user_window AS
SELECT
    userId,
    window_start,
    window_end,
    SUM(amount) AS total,
    COUNT(*)    AS cnt
FROM TABLE(
    TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
WHERE type = 'purchase'          -- filter BEFORE aggregating: less state, less work
GROUP BY userId, window_start, window_end;


-- ============================================================
-- ④ WINDOW TOP-N (chapter 49)
--    PARTITION BY the window columns → the ranking within a
--    completed window is final → APPEND-ONLY
-- ============================================================
CREATE TEMPORARY VIEW top3 AS
SELECT window_start, window_end, userId, total, rnk
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
             PARTITION BY window_start, window_end
             ORDER BY total DESC
         ) AS rnk
  FROM per_user_window
)
WHERE rnk <= 3;


-- ============================================================
-- ⑤ STATEMENT SET: both inserts, ONE job, ONE source read
-- ============================================================
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO user_window_totals
  SELECT userId, window_start, window_end, total, cnt FROM per_user_window;

  INSERT INTO top_spenders
  SELECT window_start, window_end, userId, total, rnk FROM top3;
END;
```

### Running it

```bash
cd flink-1.20.0
./bin/start-cluster.sh

./bin/sql-client.sh -f capstone-local.sql
```

Or interactively, which is better for learning:

```bash
./bin/sql-client.sh
```

then paste the blocks one at a time. Statements are terminated by `;` — the multi-line `EXECUTE STATEMENT SET ... END;` is submitted as one unit.

### Expected output

`print` writes to the **TaskManager log**, not your terminal:

```bash
tail -f log/flink-*-taskexecutor-*.out
```

After the first minute of event time elapses:

```
TOTALS:1> +I[u-004, 2026-08-29T14:23:00Z, 2026-08-29T14:24:00Z, 1832.00, 11]
TOTALS:2> +I[u-011, 2026-08-29T14:23:00Z, 2026-08-29T14:24:00Z, 1420.50, 9]
TOTALS:1> +I[u-007, 2026-08-29T14:23:00Z, 2026-08-29T14:24:00Z, 1187.25, 8]
TOTALS:2> +I[u-002, 2026-08-29T14:23:00Z, 2026-08-29T14:24:00Z,  940.00, 6]
...
TOPN:1> +I[2026-08-29T14:23:00Z, 2026-08-29T14:24:00Z, u-004, 1832.00, 1]
TOPN:1> +I[2026-08-29T14:23:00Z, 2026-08-29T14:24:00Z, u-011, 1420.50, 2]
TOPN:1> +I[2026-08-29T14:23:00Z, 2026-08-29T14:24:00Z, u-007, 1187.25, 3]
```

**Everything is `+I`.** No `-U`, no `+U`. That is the whole design goal — every stage is append-only, so every sink is simple and state is bounded. Compare with what you'd get from a non-windowed `GROUP BY userId`:

```
TOTALS:1> +I[u-004, ..., 120.00, 1]
TOTALS:1> -U[u-004, ..., 120.00, 1]
TOTALS:1> +U[u-004, ..., 265.00, 2]
TOTALS:1> -U[u-004, ..., 265.00, 2]
TOTALS:1> +U[u-004, ..., 390.00, 3]      ← forever, on every record
```

To watch the results interactively instead of in a log, replace the statement set with a plain query:

```sql
SELECT * FROM top3;
```

The SQL Client renders it live in `tableau` mode.

### Local substitute for the temporal join

Since `datagen` can't declare a `PRIMARY KEY`, use a `VALUES` table as the dimension for local testing:

```sql
CREATE TEMPORARY VIEW rates AS
SELECT * FROM (VALUES
  ('USD', CAST(1.0000 AS DECIMAL(10,4))),
  ('EUR', CAST(1.0850 AS DECIMAL(10,4))),
  ('GBP', CAST(1.2700 AS DECIMAL(10,4)))
) AS t(currency, rate);

-- a bounded table joined to a stream: the planner treats it as a
-- broadcast-style regular join with tiny, fixed state
CREATE TEMPORARY VIEW transactions_usd AS
SELECT t.userId, t.type, t.amount * r.rate AS amount_usd, t.ts
FROM transactions t
JOIN rates r ON t.currency = r.currency;
```

A join to a bounded `VALUES` table is fine — the right side is finite and tiny. Note it still **destroys the time attribute** (chapter 48), so you cannot window `transactions_usd` afterwards. That's precisely why the production version uses a temporal join, which preserves it.

---

## Part 2 — The production version (Kafka)

Save as `capstone.sql`.

```sql
-- ============================================================
-- CONFIGURATION
-- ============================================================
SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.source.idle-timeout' = '30 s';
SET 'table.exec.state.ttl' = '24 h';

SET 'execution.checkpointing.interval' = '60 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause' = '30 s';
SET 'execution.checkpointing.timeout' = '10 min';
SET 'execution.checkpointing.externalized-checkpoint-retention'
    = 'RETAIN_ON_CANCELLATION';
SET 'state.backend.type' = 'rocksdb';
SET 'state.backend.incremental' = 'true';

SET 'restart-strategy.type' = 'exponential-delay';
SET 'parallelism.default' = '4';


-- ============================================================
-- ① SOURCE: transactions on Kafka, JSON, epoch-millis timestamp
--    (chapters 48 + 51)
-- ============================================================
CREATE TABLE transactions (
  -- physical columns, matching the JSON payload / the Event POJO
  userId       STRING,
  type         STRING,
  amount       DECIMAL(12, 2),
  currency     STRING,
  `timestamp`  BIGINT,                -- backticks: TIMESTAMP is reserved

  -- Kafka record metadata, for debugging
  kafka_part   INT              METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT           METADATA FROM 'offset'    VIRTUAL,

  -- computed: epoch millis → a real time attribute
  ts           AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
  proc         AS PROCTIME(),

  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'transactions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-sql-capstone',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true',       -- or ONE bad message crash-loops forever
  'json.fail-on-missing-field'   = 'false'
);


-- ============================================================
-- ② VERSIONED DIMENSION: currency rates on a compacted topic
--    PRIMARY KEY + WATERMARK ⇒ Flink tracks version history
-- ============================================================
CREATE TABLE currency_rates (
  currency   STRING,
  rate       DECIMAL(10, 4),
  update_ts  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR update_ts AS update_ts - INTERVAL '10' SECOND,
  PRIMARY KEY (currency) NOT ENFORCED
) WITH (
  'connector'    = 'upsert-kafka',
  'topic'        = 'currency-rates',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format'   = 'json',
  'value.format' = 'json'
);


-- ============================================================
-- SINKS: upsert-kafka, because we key the results
--        (compacted topics ⇒ the topic IS the current table)
-- ============================================================
CREATE TABLE user_window_totals (
  userId       STRING,
  window_start TIMESTAMP_LTZ(3),
  window_end   TIMESTAMP_LTZ(3),
  total_usd    DECIMAL(20, 2),
  cnt          BIGINT,
  PRIMARY KEY (userId, window_start) NOT ENFORCED
) WITH (
  'connector'    = 'upsert-kafka',
  'topic'        = 'user-window-totals',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format'   = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

CREATE TABLE top_spenders (
  window_start TIMESTAMP_LTZ(3),
  window_end   TIMESTAMP_LTZ(3),
  rnk          BIGINT,
  userId       STRING,
  total_usd    DECIMAL(20, 2),
  PRIMARY KEY (window_start, rnk) NOT ENFORCED
) WITH (
  'connector'    = 'upsert-kafka',
  'topic'        = 'top-spenders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format'   = 'json',
  'value.format' = 'json'
);


-- ============================================================
-- ③ TEMPORAL JOIN — enrich with the rate in effect AT EVENT TIME
--    Append-only, AND it preserves ts as a time attribute,
--    which is what lets us window the result afterwards.
-- ============================================================
CREATE VIEW transactions_usd AS
SELECT
    t.userId,
    t.type,
    t.amount,
    t.currency,
    t.amount * COALESCE(r.rate, CAST(1.0 AS DECIMAL(10,4))) AS amount_usd,
    t.ts
FROM transactions AS t
LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF t.ts AS r
       ON t.currency = r.currency;


-- ============================================================
-- ④ WINDOWED AGGREGATION — per-user 10-minute sums
-- ============================================================
CREATE VIEW per_user_window AS
SELECT
    userId,
    window_start,
    window_end,
    SUM(amount_usd) AS total_usd,
    COUNT(*)        AS cnt
FROM TABLE(
    TUMBLE(TABLE transactions_usd, DESCRIPTOR(ts), INTERVAL '10' MINUTES)
)
WHERE type = 'purchase'
GROUP BY userId, window_start, window_end;


-- ============================================================
-- ⑤ WINDOW TOP-N — biggest 3 spenders per window
-- ============================================================
CREATE VIEW top3 AS
SELECT window_start, window_end, rnk, userId, total_usd
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
             PARTITION BY window_start, window_end
             ORDER BY total_usd DESC
         ) AS rnk
  FROM per_user_window
)
WHERE rnk <= 3;


-- ============================================================
-- ⑥ STATEMENT SET — both sinks in ONE job
-- ============================================================
EXECUTE STATEMENT SET
BEGIN

  INSERT INTO user_window_totals
  SELECT userId, window_start, window_end, total_usd, cnt
  FROM per_user_window;

  INSERT INTO top_spenders
  SELECT window_start, window_end, rnk, userId, total_usd
  FROM top3;

END;
```

### Why STATEMENT SET matters

Without it, two separate `INSERT INTO` statements produce **two independent Flink jobs**:

```
WITHOUT statement set                  WITH statement set
──────────────────────────────         ──────────────────────────────
JOB 1                                  ONE JOB
  Kafka read ──► join ──► TUMBLE         Kafka read ──► join ──► TUMBLE
        ──► sink A                             │
                                               ├──► sink A
JOB 2                                          └──► rank ──► sink B
  Kafka read ──► join ──► TUMBLE
        ──► rank ──► sink B            source read ONCE
                                       join computed ONCE
source read TWICE                      window state held ONCE
join computed TWICE                    one checkpoint, one savepoint,
window state held TWICE                one thing to operate
two checkpoints, two savepoints
```

> **Key idea:** `EXECUTE STATEMENT SET BEGIN ... END;` compiles multiple `INSERT INTO` statements into a **single job with a shared plan**. Common sub-expressions are computed once and fanned out. Use it whenever two outputs derive from the same source.

Java equivalent:

```java
StatementSet stmtSet = tEnv.createStatementSet();
stmtSet.addInsertSql("INSERT INTO user_window_totals SELECT ...");
stmtSet.addInsertSql("INSERT INTO top_spenders SELECT ...");
TableResult result = stmtSet.execute();      // ONE job submitted here
```

### Running it

```bash
# 1. connector jars (chapter 51) — needed BEFORE the cluster starts
cd flink-1.20.0/lib
curl -O https://repo.maven.apache.org/maven2/org/apache/flink/\
flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
cd ..

# 2. start
./bin/start-cluster.sh

# 3. create the topics (compacted for the upsert ones)
kafka-topics.sh --bootstrap-server kafka:9092 --create \
  --topic transactions --partitions 6

kafka-topics.sh --bootstrap-server kafka:9092 --create \
  --topic currency-rates --partitions 3 \
  --config cleanup.policy=compact

kafka-topics.sh --bootstrap-server kafka:9092 --create \
  --topic user-window-totals --partitions 6 \
  --config cleanup.policy=compact

kafka-topics.sh --bootstrap-server kafka:9092 --create \
  --topic top-spenders --partitions 3 \
  --config cleanup.policy=compact

# 4. submit
./bin/sql-client.sh -f capstone.sql

# 5. watch
open http://localhost:8081
```

Sanity-check before submitting — the workflow from chapter 52:

```sql
DESCRIBE transactions;                        -- *ROWTIME* present on ts?
SELECT userId, `timestamp`, ts FROM transactions LIMIT 5;   -- sane dates?
EXPLAIN CHANGELOG_MODE SELECT * FROM per_user_window;       -- [I] only?
EXPLAIN SELECT * FROM top3;                   -- WindowAggregate + Rank, no GroupAggregate?
```

Verify the output topic:

```bash
kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic top-spenders --from-beginning \
  --property print.key=true --property key.separator=' | '
```

```
{"window_start":"2026-08-29 14:20:00","rnk":1} | {"window_start":"2026-08-29 14:20:00","window_end":"2026-08-29 14:30:00","rnk":1,"userId":"u-004","total_usd":8420.50}
{"window_start":"2026-08-29 14:20:00","rnk":2} | {"window_start":"2026-08-29 14:20:00","window_end":"2026-08-29 14:30:00","rnk":2,"userId":"u-011","total_usd":6100.00}
```

Key = the declared primary key; value = the full row.

### Stopping with a savepoint

```bash
./bin/flink list                                     # get the JobID
./bin/flink stop --savepointPath s3://flink/savepoints <JobID>

# restore later
./bin/flink run -s s3://flink/savepoints/savepoint-abc123 -py ...  # or the SQL runner
```

Read chapter 52's savepoint-compatibility section before you change a single character of this SQL. To make the job safely upgradeable, pin the plan:

```sql
COMPILE PLAN '/opt/flink/plans/capstone-v1.json' FOR
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO user_window_totals SELECT ...;
  INSERT INTO top_spenders SELECT ...;
END;
```

```sql
EXECUTE PLAN '/opt/flink/plans/capstone-v1.json';
```

Then the deployed artifact is the JSON plan, whose operator IDs are stable across Flink minor upgrades.

---

## Part 3 — The Java-embedded version

Same pipeline, packaged as a jar. Use this shape when the pipeline needs Java operators mixed in (chapter 52) or when your deployment tooling expects a jar.

```java
package com.akash.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SqlCapstone {

    public static void main(String[] args) throws Exception {

        // ---------- runtime configuration ----------
        // Note: parallelism / checkpointing go on the STREAM env,
        // planner config goes on tEnv.getConfig().
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(60_000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.getConfig().set("table.local-time-zone", "UTC");
        tEnv.getConfig().set("table.exec.source.idle-timeout", "30 s");
        tEnv.getConfig().set("table.exec.state.ttl", "24 h");

        String bootstrap = args.length > 0 ? args[0] : "localhost:9092";

        // ---------- ① source ----------
        tEnv.executeSql(
            "CREATE TABLE transactions (" +
            "  userId      STRING," +
            "  type        STRING," +
            "  amount      DECIMAL(12, 2)," +
            "  currency    STRING," +
            "  `timestamp` BIGINT," +
            "  ts          AS TO_TIMESTAMP_LTZ(`timestamp`, 3)," +
            "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic'     = 'transactions'," +
            "  'properties.bootstrap.servers' = '" + bootstrap + "'," +
            "  'properties.group.id' = 'flink-sql-capstone'," +
            "  'scan.startup.mode' = 'latest-offset'," +
            "  'format' = 'json'," +
            "  'json.ignore-parse-errors' = 'true'" +
            ")");

        // ---------- ② versioned dimension ----------
        tEnv.executeSql(
            "CREATE TABLE currency_rates (" +
            "  currency  STRING," +
            "  rate      DECIMAL(10, 4)," +
            "  update_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'," +
            "  WATERMARK FOR update_ts AS update_ts - INTERVAL '10' SECOND," +
            "  PRIMARY KEY (currency) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'upsert-kafka'," +
            "  'topic' = 'currency-rates'," +
            "  'properties.bootstrap.servers' = '" + bootstrap + "'," +
            "  'key.format' = 'json'," +
            "  'value.format' = 'json'" +
            ")");

        // ---------- sinks ----------
        tEnv.executeSql(
            "CREATE TABLE user_window_totals (" +
            "  userId STRING," +
            "  window_start TIMESTAMP_LTZ(3)," +
            "  window_end   TIMESTAMP_LTZ(3)," +
            "  total_usd    DECIMAL(20, 2)," +
            "  cnt          BIGINT," +
            "  PRIMARY KEY (userId, window_start) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'upsert-kafka'," +
            "  'topic' = 'user-window-totals'," +
            "  'properties.bootstrap.servers' = '" + bootstrap + "'," +
            "  'key.format' = 'json'," +
            "  'value.format' = 'json'" +
            ")");

        tEnv.executeSql(
            "CREATE TABLE top_spenders (" +
            "  window_start TIMESTAMP_LTZ(3)," +
            "  window_end   TIMESTAMP_LTZ(3)," +
            "  rnk          BIGINT," +
            "  userId       STRING," +
            "  total_usd    DECIMAL(20, 2)," +
            "  PRIMARY KEY (window_start, rnk) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'upsert-kafka'," +
            "  'topic' = 'top-spenders'," +
            "  'properties.bootstrap.servers' = '" + bootstrap + "'," +
            "  'key.format' = 'json'," +
            "  'value.format' = 'json'" +
            ")");

        // ---------- ③ temporal join ----------
        tEnv.executeSql(
            "CREATE TEMPORARY VIEW transactions_usd AS " +
            "SELECT t.userId, t.type, t.amount, t.currency, " +
            "       t.amount * COALESCE(r.rate, CAST(1.0 AS DECIMAL(10,4))) AS amount_usd, " +
            "       t.ts " +
            "FROM transactions AS t " +
            "LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF t.ts AS r " +
            "       ON t.currency = r.currency");

        // ---------- ④ windowed aggregation ----------
        tEnv.executeSql(
            "CREATE TEMPORARY VIEW per_user_window AS " +
            "SELECT userId, window_start, window_end, " +
            "       SUM(amount_usd) AS total_usd, COUNT(*) AS cnt " +
            "FROM TABLE(TUMBLE(TABLE transactions_usd, DESCRIPTOR(ts), INTERVAL '10' MINUTES)) " +
            "WHERE type = 'purchase' " +
            "GROUP BY userId, window_start, window_end");

        // ---------- ⑤ window Top-N ----------
        tEnv.executeSql(
            "CREATE TEMPORARY VIEW top3 AS " +
            "SELECT window_start, window_end, rnk, userId, total_usd FROM (" +
            "  SELECT *, ROW_NUMBER() OVER (" +
            "      PARTITION BY window_start, window_end ORDER BY total_usd DESC) AS rnk " +
            "  FROM per_user_window" +
            ") WHERE rnk <= 3");

        // ---------- inspect the plan before submitting ----------
        System.out.println(tEnv.explainSql("SELECT * FROM top3"));

        // ---------- ⑥ statement set: ONE job, both sinks ----------
        StatementSet stmtSet = tEnv.createStatementSet();

        stmtSet.addInsertSql(
            "INSERT INTO user_window_totals " +
            "SELECT userId, window_start, window_end, total_usd, cnt FROM per_user_window");

        stmtSet.addInsertSql(
            "INSERT INTO top_spenders " +
            "SELECT window_start, window_end, rnk, userId, total_usd FROM top3");

        TableResult result = stmtSet.execute();     // ← submits the job

        System.out.println("submitted: " + result.getJobClient().get().getJobID());

        // NO env.execute() — stmtSet.execute() already submitted.
    }
}
```

Build and submit:

```bash
mvn clean package
./bin/flink run -c com.akash.flink.sql.SqlCapstone target/flink-sql-capstone-1.0.jar kafka:9092
```

Package the connector jar into your shaded artifact, or make sure it is in the cluster's `lib/`.

---

## Design decisions, and why

| Decision | Why |
|---|---|
| Temporal join **before** the window | It preserves the time attribute (ch. 48). A regular join would destroy it and the window would fail to plan. |
| `WHERE type = 'purchase'` inside the window query | Filter before aggregation — less state, less CPU, and the planner may push it into the source. |
| `TUMBLE` rather than `GROUP BY userId` | Append-only, bounded state, one emission per window (ch. 47/49). |
| Window Top-N rather than global Top-N | `PARTITION BY window_start, window_end` makes the ranking final per window → append-only. Global Top-N is upsert with unbounded upstream state. |
| `upsert-kafka` + `PRIMARY KEY` on the sinks | Idempotent on replay; with a compacted topic the topic *is* the current-state table. |
| One `STATEMENT SET` | One job, one source read, one shared window state, one savepoint to operate. |
| `LEFT JOIN` + `COALESCE` on the rate | A transaction in an unknown currency shouldn't vanish, and `NULL` shouldn't poison the `SUM`. |
| `json.ignore-parse-errors = true` | One malformed message otherwise crash-loops the job forever. |
| `table.exec.source.idle-timeout` | A quiet partition would freeze the watermark and no window would ever fire. |
| `table.exec.state.ttl` | Insurance for any operator that turns out to be unbounded. |
| Explicit `table.local-time-zone` | Daily/hourly windows must not shift between laptop and cluster. |

---

## Exercises

1. Change `TUMBLE` to `CUMULATE(TABLE transactions_usd, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '10' MINUTES)` and watch the running totals build up within each 10-minute period, emitting every minute.
2. Replace the window Top-N with a **global** Top-N (`ROW_NUMBER() OVER (ORDER BY total_usd DESC)` on a non-windowed `GROUP BY userId`). Run `EXPLAIN CHANGELOG_MODE` before and after and observe `[I]` become `[I,UA,D]`.
3. Point the `top_spenders` sink at the plain `kafka` connector instead of `upsert-kafka` after doing exercise 2, and read the exact error from chapter 47.
4. Add a `SESSION` window view (Flink 1.19+) computing per-user session lengths, and add a third `INSERT INTO` to the statement set.
5. Add an `OVER` window to `transactions_usd` computing each user's running 5-minute purchase count, and filter to `>= 5` — the fraud rule from phase 4, in SQL.
6. Run `EXPLAIN PLAN_ADVICE` on the whole statement set and read every warning it prints.

---

## Remember

- Compose the pipeline so **every stage is append-only**: temporal join → window TVF → window Top-N. Bounded state end to end, and a simple sink.
- Do the time-dependent join **before** the window — a regular join destroys the time attribute; a temporal join preserves it.
- `EXECUTE STATEMENT SET BEGIN ... END;` = multiple `INSERT INTO`, **one job**, shared plan, one source read, one savepoint. In Java: `tEnv.createStatementSet()` + `addInsertSql` + `execute()`.
- `upsert-kafka` sinks need `PRIMARY KEY ... NOT ENFORCED` and `key.format`/`value.format`. Pair with `cleanup.policy=compact`.
- `datagen` + `fields.<ts>.max-past` + `print` = a complete testable pipeline with zero infrastructure. This is how you iterate.
- `print` writes to the **TaskManager** log, not the client. `tail -f log/flink-*-taskexecutor-*.out`.
- Pre-flight every deploy: `DESCRIBE`, a `LIMIT 5` on the source, `EXPLAIN CHANGELOG_MODE`, `EXPLAIN`, `EXPLAIN PLAN_ADVICE`.
- Exactly one submission per job: `executeSql("INSERT ...")`, `stmtSet.execute()`, `table.execute()`, or `env.execute()`. Never two.
- `COMPILE PLAN ... FOR` + `EXECUTE PLAN` to pin operator IDs and keep savepoints restorable across query edits and Flink upgrades.

**Interview one-liners**

- *"How do you write two outputs from one Flink SQL pipeline?"* → A `STATEMENT SET`. Multiple `INSERT INTO` in one compiled job — the source is read once and the shared sub-plan is computed once, instead of two jobs each doing the full work.
- *"Walk me through a production Flink SQL pipeline."* → Kafka source with a JSON payload and a computed `TO_TIMESTAMP_LTZ` rowtime plus a watermark; a temporal join to an upsert-kafka versioned dimension for enrichment; a `TUMBLE` window TVF for the aggregation; a window Top-N; and a statement set writing to upsert-kafka sinks on compacted topics.
- *"Why is that whole pipeline append-only?"* → The temporal join fixes the dimension version by event time, the window fires once at the watermark, and the Top-N partitions by the window columns so the ranking is final. Nothing can invalidate an emitted row.
- *"How do you test Flink SQL without a cluster or Kafka?"* → A `datagen` source with `number-of-rows` and `fields.<ts>.max-past`, and a `print` or `blackhole` sink, run through `sql-client.sh -f`.
