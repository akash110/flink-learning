# 51. Connectors and Formats in SQL

A `CREATE TABLE` in Flink does **not** create anything. It registers a *description* of where data lives and how it's encoded. The `WITH (...)` clause is that description.

```
CREATE TABLE t (
   ... schema: what Flink believes the columns are ...
) WITH (
   'connector' = 'kafka'      ← WHERE the bytes are
   'format'    = 'json'       ← HOW the bytes are encoded
   ... plus connector-specific options ...
);
```

> **Key idea:** Connector and format are orthogonal. Kafka+JSON, Kafka+Avro, filesystem+CSV, filesystem+Parquet — any combination the connector supports. Flink has **no schema inference for streaming sources**: the schema you write is taken as truth, and a mismatch surfaces as nulls or parse errors, not as a helpful message.

---

## Kafka — the full source DDL

```sql
CREATE TABLE transactions (
  userId      STRING,
  type        STRING,
  amount      DOUBLE,
  `timestamp` BIGINT,
  ts          AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'transactions',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id'          = 'flink-sql-transactions',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true',
  'json.fail-on-missing-field'   = 'false'
);
```

Option by option:

| Option | Notes |
|---|---|
| `'connector' = 'kafka'` | The append-only Kafka connector. Source and sink. |
| `'topic'` | One topic, or several semicolon-separated: `'t1;t2'`. Sink accepts exactly one. |
| `'topic-pattern'` | A regex instead of `topic`, for dynamic topic discovery. Mutually exclusive with `topic`. |
| `'properties.bootstrap.servers'` | Required. |
| `'properties.group.id'` | Optional for a source (a random one is generated). **Only used for offset committing and lag visibility** — Flink assigns partitions itself, exactly as in chapter 25. |
| `'properties.*'` | Any Kafka client property passes through: `properties.security.protocol`, `properties.sasl.mechanism`, `properties.max.poll.records`, etc. |
| `'scan.startup.mode'` | Cold-start position only. See below. |
| `'format'` | The value format. |

`scan.startup.mode` values:

```sql
'scan.startup.mode' = 'earliest-offset'     -- from the beginning of the topic
'scan.startup.mode' = 'latest-offset'       -- only new records (default)
'scan.startup.mode' = 'group-offsets'       -- from the committed offsets of properties.group.id
'scan.startup.mode' = 'timestamp'           -- requires scan.startup.timestamp-millis
'scan.startup.mode' = 'specific-offsets'    -- requires scan.startup.specific-offsets
```

```sql
'scan.startup.mode' = 'timestamp',
'scan.startup.timestamp-millis' = '1756483200000'

'scan.startup.mode' = 'specific-offsets',
'scan.startup.specific-offsets' = 'partition:0,offset:42;partition:1,offset:300'
```

**This is a cold-start setting only.** On restore from a checkpoint or savepoint, the offsets in the snapshot win — exactly as in the DataStream API. A job that "reprocessed a week of data" started fresh and fell back to `earliest-offset`.

Bounded reads (turns a streaming source into a batch one):

```sql
'scan.bounded.mode' = 'latest-offset'       -- read up to the end-of-topic at startup, then finish
'scan.bounded.mode' = 'timestamp',
'scan.bounded.timestamp-millis' = '1756569600000'
```

Handy for backfills and for testing a query over a fixed slice.

Partition discovery, on by default:

```sql
'scan.topic-partition-discovery.interval' = '5 min'
```

### Kafka as an append sink

```sql
CREATE TABLE alerts_out (
  userId STRING,
  reason STRING,
  ts     TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic'     = 'alerts',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format'    = 'json',
  'sink.partitioner' = 'default',
  'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO alerts_out
SELECT userId, 'large-amount', ts FROM transactions WHERE amount > 5000;
```

- `sink.partitioner`: `default` (Kafka's own partitioner — round-robin/sticky if no key), `fixed` (each Flink subtask writes to exactly one partition — fewer connections, but can skew), `round-robin`, or a fully-qualified class name.
- `sink.delivery-guarantee`: `none` | `at-least-once` (default) | `exactly-once`.

For exactly-once you must also configure the transaction prefix and have checkpointing on:

```sql
'sink.delivery-guarantee' = 'exactly-once',
'sink.transactional-id-prefix' = 'tx-alerts-v1',
'properties.transaction.timeout.ms' = '900000'
```

- `sink.transactional-id-prefix` must be **unique per job** and stable across restarts. Two jobs sharing a prefix will fence each other.
- Kafka's broker-side `transaction.max.timeout.ms` (default 15 min) must be ≥ the client timeout, and the timeout must exceed your maximum expected checkpoint interval + recovery time, or in-flight transactions expire and you lose data.
- Consumers must set `isolation.level=read_committed` to actually get exactly-once, otherwise they read uncommitted records.

This is the same two-phase-commit machinery from phase 5, just configured in strings.

---

## upsert-kafka — for results that update

This is the connector you reach for whenever chapter 47's error appears.

```sql
CREATE TABLE user_totals (
  userId STRING,
  total  DOUBLE,
  cnt    BIGINT,
  PRIMARY KEY (userId) NOT ENFORCED
) WITH (
  'connector'    = 'upsert-kafka',
  'topic'        = 'user-totals',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format'   = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

INSERT INTO user_totals
SELECT userId, SUM(amount), COUNT(*) FROM transactions GROUP BY userId;
```

Differences from the plain `kafka` connector:

| | `kafka` | `upsert-kafka` |
|---|---|---|
| Changelog it accepts as a **sink** | append (`+I`) only | `+I`, `+U`, `-D` |
| Changelog it produces as a **source** | append | upsert (a full changelog stream) |
| `PRIMARY KEY` | not allowed | **required** |
| Format option | single `format` | separate `key.format` and `value.format` |
| Delete | impossible | a **tombstone**: same key, `null` value |
| Partitioning | `sink.partitioner` | always hash-by-key (so a key's history stays ordered in one partition) |

**When you must use it:**

1. Your query is retract/upsert mode and the sink is Kafka. (The most common reason.)
2. You want the topic to *be* a table — enable **log compaction** on the topic and the latest value per key is retained forever, deletes included. Downstream consumers replaying the topic reconstruct the current state.
3. You want to read a compacted topic back into Flink as a changelog table — `upsert-kafka` as a **source** produces `+U`/`-D` rows and can be used as a versioned table for temporal joins (chapter 50).

`'value.fields-include'`:

- `'ALL'` (default) — key columns appear in the value payload too. Slight duplication, but the value is self-describing.
- `'EXCEPT_KEY'` — key columns are omitted from the value. Smaller messages.

The resulting topic:

```
key: {"userId":"u-001"}   value: {"userId":"u-001","total":100.0,"cnt":1}
key: {"userId":"u-001"}   value: {"userId":"u-001","total":300.0,"cnt":2}
key: {"userId":"u-001"}   value: null                                      ← delete
```

Configure the topic with `cleanup.policy=compact` and Kafka keeps only the last value per key.

---

## Formats

### json

```sql
'format' = 'json',
'json.fail-on-missing-field'      = 'false',   -- true = throw if a declared column is absent
'json.ignore-parse-errors'        = 'true',    -- true = skip malformed records instead of failing
'json.timestamp-format.standard'  = 'ISO-8601', -- or 'SQL' (default)
'json.map-null-key.mode'          = 'FAIL',    -- FAIL | DROP | LITERAL, on write
'json.map-null-key.literal'       = 'null',
'json.encode.decimal-as-plain-number' = 'true'  -- avoid scientific notation on write
```

`fail-on-missing-field` and `ignore-parse-errors` are mutually exclusive — setting both to `true` is rejected.

**Always set `'json.ignore-parse-errors' = 'true'` in production.** One malformed message in a topic will otherwise crash-loop the job forever: it fails, restarts from the checkpoint, reads the same bad message, fails again. Watch the `numRecordsIn` vs output gap to detect how many you're silently dropping.

Nested JSON maps to `ROW` and `ARRAY`:

```sql
CREATE TABLE events (
  userId  STRING,
  device  ROW<os STRING, version STRING>,     -- {"device": {"os": "iOS", ...}}
  tags    ARRAY<STRING>,
  props   MAP<STRING, STRING>
) WITH ('connector' = 'kafka', ..., 'format' = 'json');
```

Access nested fields with dot notation: `SELECT device.os FROM events`.

### csv

```sql
'format' = 'csv',
'csv.field-delimiter'      = ',',
'csv.disable-quote-character' = 'false',
'csv.quote-character'      = '"',
'csv.allow-comments'       = 'false',
'csv.ignore-parse-errors'  = 'true',
'csv.array-element-delimiter' = ';',
'csv.null-literal'         = '\\N'
```

No header handling for streaming sources — CSV in Flink is positional, matched to your declared column order. Mostly used with the `filesystem` connector.

### avro

```sql
'format' = 'avro'
```

The schema is **derived from your Flink table schema**. There is no schema file. Good for compact encoding when producer and consumer are both Flink; risky when a separate producer's writer schema drifts from your declared schema.

### avro-confluent — Schema Registry

The one you'll actually use in a company that has a registry.

```sql
CREATE TABLE transactions (
  userId      STRING,
  type        STRING,
  amount      DOUBLE,
  `timestamp` BIGINT,
  ts          AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic'     = 'transactions',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format'    = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081',
  'avro-confluent.subject' = 'transactions-value',
  'avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
  'avro-confluent.basic-auth.user-info' = 'user:password'
);
```

- Reads the 5-byte Confluent wire header (magic byte + 4-byte schema id), fetches the writer schema from the registry, and reads it into your declared schema.
- `avro-confluent.subject` is required when **writing** (it's how the schema gets registered); optional when reading.
- Registry TLS options: `avro-confluent.ssl.keystore.location`, `avro-confluent.ssl.truststore.location`, and their passwords.

Combined with `upsert-kafka`, key and value formats are configured separately with their own prefixes:

```sql
'key.format'  = 'avro-confluent',
'key.avro-confluent.url'   = 'http://schema-registry:8081',
'value.format' = 'avro-confluent',
'value.avro-confluent.url' = 'http://schema-registry:8081'
```

There's also `'format' = 'json-registry'`/`debezium-avro-confluent` variants in the same family.

### debezium-json — CDC

Debezium captures row changes from a database's write-ahead log and publishes them to Kafka. The payload carries `before`, `after`, and an operation code. Flink decodes it directly into a changelog table.

```sql
CREATE TABLE customers (
  id      INT,
  name    STRING,
  country STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic'     = 'dbserver1.inventory.customers',
  'properties.bootstrap.servers' = 'localhost:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format'    = 'debezium-json',
  'debezium-json.schema-include' = 'false',
  'debezium-json.ignore-parse-errors' = 'true'
);
```

- `'connector' = 'kafka'` — note it's the *plain* Kafka connector. The **format** is what makes the table a changelog source; the connector doesn't need to know.
- `debezium-json.schema-include` — `true` if Debezium was configured with `value.converter.schemas.enable=true` (payload wrapped in `{"schema":..., "payload":...}`). Getting this wrong yields a parse error on every record.
- `PRIMARY KEY` — declare it so the table can serve as a versioned table for temporal joins.

The wire format Flink decodes:

```json
{"before": null,
 "after":  {"id": 1, "name": "Ada", "country": "UK"},
 "op": "c"}                                            → +I

{"before": {"id": 1, "name": "Ada",  "country": "UK"},
 "after":  {"id": 1, "name": "Ada L","country": "UK"},
 "op": "u"}                                            → -U then +U

{"before": {"id": 1, "name": "Ada L","country": "UK"},
 "after":  null,
 "op": "d"}                                            → -D
```

Because a Debezium source produces the full changelog vocabulary, **it can be joined and aggregated exactly like any other table** — Flink handles the retractions. This is how you keep a Flink-side mirror of an operational database, and it's the standard pattern for streaming enrichment against a real dimension table.

Related formats: `canal-json` (Canal, MySQL), `maxwell-json`, `ogg-json` (Oracle GoldenGate), and `debezium-avro-confluent`. Same idea, different producers.

A note on the alternative: **Flink CDC** (`flink-connector-mysql-cdc`, `'connector' = 'mysql-cdc'`) reads the database binlog *directly*, with no Kafka and no Debezium Connect cluster. Simpler for small setups; Kafka in the middle is better when several consumers need the same feed.

---

## datagen — the fastest way to experiment

No Kafka, no files, no setup. Generates rows in memory.

```sql
CREATE TABLE transactions (
  userId      STRING,
  type        STRING,
  amount      DOUBLE,
  `timestamp` BIGINT,
  ts          AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'       = 'datagen',
  'rows-per-second' = '10',

  -- userId: random string of length 4 → about 36^4 distinct values; too many.
  -- Use a bounded set instead by generating a small int and casting. See below.
  'fields.userId.length' = '4',

  'fields.type.length'   = '8',

  'fields.amount.min'    = '1',
  'fields.amount.max'    = '2000',

  -- timestamps near "now": sequence won't do it; see the recipe below
  'fields.timestamp.min' = '1756483200000',
  'fields.timestamp.max' = '1756483800000'
);
```

Option families:

| Option | Meaning |
|---|---|
| `rows-per-second` | Throughput. Default 10000. |
| `number-of-rows` | Total rows, then the source **finishes** (bounded). Omit for infinite. |
| `fields.<name>.kind` | `random` (default) or `sequence` |
| `fields.<name>.min` / `.max` | Numeric range, for `random` |
| `fields.<name>.length` | String/bytes length, for `random` |
| `fields.<name>.start` / `.end` | For `kind = 'sequence'` |
| `fields.<name>.null-rate` | Fraction of nulls, 0.0–1.0 |
| `fields.<name>.max-past` | For timestamp columns: max lag behind current processing time |

**The recipe you actually want** — a small number of distinct users, and timestamps near *now* so windows fire:

```sql
CREATE TABLE transactions (
  user_num    INT,
  type_num    INT,
  amount      DOUBLE,
  ts          TIMESTAMP_LTZ(3),

  userId      AS CONCAT('u-', CAST(user_num AS STRING)),
  type        AS CASE type_num WHEN 0 THEN 'purchase'
                               WHEN 1 THEN 'refund'
                               ELSE 'view' END,

  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'       = 'datagen',
  'rows-per-second' = '10',
  'fields.user_num.min' = '1',
  'fields.user_num.max' = '20',        -- only 20 distinct users → aggregations actually group
  'fields.type_num.min' = '0',
  'fields.type_num.max' = '2',
  'fields.amount.min'   = '1',
  'fields.amount.max'   = '2000',
  'fields.ts.max-past'  = '5000'       -- timestamps within the last 5s of now
);
```

- `fields.ts.max-past` on a `TIMESTAMP_LTZ(3)` column generates timestamps between `now - 5s` and `now`. This is the key to making event-time windows fire immediately — without it, `datagen` timestamps sit at the epoch and your watermark never reaches a sensible window.
- Deriving `userId` and `type` from small ints via computed columns is the trick for a controlled cardinality. Random 4-char strings give you ~1.6M distinct users, and every group has exactly one row.

Bounded generation for a deterministic test:

```sql
'number-of-rows' = '1000'
```

The source finishes after 1000 rows, the job terminates, and `SELECT` returns a final result. Excellent for testing a query end to end without a cluster.

---

## print — see the output

```sql
CREATE TABLE console (
  userId STRING,
  total  DOUBLE
) WITH (
  'connector' = 'print',
  'print-identifier' = 'RESULT',
  'standard-error' = 'false'
);

INSERT INTO console
SELECT userId, SUM(amount) FROM transactions GROUP BY userId;
```

Output appears in the **TaskManager logs**, not your client:

```
RESULT:2> +I[u-001, 100.0]
RESULT:2> -U[u-001, 100.0]
RESULT:2> +U[u-001, 300.0]
```

- `2>` is the subtask index.
- `+I` / `-U` / `+U` — the print sink accepts **all** changelog modes, which makes it the ideal debugging sink for a retract query.
- `print-identifier` prefixes each line so you can grep for it when several print sinks exist.

There's also `'connector' = 'blackhole'` — accepts everything, writes nothing. Use it to benchmark a query with the sink cost removed, exactly like Spark's `noop` format.

```sql
CREATE TABLE devnull (userId STRING, total DOUBLE) WITH ('connector' = 'blackhole');
```

---

## filesystem

```sql
CREATE TABLE archive (
  userId STRING,
  amount DOUBLE,
  ts     TIMESTAMP(3),
  dt     STRING,
  hr     STRING
) PARTITIONED BY (dt, hr) WITH (
  'connector' = 'filesystem',
  'path'      = 's3://lake/transactions',
  'format'    = 'parquet',
  'sink.partition-commit.trigger'       = 'partition-time',
  'sink.partition-commit.delay'         = '1 h',
  'sink.partition-commit.policy.kind'   = 'success-file',
  'sink.rolling-policy.file-size'       = '128MB',
  'sink.rolling-policy.rollover-interval' = '15 min'
);
```

- `PARTITIONED BY (dt, hr)` — Hive-style directory partitioning: `dt=2026-08-29/hr=14/`.
- `sink.partition-commit.trigger = 'partition-time'` — commit a partition when the **watermark** passes its end plus the delay. `'process-time'` uses the wall clock instead.
- `sink.partition-commit.policy.kind = 'success-file'` writes `_SUCCESS`; `'metastore'` adds the partition to Hive; `'metastore,success-file'` does both.
- Rolling policy controls file size — this is where the small-files problem lives, exactly as in Spark.

`format` here can be `parquet`, `orc`, `avro`, `json`, `csv`, `raw`.

---

## Catalogs

A **catalog** holds table definitions. `CREATE TABLE` writes into the current catalog.

```sql
Flink SQL> SHOW CATALOGS;
+-----------------+
|    catalog name |
+-----------------+
| default_catalog |
+-----------------+
```

The default is `GenericInMemoryCatalog`: **everything you define disappears when the session ends**. Fine for exploration, useless for a team.

Hierarchy is `catalog.database.table`. `default_catalog.default_database.transactions` is the full name of a table you created with no qualification.

### Hive catalog — persistent definitions

```sql
CREATE CATALOG hive_cat WITH (
  'type'                = 'hive',
  'default-database'    = 'analytics',
  'hive-conf-dir'       = '/opt/flink/conf/hive'
);

USE CATALOG hive_cat;

CREATE TABLE transactions ( ... ) WITH ( 'connector' = 'kafka', ... );
```

Now the DDL lives in the Hive Metastore. Another session, another job, another team runs `USE CATALOG hive_cat; SELECT * FROM transactions;` and it works. Flink stores non-Hive tables in the metastore as "generic" tables with the `WITH` options as table properties — Hive itself can't read them, but Flink can.

Requires `flink-sql-connector-hive-<version>.jar` in `lib/`.

Other catalogs you'll meet: `jdbc` (Postgres/MySQL catalogs), `paimon`, `iceberg`, and vendor catalogs in managed services (AWS Glue, Confluent). The point is the same: **the table definition outlives the session**.

### Temporary vs permanent

```sql
CREATE TEMPORARY TABLE t (...) WITH (...);   -- session only, not written to the catalog
CREATE TABLE t (...) WITH (...);             -- written to the catalog
CREATE TEMPORARY VIEW v AS SELECT ...;       -- session only
```

`TEMPORARY` objects shadow permanent ones of the same name and are dropped at session end. Useful for overriding a production table with a `datagen` version in a test.

---

## The jar situation

Connectors are **not** in the Flink distribution. This is the single biggest practical friction in Flink SQL.

```
flink-1.20.0/
├── lib/
│   ├── flink-dist-1.20.0.jar
│   ├── flink-table-planner-loader-1.20.0.jar
│   ├── flink-table-runtime-1.20.0.jar
│   ├── flink-connector-files-1.20.0.jar
│   ├── flink-csv-1.20.0.jar
│   ├── flink-json-1.20.0.jar          ← json IS bundled
│   └── log4j-*.jar
└── (kafka, jdbc, avro, hive: NOT here — you download them)
```

The error when a jar is missing:

```
org.apache.flink.table.api.ValidationException: Could not find any factory for identifier 'kafka'
that implements 'org.apache.flink.table.factories.DynamicTableFactory' in the classpath.

Available factory identifiers are:
blackhole
datagen
filesystem
print
```

That "Available factory identifiers" list is genuinely useful — it tells you exactly what *is* loaded.

Download the **`flink-sql-connector-*`** artifacts, not the `flink-connector-*` ones. The `sql-` variants are fat jars with the connector's transitive dependencies shaded in; the plain ones expect you to manage dependencies yourself and will fail with `NoClassDefFoundError` in the SQL Client.

```bash
cd flink-1.20.0/lib

# Kafka (the version suffix pairs a connector version with a Flink version)
curl -O https://repo.maven.apache.org/maven2/org/apache/flink/\
flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

# JDBC + the actual database driver (the driver is separate!)
curl -O https://repo.maven.apache.org/maven2/org/apache/flink/\
flink-sql-connector-jdbc/3.2.0-1.19/flink-sql-connector-jdbc-3.2.0-1.19.jar
curl -O https://repo.maven.apache.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar

# Avro + Confluent Schema Registry
curl -O https://repo.maven.apache.org/maven2/org/apache/flink/\
flink-sql-avro-confluent-registry/1.20.0/flink-sql-avro-confluent-registry-1.20.0.jar

cd .. && ./bin/stop-cluster.sh && ./bin/start-cluster.sh   # RESTART — lib/ is read at startup
```

**You must restart the cluster.** `lib/` is scanned when the JVM starts.

Alternative, without touching `lib/` — add jars at SQL Client startup or from inside a session:

```bash
./bin/sql-client.sh -j /path/to/flink-sql-connector-kafka-3.2.0-1.19.jar \
                    -j /path/to/mysql-connector-j-8.4.0.jar
```

```sql
Flink SQL> ADD JAR '/path/to/flink-sql-connector-kafka-3.2.0-1.19.jar';
Flink SQL> SHOW JARS;
```

For a **Java job**, the connector is a Maven dependency at `compile` scope (or shaded into your job jar):

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka</artifactId>
  <version>3.2.0-1.19</version>
</dependency>
```

Note the version format `3.2.0-1.19` — connector version, then the Flink version it targets. Externalized connectors have their own release cadence since Flink 1.16, which is why they no longer match the Flink version number.

---

## Init file for the SQL Client

Rather than retyping DDL every session:

```sql
-- init.sql
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.state.ttl' = '24 h';
SET 'table.exec.source.idle-timeout' = '30 s';
SET 'execution.checkpointing.interval' = '30 s';

CREATE TEMPORARY TABLE transactions ( ... ) WITH ( ... );
CREATE TEMPORARY TABLE alerts ( ... ) WITH ( ... );
```

```bash
./bin/sql-client.sh -i init.sql
./bin/sql-client.sh -i init.sql -f query.sql   # init, then run a query file
```

---

## Remember

- `CREATE TABLE` registers a description; it never creates a topic, file, or database table.
- `connector` = where the bytes are; `format` = how they're encoded. Orthogonal.
- No schema inference for streaming sources. Your declared schema is taken as truth.
- `scan.startup.mode` applies on **cold start only** — checkpoints and savepoints override it.
- `properties.group.id` on a Kafka source is for offset committing and lag monitoring; Flink assigns partitions itself.
- `'json.ignore-parse-errors' = 'true'` in production, or one bad message crash-loops the job forever.
- **`upsert-kafka`** for any retract/upsert result: requires `PRIMARY KEY ... NOT ENFORCED`, uses separate `key.format`/`value.format`, writes tombstones for deletes, always hash-partitions by key. Pair with a compacted topic.
- Exactly-once Kafka sink: `sink.delivery-guarantee = 'exactly-once'` + a **unique, stable** `sink.transactional-id-prefix` + checkpointing + `read_committed` consumers + a big enough transaction timeout.
- `debezium-json` is a *format*, used with the plain `kafka` connector, and produces a full changelog you can join and aggregate normally.
- `avro-confluent` needs `avro-confluent.url`; `avro-confluent.subject` is required for writes.
- `datagen` is the fastest experimentation tool. Use small int fields + computed columns for controlled cardinality, and `fields.<ts>.max-past` so event-time windows actually fire. `number-of-rows` makes it bounded.
- `print` accepts all changelog modes and writes to the **TaskManager** log. `blackhole` for benchmarking.
- Default catalog is in-memory: your DDL dies with the session. Use a Hive catalog for persistent, shared table definitions.
- Connector jars are **not** bundled. Download `flink-sql-connector-*` (the fat variants) into `lib/` and restart, or use `-j` / `ADD JAR`. `json`, `csv`, `filesystem`, `datagen`, `print`, `blackhole` are built in.
- `"Could not find any factory for identifier 'kafka'"` = missing jar. Read the "Available factory identifiers" list it prints.

**Interview one-liners**

- *"How do you read Kafka in Flink SQL?"* → `CREATE TABLE ... WITH ('connector'='kafka', 'topic'=..., 'properties.bootstrap.servers'=..., 'format'='json')` plus a `WATERMARK` clause for event time.
- *"When do you need upsert-kafka?"* → Whenever the query produces updates — any non-windowed `GROUP BY`, regular join, or Top-N — and the sink is Kafka. It needs a primary key and writes tombstones for deletes.
- *"What's a tombstone?"* → A Kafka message with a key and a null value, meaning "this key is deleted". Log compaction then removes the key entirely.
- *"How does Flink consume CDC?"* → The `debezium-json` (or `canal-json`, `maxwell-json`) format on a plain Kafka table, or the `mysql-cdc` connector reading the binlog directly. Either way it becomes a changelog table you can join and aggregate.
- *"How do you test a Flink SQL query with no infrastructure?"* → A `datagen` source with `number-of-rows` set and a `print` sink. Whole pipeline, no Kafka.
- *"'Could not find any factory for identifier kafka' — what happened?"* → The connector jar isn't on the classpath. Put `flink-sql-connector-kafka` in `lib/` and restart the cluster.
- *"Where do your SQL table definitions live?"* → In-memory by default, so they vanish with the session. Use a Hive (or JDBC/Iceberg/Glue) catalog to persist and share them.
