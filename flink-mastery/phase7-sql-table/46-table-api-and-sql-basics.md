# 46. Table API and SQL — The Basics

Phases 1–6 taught you the **DataStream API**: you describe *how* the job runs. You wire `map`, `keyBy`, `window`, `KeyedProcessFunction`, timers, state descriptors. Every byte in state is there because you put it there.

Flink SQL is the other half of Flink. You describe *what* you want, and a planner writes the DataStream job for you.

```
YOU WRITE                    FLINK BUILDS
─────────────────────────────────────────────────────────────
SELECT userId,               ┌──────────────┐
       SUM(amount)           │  SQL string  │
FROM transactions            └──────┬───────┘
GROUP BY userId                     │  parse
                             ┌──────▼───────┐
                             │ Calcite AST  │
                             └──────┬───────┘
                                    │  validate against catalog
                             ┌──────▼───────┐
                             │ Logical plan │
                             └──────┬───────┘
                                    │  optimize (rules + cost)
                             ┌──────▼───────┐
                             │Physical plan │  StreamExecGroupAggregate...
                             └──────┬───────┘
                                    │  code generation (Java source, compiled)
                             ┌──────▼───────┐
                             │  Transform-  │  the same JobGraph a
                             │  ations →    │  DataStream job produces
                             │  JobGraph    │
                             └──────────────┘
```

> **Key idea:** SQL is not a separate engine. It compiles down to the *exact same* operators, state backends, checkpoints, and watermarks you learned in phases 1–6. Everything you know about watermarks and state still applies — you just have less direct control over it.

---

## The two environments

There are two entry points and the difference matters.

```java
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

// (1) Pure Table/SQL. No DataStream anywhere in the job.
EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .inStreamingMode()      // .inBatchMode() for bounded sources
        .build();
TableEnvironment tEnv = TableEnvironment.create(settings);
```

Line by line:

- `EnvironmentSettings.newInstance()` — a builder for planner configuration.
- `.inStreamingMode()` — the query runs forever and produces incremental results. `.inBatchMode()` runs to completion and produces one final result. **Same SQL, different semantics.**
- `TableEnvironment.create(settings)` — a *pure* table environment. It cannot see a `DataStream`. This is what the SQL Client and SQL-only jobs use.

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// (2) The bridge. Needed the moment you want to mix SQL and DataStream.
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);

StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
```

- `StreamExecutionEnvironment` — the same object from chapter 3. Parallelism, checkpointing, restart strategy are set **here**, not on the table env.
- `StreamTableEnvironment.create(env)` — wraps it. Now the table env and the DataStream env share one job graph.

| You need... | Use |
|---|---|
| SQL only, `INSERT INTO` sink, no Java operators | `TableEnvironment` |
| `toDataStream` / `fromDataStream` anywhere | `StreamTableEnvironment` |
| A `KeyedProcessFunction` in the middle of a SQL pipeline | `StreamTableEnvironment` |
| SQL Client / `flink-sql-runner` style deployment | `TableEnvironment` (implicit) |

Maven dependency you need for the bridge (Flink 1.18+, Scala-free artifact names):

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge</artifactId>
  <version>1.20.0</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner-loader</artifactId>
  <version>1.20.0</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-runtime</artifactId>
  <version>1.20.0</version>
  <scope>provided</scope>
</dependency>
```

`flink-table-planner-loader` is the shaded planner that ships in `lib/` on a real cluster — hence `provided`. If you get `Could not instantiate the executor` at runtime, this jar is missing.

---

## The three methods you will use constantly

### 1. `executeSql` — DDL and anything with a side effect

```java
tEnv.executeSql(
    "CREATE TABLE transactions (" +
    "  userId    STRING," +
    "  type      STRING," +
    "  amount    DOUBLE," +
    "  ts        TIMESTAMP(3)," +
    "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
    ") WITH (" +
    "  'connector' = 'datagen'," +
    "  'rows-per-second' = '5'" +
    ")");
```

- `executeSql` runs the statement **immediately and eagerly**. For DDL that means the table is registered in the catalog right now.
- The column list is the *schema Flink believes*, not something read from Kafka. Flink has no schema inference for streaming connectors — you declare it.
- `WATERMARK FOR ts AS ...` makes `ts` an **event-time attribute**. Chapter 48 is entirely about this line.
- `WITH (...)` is the connector configuration. Every option is a string key/value. Chapter 51 covers connectors.

`executeSql` returns a `TableResult`. For DDL you ignore it. For `INSERT INTO` it holds the submitted job:

```java
TableResult result = tEnv.executeSql(
    "INSERT INTO alerts SELECT userId, amount FROM transactions WHERE amount > 1000");

result.getJobClient().get().getJobID();   // the job is ALREADY running
```

> **Key idea:** `executeSql("INSERT INTO ...")` **submits a job by itself**. You do not call `env.execute()` afterwards. Calling it anyway throws `No operators defined in streaming topology`.

### 2. `sqlQuery` — build a `Table`, run nothing

```java
Table bigSpend = tEnv.sqlQuery(
    "SELECT userId, amount, ts " +
    "FROM transactions " +
    "WHERE amount > 500");
```

- `sqlQuery` is **lazy**. Nothing executes. You get a `Table` object, which is a handle on an unexecuted logical plan.
- The query can only reference tables already in the catalog.

Register it so other SQL can see it:

```java
tEnv.createTemporaryView("big_spend", bigSpend);

Table counted = tEnv.sqlQuery("SELECT userId, COUNT(*) AS c FROM big_spend GROUP BY userId");
```

`createTemporaryView` puts the *plan* into the catalog under a name — it does not materialize or cache anything. `big_spend` is inlined into every query that references it. Referencing it twice means the filter runs twice unless the optimizer decides to reuse the subplan.

### 3. `table.execute().print()` — see the rows

```java
counted.execute().print();
```

- `.execute()` submits a job whose sink is a local collect sink.
- `.print()` blocks and prints rows to stdout as they arrive.

Output for a streaming aggregation:

```
+----+--------------------------------+----------------------+
| op |                         userId |                    c |
+----+--------------------------------+----------------------+
| +I |                          u-001 |                    1 |
| -U |                          u-001 |                    1 |
| +U |                          u-001 |                    2 |
| +I |                          u-002 |                    1 |
```

That `op` column is the whole subject of chapter 47. `+I` = insert, `-U` = retract the old row, `+U` = the new row. A streaming `GROUP BY` **corrects itself** by emitting a retraction and a new value.

**This is a debugging tool, not production.** `print()` runs the sink at parallelism 1 in your client JVM.

---

## Converting between Table and DataStream

This is the bridge in both directions, and picking the wrong variant is the most common early mistake.

### `fromDataStream` — DataStream → Table

```java
DataStream<Event> events = env.fromSource(kafkaSource, wmStrategy, "kafka");

Table t = tEnv.fromDataStream(events);          // schema derived from the POJO
tEnv.createTemporaryView("events", t);
```

Flink reflects over the `Event` POJO (`userId` String, `type` String, `amount` double, `timestamp` long) and produces columns of those names and types. Field order follows the POJO's declaration order only if it is a valid POJO (public no-arg constructor, public fields or getters/setters).

To declare time attributes and column names explicitly, use a `Schema`:

```java
import org.apache.flink.table.api.Schema;
import static org.apache.flink.table.api.Expressions.$;

Table t = tEnv.fromDataStream(
    events,
    Schema.newBuilder()
        .column("userId", "STRING")
        .column("type", "STRING")
        .column("amount", "DOUBLE")
        .column("timestamp", "BIGINT")
        .columnByExpression("ts", "TO_TIMESTAMP_LTZ(`timestamp`, 3)")
        .watermark("ts", "SOURCE_WATERMARK()")
        .build());
```

- `.column(name, type)` — a physical column read from the POJO field of that name.
- `.columnByExpression("ts", ...)` — a **computed column**. It does not exist in the POJO; it is derived. `TO_TIMESTAMP_LTZ(millis, 3)` turns the epoch-millis `long` into a `TIMESTAMP_LTZ(3)`.
- `.watermark("ts", "SOURCE_WATERMARK()")` — **reuse the watermarks already in the DataStream**. This is the important one. If your `DataStream` already had a `WatermarkStrategy` applied (it did — chapter 10), do *not* declare a new bounded-out-of-orderness watermark here; you would be generating a second, unrelated watermark. `SOURCE_WATERMARK()` says "the underlying stream's watermarks are the truth."
- `` `timestamp` `` is backtick-quoted because `TIMESTAMP` is a reserved SQL keyword.

### `toDataStream` — Table → DataStream, **append-only** results only

```java
Table filtered = tEnv.sqlQuery("SELECT userId, amount FROM events WHERE amount > 100");

DataStream<Row> out = tEnv.toDataStream(filtered);
out.print();
```

`toDataStream` produces a `DataStream<Row>` of plain inserts. It **throws at plan time** if the query produces updates:

```
org.apache.flink.table.api.TableException: Table sink 'default_catalog.default_database.Unregistered_DataStream_Sink_1'
doesn't support consuming update changes which is produced by node GroupAggregate(...)
```

That error means: your query updates rows; a plain `DataStream` cannot express "forget the last value I sent."

Convert into a concrete class instead of `Row`:

```java
DataStream<Event> typed = tEnv.toDataStream(filtered, Event.class);
```

### `toChangelogStream` — Table → DataStream, **updating** results

```java
Table perUser = tEnv.sqlQuery("SELECT userId, SUM(amount) AS total FROM events GROUP BY userId");

DataStream<Row> changelog = tEnv.toChangelogStream(perUser);

changelog.process(new ProcessFunction<Row, String>() {
    @Override
    public void processElement(Row row, Context ctx, Collector<String> out) {
        RowKind kind = row.getKind();          // INSERT / UPDATE_BEFORE / UPDATE_AFTER / DELETE
        if (kind == RowKind.UPDATE_AFTER || kind == RowKind.INSERT) {
            out.collect(row.getField("userId") + " = " + row.getField("total"));
        }
        // ignore UPDATE_BEFORE and DELETE: they are the "undo" half
    }
});
```

- `row.getKind()` returns the changelog flag. This is the only way to see it from Java.
- You must handle all four kinds, or at minimum filter deliberately. Ignoring `UPDATE_BEFORE` and summing `UPDATE_AFTER` values downstream would double-count.

### `fromChangelogStream` — DataStream of updates → Table

```java
DataStream<Row> updates = env.fromElements(
    Row.ofKind(RowKind.INSERT,        "u-001", 100.0),
    Row.ofKind(RowKind.UPDATE_BEFORE, "u-001", 100.0),
    Row.ofKind(RowKind.UPDATE_AFTER,  "u-001", 250.0));

Table t = tEnv.fromChangelogStream(updates);
```

Use this when the DataStream is *already* a changelog — a CDC feed you deserialized yourself, or the output of your own stateful operator that emits corrections.

```
DECISION TABLE
────────────────────────────────────────────────────────────
Query only INSERTs rows?   ── yes ──► toDataStream       (simpler, faster)
                            └─ no ──► toChangelogStream  (mandatory)

Stream carries only new facts? ─ yes ─► fromDataStream
Stream carries corrections?    ─ yes ─► fromChangelogStream
```

---

## Table API — the fluent alternative

Every SQL query has an equivalent written as Java method calls.

```java
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

Table result = tEnv.from("transactions")     // FROM transactions
        .filter($("amount").isGreater(lit(500)))     // WHERE amount > 500
        .groupBy($("userId"))                        // GROUP BY userId
        .select($("userId"),                         // SELECT userId,
                $("amount").sum().as("total"));      //        SUM(amount) AS total
```

- `$("col")` builds a column reference expression. The `$` is a static method, not syntax.
- `lit(500)` builds a literal. Mixing raw Java values into expressions sometimes works via overloads, but `lit` is unambiguous.
- `.as("total")` is the column alias.

Both forms hit the same planner and produce identical plans. So which to use?

**Use SQL strings.** Reasons:

1. **Portable.** The same string runs in the SQL Client, in a Java job, in PyFlink, and in most managed Flink services. A Java fluent chain runs only in Java.
2. **Tooling.** Formatters, linters, diff review, and every SQL-literate person on your team can read it.
3. **Coverage.** Some constructs (window TVFs, `MATCH_RECOGNIZE`, `STATEMENT SET`) are SQL-only or awkward in the fluent API.
4. **Non-engineers can contribute.** That is half the point of Flink SQL existing.

Where the Table API genuinely wins: **programmatic query construction** — building a filter chain from a config file, or applying the same transformation to 40 tables in a loop. String concatenation for that is miserable.

---

## PyFlink

> **Key idea:** The Table API / SQL is where PyFlink is genuinely first-class. There is no performance penalty, because your Python never touches a record — the planner compiles the SQL to JVM operators and Python only submits it.

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(settings)

t_env.execute_sql("""
    CREATE TABLE transactions (
      userId  STRING,
      amount  DOUBLE,
      ts      TIMESTAMP(3),
      WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    ) WITH ('connector' = 'datagen', 'rows-per-second' = '5')
""")

t_env.sql_query("SELECT userId, SUM(amount) AS total FROM transactions GROUP BY userId") \
     .execute().print()
```

Method names are snake_case (`execute_sql`, `sql_query`, `to_data_stream`); everything else is identical.

Contrast with the **PyFlink DataStream API**, where a Python `MapFunction` runs in a separate Python process and every record is serialized across a socket. That boundary costs real throughput. Pure SQL has no such boundary. If your team is Python-first, write Flink SQL, not PyFlink DataStream.

---

## The SQL Client — the fastest way to learn

```bash
# start a local cluster (one JobManager, one TaskManager)
cd flink-1.20.0
./bin/start-cluster.sh

# open the interactive SQL shell
./bin/sql-client.sh
```

You get a prompt:

```
Flink SQL> SET 'execution.runtime-mode' = 'streaming';
[INFO] Execute statement succeeded.

Flink SQL> SET 'sql-client.execution.result-mode' = 'tableau';
[INFO] Execute statement succeeded.

Flink SQL> CREATE TABLE fake (
>   userId STRING,
>   amount DOUBLE
> ) WITH (
>   'connector' = 'datagen',
>   'rows-per-second' = '2'
> );
[INFO] Execute statement succeeded.

Flink SQL> SELECT userId, COUNT(*) FROM fake GROUP BY userId;
```

Useful client commands:

```bash
Flink SQL> SHOW TABLES;                       -- list tables in the current database
Flink SQL> DESCRIBE fake;                     -- column names, types, nullability, watermark
Flink SQL> SHOW CREATE TABLE fake;            -- the full DDL, including WITH options
Flink SQL> EXPLAIN SELECT ... ;               -- the plan (chapter 52)
Flink SQL> SET;                               -- show all current config
Flink SQL> RESET;                             -- back to defaults
Flink SQL> QUIT;
```

Result modes:

| Mode | Behaviour |
|---|---|
| `table` | Paged, interactive, updates in place. Default. |
| `changelog` | Shows the `op` column (`+I`/`-U`/`+U`/`-D`). **Best for learning chapter 47.** |
| `tableau` | Streams rows to stdout in ASCII table form. Best for scripts and screenshots. |

```bash
Flink SQL> SET 'sql-client.execution.result-mode' = 'changelog';
```

Run a file of statements non-interactively:

```bash
./bin/sql-client.sh -f pipeline.sql
```

And add connector jars (they must be on the classpath before startup — chapter 51):

```bash
./bin/sql-client.sh -j lib/flink-sql-connector-kafka-3.2.0-1.19.jar
```

Stop the cluster when done:

```bash
./bin/stop-cluster.sh
```

---

## A complete minimal program

```java
package com.akash.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FirstSqlJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
            "CREATE TABLE transactions (" +
            "  userId STRING," +
            "  amount DOUBLE," +
            "  ts     TIMESTAMP(3)," +
            "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '3'," +
            "  'fields.userId.length' = '4'," +
            "  'fields.amount.min' = '1'," +
            "  'fields.amount.max' = '999'" +
            ")");

        Table result = tEnv.sqlQuery(
            "SELECT userId, COUNT(*) AS cnt, SUM(amount) AS total " +
            "FROM transactions GROUP BY userId");

        result.execute().print();   // blocks; Ctrl-C to stop
    }
}
```

Note there is **no `env.execute()`**. `result.execute()` already submitted the job.

---

## Remember

- SQL compiles to the same DataStream operators, state backend, checkpoints and watermarks you already know.
- `TableEnvironment` = SQL only. `StreamTableEnvironment` = SQL + DataStream in one job.
- Parallelism, checkpointing, and restart strategy are set on the `StreamExecutionEnvironment`, not the table env.
- `executeSql` runs **now** (DDL, `INSERT INTO`). `sqlQuery` is **lazy** and returns a `Table`.
- `executeSql("INSERT INTO ...")` submits its own job — do not also call `env.execute()`.
- `createTemporaryView` registers a *plan*, not a cache.
- `toDataStream` = append-only. `toChangelogStream` = anything that updates. `RowKind` on the `Row` carries the flag.
- Use `SOURCE_WATERMARK()` in `fromDataStream` to inherit the DataStream's existing watermarks instead of generating new ones.
- Prefer SQL strings over the fluent Table API — portable, reviewable, wider coverage. Use the fluent API only to build queries programmatically.
- PyFlink Table API/SQL has **no** Python-process overhead; PyFlink DataStream does.
- `./bin/sql-client.sh`, then `SET 'sql-client.execution.result-mode' = 'changelog';` is the best learning environment there is.

**Interview one-liners**

- *"Is Flink SQL a different engine?"* → No. It's a planner in front of the DataStream runtime. Same operators, same checkpoints, same watermarks.
- *"TableEnvironment vs StreamTableEnvironment?"* → The stream variant is a bridge that can convert to and from `DataStream`. Use it only if you need that.
- *"Why doesn't my SQL job start?"* → You used `sqlQuery` (lazy) without `.execute()`, or you added `env.execute()` after `executeSql("INSERT INTO ...")`, which already submitted.
- *"toDataStream threw 'doesn't support consuming update changes'."* → The query produces retractions. Use `toChangelogStream`.
- *"Is PyFlink slower?"* → Not for Table API/SQL — no records cross the Python boundary. PyFlink DataStream with Python UDFs is a different story.
- *"SQL or Table API?"* → SQL, except when you need to build a query programmatically.
