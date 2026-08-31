# 52. SQL vs DataStream — Choosing, Mixing, Operating

Phases 1–6 were DataStream. Chapters 46–51 were SQL. Both compile to the same runtime. So which do you write?

The honest answer is **both, in the same job**, and this chapter is about knowing which part goes where.

---

## What SQL does better

**1. Aggregations, joins, dedup, and Top-N.**

A per-key windowed sum in DataStream:

```java
events
    .keyBy(Event::getUserId)
    .window(TumblingEventTimeWindows.of(Time.minutes(10)))
    .aggregate(new AggregateFunction<Event, Tuple2<Double, Long>, Result>() {
        public Tuple2<Double, Long> createAccumulator() { return Tuple2.of(0.0, 0L); }
        public Tuple2<Double, Long> add(Event e, Tuple2<Double, Long> acc) {
            return Tuple2.of(acc.f0 + e.getAmount(), acc.f1 + 1);
        }
        public Result getResult(Tuple2<Double, Long> acc) { return new Result(acc.f0, acc.f1); }
        public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }, new ProcessWindowFunction<...>() { /* attach the window key and bounds */ });
```

Same thing in SQL:

```sql
SELECT userId, window_start, window_end, SUM(amount), COUNT(*)
FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '10' MINUTES))
GROUP BY userId, window_start, window_end;
```

Forty lines versus four, and the SQL version is harder to get wrong.

**2. The optimizer.** SQL gets things you would have to implement manually:

- **Filter pushdown** into the source — the Kafka connector can't filter, but a JDBC or filesystem source will push `WHERE dt = '2026-08-29'` down and read less.
- **Projection pushdown** — only the columns your query touches are deserialized. On a wide JSON payload this is a large win, and in DataStream you'd have to hand-write it.
- **Local-global (two-phase) aggregation** — a pre-aggregate before the shuffle, exactly what `reduce` gives you in DataStream, applied automatically.
- **Mini-batch** — buffering state accesses, which has no DataStream equivalent short of writing it yourself.
- **Sub-plan reuse** — a view referenced twice may be computed once.

**3. Less code means fewer bugs.** No serializer registration, no type erasure with `TypeInformation`, no forgetting to clear state in a `ProcessFunction`.

**4. Non-Java people can contribute.** An analyst who knows SQL can write and review a Flink pipeline. That is a genuine organizational advantage, and it's why PyFlink Table API exists (chapter 46).

**5. Faster iteration.** SQL Client: paste, run, look, edit, run. No compile, no jar, no submit. Iteration speed is a real engineering property.

---

## What DataStream does better

**1. Arbitrary state layout.** In SQL, the shape of state is chosen by the planner. In DataStream you decide:

```java
private transient MapState<String, ProfileEntry> profile;      // a nested map
private transient ListState<Event> recentEvents;               // an ordered buffer
private transient ValueState<CircularBuffer> ring;             // a custom serialized object
private transient AggregatingState<Event, Stats> stats;        // custom aggregation
```

There is no SQL construct that gives you "a bounded circular buffer of the last 50 events per user, plus a Bloom filter of seen IDs".

**2. Custom timers.** The single biggest gap.

```java
@Override
public void processElement(Event e, Context ctx, Collector<Alert> out) throws Exception {
    count.update(count.value() == null ? 1 : count.value() + 1);
    if (count.value() == 1) {
        // start a 30-minute deadline the first time we see this user
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 1_800_000L);
    }
}

@Override
public void onTimer(long ts, OnTimerContext ctx, Collector<Alert> out) throws Exception {
    if (count.value() != null && count.value() < 3) {
        out.collect(new Alert(ctx.getCurrentKey(), "abandoned after " + count.value() + " steps"));
    }
    count.clear();
}
```

"If a user starts checkout and doesn't finish within 30 minutes, alert." SQL has no notion of *a thing that did not happen*. `MATCH_RECOGNIZE` can express some absence patterns, but not with an arbitrary deadline and arbitrary side effects on expiry.

**3. Precise control over state size and TTL.** In SQL, `table.exec.state.ttl` is a blunt job-wide instrument (with per-join hints as the only refinement). In DataStream:

```java
StateTtlConfig ttl = StateTtlConfig
    .newBuilder(Time.hours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupInRocksdbCompactFilter(10_000)
    .build();
descriptor.enableTimeToLive(ttl);
```

Per state object, with a choice of update semantics and cleanup strategy. Plus you can bound state explicitly — evict the oldest entry when a list exceeds 100, cap a map at 1000 keys. SQL gives you no way to say "at most N".

**4. Complex event-driven logic.** Branching state machines, per-key mode switches, feedback from a control stream:

```java
BroadcastStream<Rule> rules = ruleStream.broadcast(RULE_STATE_DESCRIPTOR);
events.connect(rules).process(new KeyedBroadcastProcessFunction<>() { ... });
```

Dynamic rules broadcast to every subtask, with state that changes the processing logic at runtime. No SQL equivalent.

**5. Custom triggers and evictors.** "Fire the window early after 100 elements, and again at the watermark" is a `Trigger` implementation. SQL windows fire exactly once, at the watermark.

**6. ML scoring, external calls with custom logic, unusual sinks.** Loading a model, warming it in `open()`, batching inference — that's a `RichMapFunction`.

**7. Anything where you must know exactly what is in state.** For a job you have to operate for years, being able to point at a state descriptor and say "this is 400 bytes per user, 2M users, 800 MB" is worth a lot. In SQL you find out by reading checkpoint sizes.

---

## The decision framework

```
Is the logic expressible as: filter, project, join, aggregate,
window, rank, dedup?
      │
     yes ──────────────► SQL. Stop here.
      │
     no
      │
Does it need: a custom timer, arbitrary state layout,
a broadcast control stream, per-record external logic,
or explicit state bounds?
      │
     yes ──────────────► DataStream for THAT PART ONLY.
      │
     no
      │
      └──► You probably haven't understood the requirement yet.
```

> **Key idea:** The unit of choice is not the job, it's the **stage**. A pipeline is usually SQL → DataStream → SQL: SQL to reduce volume, DataStream for the one genuinely custom step, SQL to enrich and write out.

---

## Mixing them — the worked example

The requirement: **per-user 10-minute spend totals; alert when a user's spend rises for three consecutive windows AND the third window is more than 3× the first; suppress duplicate alerts for the same user within an hour; write alerts to Kafka enriched with the user's tier.**

The aggregation is SQL. The three-consecutive-windows state machine with suppression is not — it needs per-key history plus a timer. The enrichment and sink are SQL again.

### Step 1 — SQL: windowed pre-aggregation

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);
env.enableCheckpointing(30_000);

StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

tEnv.executeSql(
    "CREATE TABLE transactions (" +
    "  userId      STRING," +
    "  type        STRING," +
    "  amount      DOUBLE," +
    "  `timestamp` BIGINT," +
    "  ts          AS TO_TIMESTAMP_LTZ(`timestamp`, 3)," +
    "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
    ") WITH (" +
    "  'connector' = 'kafka'," +
    "  'topic'     = 'transactions'," +
    "  'properties.bootstrap.servers' = 'localhost:9092'," +
    "  'scan.startup.mode' = 'latest-offset'," +
    "  'format'    = 'json'," +
    "  'json.ignore-parse-errors' = 'true'" +
    ")");

Table windowed = tEnv.sqlQuery(
    "SELECT userId, window_start, window_end, SUM(amount) AS total, COUNT(*) AS cnt " +
    "FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '10' MINUTES)) " +
    "GROUP BY userId, window_start, window_end");
```

This does the heavy lifting: millions of records/sec collapse to one row per user per 10 minutes. **Reduce volume in SQL before you hand anything to a custom operator** — that's the whole reason to structure it this way.

### Step 2 — bridge to DataStream

```java
DataStream<Row> windowRows = tEnv.toDataStream(windowed);
```

`toDataStream`, not `toChangelogStream`, because a **windowed** aggregation is append-only (chapter 47). If step 1 had been a plain `GROUP BY`, this line would throw and I'd need `toChangelogStream`.

Map `Row` to a typed POJO immediately — working with `Row` and string field names deep in a `ProcessFunction` is miserable:

```java
public class WindowTotal {
    public String userId;
    public long windowStart;    // epoch millis
    public double total;
    public WindowTotal() {}     // required no-arg constructor for the POJO serializer
    public WindowTotal(String userId, long windowStart, double total) {
        this.userId = userId; this.windowStart = windowStart; this.total = total;
    }
}
```

```java
DataStream<WindowTotal> totals = windowRows.map(row -> new WindowTotal(
        (String) row.getField("userId"),
        ((java.time.Instant) row.getField("window_start")).toEpochMilli(),
        (Double) row.getField("total")
)).returns(WindowTotal.class);
```

- `row.getField("window_start")` returns a `java.time.Instant` for a `TIMESTAMP_LTZ(3)` column (it would be a `LocalDateTime` for `TIMESTAMP(3)`).
- `.returns(WindowTotal.class)` — required. Java's lambda erases the generic type and Flink cannot infer it. Omitting this gives `The generic type parameters of 'Collector' are missing`.

**Watermarks carry across the bridge.** The window operator emits `window_end - 1ms` as each row's timestamp, and watermarks propagate, so event-time timers work downstream without re-assigning anything.

### Step 3 — DataStream: the custom logic

```java
public class RisingSpendDetector extends KeyedProcessFunction<String, WindowTotal, Alert> {

    private transient ListState<Double> lastThree;      // rolling history of window totals
    private transient ValueState<Long> suppressUntil;   // suppression deadline

    @Override
    public void open(Configuration conf) {
        lastThree = getRuntimeContext().getListState(
                new ListStateDescriptor<>("lastThree", Double.class));
        suppressUntil = getRuntimeContext().getState(
                new ValueStateDescriptor<>("suppressUntil", Long.class));
    }

    @Override
    public void processElement(WindowTotal w, Context ctx, Collector<Alert> out) throws Exception {

        // --- maintain a bounded history of the last three window totals ---
        List<Double> history = new ArrayList<>();
        for (Double d : lastThree.get()) history.add(d);
        history.add(w.total);
        if (history.size() > 3) history.remove(0);      // explicit bound: SQL cannot do this
        lastThree.update(history);

        if (history.size() < 3) return;

        boolean rising = history.get(0) < history.get(1) && history.get(1) < history.get(2);
        boolean tripled = history.get(2) > history.get(0) * 3;
        if (!rising || !tripled) return;

        // --- suppression: at most one alert per user per hour ---
        Long until = suppressUntil.value();
        if (until != null && ctx.timestamp() < until) return;

        out.collect(new Alert(w.userId, history.get(0), history.get(2), w.windowStart));

        suppressUntil.update(ctx.timestamp() + 3_600_000L);
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 3_600_000L);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        suppressUntil.clear();     // suppression window over; free the state
    }
}
```

```java
DataStream<Alert> alerts = totals
        .keyBy(w -> w.userId)
        .process(new RisingSpendDetector());
```

Line notes:

- `ListState<Double>` with an explicit `if (history.size() > 3) remove(0)` — **bounded state by construction**. Three doubles per user, forever, and you can compute the exact memory. SQL's `table.exec.state.ttl` cannot express this.
- `ctx.timestamp()` is the record's event time — inherited from the SQL window, per step 2.
- The timer + `clear()` means suppression state is *deleted*, not merely expired. Nothing accumulates.

Could you express the rising-three-windows part in SQL? Partly — `LAG()` in an `OVER` window would give you the previous two totals. But the hour-long suppression with deterministic state cleanup has no SQL form, and the moment you need to add a fourth condition or a side output, the SQL version collapses.

### Step 4 — back to SQL: enrich and sink

```java
tEnv.createTemporaryView("alerts_raw",
    tEnv.fromDataStream(alerts,
        Schema.newBuilder()
            .column("userId", "STRING")
            .column("firstTotal", "DOUBLE")
            .column("lastTotal", "DOUBLE")
            .column("windowStart", "BIGINT")
            .columnByExpression("ts", "TO_TIMESTAMP_LTZ(windowStart, 3)")
            .columnByExpression("proc", "PROCTIME()")
            .watermark("ts", "SOURCE_WATERMARK()")
            .build()));
```

- `SOURCE_WATERMARK()` inherits the watermarks already flowing in the DataStream instead of generating new ones (chapter 46).
- `proc AS PROCTIME()` because the next step is a lookup join, which needs a processing-time attribute.

```java
tEnv.executeSql(
    "CREATE TABLE user_dim (" +
    "  userId STRING, tier STRING, email STRING," +
    "  PRIMARY KEY (userId) NOT ENFORCED" +
    ") WITH (" +
    "  'connector' = 'jdbc'," +
    "  'url' = 'jdbc:mysql://localhost:3306/crm'," +
    "  'table-name' = 'users'," +
    "  'username' = 'flink'," +
    "  'password' = 'secret'," +
    "  'lookup.cache' = 'PARTIAL'," +
    "  'lookup.partial-cache.max-rows' = '50000'," +
    "  'lookup.partial-cache.expire-after-write' = '10 min'," +
    "  'lookup.partial-cache.cache-missing-key' = 'true'" +
    ")");

tEnv.executeSql(
    "CREATE TABLE alerts_out (" +
    "  userId STRING, tier STRING, email STRING," +
    "  firstTotal DOUBLE, lastTotal DOUBLE, ts TIMESTAMP_LTZ(3)," +
    "  PRIMARY KEY (userId) NOT ENFORCED" +
    ") WITH (" +
    "  'connector' = 'upsert-kafka'," +
    "  'topic' = 'spend-alerts'," +
    "  'properties.bootstrap.servers' = 'localhost:9092'," +
    "  'key.format' = 'json'," +
    "  'value.format' = 'json'" +
    ")");

tEnv.executeSql(
    "INSERT INTO alerts_out " +
    "SELECT a.userId, u.tier, u.email, a.firstTotal, a.lastTotal, a.ts " +
    "FROM alerts_raw AS a " +
    "LEFT JOIN user_dim FOR SYSTEM_TIME AS OF a.proc AS u ON a.userId = u.userId");
```

The final `executeSql("INSERT INTO ...")` submits the whole job — sources, SQL windows, the `KeyedProcessFunction`, the lookup join, the sink — as **one JobGraph**. There is no serialization boundary between the SQL and DataStream parts; they're chained operators.

```
Kafka ──► [SQL: TUMBLE + SUM]  ──► [Java: RisingSpendDetector]  ──► [SQL: lookup join] ──► upsert-kafka
   millions/sec        │                      │                            │
                  reduced to             custom timers               enrichment from
                  ~1 row/user/10min      + bounded state             MySQL, cached
```

**Do not call `env.execute()` after this.** The `executeSql` already submitted.

### The exception: when you build a Table but sink via DataStream

If your job ends in a `DataStream` sink rather than `INSERT INTO`, then you *do* call `env.execute()`:

```java
alerts.sinkTo(kafkaSink);
env.execute("spend-alerts");
```

Rule of thumb: **whoever holds the sink submits the job.** Exactly one of `executeSql("INSERT INTO ...")`, `table.execute()`, or `env.execute()` per job.

---

## Operational differences — the serious part

### Savepoint compatibility

This is the biggest practical reason experienced teams keep critical logic in DataStream.

In DataStream, every stateful operator has a `uid` you set explicitly:

```java
.keyBy(w -> w.userId)
.process(new RisingSpendDetector())
.uid("rising-spend-detector-v1")      // ← YOUR identifier, stable across code changes
.name("rising spend detector");
```

State in a savepoint is keyed by that `uid`. You can restructure the whole job — add operators, rename classes, change parallelism — and as long as the `uid` and the state's type are unchanged, `flink run -s <savepoint>` restores it.

**In SQL you cannot set uids.** They are generated from the query plan. Which means:

> **Key idea:** Changing a SQL query can change the generated plan, which changes the operator IDs, which makes the savepoint unrestorable. Your job restarts **with empty state**.

The error:

```
java.lang.IllegalStateException: Failed to rollback to checkpoint/savepoint
file:/savepoints/savepoint-abc123.
Cannot map checkpoint/savepoint state for operator a1b2c3d4e5f6... to the new program,
because the operator is not available in the new program.
If you want to allow to skip this, you can set the --allowNonRestoredState option on the CLI.
```

`--allowNonRestoredState` does **not** fix this. It tells Flink to discard the unmapped state, which for an aggregation means every running total resets to zero and every dedup key is forgotten. That is a data correctness incident, not a workaround.

What actually changes the plan (and thus breaks restore):

- Adding or removing a column in an aggregation's `SELECT` list
- Adding a `WHERE` predicate
- Changing a join type or join order
- Changing a window size
- Upgrading Flink to a version with different optimizer rules
- Sometimes: changing config that alters optimization (`table.optimizer.agg-phase-strategy`, mini-batch on/off)

What is generally safe:

- Changing a connector option (`scan.startup.mode`, bootstrap servers, cache settings)
- Changing parallelism (state is rescaled, not remapped)
- Changing a sink's target topic

Mitigations:

1. **Version your jobs.** Deploy the new query as a *new* job reading from `earliest-offset`, run it in parallel, compare output, then cut over and kill the old one. This is the standard practice, and it's why you keep enough retention in Kafka to rebuild state.
2. **`table.exec.uid.generation`** (Flink 1.16+) controls uid generation:
   ```sql
   SET 'table.exec.uid.generation' = 'PLAN_ONLY';
   ```
   `PLAN_ONLY` (the default in recent versions) derives uids from the compiled plan rather than the whole topology, which makes some changes survivable. `ALWAYS` and `DISABLED` are the other values. It reduces the problem; it does not eliminate it.
3. **Compiled plans** (Flink 1.15+) — the real solution for production SQL:
   ```sql
   COMPILE PLAN '/plans/alerts.json' FOR
     INSERT INTO alerts_out SELECT ...;

   EXECUTE PLAN '/plans/alerts.json';
   ```
   The JSON plan is a **pinned, versioned artifact**. You deploy the plan file, not the SQL. Flink guarantees plan-file compatibility across minor upgrades, so operator IDs stay stable and savepoints keep working. Editing the SQL requires recompiling the plan — which is exactly the point: it makes the breaking change explicit and reviewable instead of accidental.

   ```sql
   COMPILE AND EXECUTE PLAN '/plans/alerts.json' FOR INSERT INTO ...;   -- compile if absent, then run
   ```

4. **Keep the state-heavy, long-lived logic in DataStream with explicit uids**, and use SQL for the stateless or short-window parts. This is a legitimate architectural choice for jobs that must survive years of iteration.

### Other operational differences

| | SQL | DataStream |
|---|---|---|
| Deploy artifact | a `.sql` file or a compiled plan JSON | a jar |
| Operator names in the UI | generated, verbose (`GroupAggregate(groupBy=[userId]...)`) | whatever you set with `.name()` |
| Setting `uid` | impossible (plan-derived) | explicit and stable |
| Metrics | per generated operator; harder to correlate | per named operator |
| Custom metrics | UDF-scoped only | anywhere, via `getRuntimeContext().getMetricGroup()` |
| Unit testing | needs a `TableEnvironment` and a bounded source | plain JUnit + test harnesses |
| Upgrade safety | fragile without compiled plans | good, with disciplined uids |
| Onboarding a new engineer | hours | days |

---

## Debugging SQL with EXPLAIN

`EXPLAIN` is the SQL equivalent of reading the Flink UI's job graph, and you should run it before deploying anything.

```sql
EXPLAIN
SELECT userId, window_start, SUM(amount) AS total
FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '10' MINUTES))
GROUP BY userId, window_start, window_end;
```

Three sections come back.

**1. Abstract Syntax Tree** — the parsed query. Rarely useful.

**2. Optimized Physical Plan** — the operators that will run. **This is the one you read.**

```
== Optimized Physical Plan ==
Calc(select=[userId, window_start, total])
+- WindowAggregate(groupBy=[userId], window=[TUMBLE(time_col=[ts], size=[10 min])],
                   select=[userId, SUM(amount) AS total, start('w$) AS window_start,
                           end('w$) AS window_end])
   +- Exchange(distribution=[hash[userId]])
      +- Calc(select=[userId, amount, ts], where=[...])
         +- TableSourceScan(table=[[default_catalog, default_database, transactions,
                             project=[userId, amount, timestamp], metadata=[]]],
                            fields=[userId, amount, timestamp])
```

Read it **bottom-up**. What to look for:

| Sign | Meaning |
|---|---|
| `project=[userId, amount, timestamp]` in the scan | **Projection pushdown worked** — only these columns are deserialized |
| `filter=[...]` in the scan | Filter pushdown worked |
| `Exchange(distribution=[hash[...]])` | A **shuffle**. Count them; each is a network boundary. |
| `WindowAggregate` | ✓ bounded state, append-only |
| `GroupAggregate` | ⚠ **unbounded state**, retract. Is that intended? |
| `IntervalJoin` | ✓ bounded |
| `Join(joinType=[InnerJoin])` | ⚠ regular join, unbounded state |
| `TemporalJoin` / `LookupJoin` | ✓ |
| `Deduplicate(keep=[FirstRow])` | ✓ cheap dedup |
| `Rank(strategy=[...], rankRange=[1..10])` | Top-N |
| `ChangelogNormalize` | An upsert source being expanded to a full changelog — costs state |
| `LocalWindowAggregate` + `GlobalWindowAggregate` | ✓ two-phase aggregation, good for skew |
| `MiniBatchAssigner` | Mini-batch is active |

**3. Execution Plan** — the physical topology with chaining. Tells you which operators are fused into a single task.

### More explain detail

```sql
EXPLAIN CHANGELOG_MODE
SELECT userId, SUM(amount) FROM transactions GROUP BY userId;
```

```
== Optimized Physical Plan With Changelog Mode ==
GroupAggregate(..., changelogMode=[I,UB,UA])
+- Exchange(distribution=[hash[userId]], changelogMode=[I])
   +- TableSourceScan(..., changelogMode=[I])
```

`changelogMode=[I,UB,UA]` — insert, update-before, update-after. **This is the definitive answer to "will my sink accept this?"** from chapter 47, printed by the planner. If you see `UB` and your sink is append-only, you know before you deploy.

Other modifiers:

```sql
EXPLAIN ESTIMATED_COST SELECT ...;      -- row-count and cost estimates
EXPLAIN JSON_EXECUTION_PLAN SELECT ...; -- the JSON graph, paste into the Flink plan visualizer
EXPLAIN PLAN_ADVICE SELECT ...;         -- Flink 1.17+: warnings and suggestions
```

`PLAN_ADVICE` is genuinely useful and underused — it flags things like non-deterministic updates and missing mini-batch:

```
== Optimized Physical Plan With Advice ==
...
advice[1]: [WARNING] The column(s): total(generated by NON_DETERMINISTIC_UPDATE) can not
satisfy the determinism requirement for correctly processing update message...
```

In Java:

```java
System.out.println(tEnv.explainSql("SELECT ...", ExplainDetail.CHANGELOG_MODE));
System.out.println(table.explain(ExplainDetail.ESTIMATED_COST));
```

### The debugging workflow for a SQL job

1. `DESCRIBE <table>;` — is `*ROWTIME*` there? (chapter 48)
2. `SELECT * FROM <table> LIMIT 5;` — are the timestamps sane? Is anything null?
3. `EXPLAIN CHANGELOG_MODE <query>;` — what mode, and will the sink take it?
4. `EXPLAIN <query>;` — `WindowAggregate` or `GroupAggregate`? `IntervalJoin` or `Join`? How many `Exchange`es?
5. `EXPLAIN PLAN_ADVICE <query>;` — any warnings?
6. Run against `datagen` with `number-of-rows` set and a `print` sink.
7. Deploy; then watch checkpoint size (state growth), `numLateRecordsDropped`, and the watermark per subtask in the UI.

---

## Remember

- Both APIs compile to the same runtime. The choice is per **stage**, not per job.
- SQL wins: aggregations, joins, dedup, Top-N, the optimizer (projection/filter pushdown, two-phase agg, mini-batch), less code, non-Java contributors, fast iteration.
- DataStream wins: arbitrary state layout, custom timers, explicit state bounds and per-descriptor TTL, broadcast control streams, custom triggers, ML scoring, and knowing exactly what's in state.
- The canonical shape is **SQL → DataStream → SQL**: aggregate in SQL to reduce volume, do the one custom thing in Java, enrich and sink in SQL.
- `toDataStream` for append-only results, `toChangelogStream` otherwise. Map `Row` to a POJO immediately and remember `.returns(Class)` after a lambda.
- `SOURCE_WATERMARK()` in `fromDataStream` inherits the existing watermarks. Watermarks and record timestamps cross the bridge intact, so event-time timers work.
- Exactly one submission per job: `executeSql("INSERT INTO ...")` **or** `table.execute()` **or** `env.execute()`. Whoever holds the sink submits.
- **SQL jobs cannot set `uid`s.** A query change can change the plan, change operator IDs, and make the savepoint unrestorable. `--allowNonRestoredState` silently discards state — it's not a fix.
- Use `COMPILE PLAN '/path.json' FOR INSERT INTO ...` + `EXECUTE PLAN` to pin the plan as a versioned artifact for upgradeable production SQL.
- Otherwise: version your jobs, run old and new in parallel from `earliest-offset`, compare, cut over.
- `EXPLAIN` before deploying. Read the physical plan bottom-up. `WindowAggregate` good, `GroupAggregate` unbounded; `IntervalJoin` good, plain `Join` unbounded; count the `Exchange` nodes.
- `EXPLAIN CHANGELOG_MODE` tells you the mode before the sink rejects it. `EXPLAIN PLAN_ADVICE` flags non-determinism.

**Interview one-liners**

- *"SQL or DataStream?"* → SQL for anything relational — aggregations, joins, dedup, ranking. DataStream when you need custom timers, a specific state layout, or explicit state bounds. Mix them in one job via `toDataStream`/`fromDataStream`.
- *"What does the SQL optimizer give you that DataStream doesn't?"* → Projection and filter pushdown, automatic two-phase aggregation, mini-batch state access, and sub-plan reuse.
- *"What's the biggest operational risk with Flink SQL?"* → You can't set operator uids, so a query change can alter the plan and make savepoints unrestorable. Mitigate with compiled plans, or by running a new job in parallel and cutting over.
- *"What does --allowNonRestoredState do?"* → Discards state that can't be mapped to the new job. For an aggregation that means starting from zero — a correctness incident, not a fix.
- *"How do you know if a SQL query has unbounded state?"* → `EXPLAIN`. `GroupAggregate` or a plain `Join` in the physical plan means unbounded; `WindowAggregate`, `IntervalJoin`, `TemporalJoin` are bounded.
- *"How do you check a query's changelog mode?"* → `EXPLAIN CHANGELOG_MODE`. `[I]` is append; `[I,UB,UA]` is retract; `[I,UA,D]` is upsert.
- *"How do you make a SQL job upgradeable?"* → `COMPILE PLAN` to a JSON file, deploy the plan as the artifact, and `EXECUTE PLAN`. Flink guarantees plan compatibility across minor versions.
