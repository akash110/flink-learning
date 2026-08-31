# State Management & Data Skew

> Typical questions: *"One subtask is much slower than the others — why?"*
> *"Your job restarts and takes 40 minutes to recover. Why?"*
> *"How do you change parallelism without losing state?"*

---

## Part 1: Key groups — why scaling out doesn't fix skew

Flink does not hash keys directly to subtasks. There's an intermediate layer:

```
key → hash(key) → key group (0..maxParallelism-1) → subtask
```

`maxParallelism` (default 128, or ~1.5× parallelism) is fixed **at first run** and
**cannot be changed** without discarding state.

Three consequences that show up as interview questions:

**1. You can never scale beyond `maxParallelism`.**
```java
// set this deliberately at job creation — you cannot change it later
env.setMaxParallelism(4096);   // allows scaling up to 4096 subtasks
```
Setting it too low permanently caps your job. Setting it absurdly high adds metadata
overhead and slows recovery. 4096 is a reasonable ceiling for a job that might grow.

**2. Rescaling redistributes whole key groups, not keys.**
With `maxParallelism=128` and parallelism 10, subtasks get 12 or 13 key groups — inherently
uneven. **Parallelism should ideally divide `maxParallelism`** to avoid built-in skew.
Going 128→10 gives you a permanent ~8% imbalance for free.

**3. A hot key lives in exactly one key group forever.** More subtasks = same subtask still
gets the hot key. This is why "just add parallelism" fails, and it's the answer to
"why didn't scaling out help?"

---

## Part 2: Detecting skew

```
Flink UI → Job → Operator → Subtasks tab → sort by "Records Received"

subtask 0:  4,200,000   ← 🔴 hot
subtask 1:     31,000
subtask 2:     29,500
...
```

Ratio > ~5x between max and median = real skew.

Find the offending key:
```java
// temporary diagnostic operator — don't ship this
.keyBy(Event::getTenantId)
.process(new KeyedProcessFunction<String, Event, Event>() {
    private transient Counter counter;

    @Override
    public void open(Configuration c) {
        counter = getRuntimeContext().getMetricGroup()
                    .addGroup("skew")
                    .counter("records");
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<Event> out) {
        counter.inc();
        out.collect(e);
    }
});
```

Better in practice: sample the stream and count keys offline. A metric per key explodes your
metrics backend if cardinality is high — worth saying, because "I'd add a metric per key" is
a plausible-sounding answer that would take down your monitoring at 1M/sec.

---

## Part 3: Skew fixes beyond salting

Salting (two-phase aggregation) is in [[01-checkpointing-slow]] Part 3. Other options:

### Local pre-aggregation before the shuffle
```java
// ✅ reduce volume BEFORE the keyBy — cuts network AND skew
events
  .keyBy(Event::getTenantId)
  .window(TumblingEventTimeWindows.of(Time.seconds(10)))
  .reduce(new SumReducer())     // incremental: one accumulator per window per key
  ...
```

**Use `reduce`/`aggregate`, never `process`/`apply`, for large windows.**

```java
// ❌ buffers EVERY record of the window in state
.window(TumblingEventTimeWindows.of(Time.hours(1)))
.process(new ProcessWindowFunction<>() {
    public void process(String k, Context c, Iterable<Event> events, Collector<R> out) {
        out.collect(new R(k, Iterables.size(events)));   // 1hr × 1M/sec = 3.6B records
    }
});

// ✅ keeps ONE accumulator per key per window
.window(TumblingEventTimeWindows.of(Time.hours(1)))
.aggregate(new CountAggregate());
```

This is a huge, common, easy-to-explain win. At 1M/sec the `process` version is an
out-of-memory error waiting to happen.

Need both incremental aggregation *and* window metadata? Combine them:
```java
// ✅ aggregate incrementally, then get window info in the process function
.aggregate(new CountAggregate(), new ProcessWindowFunction<>() {
    public void process(String k, Context ctx, Iterable<Long> counts, Collector<R> out) {
        // 'counts' has exactly ONE element — the final accumulator
        out.collect(new R(k, ctx.window().getStart(), counts.iterator().next()));
    }
});
```

### Route the hot key separately
```java
// if you know ACME_CORP is hot, give it its own pipeline
SingleOutputStreamOperator<Event> main = events.process(new SplitHotKeys());
DataStream<Event> hot = main.getSideOutput(HOT_TAG);
// hot → dedicated higher-parallelism topology with salting
// main → normal topology
```
Pragmatic and often what actually happens in prod.

---

## Part 4: Slow recovery

> *"The job restarts and takes 40 minutes to come back. Why?"*

Recovery cost = **download state from S3** + **rebuild RocksDB** + **replay from Kafka**.

```
200 GB state / 200 subtasks = 1 GB per subtask to download
at 100 MB/s per node       = 10 seconds  (fine)
BUT with a long incremental chain, you download many small SST files
  → 50,000 small files → S3 request latency dominates → 20+ minutes
```

Fixes:
```java
// 1. Local recovery — keep a copy on local disk, skip the S3 download entirely
config.set(CheckpointingOptions.LOCAL_RECOVERY, true);

// 2. Bound the incremental chain — periodic full snapshots
//    (Flink does this automatically via RocksDB compaction, but tune it)

// 3. Task-local + working directory retention across TaskManager restarts
config.set(CheckpointingOptions.LOCAL_RECOVERY, true);
```

Also relevant: **restart strategy**.
```java
// ❌ tight restart loop hammers S3 and never recovers
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
        Integer.MAX_VALUE, Time.seconds(0)));

// ✅ exponential backoff
env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
        Time.seconds(1),      // initial
        Time.minutes(5),      // max
        2.0,                  // multiplier
        Time.minutes(10),     // reset threshold
        0.1));                // jitter
```

And the biggest recovery win of all:
```java
// Region failover — restart ONLY the failed pipeline region, not the whole job.
// Works when operators are connected pointwise (no shuffle between them).
config.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
```
Default in modern Flink, but worth knowing *why* it sometimes doesn't help: a `keyBy`
creates an all-to-all connection, which puts everything in one failover region. A heavily
shuffled job effectively always does full restarts.

---

## Part 5: Savepoints vs Checkpoints

| | Checkpoint | Savepoint |
|---|---|---|
| Purpose | automatic fault recovery | manual, planned |
| Trigger | Flink, on a timer | you, via CLI |
| Format | backend-specific, may be incremental | canonical, portable |
| Cost | cheap (incremental) | expensive (always full) |
| Survives job cancel | only if externalized | yes |
| Code changes on restore | fragile | supported |

```bash
# stop with a savepoint (drains and stops cleanly)
flink stop --savepointPath s3://bucket/savepoints <jobId>

# restore
flink run -s s3://bucket/savepoints/savepoint-abc123 job.jar
```

**Critical for upgrades:** to change your job graph and keep state, you must set stable UIDs.

```java
// ✅ ALWAYS set uid() on every stateful operator
events
  .keyBy(Event::getUserId)
  .process(new SessionTracker())
  .uid("session-tracker")          // ← state maps to THIS, not to position in the graph
  .name("Session Tracker");        // display name only, not used for state mapping
```

Without `uid()`, Flink auto-generates IDs from the graph structure. **Adding a single
`filter()` upstream changes every downstream auto-generated UID and your entire state is
orphaned on restore.** The job starts up "fine" — with empty state. That silent data loss is
one of the nastiest real-world Flink failures and a great thing to bring up unprompted.

```java
// force a failure instead of silently dropping state
// (default is true; never set it false to "get past" a restore error)
env.getCheckpointConfig().setApproximateLocalRecovery(false);
// on the CLI, the equivalent guard is: do NOT pass --allowNonRestoredState
```

`--allowNonRestoredState` is the flag people reach for when a restore fails. It tells Flink
"discard state you can't map." Sometimes correct (you deliberately removed an operator),
often a silent disaster.

See also: [[01-checkpointing-slow]], [[02-backpressure]], [[04-exactly-once]]
