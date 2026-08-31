# Flink Cheatsheet — One Page

Everything you'll look up repeatedly. Java DataStream API, Flink 1.18+.

---

## The shape of every Flink job

```java
// 1. Environment — the handle to Flink
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 2. Source — where data comes from
DataStream<Event> events = env.fromSource(kafkaSource, watermarkStrategy, "kafka");

// 3. Transformations — build the graph (NOTHING RUNS YET)
DataStream<Alert> alerts = events
    .filter(e -> e.getAmount() > 0)
    .keyBy(Event::getUserId)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .aggregate(new MyAggregate());

// 4. Sink — where results go
alerts.sinkTo(kafkaSink);

// 5. GO. This is the line that actually runs everything.
env.execute("job name");
```

> **The #1 beginner confusion:** steps 2–4 build a *description* of a dataflow.
> `env.execute()` ships it to the cluster and starts it. A `System.out.println`
> written between steps runs **once, on your laptop**, at graph-build time.

---

## Operator quick reference

| Operator | In → Out | Notes |
|---|---|---|
| `map(f)` | 1 → 1 | Type may change |
| `filter(f)` | 1 → 0 or 1 | Type never changes; `true` = keep |
| `flatMap(f)` | 1 → 0..N | Push to `Collector`, don't return |
| `keyBy(f)` | logical repartition | Required before keyed state / keyed windows |
| `reduce(f)` | rolling | Same type in and out |
| `process(f)` | 1 → 0..N | Full power: state, timers, side outputs |
| `union(s)` | merge | Same type only |
| `connect(s)` | pair | Different types; use `CoProcessFunction` |
| `broadcast(d)` | fan out | Every subtask gets a copy |

### Rolling aggregations — the `max` vs `maxBy` trap

```java
keyed.max("amount")    // ONLY the amount field is updated.
                       // Other fields keep values from the FIRST record. Usually a bug.
keyed.maxBy("amount")  // Emits the WHOLE record that had the max. Usually what you want.
```

---

## Time and watermarks

```java
WatermarkStrategy<Event> ws = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))  // tolerate 5s of disorder
    .withTimestampAssigner((event, recordTs) -> event.getTimestamp())  // millis since epoch
    .withIdleness(Duration.ofMinutes(1));   // don't let a quiet partition stall the job
```

| Strategy | Use when |
|---|---|
| `forBoundedOutOfOrderness(d)` | Normal case. Events arrive up to `d` out of order. |
| `forMonotonousTimestamps()` | Timestamps never go backwards per partition. |
| `noWatermarks()` | Processing time only. |

> **The rule that causes most incidents:** an operator's watermark is the
> **minimum** across all its input channels. One idle or slow partition freezes
> the whole job. `withIdleness()` is the fix.

**Late** = event timestamp < current watermark. Default: silently dropped.

---

## Windows

```java
// Tumbling — fixed, non-overlapping
.window(TumblingEventTimeWindows.of(Duration.ofMinutes(10)))

// Sliding — overlapping. Each element lands in size/slide windows.
.window(SlidingEventTimeWindows.of(Duration.ofMinutes(10), Duration.ofMinutes(1)))

// Session — gap-based, merges
.window(EventTimeSessionWindows.withGap(Duration.ofMinutes(30)))

// Global — never fires without a custom trigger
.window(GlobalWindows.create()).trigger(CountTrigger.of(100))
```

```
Tumbling (10s)   |----0----|----1----|----2----|
Sliding (10s/5s) |----0----|
                      |----1----|
                           |----2----|
Session (gap 5)  |--A--|      |--B--|         |--C--|
                  ^gap<5 merges    ^gap>5 splits
```

### Window function families

| Function | State held | Gets window metadata? |
|---|---|---|
| `reduce(ReduceFunction)` | 1 accumulator | No |
| `aggregate(AggregateFunction)` | 1 accumulator | No |
| `process(ProcessWindowFunction)` | **all elements** | Yes |
| `aggregate(agg, processWindowFn)` | 1 accumulator | **Yes** ← best of both |

> Use the combined form. Incremental aggregation *and* window start/end.

### Late data

```java
.window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
.allowedLateness(Duration.ofSeconds(30))   // keep window state 30s longer, RE-FIRE per late event
.sideOutputLateData(lateTag)               // anything later than that goes here
```

```java
// The {} is REQUIRED — it creates an anonymous subclass so Java keeps the generic type.
OutputTag<Event> lateTag = new OutputTag<Event>("late-events"){};
...
DataStream<Event> late = result.getSideOutput(lateTag);
```

---

## State

```java
public class Counter extends KeyedProcessFunction<String, Event, Long> {

    // transient: don't try to serialize this when shipping the function to workers
    private transient ValueState<Long> count;

    @Override
    public void open(OpenContext ctx) {
        // Descriptor = state name + type. Flink uses it to find/restore the state.
        count = getRuntimeContext().getState(
            new ValueStateDescriptor<>("count", Long.class));
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<Long> out) throws Exception {
        Long c = count.value();          // reads state FOR THE CURRENT KEY automatically
        c = (c == null ? 0L : c) + 1;    // ← always null-check; first time is null
        count.update(c);
        out.collect(c);
    }
}
```

> **You never pass the key.** State access is implicitly scoped to the key of the
> record being processed. That's why `keyBy()` is mandatory.

| Type | Use for |
|---|---|
| `ValueState<T>` | One value per key — a count, a flag, a last-seen record |
| `ListState<T>` | Append-only list — recent events (you must bound it yourself) |
| `MapState<K,V>` | Keyed lookups — far cheaper than `ValueState<HashMap>` on RocksDB |
| `ReducingState<T>` | Pre-aggregate on write, same type |
| `AggregatingState<I,O>` | Pre-aggregate on write, different in/out type |

### TTL — set it on anything with an unbounded keyspace

```java
StateTtlConfig ttl = StateTtlConfig
    .newBuilder(Duration.ofHours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();
descriptor.enableTimeToLive(ttl);
```

> TTL is **processing time**, not event time. Unbounded keyspace with no TTL is
> the single most common reason Flink jobs die in production.

---

## Timers

```java
// Register — fires when the WATERMARK passes this timestamp
ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60_000);
ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 60_000);
ctx.timerService().deleteEventTimeTimer(savedTimestamp);   // needs the exact timestamp

@Override
public void onTimer(long ts, OnTimerContext ctx, Collector<O> out) { ... }
```

> Timers are **per key**, are checkpointed, and duplicate (key, timestamp) pairs
> collapse into one. Store the timestamp in `ValueState` if you need to delete it later.

---

## Checkpoints

```java
env.enableCheckpointing(60_000);                      // every 60s
CheckpointConfig c = env.getCheckpointConfig();
c.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
c.setMinPauseBetweenCheckpoints(30_000);              // matters MORE than the interval
c.setCheckpointTimeout(600_000);
c.setMaxConcurrentCheckpoints(1);
c.setTolerableCheckpointFailureNumber(3);
c.enableUnalignedCheckpoints();                       // helps under backpressure
c.setExternalizedCheckpointRetention(
    ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
```

| | Checkpoint | Savepoint |
|---|---|---|
| Triggered by | Flink, automatically | You, manually |
| Purpose | Recover from failure | Upgrade, rescale, migrate |
| Lifecycle | Flink deletes old ones | You own it forever |
| Incremental | Yes (RocksDB) | No (canonical format) |

```bash
flink stop --savepointPath s3://bucket/savepoints <jobId>
flink run -s s3://bucket/savepoints/savepoint-xxx -c com.example.Job job.jar
```

> **Set `uid("...")` on every stateful operator from day one.** Without it Flink
> auto-generates IDs from the graph structure, and any change to your job breaks
> state restore.

---

## State backends

| | `HashMapStateBackend` | `EmbeddedRocksDBStateBackend` |
|---|---|---|
| Lives in | JVM heap | Off-heap + local disk |
| Speed | Fastest (objects) | Slower (serialize per access) |
| Size limit | Heap | Disk |
| Incremental checkpoints | No | **Yes** |
| Use when | State fits comfortably in memory | Large state, or you need incremental |

---

## Exactly-once — say it precisely

> Flink's exactly-once means **each record affects state exactly once**.
> It does *not* mean each record is processed exactly once — after a restore,
> records **are** reprocessed. The state effect is what happens once.

End-to-end exactly-once needs **all three**:
1. A **replayable source** (Kafka — rewind to checkpointed offsets)
2. Flink's checkpointing in `EXACTLY_ONCE` mode
3. A **transactional or idempotent sink** (two-phase commit, or upsert by key)

Kafka exactly-once cost: consumers with `read_committed` only see data after each
checkpoint completes. **Your end-to-end latency becomes your checkpoint interval.**

---

## Parallelism and slots

```
JobManager  ── coordinates, schedules, triggers checkpoints
TaskManager ── does the work; has N task slots
Task slot   ── a fixed slice of MEMORY, shared CPU
```

- Slots required = **max parallelism of any operator**, not the sum
  (default slot sharing puts one whole pipeline slice in one slot)
- Precedence: operator `.setParallelism()` > `env.setParallelism()` > `-p` flag > cluster default
- Source parallelism is capped in practice by **Kafka partition count**
- `setMaxParallelism` fixes the key-group count — **you cannot change it after the first savepoint**

```java
.rebalance()   // round-robin to ALL downstream subtasks — full network shuffle
.rescale()     // round-robin to a LOCAL subset — cheaper
.broadcast()   // every subtask gets every record
.shuffle()     // random
```

---

## Diagnosing a sick job — the order to check

1. **Kafka consumer lag** (`records-lag-max`) — is it growing? Then you're behind.
2. **Backpressure tab** — walk downstream; the first operator that is *busy but
   NOT backpressured* is your bottleneck.
3. **Checkpoint tab** — duration climbing? Alignment time high means backpressure.
   Async time high means slow state upload.
4. **Watermark tab** — is one subtask's watermark far behind? Idle or skewed partition.
5. **TaskManager metrics** — GC time, heap, RocksDB disk I/O.

---

## Deprecated → current (you'll see the old ones in tutorials)

| Old | Current |
|---|---|
| `FlinkKafkaConsumer` | `KafkaSource.builder()` |
| `FlinkKafkaProducer` | `KafkaSink.builder()` |
| `env.setStreamTimeCharacteristic(...)` | Removed — event time is default |
| `assignTimestampsAndWatermarks(AssignerWith...)` | `WatermarkStrategy` |
| `Time.seconds(10)` | `Duration.ofSeconds(10)` |
| `addSink(...)` | `sinkTo(...)` |
| `flink-conf.yaml` | `config.yaml` (1.19+) |
| `TUMBLE(ts, INTERVAL...)` in GROUP BY | Window TVF: `TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), ...))` |

---

## Flink SQL, minimum viable

```sql
CREATE TABLE transactions (
  user_id   STRING,
  amount    DOUBLE,
  ts        TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND   -- event time + 5s disorder tolerance
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'localhost:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

SELECT user_id, window_start, window_end, SUM(amount) AS total
FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(ts), INTERVAL '10' MINUTES))
GROUP BY user_id, window_start, window_end;
```

| Query shape | Changelog mode | Can write to append-only sink? |
|---|---|---|
| `SELECT ... WHERE` | append | Yes |
| windowed `GROUP BY` | append | Yes |
| plain `GROUP BY` | **retract** | **No** — use `upsert-kafka` |
| `ROW_NUMBER()` dedup | upsert | Needs a primary key |

---

## Errors you will hit, and what they mean

| Error | Cause | Fix |
|---|---|---|
| `Task not serializable` | Lambda captured a non-serializable outer field | Make it `transient` + init in `open()`, or make the class static |
| `The return type of function could not be determined` | Type erasure on a lambda | Add `.returns(Types.TUPLE(Types.STRING, Types.LONG))` |
| Windows never fire | Watermark stalled — idle partition | `.withIdleness(...)` |
| Everything dropped as late | Timestamp in seconds, not **milliseconds** | Multiply by 1000 |
| `Cannot map checkpoint state for operator ...` | Job graph changed, no `uid()` | Set `uid()`; last resort `--allowNonRestoredState` |
| State grows forever | Unbounded keyspace, no TTL | `enableTimeToLive(...)` |
| Checkpoints timing out | Backpressure delaying barriers | Fix the bottleneck; enable unaligned checkpoints |

---

## Related notes in this folder

Deeper troubleshooting write-ups live one level up:

- [Checkpointing slow](../01-checkpointing-slow.md)
- [Backpressure](../02-backpressure.md)
- [State and skew](../03-state-and-skew.md)
- [Exactly-once](../04-exactly-once.md)
- [Watermarks and time](../05-watermarks-and-time.md)
- [Scale arithmetic](../06-scale-arithmetic.md)
