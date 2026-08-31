# 25. KafkaSource — Reading from Kafka

Until now every job started with `env.fromElements(...)`. That is a toy. Real jobs start with a **source connector** that reads from an unbounded system, and 90% of the time that system is Kafka.

## First: the class you must NOT use

```java
// DEAD. Do not write this.
FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(...);
```

`FlinkKafkaConsumer` (and `FlinkKafkaProducer`) were the old `SourceFunction`-based connectors. They were **deprecated in Flink 1.14 and removed in Flink 1.17**. Every StackOverflow answer older than ~2022 uses them. If you copy one, it will not compile against Flink 1.18/1.20.

| Old (removed) | New (FLIP-27 / FLIP-143) |
|---|---|
| `FlinkKafkaConsumer` | `KafkaSource` |
| `FlinkKafkaProducer` | `KafkaSink` |
| `env.addSource(consumer)` | `env.fromSource(source, wmStrategy, "name")` |
| `stream.addSink(producer)` | `stream.sinkTo(sink)` |

> **Key idea:** FLIP-27 split a source into **enumerator** (runs on the JobManager, discovers work) and **readers** (run on TaskManagers, read the work). That split is what makes the new source able to do event-time alignment, per-split watermarks, and unified batch/stream execution. The old API had none of that.

---

## The dependency

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka</artifactId>
  <!-- Connectors version independently of Flink now.
       3.2.0-1.19 works with 1.19/1.20; 3.1.0-1.18 for 1.18. -->
  <version>3.2.0-1.19</version>
  <!-- NOT provided: the cluster does NOT ship connectors.
       This must end up inside your fat jar. -->
</dependency>
```

Note the version string `3.2.0-1.19`: connector version `3.2.0`, built against Flink `1.19`. Since Flink 1.16 the connectors live in their own repos with their own release cadence.

---

## The minimal source

```java
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("events")
        .setGroupId("flink-events-consumer")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();
```

Line by line, with the Java syntax explained:

- `KafkaSource.<String>builder()` — `KafkaSource` is a **generic class**: `KafkaSource<T>` where `T` is the type of record it produces. `.<String>builder()` is Java's syntax for calling a **generic static method** with the type argument written explicitly, before the method name. You need it here because Java cannot infer `T` from an empty argument list. Read it as "give me a builder that will produce a `KafkaSource<String>`".
- `.setBootstrapServers(...)` — comma-separated `host:port` list. This is only a *discovery* address; the client then learns the full broker list from the cluster. Two or three brokers is enough for resilience.
- `.setTopics("events")` — one or more topic names (it is varargs: `setTopics("a", "b", "c")`, or an overload taking a `List<String>`).
- `.setGroupId(...)` — read the big section below. It does **not** do what you think.
- `.setStartingOffsets(...)` — where to begin when there is no checkpoint to restore from.
- `.setValueOnlyDeserializer(...)` — turns the Kafka record's `byte[]` value into a `String`.
- `.build()` — the builder pattern: each `setX` returns `this`, so the calls chain, and `build()` produces the immutable object. Very common in modern Java.

---

## `setStartingOffsets` — all four initializers

This is only consulted on a **fresh start**. If the job restores from a checkpoint or savepoint, the offsets in that snapshot win and this setting is ignored entirely.

```java
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

// 1. Oldest record still retained in each partition.
OffsetsInitializer.earliest()

// 2. Only records produced after the job starts.
OffsetsInitializer.latest()

// 3. Use offsets previously committed under this group id;
//    if the group has no committed offset for a partition, fall back.
OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)

// 4. First record whose Kafka RECORD TIMESTAMP is >= this epoch millisecond.
OffsetsInitializer.timestamp(1735689600000L)

// 5. Explicit per-partition offsets (rare; recovery tooling)
Map<TopicPartition, Long> offsets = new HashMap<>();
offsets.put(new TopicPartition("events", 0), 12345L);
OffsetsInitializer.offsets(offsets);
```

When to use which:

| Initializer | Use it when |
|---|---|
| `earliest()` | Local dev, backfills, reprocessing history. **Default for a new stateful job you want warm.** |
| `latest()` | You genuinely only care about now: live dashboards, alerting where old alerts are useless. Dangerous for stateful jobs — you start with cold state. |
| `committedOffsets(EARLIEST)` | Migrating from a non-Flink consumer that already committed progress under that group id, and you would rather re-read than lose data. |
| `committedOffsets(LATEST)` | Same migration, but you would rather skip than duplicate. |
| `timestamp(ms)` | "Replay from 09:00 this morning" — incident recovery. Note it matches on the *Kafka record timestamp*, not a field inside your payload. |

`OffsetsInitializer.committedOffsets()` with **no argument** exists too, and it **throws** if any partition has no committed offset. That is a feature — it makes an operator mistake loud instead of silently re-reading a month of data. Use it when a committed offset must exist.

> **Key idea:** `setStartingOffsets` is a **cold-start policy only**. Once the job has a checkpoint, it is dead code. If your restarted job re-read everything from the beginning, the real bug is that it did not restore from a checkpoint — not that this line is wrong.

---

## `setTopics` vs `setTopicPattern`

```java
.setTopics("orders", "refunds")                       // explicit list

.setTopicPattern(java.util.regex.Pattern.compile("txn-.*"))  // regex
```

With a pattern, the **split enumerator on the JobManager periodically re-lists topics**, so topics created *after* the job starts get picked up. Control the interval with a Kafka client property:

```java
.setProperty("partition.discovery.interval.ms", "300000")   // 5 minutes
```

This same property enables discovery of **new partitions added to existing topics**. In the new `KafkaSource` partition discovery is **enabled by default** (30 s) — set it to a negative value to disable it. If you ever add partitions to a live topic and Flink never reads them, this is the knob.

---

## The group id trap — the thing interviewers ask

```java
.setGroupId("flink-events-consumer")
```

Everybody assumes this makes Flink a member of a Kafka consumer group, and that Kafka's group coordinator assigns partitions to Flink's parallel readers. **It does not.**

```
NORMAL KAFKA CONSUMER GROUP            FLINK KafkaSource
──────────────────────────             ─────────────────
 consumer1 ─┐                           JobManager
 consumer2 ─┼─► Group Coordinator        └─ KafkaSourceEnumerator
 consumer3 ─┘   (broker) assigns              │  lists partitions,
                partitions, rebalances        │  assigns SPLITS itself
                on join/leave                 ▼
                                        reader-0  reader-1  reader-2
                                          p0,p3     p1,p4     p2,p5
                                        (uses assign(), never subscribe())
```

Flink's `KafkaSourceEnumerator` runs on the JobManager, lists the partitions itself, and hands each reader subtask a fixed set of **splits** (a split ≈ a partition). Under the hood the readers call the Kafka consumer's `assign()` API, never `subscribe()`. There is no group coordinator, no rebalance protocol, no heartbeat-driven partition revocation.

So what *is* the group id for?

1. **Offset committing.** On each successful checkpoint, Flink commits the current offsets to Kafka's `__consumer_offsets` under this group id.
2. **Monitoring.** So `kafka-consumer-groups.sh --describe --group flink-events-consumer` shows you a lag number, and your existing Kafka dashboards work.
3. **Client-side quotas / ACLs**, which are group-scoped in some setups.

Consequences you should be able to state:

- **Two Flink jobs with the same group id do NOT split the partitions between them.** Both read everything, and they stomp on each other's committed offsets. Give every job a unique group id.
- **Adding parallelism does not trigger a Kafka rebalance.** Flink redistributes splits itself on restart from the checkpoint.
- **Kafka-reported consumer lag can be stale or wrong** for a Flink job: it only advances at checkpoint time, and it is not what recovery uses.

> **Key idea:** For a Flink `KafkaSource`, the group id is a **label for offset bookkeeping**, not a partition-assignment mechanism. Flink owns assignment.

---

## Offsets are not the source of truth

Flink's checkpoint contains the source offsets **plus all operator state**, snapshotted at one consistent point. That is the only thing recovery uses.

```
CHECKPOINT n contains:
   ┌──────────────────────────────────────────┐
   │ source offsets:  p0=4820  p1=4791 ...    │  ← where to resume reading
   │ window contents: {u1: 3 events, ...}     │  ← in-flight aggregation
   │ keyed state:     {u1: lastSeen=...}      │  ← your ValueState
   └──────────────────────────────────────────┘
         ALL of it, as of the SAME logical instant.
```

If Flink restored the offsets from Kafka instead, it would restart from those offsets with **state from a different moment** — double counting or losing events. That is why:

```java
// Kafka offset committing on checkpoint: ON by default when
// checkpointing is enabled. To turn it off:
.setProperty("commit.offsets.on.checkpoint", "false")

// Never enable Kafka's own auto-commit for a Flink source:
.setProperty("enable.auto.commit", "false")   // this is the default; keep it
```

Kafka auto-commit is time-driven ("every 5 s"), completely unrelated to Flink's checkpoint boundary. It would commit offsets for records whose effects were never checkpointed.

> **Key idea:** Kafka offsets in Flink are for **external visibility** (lag dashboards), not for recovery. Flink's checkpoint is the source of truth. If checkpointing is disabled, nothing is committed at all and there is no exactly-once anything.

---

## `setProperty` — the escape hatch

Anything from the Kafka consumer configuration that has no dedicated builder method goes here as a raw string key/value:

```java
.setProperty("partition.discovery.interval.ms", "60000")
.setProperty("security.protocol", "SASL_SSL")
.setProperty("sasl.mechanism", "SCRAM-SHA-512")
.setProperty("sasl.jaas.config",
    "org.apache.kafka.common.security.scram.ScramLoginModule required "
    + "username=\"svc_flink\" password=\"...\";")
.setProperty("max.poll.records", "1000")
.setProperty("fetch.max.bytes", "52428800")
.setProperty("isolation.level", "read_committed")   // see ch. 26
```

`setProperties(Properties p)` sets several at once. Values are **always strings**, even numbers — that is the Kafka client convention.

---

## Attaching the source: `env.fromSource`

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import java.time.Duration;

DataStream<String> stream = env.fromSource(
        source,                                   // 1. the KafkaSource
        WatermarkStrategy.<String>forMonotonousTimestamps(),  // 2. watermarks
        "kafka-events"                            // 3. operator name in the UI
);
```

Three arguments:

1. The source object.
2. A `WatermarkStrategy<T>` — see below, this is the important one.
3. A human-readable name. It shows up in the Web UI job graph and in metric names. Name it well; you will be reading it during an incident at 2am.

`env.fromSource` returns a `DataStreamSource<T>`, which is a `DataStream<T>`, so you chain operators onto it normally.

A realistic strategy for real data:

```java
WatermarkStrategy<Event> ws = WatermarkStrategy
        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, recordTimestamp) -> event.timestamp)
        .withIdleness(Duration.ofMinutes(1));
```

- `forBoundedOutOfOrderness(5s)` — tolerate events arriving up to 5 s late.
- `withTimestampAssigner(...)` — a lambda `(T element, long recordTimestamp) -> long`. `recordTimestamp` is the **Kafka record timestamp**; return the field you actually want (`event.timestamp`). Return `recordTimestamp` if you want to trust Kafka's.
- `withIdleness(1min)` — mark a partition idle if silent for a minute so it stops holding the watermark back. Covered in the next section.

---

## Why the WatermarkStrategy goes HERE and not later

You *can* write `stream.assignTimestampsAndWatermarks(ws)` as a separate operator afterwards. For a Kafka source that is **wrong**, and here is exactly why.

A source subtask may read **several partitions**. Kafka guarantees order *within* a partition, not across them. If you generate watermarks after the source, you see one interleaved stream:

```
AFTER-THE-SOURCE watermark generation (BAD)
  p0: 10:00:00, 10:00:01, 10:00:02 ...
  p1: 09:30:00, 09:30:01, 09:30:02 ...   (partition lagging / replaying)
                    │
                    ▼ interleaved into one stream
  10:00:00, 09:30:00, 10:00:01, 09:30:01, ...
                    │
                    ▼ one watermark generator sees max = 10:00:00
  watermark jumps to 10:00:00 - 5s
                    │
                    ▼
  EVERY p1 record is now LATE and gets dropped.
```

Passing the strategy to `fromSource` instead makes Flink run a **separate watermark generator per split**, and emit the **minimum** across the splits that subtask owns:

```
PER-SPLIT watermark generation (GOOD — what fromSource does)
  split p0 ──► generator ──► wm = 09:59:55 ┐
  split p1 ──► generator ──► wm = 09:29:55 ┼─► min = 09:29:55 ──► emitted
  split p2 ──► generator ──► wm = 09:58:00 ┘
                    │
                    ▼
  The lagging partition holds the watermark back. Nothing is
  wrongly declared late. Correct, if slower.
```

This is called **per-split (per-partition) watermarking**, and it is the single biggest correctness reason to use `fromSource` properly.

The same mechanism handles idleness. A partition with no traffic has a watermark stuck at its last event forever, which would freeze the `min` and stall every downstream window. `withIdleness(Duration)` excludes a split from the `min` after it has been silent that long.

> **Key idea:** Watermarks belong **inside the source**, where Flink still knows which partition each record came from. Once the streams are interleaved that information is gone, and a single slow partition silently turns into mass data loss.

---

## Putting it together

```java
public class KafkaSourceJob {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Without checkpointing, no offsets are committed and there is
        // no fault tolerance at all. Always enable it for a Kafka job.
        env.enableCheckpointing(60_000);   // every 60 s

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("events")
                .setGroupId("flink-events-consumer-v1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "60000")
                .build();

        DataStream<String> raw = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),   // strings have no time yet
                "kafka-source");

        raw.print();

        env.execute("kafka-source-demo");
    }
}
```

`WatermarkStrategy.noWatermarks()` is the honest choice while the records are still raw `String`s — you cannot extract a timestamp from bytes you have not parsed. Chapter 31 parses to `Event` inside the deserializer so the strategy can do its job at the source.

`throws Exception` on `main` — `env.execute()` declares a checked exception, and Java forces you to either catch it or declare it. Declaring it on `main` is standard for Flink jobs.

---

## Remember

- `FlinkKafkaConsumer` is **removed**. Use `KafkaSource` + `env.fromSource`.
- `KafkaSource.<T>builder()` — explicit type argument goes before the method name.
- `setStartingOffsets` applies **only on a cold start**; checkpoints override it.
- `earliest()` / `latest()` / `committedOffsets(OffsetResetStrategy)` / `timestamp(ms)`.
- Flink **assigns partitions itself** via a JobManager-side enumerator. The group id is for **offset committing and lag monitoring only**. Unique group id per job.
- Offsets are committed **on checkpoint**, and are never the recovery source of truth.
- Turn off Kafka `enable.auto.commit` (it is off by default — keep it that way).
- Pass the `WatermarkStrategy` to `fromSource` so you get **per-split watermarks**; a later `assignTimestampsAndWatermarks` will drop data from lagging partitions.
- `withIdleness(...)` stops a silent partition from freezing the watermark.
- `setTopicPattern` + `partition.discovery.interval.ms` picks up new topics/partitions at runtime.

**Interview one-liners**

- *"Does Flink use Kafka consumer groups?"* → No. The `KafkaSourceEnumerator` on the JobManager assigns splits to readers using `assign()`. The group id is only for committing offsets and lag monitoring.
- *"What happens if two Flink jobs share a group id?"* → Both read all partitions and overwrite each other's committed offsets. Nothing splits.
- *"Where do Kafka offsets live for a Flink job?"* → In the checkpoint. They are also committed to Kafka on checkpoint, but purely for visibility.
- *"Why pass the WatermarkStrategy to fromSource?"* → Per-split watermark generation with a min across splits. Applying it downstream means one lagging partition advances the watermark past its own data and it all gets dropped as late.
- *"Job restarted and reprocessed a week of data — why?"* → It started fresh (no checkpoint/savepoint restore) and fell back to `setStartingOffsets(earliest())`.
- *"How does Flink pick up a newly added partition?"* → The enumerator re-lists periodically; `partition.discovery.interval.ms`, on by default at 30 s.
