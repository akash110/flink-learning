# 26. KafkaSink — Writing to Kafka

The mirror of chapter 25. Same generational split: `FlinkKafkaProducer` is **gone**, `KafkaSink` (FLIP-143) replaced it.

| Old (removed in 1.15+/1.17) | New |
|---|---|
| `FlinkKafkaProducer<T>` | `KafkaSink<T>` |
| `stream.addSink(producer)` | `stream.sinkTo(sink)` |
| `Semantic.EXACTLY_ONCE` | `DeliveryGuarantee.EXACTLY_ONCE` |
| `KeyedSerializationSchema` | `KafkaRecordSerializationSchema` |

---

## The minimal sink

```java
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setRecordSerializer(
                KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("output-events")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();

stream.sinkTo(sink);
```

Note there are **two** builders nested: the outer one builds the sink (where to connect, what guarantee), the inner one builds the record serializer (which topic, how to turn `T` into key/value bytes). Beginners routinely try to call `.setTopic(...)` on the outer builder — it does not exist there.

```
KafkaSink.builder()
 ├── setBootstrapServers      "which cluster"
 ├── setRecordSerializer  ──► KafkaRecordSerializationSchema.builder()
 │                             ├── setTopic / setTopicSelector
 │                             ├── setValueSerializationSchema
 │                             ├── setKeySerializationSchema
 │                             └── setPartitioner
 ├── setDeliveryGuarantee     "how hard do we try"
 ├── setTransactionalIdPrefix "required for EXACTLY_ONCE"
 └── setProperty              "raw producer configs"
```

---

## The record serializer

```java
KafkaRecordSerializationSchema<Event> serializer =
    KafkaRecordSerializationSchema.<Event>builder()
        .setTopic("enriched-events")
        .setKeySerializationSchema(
                (Event e) -> e.userId.getBytes(StandardCharsets.UTF_8))
        .setValueSerializationSchema(new EventSerializationSchema())
        .build();
```

- `setTopic(String)` — a fixed destination topic.
- `setKeySerializationSchema(SerializationSchema<T>)` — produces the Kafka **message key** bytes. `SerializationSchema<T>` has a single abstract method `byte[] serialize(T element)`, which makes it a **functional interface**, so you can pass a lambda instead of writing a class. That is what `(Event e) -> e.userId.getBytes(...)` is.
- `setValueSerializationSchema(SerializationSchema<T>)` — the message body. Chapter 27 writes `EventSerializationSchema`.
- `setPartitioner(FlinkKafkaPartitioner<T>)` — optional; override how a record maps to a Kafka partition.

**Why the key matters.** Kafka's default partitioner routes by key hash, so a key gives you:

1. **Ordering** — all records for `user-42` land in one partition, so they are consumed in order.
2. **Compaction** — a log-compacted topic keeps only the last value per key.
3. **Downstream co-partitioning** — the next Flink job's `keyBy(userId)` lines up with the partitioning.

**No key = round-robin (sticky) partitioning = no per-entity ordering.** If your consumer cares about "the latest state of user X", set a key.

For a dynamic destination:

```java
.setTopicSelector((Event e) -> "events-" + e.type)   // topic per event type
```

Be careful: creating topics on the fly relies on broker auto-creation, which is usually disabled in production.

---

## `setDeliveryGuarantee` — the three levels

```java
DeliveryGuarantee.NONE
DeliveryGuarantee.AT_LEAST_ONCE   // default
DeliveryGuarantee.EXACTLY_ONCE
```

### `NONE`

Fire and forget. Flink does not wait for producer acks at checkpoint time. Records can be silently lost on broker failure or on a Flink restart.

Use when: metrics, logs, sampled telemetry — data where a gap is cheaper than latency.

### `AT_LEAST_ONCE` (default)

At each checkpoint, the sink **flushes** all buffered records and waits for the broker acks before the checkpoint is acknowledged. Nothing is lost.

But on recovery Flink rewinds the source to the last checkpoint and **replays** everything after it — including records already written to Kafka. So downstream sees **duplicates**.

```
                 checkpoint 7        CRASH
 source offsets  ──────┬──────────────┬────►
                       │              │
 written to Kafka      │  A B C D E   │
 restore from cp7 ─────┘              │
 replayed:                A B C D E   ▼  (written again)
 topic now contains:   A B C D E A B C D E
```

Use when: the consumer is idempotent (upserts keyed by an id), or duplicates are tolerable. This covers most real pipelines.

### `EXACTLY_ONCE`

Flink uses **Kafka transactions** plus its own two-phase commit protocol:

```
 ─── between checkpoints ───────────────────────────────────
   producer.beginTransaction()
   records written into an OPEN transaction
   consumers with read_committed CANNOT see them yet

 ─── checkpoint barrier arrives ────────────────────────────
   PRE-COMMIT: producer.flush(); transaction kept open;
               the transaction's state goes INTO the checkpoint

 ─── JobManager: all operators acked the checkpoint ────────
   notifyCheckpointComplete → producer.commitTransaction()
   NOW read_committed consumers can see the records

 ─── on failure before the commit ──────────────────────────
   the open transaction is ABORTED; the records never
   become visible; the source rewinds and redoes the work
```

So visibility is tied to checkpoint completion. Two consequences people miss:

1. **Latency becomes your checkpoint interval.** With a 60 s checkpoint interval, a downstream `read_committed` consumer sees output up to 60 s after Flink produced it. Exactly-once is not free — you pay in latency.
2. **The consumer must opt in.** See below.

> **Key idea:** `EXACTLY_ONCE` means *exactly-once end-to-end effect*, not "each record processed once". Records may be **processed** many times; the transaction ensures only one attempt is ever **made visible**.

---

## The consumer must use `read_committed`

Kafka consumers default to `isolation.level=read_uncommitted`, which means they see records from **open and aborted transactions too**. If your downstream consumer does not change this, Flink's exactly-once sink gives it duplicates and phantom records, and you have paid all the cost for nothing.

```java
// downstream Flink job
.setProperty("isolation.level", "read_committed")
```

```properties
# any other consumer, e.g. kafka-console-consumer --consumer.config
isolation.level=read_committed
```

`read_committed` also means the consumer cannot read past the **Last Stable Offset** — the offset of the earliest still-open transaction. One hung transaction blocks consumption of everything after it until it commits or times out. That is the mechanism behind the timeout trap below.

---

## `setTransactionalIdPrefix` — required, and must be unique

```java
.setTransactionalIdPrefix("fraud-detector-v1-")
```

Kafka identifies a transactional producer by a `transactional.id`. Flink builds one per sink subtask as `<prefix><subtaskIndex><counter>`.

The critical Kafka semantics: **when a producer registers with a `transactional.id` that already exists, the broker fences the previous producer** — bumps the epoch, and the old producer's in-flight transactions are aborted. That is exactly what you want on recovery (the zombie from before the crash gets fenced). It is a disaster between two unrelated jobs.

If two different jobs share a prefix:

```
 Job A subtask 0 → transactional.id "myjob-0-3"
 Job B subtask 0 → transactional.id "myjob-0-3"   ← same!

 Job B starts → broker fences Job A's producer
 Job A's next commit → ProducerFencedException → job fails
 Job A restarts → fences Job B
 → infinite mutual crash loop
```

Rules:

- **Unique prefix per job.** Include the job name.
- **Also bump it on a breaking redeploy** if you change the sink's parallelism or topology in a way that invalidates old transactions — though normally you want it *stable* across restarts so recovery can find and abort leftover transactions.
- It is **required** for `EXACTLY_ONCE`; `build()` throws without it.
- Keep it short — Kafka's `transactional.id` participates in broker-side state.

> **Key idea:** The transactional id prefix is the identity Kafka uses to fence zombies. Unique per job, stable across restarts of that job.

---

## The `transaction.timeout.ms` trap

The classic production incident, and a favourite interview question.

```java
.setProperty("transaction.timeout.ms", "900000")   // 15 minutes
```

Two constraints, from opposite directions:

```
                      MUST BE ABOVE                    MUST BE BELOW
                  ┌────────────────────┐          ┌──────────────────────┐
 transaction.timeout.ms                                 broker's
   >  max checkpoint interval                    transaction.max.timeout.ms
   +  worst-case checkpoint duration                 (default 15 min)
   +  worst-case restart/recovery time
```

**Too low** → a transaction opened at checkpoint *n* expires before checkpoint *n+1* commits it. Kafka aborts it. Flink then tries to commit an expired transaction and the job fails — and worse, on recovery it may find its transaction already gone, which is **silent data loss** in the window. The Flink producer default (`1 hour`) is deliberately generous; the Kafka producer default is only **15 minutes**, which is why the connector overrides it.

**Too high** → exceeds the broker's `transaction.max.timeout.ms` (default 900000 ms = 15 min) and the broker **rejects the producer at init** with `InvalidTxnTimeoutException`. Also, a genuinely hung transaction now blocks `read_committed` consumers for that entire duration — an hour-long consumer stall.

The fix in practice is to raise the **broker** setting and then set the producer to match:

```properties
# broker: server.properties
transaction.max.timeout.ms=3600000    # 1 hour
```

```java
// job
.setProperty("transaction.timeout.ms", "3600000")
```

Worked sizing:

```
checkpoint interval        : 60 s
p99 checkpoint duration    : 90 s
worst-case restart backoff : 60 s
job restart + restore      : 300 s
------------------------------------------
minimum safe timeout       ≈ 510 s   → set 15 min with headroom
broker max must be         ≥ 15 min
```

> **Key idea:** `transaction.timeout.ms` must be **longer than your worst downtime** and **no longer than the broker allows**. Both walls have caused real outages.

---

## Full production sink

```java
KafkaSink<Event> sink = KafkaSink.<Event>builder()
        .setBootstrapServers("broker1:9092,broker2:9092")
        .setRecordSerializer(
                KafkaRecordSerializationSchema.<Event>builder()
                        .setTopic("enriched-events")
                        .setKeySerializationSchema(
                                (Event e) -> e.userId.getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema(new EventSerializationSchema())
                        .build())
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix("enrichment-job-v1-")
        .setProperty("transaction.timeout.ms", "900000")
        // Durability on the broker side. acks=all is the default for the
        // idempotent producer that EXACTLY_ONCE turns on, but be explicit.
        .setProperty("acks", "all")
        .setProperty("compression.type", "lz4")
        .setProperty("linger.ms", "50")      // batch a little; big throughput win
        .setProperty("batch.size", "65536")
        .build();

stream.sinkTo(sink)
      .name("kafka-enriched-sink")   // shows in the Web UI
      .uid("kafka-enriched-sink");   // STABLE id for savepoint restore
```

`.name(...)` is cosmetic; `.uid(...)` is not. The `uid` is how Flink matches operator state in a savepoint to an operator in the new job graph. Without an explicit uid, Flink generates one from the topology — and any topology edit changes it, so your state cannot be restored. **Set `uid()` on every stateful operator, and a sink with transactions is stateful.**

---

## Checkpointing must be enabled

```java
env.enableCheckpointing(60_000);
```

Without checkpointing:
- `AT_LEAST_ONCE` degrades to no guarantee at all (no flush barrier, no replay).
- `EXACTLY_ONCE` transactions are never committed, so a `read_committed` consumer sees **nothing, ever**. This is the "my topic is empty but Flink says it wrote 4 million records" bug.

---

## Choosing a guarantee — decision table

| Your situation | Guarantee |
|---|---|
| Downstream does upserts by primary key | `AT_LEAST_ONCE` (dedup is free) |
| Downstream counts / sums | `EXACTLY_ONCE` |
| Money, billing, ledgers | `EXACTLY_ONCE`, no debate |
| Latency budget < checkpoint interval | `AT_LEAST_ONCE` + downstream dedup |
| Metrics, logs, sampling | `NONE` or `AT_LEAST_ONCE` |
| You cannot control the consumer's `isolation.level` | `AT_LEAST_ONCE` — exactly-once would be a lie |

---

## Remember

- `FlinkKafkaProducer` is **removed**. `KafkaSink` + `stream.sinkTo(sink)`.
- Two nested builders: sink-level (servers, guarantee) and serializer-level (topic, key, value).
- Set a **key** for ordering, compaction, and downstream co-partitioning. No key = round robin.
- `NONE` / `AT_LEAST_ONCE` (default) / `EXACTLY_ONCE`.
- Exactly-once = Kafka transactions + Flink 2PC. **Visibility latency = checkpoint interval.**
- The **consumer must set `isolation.level=read_committed`** or exactly-once buys nothing.
- `setTransactionalIdPrefix` is **required** for exactly-once, **unique per job** (shared prefixes fence each other into a crash loop), stable across restarts of the same job.
- `transaction.timeout.ms` > checkpoint interval + checkpoint duration + recovery time, and ≤ broker `transaction.max.timeout.ms` (default 15 min).
- No checkpointing → no guarantees, and exactly-once output is invisible forever.
- Always set `.uid()` on the sink.

**Interview one-liners**

- *"How does Flink do exactly-once into Kafka?"* → Two-phase commit: pre-commit flushes and snapshots the open Kafka transaction at the checkpoint barrier; commit happens in `notifyCheckpointComplete`. Failure before commit aborts the transaction.
- *"Does exactly-once mean each record is processed once?"* → No. Records are reprocessed on recovery; the transaction ensures only one attempt becomes visible.
- *"What's the cost of exactly-once?"* → End-to-end latency rises to the checkpoint interval, plus transaction overhead on the broker.
- *"Why is `transactionalIdPrefix` required?"* → It is the producer identity Kafka uses to fence zombies on recovery. Two jobs sharing one fence each other permanently.
- *"transaction.timeout.ms too low — what happens?"* → Transactions expire before commit; the job fails on commit and can lose that window's data. Too high and the broker rejects the producer.
- *"Flink says it wrote records but my consumer sees nothing."* → Exactly-once sink with checkpointing disabled or failing, so nothing ever commits; or the consumer is fine but the transactions are still open.
