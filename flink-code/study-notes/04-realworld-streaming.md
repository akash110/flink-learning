# Phase 4 — Real-world streaming (Kafka + how Flink actually runs)

Until now, `env.fromElements(...)` gave you a tiny finite stream. Real Flink reads an **unbounded** stream from a message broker — almost always **Kafka** — and this phase is also where you finally understand **parallelism**: what actually runs, on how many threads, and why.

### Extra Maven dependency

Add the Kafka connector to your `pom.xml` (same `${flink.version}` = 1.18.1):

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>3.1.0-1.18</version>   <!-- connector version aligned to Flink 1.18 -->
</dependency>
<!-- for JSON: -->
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.15.2</version>
</dependency>
```

> Note: the modern connector's version string looks like `3.x-1.18` (connector-major aligned to Flink-minor). If you use the Flink SQL Kafka connector later, the artifact is `flink-sql-connector-kafka`.

---

## 1. Kafka source (the modern `KafkaSource` API)

`FlinkKafkaConsumer` is deprecated in 1.18. Use `KafkaSource`:

```java
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("transactions")
        .setGroupId("flink-study")
        .setStartingOffsets(OffsetsInitializer.earliest())   // or .latest(), .committedOffsets(...)
        .setValueOnlyDeserializer(new SimpleStringSchema())   // bytes -> String
        .build();

DataStream<String> raw = env.fromSource(
        source,
        WatermarkStrategy.noWatermarks(),   // or a real strategy after you parse timestamps
        "kafka-source");
```

- `OffsetsInitializer.earliest()` — replay from the start (great for testing).
- `committedOffsets(...)` — resume from the consumer group's committed position (production).
- Offsets are stored in **operator state** and committed on checkpoint → exactly-once-friendly (Phase 5).

---

## 2. JSON events: deserialization / serialization

Real Kafka messages are usually JSON. Parse `String → POJO` with Jackson.

**Transaction POJO** (reuse the Phase 2/3 shape):
```java
public static class Transaction {
    public String user;
    public int amount;
    public long timestamp;
    public Transaction() {}
    // getters + toString...
}
```

**Deserialize in a `map`:**
```java
import com.fasterxml.jackson.databind.ObjectMapper;

// ObjectMapper is thread-safe & reusable; make it static
private static final ObjectMapper MAPPER = new ObjectMapper();

DataStream<Transaction> txns = raw
        .map(json -> MAPPER.readValue(json, Transaction.class))
        .returns(Transaction.class);
```

**Cleaner: a custom `DeserializationSchema`** so the source emits POJOs directly:
```java
public class TxnDeserializer extends AbstractDeserializationSchema<Transaction> {
    private static final ObjectMapper M = new ObjectMapper();
    @Override public Transaction deserialize(byte[] bytes) throws java.io.IOException {
        return M.readValue(bytes, Transaction.class);
    }
}
// KafkaSource.<Transaction>builder()....setValueOnlyDeserializer(new TxnDeserializer())
```

Then assign event-time watermarks off the parsed timestamp (Phase 2 §0 pattern).

---

## 3. Kafka sink

Write results back to Kafka with `KafkaSink`:

```java
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;

KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("alerts")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)   // or EXACTLY_ONCE (Phase 5)
        .build();

someStream.map(Object::toString).sinkTo(sink);
```

To serialize a POJO to JSON before sinking, `map` it through the `ObjectMapper` first (`writeValueAsString`).

---

## 4. Running Kafka locally (so you can actually test)

Quickest path — a `docker-compose.yml`:

```yaml
services:
  kafka:
    image: bitnami/kafka:3.7
    ports: ["9092:9092"]
    environment:
      KAFKA_CFG_NODE_ID: "1"
      KAFKA_CFG_PROCESS_ROLES: "broker,controller"
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "1@localhost:9093"
      KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      ALLOW_PLAINTEXT_LISTENER: "yes"
```

```bash
docker compose up -d
# produce a couple of JSON events by hand:
docker compose exec kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic transactions
> {"user":"alice","amount":100,"timestamp":1000}
> {"user":"alice","amount":900,"timestamp":2000}
```

---

## 5. Parallelism, task slots, operators, subtasks — the runtime model

This is the conceptual core of Phase 4. Get these five words straight:

- **Operator** — one logical step in your graph (`map`, `keyBy→window`, sink).
- **Subtask** — one *parallel instance* of an operator. An operator with parallelism 4 runs as 4 subtasks. **This is the unit that actually executes.**
- **Parallelism** — how many subtasks an operator (or the whole job) runs. Set globally `env.setParallelism(4)` or per-operator `.setParallelism(2)`.
- **Task slot** — a fixed "compute lane" on a **TaskManager** (worker process). A cluster with 3 TaskManagers × 4 slots = 12 slots = max total parallelism (for one slot-sharing group).
- **Task** — a chain of operator subtasks fused together (see §6).

```
Job parallelism = how many subtasks per operator
Cluster capacity = number of TaskManagers × slots per TaskManager
A job runs only if it has enough slots for its highest parallelism.
```

**Why you used `setParallelism(1)` while learning:** at parallelism 1 there's exactly one subtask per operator, so output is ordered and deterministic. Bump it up now to see real behavior.

---

## 6. Operator chaining & the diagram in the Web UI

Flink **fuses** adjacent operators that don't need a network shuffle into one **task**, run by one thread — this avoids serialization between steps.

```
source → map → filter        ... chained into ONE task (no shuffle between them)
        keyBy                ... forces a network shuffle (repartition by key) → chain BREAKS
        window → sink         ... a new chained task
```

- Chaining boundaries are exactly where data must be **redistributed**.
- `keyBy` always breaks the chain (records must travel to the subtask that owns the key).
- You'll see these boxes in the Flink Web UI (Phase 8). Each box = a task; the number inside = its parallelism.

---

## 7. Partitioning / how records move between subtasks

When a chain breaks, Flink must decide *which downstream subtask* each record goes to. The partitioning strategy:

| Strategy | Meaning | When |
|----------|---------|------|
| **forward** | stay on the same subtask | chained, same parallelism |
| **hash (keyBy)** | route by `hash(key) % n` | after `keyBy` — same key → same subtask |
| **rebalance** | round-robin across all subtasks | fix skew; `.rebalance()` |
| **rescale** | round-robin within a local subset | cheaper rebalance |
| **broadcast** | send every record to every subtask | broadcast-join / config streams |
| **shuffle** | random | rarely needed |

```java
stream.rebalance();    // evenly spread load (e.g., after a skewed source)
stream.keyBy(k -> ...);// hash partition by key (correctness, not just balance)
```

**Key correctness fact:** `keyBy` guarantees all records with the same key hit the same subtask — that's *why* keyed state and keyed windows work. It's not just load balancing; it's the partitioning that makes per-key state possible.

---

## 8. Multiple Kafka partitions ↔ Flink parallelism

- A Kafka topic has **P partitions**. Ordering is guaranteed **within** a partition, not across.
- `KafkaSource` assigns partitions to source subtasks. If source parallelism ≥ P, some subtasks are idle; if < P, some subtasks read multiple partitions.
- **Rule of thumb:** set source parallelism ≤ number of partitions. To scale beyond P, you must add partitions.
- **Idle partitions** can stall event-time watermarks (a silent partition never advances its watermark, holding back the whole job). Fix with `WatermarkStrategy.withIdleness(Duration.ofSeconds(30))`.

---

## 9. Backpressure

**Backpressure** = a slow downstream operator forces upstream operators (and ultimately the source) to slow down, because buffers fill up. It's Flink's built-in flow control — it prevents out-of-memory blowups, but sustained backpressure means a bottleneck.

- **How to spot it:** the Flink Web UI (Phase 8) shows a **Backpressure** tab per operator (OK / LOW / HIGH), and the "busy" metric.
- **Common causes:** an under-parallelized operator, a slow sink (e.g., a database), data skew (one hot key), or expensive per-record work.
- **Fixes:** raise parallelism of the bottleneck operator, `rebalance()` to fix skew, batch/async the slow sink (`AsyncDataStream` / async I/O), or reduce per-record cost.

> Backpressure is *normal and healthy* in bursts (it's how Flink stays stable). *Sustained* backpressure is the signal to tune (Phase 8).

---

## 10. Putting it together — a realistic skeleton

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(3);                       // real parallelism now
env.enableCheckpointing(60_000);             // Phase 5 — required for Kafka exactly-once

KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("transactions")
        .setGroupId("flink-study")
        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
        .setValueOnlyDeserializer(new TxnDeserializer())
        .build();

DataStream<Transaction> txns = env.fromSource(
        source,
        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((t, ts) -> t.timestamp)
                .withIdleness(Duration.ofSeconds(30)),      // guard against idle partitions
        "txns");

DataStream<String> alerts = txns
        .keyBy(t -> t.user)
        .process(new FraudDetector());        // your Phase 3 detector

alerts.sinkTo(kafkaAlertSink);
env.execute("fraud-pipeline");
```

---

### ✅ Phase 4 checklist

- [ ] `KafkaSource` (offsets initializers)
- [ ] `KafkaSink` + delivery guarantee
- [ ] JSON deserialize/serialize (map vs `DeserializationSchema`)
- [ ] Local Kafka running; produced & consumed a message
- [ ] Parallelism / task slots / operators / **subtasks**
- [ ] Operator chaining & where chains break
- [ ] Partitioning strategies (forward/hash/rebalance/broadcast)
- [ ] Kafka partitions ↔ source parallelism + idleness
- [ ] Backpressure: spot it & fix it

⬅️ [Phase 3](03-state.md)  ·  ➡️ [Phase 5 — Reliability](05-reliability.md)
