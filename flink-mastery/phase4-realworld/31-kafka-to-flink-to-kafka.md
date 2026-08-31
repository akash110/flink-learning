# 31. The Complete Job: Kafka → Flink → Kafka

Everything from chapters 25–30 assembled into one runnable, production-shaped job. Run it end to end and you will have done the thing this entire phase was for.

```
 kafka-console-producer
        │
        ▼
   topic: events  ──►  KafkaSource ──► parse JSON ──► watermarks(+idleness)
                                                            │
                                                       keyBy(userId)
                                                            │
                                                    1-min tumbling window
                                                            │
                                                      AggregateFunction
                                                            │
                                          JSON serialize ──► KafkaSink (EXACTLY_ONCE)
                                                            │
                                                            ▼
                                                   topic: user-stats
                                                            │
                                                            ▼
                                        kafka-console-consumer --read-committed
```

---

## 1. `pom.xml` — the additions

On top of the Phase 1 pom (`flink-streaming-java`, `flink-clients`, log4j, shade plugin):

```xml
<properties>
  <flink.version>1.20.0</flink.version>
  <kafka.connector.version>3.2.0-1.19</kafka.connector.version>
  <jackson.version>2.15.3</jackson.version>
</properties>

<dependencies>

  <!-- KafkaSource / KafkaSink. NOT 'provided': the Flink distribution
       does NOT ship connectors, so this must land in your fat jar. -->
  <dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>${kafka.connector.version}</version>
  </dependency>

  <!-- JSON. Also NOT provided. -->
  <dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>${jackson.version}</version>
  </dependency>

  <!-- Optional: RocksDB state backend, once state outgrows the heap. -->
  <dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
  </dependency>

</dependencies>
```

> **Key idea:** Flink **core** is `provided` (the cluster has it — bundling it causes classloader conflicts). **Connectors and libraries** are not provided (the cluster does *not* have them — omitting them causes `ClassNotFoundException` at runtime, only on the cluster, never in your IDE).

---

## 2. `docker-compose.yml` — local Kafka in KRaft mode

KRaft = Kafka Raft. Since Kafka 3.3 (production-ready) Kafka manages its own metadata; **no ZooKeeper**. ZooKeeper support was removed entirely in Kafka 4.0. Every tutorial with a `zookeeper:` service is outdated.

```yaml
# docker-compose.yml
services:
  kafka:
    image: apache/kafka:3.8.0
    container_name: kafka
    ports:
      - "9092:9092"          # host -> broker
    environment:
      # --- KRaft: this node is both broker and controller ---
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      # --- Listeners ---
      # Two listeners: one for clients, one for the controller quorum.
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      # What clients are TOLD to connect to. From your Mac that must be
      # localhost:9092, not the container hostname.
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      # --- Single-node settings. NEVER use these values in production. ---
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

      # --- REQUIRED for the exactly-once sink ---
      # Our job sets transaction.timeout.ms = 15 min; the broker rejects
      # any producer asking for more than this. Default is also 15 min,
      # so raise it here to leave headroom.
      KAFKA_TRANSACTION_MAX_TIMEOUT_MS: 3600000

      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
```

`KAFKA_ADVERTISED_LISTENERS` is the #1 local-Kafka trap: the broker returns this address to clients during metadata discovery. Set it to the container name and your Mac cannot connect; set it to `localhost` and other containers cannot. For a single-node dev setup, `localhost:9092` is what you want.

```bash
docker compose up -d
docker compose logs -f kafka        # wait for "Kafka Server started"
```

Create the topics explicitly (auto-creation gives you 1 partition, which hides all the partitioning behaviour you just learned):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic events --partitions 3 --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic user-stats --partitions 3 --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic events-dlq --partitions 1 --replication-factor 1

# verify
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

---

## 3. Configuration externalization with `ParameterTool`

Never hardcode a broker address. `ParameterTool` parses `--key value` arguments, properties files, or environment variables into one lookup object.

```java
import org.apache.flink.api.java.utils.ParameterTool;

// From command-line args: --bootstrap.servers localhost:9092 --parallelism 4
ParameterTool params = ParameterTool.fromArgs(args);

// From a properties file on disk or the classpath
ParameterTool fileParams = ParameterTool.fromPropertiesFile("/etc/job.properties");

// From environment variables
ParameterTool envParams = ParameterTool.fromSystemProperties();

// LAYERED: file provides defaults, args override them. This is the pattern.
ParameterTool config = fileParams.mergeWith(ParameterTool.fromArgs(args));

// get(key, defaultValue) never throws; get(key) throws if missing —
// use the no-default form for values that MUST be supplied.
String brokers   = config.get("bootstrap.servers", "localhost:9092");
String inTopic   = config.get("input.topic", "events");
String outTopic  = config.get("output.topic", "user-stats");
int    parallel  = config.getInt("parallelism", 2);
long   ckptMs    = config.getLong("checkpoint.interval.ms", 60_000L);
boolean eos      = config.getBoolean("exactly.once", true);
```

Make the config visible in the Web UI and available inside functions:

```java
// Registers the parameters as the job's global configuration. They show
// up in the Web UI's Job Configuration panel, which is invaluable when
// you're staring at a job someone else deployed.
env.getConfig().setGlobalJobParameters(config);
```

Inside any `RichFunction`:

```java
@Override
public void open(Configuration parameters) {
    ParameterTool p = (ParameterTool)
        getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    this.threshold = p.getDouble("alert.threshold", 1000.0);
}
```

Run it:

```bash
./bin/flink run -c com.akash.flink.KafkaToKafkaJob target/job.jar \
  --bootstrap.servers broker1:9092,broker2:9092 \
  --input.topic events \
  --output.topic user-stats \
  --parallelism 8
```

---

## 4. The supporting classes

### `UserStats` — the output POJO

```java
package com.akash.flink.model;

public class UserStats {
    public String userId;
    public long   windowStart;   // epoch millis, inclusive
    public long   windowEnd;     // epoch millis, exclusive
    public long   eventCount;
    public double totalAmount;
    public double avgAmount;
    public double maxAmount;

    public UserStats() {}   // required: POJO + Jackson

    @Override
    public String toString() {
        return "UserStats{" + userId + " [" + windowStart + "," + windowEnd + ") "
                + "n=" + eventCount + " sum=" + totalAmount + "}";
    }
}
```

### `EventAggregator` — incremental aggregation

```java
package com.akash.flink.agg;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import com.akash.flink.model.Event;

/**
 * AggregateFunction<IN, ACC, OUT>
 *   IN  = Event                          what arrives
 *   ACC = Tuple3<count, sum, max>        the running accumulator
 *   OUT = Tuple3<count, sum, max>        what the window emits
 *
 * This is INCREMENTAL: Flink folds each record into the accumulator on
 * arrival and stores only the accumulator in state. A ProcessWindowFunction
 * alone would buffer EVERY record until the window fires — for a 1-minute
 * window at 100k rec/s that is 6 million objects in state per key range.
 */
public class EventAggregator
        implements AggregateFunction<Event, Tuple3<Long, Double, Double>,
                                            Tuple3<Long, Double, Double>> {

    @Override
    public Tuple3<Long, Double, Double> createAccumulator() {
        // count=0, sum=0, max = smallest possible double
        return Tuple3.of(0L, 0.0, -Double.MAX_VALUE);
    }

    @Override
    public Tuple3<Long, Double, Double> add(Event e, Tuple3<Long, Double, Double> acc) {
        // f0/f1/f2 are Flink Tuple's public field names.
        return Tuple3.of(acc.f0 + 1,
                         acc.f1 + e.amount,
                         Math.max(acc.f2, e.amount));
    }

    @Override
    public Tuple3<Long, Double, Double> getResult(Tuple3<Long, Double, Double> acc) {
        return acc;
    }

    @Override
    public Tuple3<Long, Double, Double> merge(Tuple3<Long, Double, Double> a,
                                              Tuple3<Long, Double, Double> b) {
        // Only called for SESSION windows (which merge) — but you must
        // implement it, and implementing it correctly is free here.
        return Tuple3.of(a.f0 + b.f0, a.f1 + b.f1, Math.max(a.f2, b.f2));
    }
}
```

### `StatsWindowFunction` — attaching window metadata

```java
package com.akash.flink.agg;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import com.akash.flink.model.UserStats;

/**
 * ProcessWindowFunction<IN, OUT, KEY, WINDOW>
 * Paired with an AggregateFunction, this receives ONE pre-aggregated
 * element, not the raw records. Its only job is to add the key and the
 * window boundaries, which the aggregator cannot see.
 */
public class StatsWindowFunction
        extends ProcessWindowFunction<Tuple3<Long, Double, Double>,
                                      UserStats, String, TimeWindow> {

    @Override
    public void process(String userId,
                        Context ctx,
                        Iterable<Tuple3<Long, Double, Double>> elements,
                        Collector<UserStats> out) {

        // Exactly one element, because an AggregateFunction preceded us.
        Tuple3<Long, Double, Double> agg = elements.iterator().next();

        UserStats s = new UserStats();
        s.userId      = userId;
        s.windowStart = ctx.window().getStart();
        s.windowEnd   = ctx.window().getEnd();
        s.eventCount  = agg.f0;
        s.totalAmount = agg.f1;
        s.avgAmount   = agg.f0 == 0 ? 0.0 : agg.f1 / agg.f0;
        s.maxAmount   = agg.f2;

        out.collect(s);   // Collector is how you emit; return type is void
    }
}
```

---

## 5. The job

```java
package com.akash.flink;

import com.akash.flink.agg.EventAggregator;
import com.akash.flink.agg.StatsWindowFunction;
import com.akash.flink.model.Event;
import com.akash.flink.model.UserStats;
import com.akash.flink.serde.EventDeserializationSchema;   // ch. 27
import com.akash.flink.serde.UserStatsSerializationSchema; // ch. 27 pattern

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class KafkaToKafkaJob {

    public static void main(String[] args) throws Exception {

        // ── 1. CONFIGURATION ────────────────────────────────────────────
        ParameterTool config = ParameterTool.fromArgs(args);

        String  brokers    = config.get("bootstrap.servers", "localhost:9092");
        String  inTopic    = config.get("input.topic",  "events");
        String  outTopic   = config.get("output.topic", "user-stats");
        String  groupId    = config.get("group.id",     "flink-user-stats-v1");
        long    ckptMs     = config.getLong("checkpoint.interval.ms", 30_000L);
        long    lateness   = config.getLong("max.out.of.orderness.sec", 5L);
        long    windowSec  = config.getLong("window.size.sec", 60L);
        boolean exactlyOnce= config.getBoolean("exactly.once", true);

        // ── 2. ENVIRONMENT ──────────────────────────────────────────────
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Publish the config to the Web UI and to every RichFunction.
        env.getConfig().setGlobalJobParameters(config);

        // NOTE: no env.setParallelism(...) here on purpose, so that the
        // `flink run -p N` flag actually controls parallelism (ch. 28).

        // Key groups. Set ONCE, on day one, and never change it: it is
        // baked into every savepoint and caps future rescaling. (ch. 28)
        env.setMaxParallelism(512);

        // ── 3. CHECKPOINTING ────────────────────────────────────────────
        // Without this: no fault tolerance, no offset commits, and the
        // exactly-once sink never commits a transaction, so the output
        // topic stays empty forever.
        env.enableCheckpointing(ckptMs, CheckpointingMode.EXACTLY_ONCE);

        var ckpt = env.getCheckpointConfig();
        // Minimum idle time BETWEEN checkpoints. Guarantees the job gets
        // real work done even if a checkpoint takes longer than the interval.
        ckpt.setMinPauseBetweenCheckpoints(5_000);
        // Fail the checkpoint if it exceeds this. Must be comfortably below
        // the sink's transaction.timeout.ms.
        ckpt.setCheckpointTimeout(120_000);
        ckpt.setMaxConcurrentCheckpoints(1);
        // Don't kill the job on a single hiccup.
        ckpt.setTolerableCheckpointFailureNumber(3);
        // Keep the last checkpoint when the job is cancelled, so you can
        // restore from it. Otherwise cancellation deletes it.
        ckpt.setExternalizedCheckpointRetention(
                org.apache.flink.streaming.api.environment.CheckpointConfig
                        .ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        // Barriers overtake buffered data → checkpoint duration stops
        // depending on backpressure. See ch. 30, detail in Phase 5.
        ckpt.enableUnalignedCheckpoints();

        // Where checkpoints are written. file:// locally; s3:// in prod.
        env.getCheckpointConfig().setCheckpointStorage(
                config.get("checkpoint.dir", "file:///tmp/flink-checkpoints"));

        // ── 4. SOURCE ───────────────────────────────────────────────────
        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers(brokers)
                .setTopics(inTopic)
                // Unique per job: it is only a label for offset commits
                // and lag monitoring, NOT partition assignment. (ch. 25)
                .setGroupId(groupId)
                // Cold-start policy only; a checkpoint restore overrides it.
                .setStartingOffsets(OffsetsInitializer.earliest())
                // byte[] -> Event, lenient, with a parseFailures counter.
                .setValueOnlyDeserializer(new EventDeserializationSchema())
                // Pick up partitions/topics added after the job started.
                .setProperty("partition.discovery.interval.ms", "60000")
                // If an upstream job writes transactionally, only read
                // committed data. Harmless otherwise.
                .setProperty("isolation.level", "read_committed")
                .build();

        // ── 5. WATERMARKS ───────────────────────────────────────────────
        // Passed to fromSource, NOT applied afterwards, so Flink generates
        // watermarks PER SPLIT and emits the min across them. Applying this
        // downstream would let one fast partition advance the watermark past
        // a lagging partition's data and silently drop it. (ch. 25)
        WatermarkStrategy<Event> watermarks = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(lateness))
                // (element, kafkaRecordTimestamp) -> long. Use OUR field.
                .withTimestampAssigner((event, recordTs) -> event.timestamp)
                // A silent partition would otherwise pin the min watermark
                // forever and no window would ever fire.
                .withIdleness(Duration.ofMinutes(1));

        DataStream<Event> events = env.fromSource(
                        source, watermarks, "kafka-source-" + inTopic)
                .uid("kafka-source")          // STABLE id for savepoint restore
                .name("Kafka Source");        // label in the Web UI

        // ── 6. PROCESSING ───────────────────────────────────────────────
        DataStream<UserStats> stats = events
                // Cheap filter FIRST, before any shuffle. (ch. 29)
                .filter(e -> e.userId != null && !e.userId.isEmpty())
                .uid("filter-valid").name("Filter Valid")

                // Hash partition. Same userId always → same subtask.
                // This ALWAYS breaks the operator chain. (ch. 28/29)
                .keyBy(e -> e.userId)

                .window(TumblingEventTimeWindows.of(Time.seconds(windowSec)))

                // Accept events up to 1 minute past the watermark: they
                // re-fire the window instead of being dropped.
                .allowedLateness(Time.minutes(1))

                // AggregateFunction + ProcessWindowFunction:
                // incremental state (one accumulator per key) PLUS access
                // to the key and window boundaries. Best of both.
                .aggregate(new EventAggregator(), new StatsWindowFunction())
                .uid("user-stats-window").name("1min User Stats");

        // ── 7. SINK ─────────────────────────────────────────────────────
        KafkaSink<UserStats> sink = KafkaSink.<UserStats>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<UserStats>builder()
                                .setTopic(outTopic)
                                // Key by userId: ordering per user, and it
                                // makes the topic compaction-friendly.
                                .setKeySerializationSchema(
                                        (UserStats s) ->
                                                s.userId.getBytes(StandardCharsets.UTF_8))
                                .setValueSerializationSchema(
                                        new UserStatsSerializationSchema())
                                .build())
                .setDeliveryGuarantee(exactlyOnce
                        ? DeliveryGuarantee.EXACTLY_ONCE
                        : DeliveryGuarantee.AT_LEAST_ONCE)
                // REQUIRED for exactly-once. Unique per job or two jobs
                // fence each other into a mutual crash loop. (ch. 26)
                .setTransactionalIdPrefix("user-stats-job-v1-")
                // > checkpoint interval + duration + recovery time,
                // and <= the broker's transaction.max.timeout.ms. (ch. 26)
                .setProperty("transaction.timeout.ms", "900000")
                .setProperty("compression.type", "lz4")
                .setProperty("linger.ms", "50")
                .build();

        stats.sinkTo(sink).uid("kafka-sink").name("Kafka Sink");

        // Also print locally while learning. Remove for production.
        stats.print().uid("debug-print").name("Debug Print").setParallelism(1);

        // ── 8. GO ───────────────────────────────────────────────────────
        // Nothing above has executed. env.execute() builds the JobGraph,
        // ships it to the JobManager, and blocks until the job ends.
        env.execute("kafka-to-flink-to-kafka");
    }
}
```

### `UserStatsSerializationSchema`

Same shape as chapter 27's `EventSerializationSchema`:

```java
package com.akash.flink.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import com.akash.flink.model.UserStats;

public class UserStatsSerializationSchema implements SerializationSchema<UserStats> {

    private transient ObjectMapper mapper;   // ObjectMapper isn't Serializable

    @Override
    public void open(InitializationContext context) {
        this.mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(UserStats element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (Exception e) {
            // Our own object failing to serialize is a bug, not bad input.
            throw new RuntimeException("Failed to serialize " + element, e);
        }
    }
}
```

---

## 6. Run it and see it work

### Terminal 1 — Kafka

```bash
docker compose up -d
# then create the three topics as above
```

### Terminal 2 — a consumer on the output topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic user-stats \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | " \
  --isolation-level read_committed
```

`--isolation-level read_committed` is **not optional**. Without it you read uncommitted and aborted transactions, which defeats the entire exactly-once sink (chapter 26).

### Terminal 3 — the job

Run `main()` in IntelliJ (MiniCluster, Web UI at `localhost:8081` if you use `createLocalEnvironmentWithWebUI`), or:

```bash
mvn clean package
./bin/flink run -c com.akash.flink.KafkaToKafkaJob \
  target/flink-playground-1.0-SNAPSHOT.jar \
  --bootstrap.servers localhost:9092 \
  --checkpoint.interval.ms 10000
```

A 10-second checkpoint interval while learning means transactions commit every 10 seconds, so you see output quickly. In production, 10 s is far too aggressive.

### Terminal 4 — produce events

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic events
```

Paste these one line at a time. **Timestamps are epoch milliseconds** and must move forward for windows to close.

```json
{"userId":"u1","type":"purchase","amount":10.0,"timestamp":1735689600000}
{"userId":"u1","type":"purchase","amount":25.5,"timestamp":1735689610000}
{"userId":"u2","type":"purchase","amount":99.9,"timestamp":1735689620000}
{"userId":"u1","type":"refund","amount":5.0,"timestamp":1735689630000}
```

Nothing appears yet. The 1-minute window `[1735689600000, 1735689660000)` closes only when the **watermark** passes `1735689660000`. Watermark = max seen timestamp − 5 s, so you need an event at ≥ `1735689665000`:

```json
{"userId":"u3","type":"purchase","amount":1.0,"timestamp":1735689665000}
```

Now terminal 2 prints (after the next checkpoint commits):

```
u1 | {"userId":"u1","windowStart":1735689600000,"windowEnd":1735689660000,
      "eventCount":3,"totalAmount":40.5,"avgAmount":13.5,"maxAmount":25.5}
u2 | {"userId":"u2","windowStart":1735689600000,"windowEnd":1735689660000,
      "eventCount":1,"totalAmount":99.9,"avgAmount":99.9,"maxAmount":99.9}
```

**Two things to internalise from that delay:**

1. **Event time**: the window fired because of the *watermark*, not the wall clock. Wait an hour with no new events and nothing fires.
2. **Exactly-once**: even after the window fires, the record is invisible until the next checkpoint **commits** the transaction. Output latency = window lateness + checkpoint interval.

### Test the failure paths

```bash
# 1. Malformed JSON -> the parseFailures counter increments, the job
#    keeps running. (With a throwing deserializer, this crash-loops.)
not json at all
{"userId":"u4","amount":"not-a-number","timestamp":1735689700000}

# 2. Late event: within allowedLateness(1min) -> the window RE-FIRES
#    with an updated result. Past it -> dropped.
{"userId":"u1","type":"purchase","amount":7.0,"timestamp":1735689640000}
```

### Useful checks

```bash
# Consumer lag under the job's group id (committed on checkpoint only)
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group flink-user-stats-v1

# Raw input, with partitions, to see how keys distributed
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic events --from-beginning \
  --property print.partition=true

# Prove exactly-once is doing something: run the consumer WITHOUT
# --isolation-level read_committed and you will see records from
# in-flight and aborted transactions too.
```

### Web UI checklist

At `localhost:8081`, confirm each thing this phase taught:

| Look at | You should see |
|---|---|
| Job graph | Source+Filter in ONE box (chained); `keyBy` breaks it before the window |
| Subtasks | `numRecordsIn` roughly even; wildly uneven = skew (ch. 29) |
| Backpressure tab | All green/idle at this volume (ch. 30) |
| Checkpoints | Completing in well under the timeout; Start Delay near zero |
| Job Configuration | Your `ParameterTool` values, because of `setGlobalJobParameters` |

---

## 7. Production checklist

```
CORRECTNESS
  [ ] Watermark strategy passed to fromSource, not applied afterwards
  [ ] withIdleness set (partitions can go quiet)
  [ ] allowedLateness chosen deliberately; late data measured
  [ ] Deserializer is lenient + dead-letter, never throwing
  [ ] Consumer of the output topic uses read_committed

FAULT TOLERANCE
  [ ] Checkpointing enabled, interval sized to the latency budget
  [ ] checkpointTimeout < sink transaction.timeout.ms
  [ ] transaction.timeout.ms <= broker transaction.max.timeout.ms
  [ ] Externalized checkpoints RETAIN_ON_CANCELLATION
  [ ] Checkpoint storage is durable (S3/HDFS), not local disk
  [ ] uid() on EVERY operator — no savepoint restore without it
  [ ] transactionalIdPrefix unique per job

PERFORMANCE
  [ ] Source parallelism <= Kafka partition count
  [ ] maxParallelism set explicitly on day one and never changed
  [ ] Uniform parallelism where possible so chains survive
  [ ] AggregateFunction/ReduceFunction, not a bare ProcessWindowFunction
  [ ] Type check: PojoType in the logs, not GenericType (Kryo)
  [ ] RocksDB + incremental checkpoints once state exceeds heap

OPERATIONS
  [ ] Bootstrap servers, topics, and intervals externalized
  [ ] setGlobalJobParameters so the config is visible in the UI
  [ ] Metrics exported (Prometheus reporter)
  [ ] Alerts on: backPressuredTimeMsPerSecond, checkpoint failures,
      consumer lag, numRecordsOut on the dead-letter path
  [ ] A restart strategy configured, not the default
```

---

## Remember

- Connectors and JSON libs are **not** `provided`; Flink core is. Get this backwards and it works in the IDE and fails on the cluster.
- Local Kafka is **KRaft**, no ZooKeeper. `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092` is the trap.
- Raise the broker's `transaction.max.timeout.ms` before asking for a long producer transaction timeout.
- `ParameterTool.fromArgs(args)`, layered with a properties file, and `setGlobalJobParameters` so it shows in the UI.
- Do **not** call `env.setParallelism()` in a production job — it overrides `-p`.
- `env.setMaxParallelism(512)` on day one. It cannot be changed later.
- Watermarks go into `fromSource`. Always `withIdleness`.
- `.aggregate(aggFn, processWindowFn)` = incremental state **and** window metadata.
- `.uid()` on every operator. No uid, no savepoint restore.
- Output latency with exactly-once = **window lateness + checkpoint interval**. Say this out loud before promising anyone sub-second latency.
- Consumer must be `read_committed` or exactly-once is theatre.

**Interview one-liners**

- *"Walk me through a Kafka-to-Kafka Flink job."* → KafkaSource with a lenient deserializer and per-split watermarks with idleness; filter early; keyBy; tumbling event-time window with an AggregateFunction plus a ProcessWindowFunction for metadata; KafkaSink with EXACTLY_ONCE, a unique transactional id prefix, and a transaction timeout above the checkpoint interval. Checkpointing on, uids everywhere, config via ParameterTool.
- *"What's the end-to-end latency?"* → Window size + allowed out-of-orderness + checkpoint interval, because exactly-once output is invisible until the transaction commits.
- *"Why is my output topic empty?"* → Exactly-once sink with checkpointing disabled or failing, or a consumer that isn't `read_committed` — or the watermark never advanced so no window ever fired.
- *"Why does `-p` not change anything?"* → `env.setParallelism()` is hardcoded in the job and outranks the CLI flag.
- *"Why can't you restore from your savepoint?"* → Missing `uid()`s, so Flink's generated operator ids changed when the topology changed; or `maxParallelism` was altered.
