# 60. Testing Flink Jobs

Most Flink code is never tested, and the reason is structural: people put everything in `main()`, where there is nothing to call. Fix the structure and testing becomes ordinary.

> **Key idea**
> Three levels, three tools, three speeds:
> **Unit** — one function, via a *test harness*. Milliseconds. You control event time exactly.
> **Integration** — the whole pipeline, via a *MiniCluster*. Seconds.
> **End-to-end** — with a real Kafka, via *Testcontainers*. Tens of seconds.
> Write many of the first, some of the second, a handful of the third.

---

## Part 0: Making the job testable

The refactor that unlocks everything.

```java
// ❌ UNTESTABLE. Everything is inside main(). There is nothing to call.
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers("prod-kafka:9092")     // hardcoded
                .setTopics("events")
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka")
           .keyBy(Event::getUserId)
           .process(new FraudDetector(500))
           .sinkTo(KafkaSink.<Alert>builder()
                   .setBootstrapServers("prod-kafka:9092")  // hardcoded
                   .build());

        env.execute("fraud-detection");
    }
}
```

To test that, you need a real Kafka. Every time. For every assertion.

```java
// ✅ TESTABLE. main() only wires; a static method holds the logic.
package com.akash.flink.jobs;

import com.akash.flink.config.JobConfig;
import com.akash.flink.functions.FraudDetector;
import com.akash.flink.model.Alert;
import com.akash.flink.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class FraudDetectionJob {

    /**
     * The whole pipeline, as a pure function of (env, source, sink, config).
     * Knows NOTHING about Kafka. Tests can pass fromElements + a collecting sink.
     */
    public static void buildPipeline(StreamExecutionEnvironment env,
                                     DataStream<Event> events,
                                     Sink<Alert> sink,
                                     JobConfig cfg) {
        events
            .keyBy(Event::getUserId)
            .process(new FraudDetector(cfg.getFraudThreshold()))
            .uid("fraud-detector")            // ← stable state identity. Never omit.
            .name("Fraud Detector")
            .sinkTo(sink)
            .uid("alert-sink")
            .name("Alert Sink");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JobConfig cfg = JobConfig.from(ParameterToolFor(args));   // ch. 55

        Source<Event, ?, ?> source = KafkaSources.events(cfg);    // your factory
        DataStream<Event> events = env.fromSource(
                source,
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1)),
                "kafka-events");

        buildPipeline(env, events, KafkaSinks.alerts(cfg), cfg);
        env.execute("fraud-detection");
    }
}
```

Two properties make this work: `buildPipeline` takes a `DataStream<Event>` (so the test supplies `fromElements`) and a `Sink<Alert>` (so the test supplies a collecting sink). The **exact same graph** runs in the test and in production — including the `uid()` calls, so the test also proves your uids exist.

---

## Part 1: Unit-testing functions with test harnesses

Test harnesses let you feed records into a single operator, **control event time and processing time by hand**, and read the output. No cluster, no threads, no waiting.

### The Maven dependencies

```xml
<!-- The harness classes live in the TEST jar, not the main jar.
     <type>test-jar</type> is what pulls them in. -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>${flink.version}</version>
    <type>test-jar</type>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-runtime</artifactId>
    <version>${flink.version}</version>
    <type>test-jar</type>
    <scope>test</scope>
</dependency>
<dependency>
    <!-- MiniClusterWithClientResource / MiniClusterExtension (Part 2) -->
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-test-utils</artifactId>
    <version>${flink.version}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>5.10.2</version>
    <scope>test</scope>
</dependency>
```

**`<type>test-jar</type>`** is the one people miss. Maven publishes two artifacts per module: the normal jar and a `-tests.jar`. `OneInputStreamOperatorTestHarness` is only in the latter. Without that line you get `cannot find symbol`.

### The function under test

```java
package com.akash.flink.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.akash.flink.model.Alert;
import com.akash.flink.model.Event;

/**
 * Fires an Alert when a user exceeds `threshold` total amount
 * within a 60-second event-time window since their first event.
 */
public class FraudDetector extends KeyedProcessFunction<String, Event, Alert> {

    private static final long WINDOW_MS = 60_000L;

    private final long threshold;

    private transient ValueState<Double> runningTotal;
    private transient ValueState<Long> timerTs;

    public FraudDetector(long threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(OpenContext ctx) {
        runningTotal = getRuntimeContext().getState(
                new ValueStateDescriptor<>("total", Double.class));
        timerTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer", Long.class));
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<Alert> out) throws Exception {
        Double total = runningTotal.value();
        if (total == null) {
            total = 0.0;
            // First event for this key: arm an event-time timer to reset the window.
            long fireAt = ctx.timestamp() + WINDOW_MS;
            ctx.timerService().registerEventTimeTimer(fireAt);
            timerTs.update(fireAt);
        }
        total += e.getAmount();
        runningTotal.update(total);

        if (total > threshold) {
            out.collect(new Alert(e.getUserId(), total));
            runningTotal.clear();          // reset so we don't alert on every subsequent event
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        runningTotal.clear();              // window expired without crossing the threshold
        timerTs.clear();
    }
}
```

### The test

```java
package com.akash.flink.functions;

import com.akash.flink.model.Alert;
import com.akash.flink.model.Event;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class FraudDetectorTest {

    private OneInputStreamOperatorTestHarness<Event, Alert> harness;

    @BeforeEach
    void setUp() throws Exception {
        FraudDetector fn = new FraudDetector(500);

        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(fn),   // wrap the function in its operator
                Event::getUserId,                 // the key selector — must match keyBy()
                TypeInformation.of(String.class)  // the key TYPE, needed for serialization
        );

        harness.open();      // runs open() on the function: state descriptors, metrics.
                             // Forgetting this gives a confusing NullPointerException.
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();     // runs close(); releases state.
    }

    @Test
    void emitsAlertWhenThresholdExceeded() throws Exception {
        // processElement(value, timestamp) — the timestamp is the EVENT TIME.
        // You set it explicitly. No clock, no flakiness.
        harness.processElement(new Event("u1", "purchase", 300.0, 1000L), 1000L);
        harness.processElement(new Event("u1", "purchase", 300.0, 2000L), 2000L);
        //                                  running total 600 > 500 → alert

        List<Alert> out = extractOutput();

        assertThat(out).hasSize(1);
        assertThat(out.get(0).getUserId()).isEqualTo("u1");
        assertThat(out.get(0).getAmount()).isEqualTo(600.0);
    }

    @Test
    void doesNotAlertBelowThreshold() throws Exception {
        harness.processElement(new Event("u1", "purchase", 100.0, 1000L), 1000L);
        harness.processElement(new Event("u1", "purchase", 100.0, 2000L), 2000L);

        assertThat(extractOutput()).isEmpty();
    }

    @Test
    void keysAreIsolated() throws Exception {
        // 300 + 300 across TWO users must not alert. Catches "forgot to key" bugs.
        harness.processElement(new Event("u1", "purchase", 300.0, 1000L), 1000L);
        harness.processElement(new Event("u2", "purchase", 300.0, 1000L), 1000L);

        assertThat(extractOutput()).isEmpty();
    }

    @Test
    void timerResetsTheWindow() throws Exception {
        harness.processElement(new Event("u1", "purchase", 400.0, 1000L), 1000L);

        // ADVANCE EVENT TIME. This fires every event-time timer at or below 62000.
        // The timer was armed at 1000 + 60000 = 61000, so it fires and clears state.
        harness.processWatermark(62_000L);

        // A new event starts a fresh window: 400 alone is under the threshold.
        harness.processElement(new Event("u1", "purchase", 400.0, 63_000L), 63_000L);

        assertThat(extractOutput()).isEmpty();
    }

    @Test
    void lateEventsStillCountWithinTheWindow() throws Exception {
        harness.processElement(new Event("u1", "purchase", 300.0, 10_000L), 10_000L);
        harness.processWatermark(30_000L);                    // window not yet expired
        harness.processElement(new Event("u1", "purchase", 300.0, 20_000L), 20_000L);

        assertThat(extractOutput()).hasSize(1);
    }

    /**
     * getOutput() returns a Queue<Object> containing BOTH StreamRecords and
     * Watermarks, in order. Filter to the records and unwrap them.
     */
    @SuppressWarnings("unchecked")
    private List<Alert> extractOutput() {
        return harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> ((StreamRecord<Alert>) o).getValue())
                .collect(Collectors.toList());
    }
}
```

> **Key idea**
> `harness.processWatermark(t)` is how you make event time move. It fires every event-time timer with a timestamp `≤ t`, synchronously, before the call returns. **No sleeping, no waiting, no flaky tests.** This is the single biggest reason to unit-test Flink functions rather than only integration-test them: you can test a 7-day session window in a microsecond.

### Processing-time timers

```java
@Test
void processingTimeTimerFires() throws Exception {
    // The harness has its OWN clock. Nothing sleeps.
    harness.setProcessingTime(0L);
    harness.processElement(new Event("u1", "purchase", 100.0, 0L), 0L);

    harness.setProcessingTime(5_000L);    // jump the clock; fires timers ≤ 5000
    assertThat(extractOutput()).hasSize(1);
}
```

Also available: `harness.getProcessingTimeService().getCurrentProcessingTime()`, and `harness.numEventTimeTimers()` / `numProcessingTimeTimers()` — the latter two are excellent for asserting you are not **leaking timers**, which is a real production state-growth bug.

### Testing state restore — snapshot and restore

This is the test that catches the bug that costs you a production incident: state that does not survive a restart.

```java
@Test
void stateSurvivesRestore() throws Exception {
    // ---- Run 1: accumulate 400 (under the 500 threshold) ----
    harness.processElement(new Event("u1", "purchase", 400.0, 1000L), 1000L);
    assertThat(extractOutput()).isEmpty();

    // Take a savepoint. Arguments: (checkpointId, timestamp).
    OperatorSubtaskState snapshot = harness.snapshot(1L, 1000L);
    harness.close();

    // ---- Run 2: a brand-new harness, restored from that snapshot ----
    OneInputStreamOperatorTestHarness<Event, Alert> restored =
            new KeyedOneInputStreamOperatorTestHarness<>(
                    new KeyedProcessOperator<>(new FraudDetector(500)),
                    Event::getUserId,
                    TypeInformation.of(String.class));

    restored.setup();
    restored.initializeState(snapshot);   // ← restore BEFORE open()
    restored.open();

    // If state restored, 400 + 200 = 600 > 500 → alert.
    // If it did NOT restore, 200 alone is under threshold → no alert, test fails.
    restored.processElement(new Event("u1", "purchase", 200.0, 2000L), 2000L);

    List<Alert> out = restored.getOutput().stream()
            .filter(o -> o instanceof StreamRecord)
            .map(o -> ((StreamRecord<Alert>) o).getValue())
            .collect(Collectors.toList());

    assertThat(out).hasSize(1);
    assertThat(out.get(0).getAmount()).isEqualTo(600.0);

    restored.close();
}
```

The ordering is strict: **`setup()` → `initializeState(snapshot)` → `open()`**. Call `open()` before `initializeState` and the restore is silently ignored.

Harness variants:

| Class | For |
|---|---|
| `OneInputStreamOperatorTestHarness` | non-keyed one-input operators (`map`, `filter`, `process`) |
| `KeyedOneInputStreamOperatorTestHarness` | keyed operators — anything after `keyBy` |
| `TwoInputStreamOperatorTestHarness` | `connect()` / `CoProcessFunction` |
| `KeyedTwoInputStreamOperatorTestHarness` | keyed connected streams, e.g. interval joins |
| `ProcessFunctionTestHarnesses` | convenience static factories that build the above for you |

`ProcessFunctionTestHarnesses` is worth knowing — it removes the operator-wrapping boilerplate:

```java
KeyedOneInputStreamOperatorTestHarness<String, Event, Alert> h =
        ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new FraudDetector(500), Event::getUserId, TypeInformation.of(String.class));
```

For simple stateless functions you do not need a harness at all — just call the method:

```java
@Test
void plainFilterFunction() throws Exception {
    ThresholdFilter f = new ThresholdFilter(100);
    assertThat(f.filter(new Event("u1", "purchase", 150.0, 0L))).isTrue();
    assertThat(f.filter(new Event("u1", "purchase",  50.0, 0L))).isFalse();
}
```

That runs in microseconds. Reach for a harness only when there is **state, timers, or side outputs**.

---

## Part 2: Integration-testing the whole job with a MiniCluster

A MiniCluster is a real Flink cluster — JobManager, TaskManagers, network stack, checkpointing — inside your test JVM.

### JUnit 5 with `MiniClusterExtension`

```java
package com.akash.flink.jobs;

import com.akash.flink.config.JobConfig;
import com.akash.flink.model.Alert;
import com.akash.flink.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FraudDetectionJobIT {

    // static: ONE cluster shared by every test in this class. Starting a
    // MiniCluster takes a second or two - do not pay it per test.
    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .setConfiguration(new Configuration())
                    .build());

    @BeforeEach
    void resetSink() {
        CollectSink.VALUES.clear();     // ← MANDATORY. See the warning below.
    }

    @Test
    void detectsFraudEndToEnd() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parallelism 1 is REQUIRED for the static-list sink to be correct.
        env.setParallelism(1);

        DataStream<Event> events = env
                .fromElements(
                        new Event("u1", "purchase", 300.0, 1_000L),
                        new Event("u1", "purchase", 300.0, 2_000L),   // u1 crosses 500
                        new Event("u2", "purchase", 100.0, 3_000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((e, ts) -> e.getTimestamp()));

        // THE SAME buildPipeline THAT PRODUCTION USES.
        FraudDetectionJob.buildPipeline(env, events, new CollectSink(), testConfig());

        // fromElements is BOUNDED, so execute() returns when the data is exhausted.
        env.execute("fraud-detection-it");

        List<Alert> alerts = CollectSink.getValues();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getUserId()).isEqualTo("u1");
    }

    private JobConfig testConfig() { /* build a JobConfig with threshold 500 */ return null; }
}
```

### The `CollectSink` pattern, and its warning

```java
package com.akash.flink.jobs;

import com.akash.flink.model.Alert;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Collects records into a STATIC list so the test JVM can read them.
 *
 * ⚠️ THIS ONLY WORKS BECAUSE:
 *    1. The MiniCluster runs in the SAME JVM as the test, so a static field
 *       is genuinely shared. On a real cluster this is a no-op and a bug.
 *    2. The test runs at PARALLELISM 1. With parallelism > 1, multiple
 *       subtask threads write to the same list concurrently.
 *
 * Even at parallelism 1 the list is written by the Flink task thread and read
 * by the JUnit thread, so it must be synchronized. Hence Collections.synchronizedList
 * plus an explicit copy in getValues().
 */
public class CollectSink implements Sink<Alert> {

    public static final List<Alert> VALUES =
            Collections.synchronizedList(new ArrayList<>());

    @Override
    public SinkWriter<Alert> createWriter(WriterInitContext context) {
        return new SinkWriter<Alert>() {
            @Override
            public void write(Alert element, Context context) { VALUES.add(element); }

            @Override
            public void flush(boolean endOfInput) { }

            @Override
            public void close() { }
        };
    }

    public static List<Alert> getValues() {
        synchronized (VALUES) { return new ArrayList<>(VALUES); }   // defensive copy
    }
}
```

> **Key idea — the static-state warning.**
> Static fields are **per-JVM**, not per-subtask. In a MiniCluster test at parallelism 1 that is exactly what you want. On a real cluster it is meaningless: each TaskManager has its own JVM, its own copy of the static, and none of them is the client. Never let this pattern escape into production code. And **clear the list in `@BeforeEach`** — JUnit reuses the JVM across tests, so leftover results from test A will make test B pass or fail for the wrong reason.

### JUnit 4 with `MiniClusterWithClientResource`

Older codebases use the JUnit 4 rule. Same cluster, different plumbing:

```java
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;

public class FraudDetectionJobJUnit4IT {

    @ClassRule
    public static final MiniClusterWithClientResource FLINK =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());
    // ... tests identical in shape
}
```

`@ClassRule` (not `@Rule`) so the cluster starts once per class rather than per test method.

### The alternative to a static sink: `executeAndCollect`

Cleaner when it fits, because it needs no static state:

```java
@Test
void usingExecuteAndCollect() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Alert> alerts = /* build without a sink */ null;

    // executeAndCollect() runs the job AND returns an iterator over the output.
    // It calls execute() itself - do not also call env.execute().
    try (CloseableIterator<Alert> it = alerts.executeAndCollect()) {
        List<Alert> result = new ArrayList<>();
        it.forEachRemaining(result::add);
        assertThat(result).hasSize(1);
    }
}
```

Works only for **bounded** sources — an unbounded job never finishes and the iterator blocks forever.

### Testing checkpointing and recovery in a MiniCluster

```java
@Test
void recoversFromFailure() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);           // very short so a checkpoint definitely happens
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

    // A map that throws exactly once, after N records, then never again.
    // On restart, Flink replays from the last checkpoint; the static flag
    // stops it failing forever.
    env.fromElements(/* ... */)
       .map(new FailOnceMapper())
       .sinkTo(new CollectSink());

    env.execute("recovery-test");

    // Assert exactly-once: no duplicates despite the restart.
    assertThat(CollectSink.getValues()).hasSize(EXPECTED);
}
```

This is the test that proves your sink is idempotent or transactional. Worth writing once per job.

---

## Part 3: End-to-end with Testcontainers

Testcontainers starts real Docker containers from your test and shuts them down afterwards. Use it to test the parts the MiniCluster cannot: **the Kafka source and sink, your serializers, your offset handling**.

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <version>1.19.7</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>1.19.7</version>
    <scope>test</scope>
</dependency>
```

```java
package com.akash.flink.jobs;

import com.akash.flink.model.Event;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class FraudDetectionKafkaE2ETest {

    // Testcontainers starts this before the tests and stops it after.
    // static → one broker for the whole class. Starting Kafka takes ~10s.
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private static final String IN  = "events-in";
    private static final String OUT = "alerts-out";

    @Test
    void endToEndThroughRealKafka() throws Exception {

        // ---- 1. produce test input ----
        Properties p = new Properties();
        // getBootstrapServers() returns the RANDOM host port Docker mapped.
        // Never hardcode 9092 - it will collide on CI.
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(p)) {
            producer.send(new ProducerRecord<>(IN, "u1",
                    "{\"userId\":\"u1\",\"type\":\"purchase\",\"amount\":300.0,\"timestamp\":1000}"));
            producer.send(new ProducerRecord<>(IN, "u1",
                    "{\"userId\":\"u1\",\"type\":\"purchase\",\"amount\":300.0,\"timestamp\":2000}"));
            producer.flush();
        }

        // ---- 2. run the job against that broker ----
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers(KAFKA.getBootstrapServers())
                .setTopics(IN)
                .setGroupId("test-" + UUID.randomUUID())      // fresh group → read from start
                .setStartingOffsets(OffsetsInitializer.earliest())
                // ⚠️ THE KEY LINE for a terminating test: switch the source to
                // BOUNDED so it stops at the current end of the topic and
                // env.execute() actually returns instead of hanging forever.
                .setBounded(OffsetsInitializer.latest())
                .setDeserializer(new EventDeserializationSchema())
                .build();

        DataStream<Event> events = env.fromSource(
                source,
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, ts) -> e.getTimestamp()),
                "kafka-source");

        KafkaSink<Alert> sink = KafkaSink.<Alert>builder()
                .setBootstrapServers(KAFKA.getBootstrapServers())
                .setRecordSerializer(new AlertSerializationSchema(OUT))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        FraudDetectionJob.buildPipeline(env, events, sink, testConfig());
        env.execute("e2e");

        // ---- 3. assert on what actually landed in Kafka ----
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringDeserializer");
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringDeserializer");

        List<String> received = new ArrayList<>();
        try (Consumer<String, String> consumer = new KafkaConsumer<>(c)) {
            consumer.subscribe(List.of(OUT));
            long deadline = System.currentTimeMillis() + 30_000;    // always bound the wait
            while (received.isEmpty() && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500))
                        .forEach(r -> received.add(r.value()));
            }
        }

        assertThat(received).hasSize(1);
        assertThat(received.get(0)).contains("u1").contains("600");
    }

    private JobConfig testConfig() { return null; }
}
```

Four things that make Testcontainers tests reliable rather than flaky:

1. **`setBounded(OffsetsInitializer.latest())`** — without it the Kafka source is unbounded, `env.execute()` never returns, and your test hangs until the CI timeout.
2. **`KAFKA.getBootstrapServers()`** — Docker maps a random host port. Hardcoding `localhost:9092` breaks on any machine that already runs Kafka, and on parallel CI.
3. **A fresh random `groupId` per test** — otherwise a committed offset from a previous run means the source reads nothing and the test fails mysteriously on the second run.
4. **A deadline on the verification poll**, never a bare `while(true)`.

Testcontainers needs Docker on the machine. On CI that usually means a Docker-in-Docker service or a mounted socket — worth confirming before you write twenty of these.

### The test pyramid for Flink

```
                       ▲
                      ╱ ╲     E2E — Testcontainers (~30s each)
                     ╱   ╲    A handful. Connectors, serde, offsets.
                    ╱─────╲
                   ╱       ╲  INTEGRATION — MiniCluster (~2s each)
                  ╱         ╲ Some. Graph wiring, watermarks, recovery.
                 ╱───────────╲
                ╱             ╲ UNIT — harnesses & plain calls (~5ms)
               ╱_______________╲ MANY. All business logic, state, timers.
```

If your logic lives in `functions/` (ch. 54), the bottom layer covers most of your risk at almost no cost. That is the whole payoff of the `buildPipeline` refactor.

---

## Remember

- **Refactor first:** `main()` only wires; a **static `buildPipeline(env, source, sink, config)`** holds the graph. Tests then pass `fromElements` and a collecting sink and exercise the *same* graph.
- Test harnesses need `<type>test-jar</type>` on `flink-streaming-java` and `flink-runtime`. Missing that line is the usual "cannot find symbol".
- `KeyedOneInputStreamOperatorTestHarness(operator, keySelector, keyType)` for anything after a `keyBy`. Call **`harness.open()`** or you get a NullPointerException.
- **`processElement(value, timestamp)`** sets event time explicitly. **`processWatermark(t)`** advances event time and fires timers synchronously. **`setProcessingTime(t)`** does the same for processing time. **No sleeping, ever.**
- `getOutput()` returns records **and** watermarks — filter to `StreamRecord` and unwrap.
- Test state restore with **`snapshot(id, ts)` → new harness → `setup()` → `initializeState(snapshot)` → `open()`**. That ordering is strict.
- Assert on `numEventTimeTimers()` to catch timer leaks.
- `MiniClusterExtension` (JUnit 5) / `MiniClusterWithClientResource` (JUnit 4) run a real cluster in-JVM. Make it **`static`/`@ClassRule`** so it starts once per class.
- The **static-list `CollectSink` works only in-JVM at parallelism 1**, needs synchronization, and must be **cleared in `@BeforeEach`**. Never let it into production code.
- `executeAndCollect()` avoids static state but needs a **bounded** source, and it calls `execute()` itself.
- Testcontainers: use **`setBounded(...)`** so the job terminates, **`getBootstrapServers()`** for the random port, a **random groupId**, and a **deadline** on every poll.
- Many unit tests, some integration tests, a handful of E2E tests.

**Interview one-liners**

- *"How do you unit-test a Flink function?"* → With an operator test harness — `KeyedOneInputStreamOperatorTestHarness` for keyed functions — feeding records with explicit timestamps and calling `processWatermark` to advance event time and fire timers deterministically.
- *"How do you test a windowed job without waiting for the window?"* → You never wait. `processWatermark(t)` advances event time synchronously, so a seven-day window fires in a microsecond.
- *"How do you test that state survives a restart?"* → `harness.snapshot()`, build a fresh harness, `setup()` then `initializeState(snapshot)` then `open()`, and assert the restored state affects the next record.
- *"What's wrong with a static list as a test sink?"* → It only works because the MiniCluster shares the test JVM and the job runs at parallelism 1. On a real cluster each TaskManager has its own JVM and its own copy, so it collects nothing.
- *"Why does my Testcontainers Flink test hang?"* → The Kafka source is unbounded by default, so `execute()` never returns. `setBounded(OffsetsInitializer.latest())` makes it stop at the end of the topic.
- *"How do you make a Flink job testable at all?"* → Extract the pipeline into a static method taking a `DataStream` and a `Sink` so tests can substitute in-memory endpoints while running the identical graph, uids included.
