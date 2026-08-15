# Phase 8 — Production architecture

The final phase: everything about *running* a Flink job well — structure, packaging, config, observability, deployment, tuning, and testing. Less new API, more engineering judgment.

---

## 1. Flink project structure

A clean layout for a real job:

```
my-flink-job/
├── pom.xml
├── src/main/java/com/company/job/
│   ├── Job.java                 # main(): wires source → pipeline → sink, calls execute()
│   ├── model/                   # POJOs (Event, Transaction, Alert)
│   ├── functions/               # KeyedProcessFunctions, AggregateFunctions, etc.
│   ├── source/                  # source builders (Kafka config)
│   ├── sink/                    # sink builders
│   └── config/                  # typed config loading
├── src/main/resources/
│   ├── log4j2.properties
│   └── application.conf         # job params
└── src/test/java/               # unit + integration tests (§10)
```

**Principles:** keep `main()` thin (just wiring); make each function independently unit-testable; no business logic in `main()`.

---

## 2. Maven — building a deployable "fat jar"

Flink jobs are submitted as a single jar. Use the **Shade** plugin, and mark Flink core deps `provided` (they're already on the cluster).

```xml
<dependencies>
  <dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>          <!-- provided by the cluster; keeps the jar small -->
  </dependency>
  <!-- connectors & your libs stay at default (compile) scope so they're bundled -->
</dependencies>

<build><plugins>
  <plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.5.1</version>
    <executions><execution>
      <phase>package</phase>
      <goals><goal>shade</goal></goals>
      <configuration>
        <transformers>
          <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
            <mainClass>com.company.job.Job</mainClass>
          </transformer>
          <!-- merges service files so connectors register correctly -->
          <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
        </transformers>
      </configuration>
    </execution></executions>
  </plugin>
</plugins></build>
```

> ⚠️ **Your current `pom.xml` uses `compile` scope with no shade plugin** — that's correct for running in the IDE (which is what you're doing while learning). Switch to `provided` + shade only when you package for a real cluster. Keep your learning pom as-is.

`mvn clean package` → `target/my-flink-job.jar`.

---

## 3. Configuration

Don't hardcode. Pass params in and read them:

```java
// Flink's ParameterTool reads --key value args or a properties file
ParameterTool params = ParameterTool.fromArgs(args);
String brokers = params.get("kafka.brokers", "localhost:9092");
int parallelism = params.getInt("parallelism", 4);

env.getConfig().setGlobalJobParameters(params);   // makes params visible in the Web UI + functions
```

Cluster-level settings live in `flink-conf.yaml` (memory, state backend, checkpoint dir, etc.). Job-level settings go in code or job args. Keep secrets out of args — use env vars / secret managers.

---

## 4. Logging

Flink 1.18 uses **Log4j2**. Put `log4j2.properties` in `src/main/resources`:

```properties
rootLogger.level = INFO
rootLogger.appenderRef.console.ref = ConsoleAppender

appender.console.name = ConsoleAppender
appender.console.type = CONSOLE
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{HH:mm:ss} %-5p %c{1} - %m%n

# quiet noisy libs
logger.kafka.name = org.apache.kafka
logger.kafka.level = WARN
```

Use SLF4J in functions:
```java
private static final Logger LOG = LoggerFactory.getLogger(FraudDetector.class);
LOG.warn("fraud alert user={}", user);
```
Avoid logging per-record at INFO in high-throughput paths — it becomes the bottleneck.

---

## 5. Metrics

Flink exposes a rich metric system. Register custom metrics in `open()`:

```java
@Override public void open(Configuration c) {
    getRuntimeContext().getMetricGroup().counter("fraud_alerts");
    getRuntimeContext().getMetricGroup().gauge("state_size", () -> currentSize);
}
// counter.inc() when an alert fires
```

**Built-in metrics you'll watch:** `numRecordsInPerSecond`, `numRecordsOutPerSecond`, `busyTimeMsPerSecond`, `backPressuredTimeMsPerSecond`, `currentInputWatermark`, checkpoint duration/size, `numRestarts`.

**Export** to Prometheus/Graphite/JMX via a reporter in `flink-conf.yaml`:
```yaml
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249
```

---

## 6. Monitoring & the Flink Web UI

The Web UI (JobManager, default **http://localhost:8081**) is your primary window:

- **Job graph** — the chained tasks (Phase 4 §6) and per-operator parallelism.
- **Backpressure tab** — OK / LOW / HIGH per operator → find the bottleneck (Phase 4 §9).
- **Checkpoints tab** — duration, size, alignment time, failures (Phase 5).
- **Watermarks** — current watermark per operator (spot stalled/idle sources).
- **Metrics** — add charts for throughput, latency, state size.
- **Exceptions / logs** — recent failures & restart history.

To get a local UI while developing from the IDE:
```java
Configuration conf = new Configuration();
StreamExecutionEnvironment env =
    StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);  // needs flink-runtime-web dep
```

---

## 7. Deployment models

| Mode | What it is | Use |
|------|-----------|-----|
| **Standalone** | manually-started JM+TMs | dev, small setups |
| **Session cluster** | long-running cluster, many jobs share it | multi-job, quick submits |
| **Application mode** | one cluster per job, `main()` runs on the JM | **production default** — best isolation |
| **Per-job (deprecated)** | one cluster per job (older) | avoid |
| **Kubernetes** | native K8s integration or the **Flink Kubernetes Operator** | cloud-native production |
| **YARN** | Hadoop clusters | on-prem Hadoop shops |

```bash
# application mode on a standalone cluster
flink run-application -t remote -Djobmanager.rpc.address=jm-host target/my-flink-job.jar \
     --kafka.brokers broker:9092 --parallelism 8
```
On Kubernetes, prefer the **Flink Kubernetes Operator** — it manages deployments, savepoint-based upgrades, and autoscaling declaratively via CRDs.

---

## 8. Parallelism tuning

- **Start point:** operator parallelism ≈ available slots; source parallelism ≤ Kafka partitions (Phase 4 §8).
- **Max parallelism** (`setMaxParallelism`) fixes the number of **key groups** — it caps how far you can *ever* rescale, and it's fixed at first checkpoint. Set it deliberately (e.g., 128) — you can't change it later without a state migration.
- Raise the parallelism of the **backpressured** operator, not everything.
- Watch for **data skew** (one hot key saturating one subtask) — no amount of parallelism fixes a single hot key; you may need to pre-aggregate or add a salt to the key.

---

## 9. Memory, checkpoint & backpressure tuning

### Memory
Flink splits TaskManager memory into: JVM heap, **managed memory** (RocksDB / batch), network buffers, and off-heap. Key knobs in `flink-conf.yaml`:
```yaml
taskmanager.memory.process.size: 4096m
taskmanager.memory.managed.fraction: 0.4      # more for RocksDB-heavy state
taskmanager.numberOfTaskSlots: 4
```
- **RocksDB** state → give **managed memory**; heap can stay modest.
- OOM in a TM is usually too-small managed memory (RocksDB) or huge windowed buffers.

### Checkpoint tuning
- Increase interval if checkpoints are frequent & expensive; decrease for faster recovery (trade-off).
- **Incremental checkpoints** (RocksDB) for large state (Phase 5).
- **Unaligned checkpoints** if alignment stalls under backpressure.
- Set `minPauseBetweenCheckpoints` so back-to-back checkpoints don't starve processing.
- Watch checkpoint **size growth** — usually a symptom of unbounded state (add TTL, Phase 3).

### Backpressure (operational recap of Phase 4 §9)
Diagnose in the UI → identify the busy/backpressured operator → raise its parallelism, fix skew (`rebalance`), or make a slow sink async (`AsyncDataStream.unorderedWait(...)`).

---

## 10. Testing Flink jobs

### Unit-testing functions in isolation — use test harnesses
`flink-test-utils` + `*OperatorTestHarness` let you feed records to a `KeyedProcessFunction`, advance watermarks/timers, and assert output & state — no cluster.

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-test-utils</artifactId>
  <version>${flink.version}</version>
  <scope>test</scope>
</dependency>
```

```java
KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
    ProcessFunctionTestHarnesses.forKeyedProcessFunction(
        new FraudDetector(), Event::getUser, Types.STRING);

harness.open();
harness.processElement(new Event("alice", 1,   1000), 1000);
harness.processElement(new Event("alice", 900, 2000), 2000);
harness.processWatermark(3000);
assertThat(harness.extractOutputValues()).contains("FRAUD? user=alice ...");
```

This is the single most valuable testing skill — it lets you test timers/state deterministically.

### Integration testing — MiniCluster
`MiniClusterWithClientResource` spins up a real (in-JVM) Flink cluster to run a whole job end-to-end in a test.

```java
@ClassRule
public static MiniClusterWithClientResource flink =
    new MiniClusterWithClientResource(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build());
```
Collect results with a test sink (e.g., a `CollectSink` writing to a static list) and assert. For Kafka-based jobs, use **Testcontainers** to run a throwaway Kafka.

### What to test
- Each function's logic + timer behavior (harness).
- Watermark/late-data handling.
- The full job wiring (MiniCluster).
- Serialization of your POJOs (a POJO that silently falls back to Kryo is a perf bug — assert with `TypeInformation`).

---

## 11. Production readiness checklist (pin this)

- [ ] `.uid()` on **every** stateful operator (Phase 5 — enables savepoint upgrades)
- [ ] Checkpointing enabled + durable checkpoint storage (S3/HDFS)
- [ ] RocksDB state backend if state is large; incremental checkpoints on
- [ ] Restart strategy configured (not the noRestart default)
- [ ] `maxParallelism` set deliberately
- [ ] State TTL on any unbounded keyspace
- [ ] Watermark idleness for Kafka partitions
- [ ] Metrics exported (Prometheus) + dashboards + alerts on backpressure/restarts/checkpoint failures
- [ ] Structured logging, no per-record INFO logs on hot paths
- [ ] Fat jar with Flink deps `provided`
- [ ] Unit tests (harness) + integration test (MiniCluster)
- [ ] Deployed in **application mode** (or via the K8s Operator)
- [ ] Savepoint-based deploy/upgrade runbook documented

---

### ✅ Phase 8 checklist

- [ ] Project structure & thin `main()`
- [ ] Maven fat jar (`provided` + shade)
- [ ] Config via `ParameterTool` / `flink-conf.yaml`
- [ ] Log4j2 logging
- [ ] Custom + built-in metrics, reporters
- [ ] Web UI: graph, backpressure, checkpoints, watermarks
- [ ] Deployment models (application mode / K8s operator)
- [ ] Parallelism, memory, checkpoint tuning
- [ ] Testing: operator harness + MiniCluster

⬅️ [Phase 7](07-sql-table-api.md)  ·  🎉 You've mapped the whole path. Back to the [index](00-index.md).
