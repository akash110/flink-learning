# 56. Logging and Metrics

A job you cannot observe is a job you cannot operate. Logs tell you **what happened once**; metrics tell you **what is happening continuously**. You need both, and you need to know which questions each one answers.

> **Key idea**
> **Logs are for events. Metrics are for rates and levels.**
> "The Kafka connection failed at 03:14" is a log line. "We are 4 million records behind" is a metric.
> The failure mode of confusing them is logging per record — which turns your job into a log-shipping job and destroys throughput.

---

## Part 1: Logging

### The stack

Flink uses **SLF4J** as the API and **Log4j 2** as the default implementation.

```
   your code
      │  LoggerFactory.getLogger(...)
      ▼
 ┌──────────┐   SLF4J = an interface only. No output logic.
 │  SLF4J   │   Lets you swap backends without touching code.
 └────┬─────┘
      │  log4j-slf4j-impl  (the bridge jar)
      ▼
 ┌──────────┐   Log4j 2 = the real implementation:
 │  Log4j2  │   levels, appenders, rolling files, formatting.
 └────┬─────┘
      │  configured by conf/log4j*.properties
      ▼
   log files on disk + the Web UI log tab
```

You always code against SLF4J. Log4j2 is a runtime detail you configure in a file.

### The standard pattern

```java
package com.akash.flink.functions;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.akash.flink.model.Event;

public class FraudDetector extends KeyedProcessFunction<String, Event, Alert> {

    // THE line. Memorise its shape.
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetector.class);

    @Override
    public void processElement(Event e, Context ctx, Collector<Alert> out) { }
}
```

Why every word is there:

- **`private`** — nobody outside needs it.
- **`static`** — one logger per *class*, not per object. Critically, **`Logger` is not `Serializable`**, and Flink Java-serializes your function object to ship it. Static fields are excluded from serialization, so `static` is what stops `NotSerializableException`. A non-static logger field is a real, common bug.
- **`final`** — assigned once, never reassigned.
- **`getLogger(FraudDetector.class)`** — the class name becomes the logger name, which is what appears in the log line and what you target in the config file (`logger.fraud.name = com.akash.flink.functions.FraudDetector`).

### Levels, and when to use each

| Level | Use for | In production? |
|---|---|---|
| `ERROR` | something broke and needs a human | always on |
| `WARN` | recoverable oddity — retry, malformed record, fallback taken | always on |
| `INFO` | lifecycle: job started, subtask opened, config loaded, checkpoint N done | always on |
| `DEBUG` | per-batch detail during investigation | temporarily, on one logger |
| `TRACE` | per-record detail | essentially never |

### The rule

> **Never log per record at INFO in a high-throughput operator.**

```java
// ❌ at 100k records/sec this writes ~100k lines/sec.
//    Log4j synchronises on the appender, so every record now waits on a lock
//    and a disk write. You have converted a stream processor into a log writer.
//    Symptom: an operator pinned at 100% Busy with trivially cheap logic.
@Override
public void processElement(Event e, Context ctx, Collector<Alert> out) {
    LOG.info("Processing event {}", e);
    ...
}
```

Four correct alternatives:

```java
// 1. Log lifecycle, not records. open() runs ONCE per subtask.
@Override
public void open(OpenContext ctx) {
    LOG.info("FraudDetector subtask {}/{} started, threshold={}",
             getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
             getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks(),
             threshold);
}

// 2. Log exceptional records only — those are rare by definition.
if (amount > 1_000_000) {
    LOG.warn("Suspiciously large amount {} for user {}", amount, e.getUserId());
}

// 3. Sample. Log 1 in 10,000.
private long seen = 0;
if (++seen % 10_000 == 0) {
    LOG.info("Processed {} events, last userId={}", seen, e.getUserId());
}

// 4. Guard expensive DEBUG. isDebugEnabled() skips the argument evaluation.
if (LOG.isDebugEnabled()) {
    LOG.debug("Full state dump: {}", expensiveToStringOfState());
}
```

On the `{}` placeholder: `LOG.info("value {}", x)` only builds the string if the level is enabled. `LOG.info("value " + x)` concatenates **before** the call, every time, even when the level is off. Always use `{}`.

For exceptions, pass the throwable as the **last argument with no matching placeholder** — SLF4J prints the full stack trace:

```java
try {
    parse(raw);
} catch (Exception ex) {
    // 2 placeholders, 3 args. The trailing Throwable is special-cased.
    LOG.error("Failed to parse record from partition {} offset {}", p, o, ex);
}
```

### `log4j2.properties` for the cluster

Lives at `$FLINK_HOME/conf/log4j.properties` — on the cluster this file is the *distribution's*, not yours; you edit it via a ConfigMap or your image. The default Flink ships is a good starting point:

```properties
# conf/log4j.properties (Log4j 2 properties format)

# Root logger: everything not otherwise configured.
rootLogger.level = INFO
rootLogger.appenderRef.rolling.ref = RollingFileAppender

# ---- The rolling file appender ----
appender.rolling.name = RollingFileAppender
appender.rolling.type = RollingFile
# ${sys:log.file} is set by Flink's start scripts to the correct per-process path.
appender.rolling.fileName = ${sys:log.file}
appender.rolling.filePattern = ${sys:log.file}.%i
appender.rolling.layout.type = PatternLayout
# %d date, %-5p level, %c logger, %x NDC, %m message, %n newline
appender.rolling.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
appender.rolling.policies.type = Policies
appender.rolling.policies.size.type = SizeBasedTriggeringPolicy
appender.rolling.policies.size.size = 100MB
appender.rolling.strategy.type = DefaultRolloverStrategy
appender.rolling.strategy.max = 10          # keep 10 rolled files, then delete

# ---- Per-logger overrides ----
# Silence noisy libraries.
logger.akka.name = org.apache.pekko
logger.akka.level = WARN
logger.kafka.name = org.apache.kafka
logger.kafka.level = WARN
logger.hadoop.name = org.apache.hadoop
logger.hadoop.level = WARN
logger.zookeeper.name = org.apache.zookeeper
logger.zookeeper.level = WARN

# Turn YOUR code up without turning everything else up.
logger.myjob.name = com.akash.flink
logger.myjob.level = INFO

# Checkpoint coordinator at INFO logs one line per checkpoint — genuinely useful.
logger.checkpoint.name = org.apache.flink.runtime.checkpoint.CheckpointCoordinator
logger.checkpoint.level = INFO

# Suppress the "channel became inactive" spam on normal shutdown.
logger.netty.name = org.apache.flink.shaded.netty4.io.netty.channel.nio.AbstractNioSelector
logger.netty.level = OFF
```

> **Key idea**
> `rootLogger.level = DEBUG` in production is a self-inflicted outage. Flink's internals log enormously at DEBUG. Raise **one specific logger**, never the root.

### `log4j2-test.properties` for tests

Put this at `src/test/resources/log4j2-test.properties`. Log4j2 prefers `*-test.properties` when it is on the test classpath, so your tests get their own config automatically.

```properties
# src/test/resources/log4j2-test.properties
rootLogger.level = OFF                      # keeps test output readable
rootLogger.appenderRef.test.ref = TestLogger

appender.test.name = TestLogger
appender.test.type = CONSOLE                # console, not a file
appender.test.layout.type = PatternLayout
appender.test.layout.pattern = %-4r [%t] %-5p %c - %m%n

# Turn your own code back on so you can see what your job is doing.
logger.myjob.name = com.akash.flink
logger.myjob.level = INFO
```

`rootLogger.level = OFF` matters: a MiniCluster at INFO produces hundreds of lines per test, and your assertion failure scrolls away.

### Where logs actually go

```
                       ┌──────────────────────────────────────────┐
                       │  $FLINK_HOME/log/                        │
   JobManager  ───────►│    flink-<user>-standalonesession-*.log  │
                       │    ...-*.out    ← stdout, i.e. print()   │
                       ├──────────────────────────────────────────┤
   TaskManager 1 ─────►│    flink-<user>-taskexecutor-0-*.log     │
   TaskManager 2 ─────►│    flink-<user>-taskexecutor-1-*.log     │
                       └──────────────────────────────────────────┘
```

Facts that save you an hour each:

1. **Your function's log lines are in the TaskManager log, not the JobManager log.** Job submission errors and checkpoint coordination are in the JobManager log. Look in the right one.
2. **Which TaskManager?** With parallelism 8 across 4 TMs, a log line from subtask 5 is in exactly one file. The Web UI is the fast way: **Job → the operator → Subtasks tab → note the TaskManager → TaskManagers → that TM → Log tab**.
3. **`System.out.println` goes to `.out`, not `.log`** — a separate file that is not rolled, not timestamped, and not searchable. Use the logger.
4. **On Kubernetes**, `kubectl logs <taskmanager-pod>` gives the console appender; the file appender writes inside the container and is lost when the pod dies. Ship to a log aggregator (Loki, ELK, CloudWatch) or you lose the logs of exactly the crash you care about.

In the **Web UI**: `TaskManagers → <tm> → Logs` (list of files), `Log` (the main log), `Stdout`, and `Thread Dump`. Also `Job → Exceptions` for the failure history.

---

## Part 2: Metrics

### The metric group

Every Rich function reaches metrics through the RuntimeContext:

```java
MetricGroup group = getRuntimeContext().getMetricGroup();
```

A `MetricGroup` is a namespace. `addGroup("fraud")` gives you a child scope, so the final metric name is a path:

```
<host>.taskmanager.<tm_id>.<job_name>.<operator_name>.<subtask_index>.fraud.alerts
└──────────────── system-supplied scope ─────────────────────────────┘└─ yours ─┘
```

The system prefix is automatic — you never have to encode the subtask index into a metric name yourself, and you must not (it would collide with Flink's own dimensioning).

### The four metric types

Register them in `open()`, update them in the processing methods. **Never register a metric per record** — that leaks.

```java
package com.akash.flink.functions;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import com.codahale.metrics.SlidingWindowReservoir;
import com.akash.flink.model.Event;

public class InstrumentedDetector extends KeyedProcessFunction<String, Event, Alert> {

    // transient: created on the TaskManager in open(), never serialized.
    private transient Counter alertsEmitted;
    private transient Counter recordsDropped;
    private transient Meter recordsPerSecond;
    private transient Histogram processingLatency;

    private transient long inFlightKeys;   // plain field the Gauge reads

    @Override
    public void open(OpenContext ctx) {
        MetricGroup g = getRuntimeContext().getMetricGroup().addGroup("fraud");

        // ---- 1. COUNTER: a number that only goes up. ----
        // For "how many X have happened". Your dashboard takes rate() of it.
        alertsEmitted  = g.counter("alertsEmitted");
        recordsDropped = g.counter("recordsDropped");

        // ---- 2. GAUGE: reports a value on demand. ----
        // For a LEVEL: queue depth, cache size, number of keys.
        // The lambda is called by the reporter, so keep it CHEAP -
        // never do I/O or iterate large state inside a gauge.
        g.gauge("inFlightKeys", (Gauge<Long>) () -> inFlightKeys);

        // ---- 3. METER: measures a rate (events per second), with EWMA. ----
        // markEvent() per occurrence; the meter derives the rate.
        recordsPerSecond = g.meter("recordsPerSecond", new MeterView(60));
        // MeterView(60) = rate averaged over a 60-second window.

        // ---- 4. HISTOGRAM: distribution - mean, min, max, p50/p75/p95/p99. ----
        // For LATENCY and SIZE. Flink has no built-in implementation;
        // wrap a Dropwizard one (needs flink-metrics-dropwizard).
        processingLatency = g.histogram("processingLatencyMs",
                new DropwizardHistogramWrapper(
                        new com.codahale.metrics.Histogram(
                                new SlidingWindowReservoir(500))));   // last 500 samples
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<Alert> out) {
        long start = System.nanoTime();

        recordsPerSecond.markEvent();          // Meter: one occurrence

        if (e.getAmount() == null) {
            recordsDropped.inc();              // Counter: +1
            return;
        }
        if (isFraud(e)) {
            out.collect(new Alert(e.getUserId(), e.getAmount()));
            alertsEmitted.inc();
        }

        // Histogram: record the observation, in ms.
        processingLatency.update((System.nanoTime() - start) / 1_000_000);
    }

    private boolean isFraud(Event e) { return e.getAmount() > 10_000; }
}
```

Maven coordinate for the histogram wrapper:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-metrics-dropwizard</artifactId>
    <version>${flink.version}</version>
</dependency>
```

| Type | Method | Answers | Cost |
|---|---|---|---|
| `Counter` | `inc()` / `dec()` / `inc(n)` | "how many since start?" | ~free |
| `Gauge<T>` | you supply a lambda | "what is it right now?" | as expensive as your lambda |
| `Meter` | `markEvent()` | "how many per second?" | ~free |
| `Histogram` | `update(long)` | "what does the distribution look like?" | real — samples + sorting |

`Histogram` is the expensive one. One per operator is fine; don't add five.

**Cardinality warning.** Never do `g.addGroup(userId).counter("events")`. That creates one metric series per user, and a million-user job takes down Prometheus. Metrics are for *low-cardinality dimensions* (operator, subtask, event type). High-cardinality investigation belongs in logs or a sampled side output — the point made in [`../../03-state-and-skew.md`].

### The built-in metrics that matter

You do not have to add these. Flink emits them; you must know what they mean.

**Throughput**
| Metric | Meaning |
|---|---|
| `numRecordsInPerSecond` | records entering this operator per second |
| `numRecordsOutPerSecond` | records leaving it |
| `numRecordsIn` / `numRecordsOut` | cumulative totals |

A large gap between in and out is a filter (expected) or a bug (unexpected). A **zero** `numRecordsOut` on a windowed operator usually means the watermark is not advancing.

**Kafka consumer lag — the single most important metric**

| Metric | Meaning |
|---|---|
| `records-lag-max` | largest lag, in records, across the partitions this subtask owns |

This is a Kafka-client metric that Flink exposes through the source operator's metric group. It answers the only question that matters at 3am: **are we falling behind?** A flat value means you are keeping up. A rising slope means the drain rate is negative and you will never catch up on your own — see the drain-rate arithmetic in [`../../06-scale-arithmetic.md`]. If it is the only metric you alert on, you have covered most incidents.

Because it is per-subtask, alert on `max` across subtasks, not the average — one lagging subtask (a hot key) averages away.

**Event-time health**

| Metric | Meaning |
|---|---|
| `currentEmitEventTimeLag` | now − timestamp of the last record **emitted** by the source. Total pipeline event-time lag. |
| `currentFetchEventTimeLag` | now − timestamp of the last record **fetched** from the external system. Excludes Flink-internal buffering. |
| `watermarkLag` | now − current watermark |
| `currentInputWatermark` | the operator's current input watermark, as a timestamp |

Reading the pair: `currentFetchEventTimeLag` high but `currentEmitEventTimeLag` similar → the data in Kafka is genuinely old (a producer is behind). Fetch lag low but emit lag high → Flink is buffering, i.e. **backpressure**.

**Backpressure — the three that add to 100%**

| Metric | Meaning |
|---|---|
| `busyTimeMsPerSecond` | ms per second the operator spent doing work |
| `backPressuredTimeMsPerSecond` | ms per second it spent blocked waiting for a downstream buffer |
| `idleTimeMsPerSecond` | ms per second it spent waiting for input |

They sum to ~1000. The diagnostic: **busy ≈ 1000 and backpressured ≈ 0 identifies the bottleneck operator**; everything upstream shows high backpressure and is a victim. Full method in [`../../02-backpressure.md`].

**Checkpointing**

| Metric | Meaning |
|---|---|
| `lastCheckpointDuration` | ms the last checkpoint took end to end |
| `lastCheckpointSize` | bytes written by the last checkpoint (the delta, if incremental) |
| `lastCheckpointFullSize` | total logical state size — this is what drives restore time |
| `numberOfCompletedCheckpoints` | cumulative successes |
| `numberOfFailedCheckpoints` | cumulative failures. **Any sustained increase is an incident.** |
| `numberOfInProgressCheckpoints` | 0 or 1 normally; a stuck 1 means a checkpoint is hanging |

`lastCheckpointSize` growing steadily day over day with stable traffic = **state leak, no TTL**. That is the single most valuable trend chart to keep on a dashboard.

**Job level**

| Metric | Meaning |
|---|---|
| `numRestarts` | how many times the job has restarted. A crash loop is visible here first. |
| `uptime` / `downtime` | ms since the job started / since it went down |
| `fullRestarts` | restarts that reset the whole job (as opposed to region failover) |

**JVM (per TaskManager)**

```
Status.JVM.Memory.Heap.Used / .Max
Status.JVM.Memory.NonHeap.Used         ← metaspace + code cache
Status.JVM.Memory.Direct.MemoryUsed    ← network buffers live here
Status.JVM.GarbageCollector.<name>.Count / .Time
Status.JVM.CPU.Load
Status.JVM.Threads.Count
```

GC time climbing precedes almost every heap-pressure incident, and long GC pauses are visually indistinguishable from backpressure in the UI. Chart `GarbageCollector.*.Time` next to `busyTimeMsPerSecond`.

---

## Part 3: Reporters

A reporter is what pushes/exposes metrics out of the TaskManager. Configured in `config.yaml` (`flink-conf.yaml` pre-1.19). The name segment (`prom`, `jmx`, `slf4j` below) is arbitrary — it just groups keys for one reporter instance.

### Prometheus (the production default)

```yaml
# config.yaml
metrics.reporters: prom

# Pull-based: Flink opens an HTTP endpoint, Prometheus scrapes it.
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
# A RANGE, because several TaskManagers can share a host - each takes a free port.
metrics.reporter.prom.port: 9249-9259

# Optional: how often Flink refreshes the values it exposes.
metrics.reporter.prom.scope.variables.excludes: task_attempt_id;task_attempt_num
```

Requires `flink-metrics-prometheus-<version>.jar` in `$FLINK_HOME/lib/` (it ships in the distribution's `opt/` folder — copy it to `lib/`).

Verify by hand:

```bash
curl -s localhost:9249/metrics | grep flink_taskmanager_job_task_operator_numRecordsInPerSecond
```

Metric names are mangled: dots become underscores and the scope becomes Prometheus labels (`job_name`, `task_name`, `subtask_index`, `operator_name`, `host`). So `records-lag-max` becomes:

```
flink_taskmanager_job_task_operator_records_lag_max{job_name="fraud-detection",subtask_index="3",...}
```

**PushGateway variant** — for short-lived jobs that die before a scrape:

```yaml
metrics.reporter.promgateway.factory.class: org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterFactory
metrics.reporter.promgateway.hostUrl: http://pushgateway:9091
metrics.reporter.promgateway.jobName: flink-fraud-detection
metrics.reporter.promgateway.randomJobNameSuffix: true
metrics.reporter.promgateway.deleteOnShutdown: true
metrics.reporter.promgateway.interval: 30 SECONDS
```

For a long-running streaming job, prefer the plain pull-based reporter.

### JMX (zero setup, good for a quick local look)

```yaml
metrics.reporters: jmx
metrics.reporter.jmx.factory.class: org.apache.flink.metrics.jmx.JMXReporterFactory
metrics.reporter.jmx.port: 8789-8799
```

Then attach VisualVM or JConsole and browse the `org.apache.flink.*` MBeans. No external system needed — useful when you are on a laptop or in a locked-down environment.

### SLF4J (metrics into the log file)

```yaml
metrics.reporters: slf4j
metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory
metrics.reporter.slf4j.interval: 60 SECONDS
```

Dumps every metric into the TaskManager log every 60 seconds. **Verbose** — a real job produces thousands of lines. Use it to confirm a custom metric is registered at all, then turn it off.

### Multiple reporters

```yaml
metrics.reporters: prom, slf4j
```

Comma-separated. Each still needs its own `metrics.reporter.<name>.*` block.

### Latency tracking — know it exists, keep it off

```java
env.getConfig().setLatencyTrackingInterval(0);   // 0 = disabled. This is the default.
```

Setting it above zero makes sources inject special *latency marker* records that measure end-to-end latency through the graph. It is genuinely expensive (markers multiply across the graph and the metric cardinality is large) and it measures *marker* latency, not your record latency, because markers skip your processing logic. Use it for a bounded experiment, never permanently.

---

## Remember

- **SLF4J is the API, Log4j2 is the implementation.** Configure Log4j2 in `conf/log4j.properties`; tests use `src/test/resources/log4j2-test.properties`.
- `private static final Logger LOG = LoggerFactory.getLogger(X.class);` — **`static` is mandatory**, because `Logger` is not serializable and Flink ships your function object.
- **Never log per record at INFO.** Log lifecycle in `open()`, log exceptions, sample, or guard with `isDebugEnabled()`.
- Use `{}` placeholders, never string concatenation. Pass a `Throwable` as the last argument for a stack trace.
- **Never set `rootLogger.level = DEBUG` in production.** Raise one named logger.
- **Function logs are in TaskManager logs, not JobManager logs.** `println` goes to `.out`, not `.log`.
- Four metric types: **Counter** (cumulative), **Gauge** (current level), **Meter** (rate), **Histogram** (distribution, needs the dropwizard wrapper).
- Register metrics in **`open()`**, in a `transient` field. Keep gauge lambdas cheap. Never create a metric per key.
- **`records-lag-max` is the most important metric you have.** Alert on max across subtasks.
- `busyTimeMsPerSecond` / `backPressuredTimeMsPerSecond` / `idleTimeMsPerSecond` sum to 1000 and locate the bottleneck.
- `lastCheckpointSize` trending up with flat traffic = state leak, missing TTL.
- Prometheus reporter on `9249-9259` is the production default; JMX for a quick local look; SLF4J only to verify registration.

**Interview one-liners**

- *"Why must the Logger be static?"* → It is not serializable and Flink Java-serializes function objects to ship them to TaskManagers; static fields are excluded from the serialized form.
- *"Why is per-record logging so bad?"* → The appender is synchronised and does I/O, so every record blocks on a lock and a disk write; the operator shows 100% busy with trivial logic.
- *"Which metric tells you a streaming job is unhealthy?"* → `records-lag-max`. A rising slope means the drain rate is negative; a flat line means you are keeping up regardless of absolute value.
- *"How do you tell which operator is the bottleneck from metrics alone?"* → The first operator downstream with `busyTimeMsPerSecond` near 1000 and `backPressuredTimeMsPerSecond` near 0. Everything upstream of it is backpressured and is a symptom.
- *"Counter vs Gauge vs Meter vs Histogram?"* → Cumulative count; current level via a lambda; rate per second via EWMA; distribution with percentiles.
- *"How would you notice a state leak?"* → `lastCheckpointSize` and `lastCheckpointFullSize` growing steadily while input rate is flat — an unbounded keyspace with no TTL.
