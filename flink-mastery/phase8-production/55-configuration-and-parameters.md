# 55. Configuration and Parameters

Every job you have written so far hardcodes something: a Kafka broker, a topic name, a threshold. That is fine in the IDE and unacceptable in production, because the same jar must run in dev, staging and prod without recompiling.

> **Key idea**
> There are **two** completely separate config systems, and confusing them wastes hours.
> **Job parameters** — your topic, your threshold. You read them in `main()` and pass them into your functions.
> **Cluster configuration** — `config.yaml`, memory sizes, state backend. Owned by whoever runs the cluster.
> `ParameterTool` does the first. `Configuration` does the second.

---

## Part 1: Job parameters with `ParameterTool`

`ParameterTool` (`org.apache.flink.api.java.utils.ParameterTool`) is Flink's small, serializable key-value holder. Three ways to fill it.

### From command-line arguments

```java
package com.akash.flink.jobs;

import org.apache.flink.api.java.utils.ParameterTool;

public class ConfiguredJob {
    public static void main(String[] args) throws Exception {

        // args is the String[] the JVM hands to main().
        // Running: flink run job.jar --topic events --threshold 500
        //   → args = {"--topic", "events", "--threshold", "500"}
        ParameterTool params = ParameterTool.fromArgs(args);

        // get(key) → String, or null if absent.
        String topic = params.get("topic");

        // get(key, default) → never null. Prefer this.
        String brokers = params.get("brokers", "localhost:9092");

        // Typed getters. They parse and throw a clear error on garbage input.
        int threshold  = params.getInt("threshold", 100);
        long windowMs  = params.getLong("window-ms", 60_000L);
        double rate    = params.getDouble("sample-rate", 1.0);
        boolean debug  = params.getBoolean("debug", false);

        // required(): throws immediately if missing. Use for things with no sane default.
        String checkpointDir = params.getRequired("checkpoint-dir");
    }
}
```

Accepted syntaxes — all equivalent:

```bash
--topic events        # the conventional form
-topic events         # single dash also works
--topic=events        # equals form
--debug               # a flag with no value → getBoolean("debug") is true
```

**Java note:** `60_000L` — underscores are digit separators the compiler ignores (readability only), and `L` makes it a `long` rather than an `int`.

### From a properties file

```properties
# src/main/resources/application.properties
kafka.brokers=localhost:9092
kafka.topic=events
kafka.group.id=fraud-detector
fraud.threshold=500
window.size.minutes=5
```

```java
// Path on the filesystem where main() runs — that is the CLIENT machine,
// not the TaskManagers. See ch. 3 on where main() runs.
ParameterTool fileParams = ParameterTool.fromPropertiesFile("/etc/flink/app.properties");

// Or from the classpath, i.e. src/main/resources bundled into the jar.
// Class.getResourceAsStream returns an InputStream; the leading "/" means
// "from the classpath root", not "from the filesystem root".
ParameterTool bundled = ParameterTool.fromPropertiesFile(
        ConfiguredJob.class.getResourceAsStream("/application.properties"));
```

`ParameterTool` also has `fromMap(Map<String,String>)` and `fromSystemProperties()`:

```java
// Reads every JVM -D flag: java -Dfraud.threshold=500 -jar ...
ParameterTool sysProps = ParameterTool.fromSystemProperties();
```

### Layering them: the pattern you actually want

`mergeWith` returns a **new** `ParameterTool` where the argument's values win on conflict.

```java
ParameterTool params =
        ParameterTool.fromPropertiesFile(                 // 1. baked-in defaults
                        ConfiguredJob.class.getResourceAsStream("/application.properties"))
                .mergeWith(ParameterTool.fromSystemProperties())  // 2. -D flags override
                .mergeWith(ParameterTool.fromArgs(args));         // 3. CLI wins outright
```

Precedence reads bottom-up: **CLI > system properties > bundled defaults.** That single expression gives you a config system good enough for most production jobs.

---

## Part 2: Getting parameters to the TaskManagers

Here is the trap. `main()` runs on the client. Your functions run on TaskManagers, in a different JVM, possibly on a different machine. A local variable in `main()` does not exist there.

```
CLIENT JVM                      TASKMANAGER JVM
──────────                      ───────────────
main() {
  int threshold = 500;   ──X──► not visible here
  ...
}
```

There are exactly two ways across the boundary.

### Way 1 — constructor injection (do this by default)

```java
package com.akash.flink.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import com.akash.flink.model.Event;

public class ThresholdFilter implements FilterFunction<Event> {

    // `final` because it is assigned once and never changes.
    // A primitive int is Serializable by definition, so it ships fine.
    private final int threshold;

    public ThresholdFilter(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public boolean filter(Event e) {
        return e.getAmount() > threshold;
    }
}
```

```java
// In main(): the value is read on the client and captured in the object.
// Flink serializes the whole ThresholdFilter object into the JobGraph
// and ships it to every TaskManager. threshold travels inside it.
stream.filter(new ThresholdFilter(params.getInt("threshold", 100)));
```

> **Key idea**
> Any function object you pass to Flink is **Java-serialized and shipped**. Every field must be `Serializable` or marked `transient`. This is why constructor injection just works — and why a `KafkaProducer` field does not (it is not serializable; create it in `open()` instead).

### Way 2 — global job parameters

Constructor injection needs a constructor argument per value. When many functions need many values, register the whole `ParameterTool` once:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// ParameterTool implements ExecutionConfig.GlobalJobParameters.
// This copies its contents into the ExecutionConfig, which is part of the
// JobGraph, so it reaches every TaskManager. It also makes the values show
// up in the Web UI under Job → Configuration.
env.getConfig().setGlobalJobParameters(params);
```

Reading them back requires a **Rich** function — `RichMapFunction`, `RichFilterFunction`, `KeyedProcessFunction`, etc. — because only Rich functions have a `RuntimeContext`.

```java
package com.akash.flink.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.OpenContext;
import com.akash.flink.model.Event;
import java.util.Map;

public class GlobalParamFilter extends RichFilterFunction<Event> {

    // transient: not part of the serialized form. It is assigned in open(),
    // which runs on the TaskManager, so it never needs to travel.
    private transient int threshold;

    // Flink 1.19+ signature. Older code uses open(Configuration parameters);
    // that overload still exists but is deprecated.
    @Override
    public void open(OpenContext openContext) {
        // getGlobalJobParameters() returns Map<String, String> in current Flink.
        Map<String, String> p = getRuntimeContext().getGlobalJobParameters();
        this.threshold = Integer.parseInt(p.getOrDefault("threshold", "100"));
    }

    @Override
    public boolean filter(Event e) {
        return e.getAmount() > threshold;    // no map lookup per record
    }
}
```

Line notes:

- **`open()` runs once per subtask**, before the first record, on the TaskManager. It is where you do setup: parse config, open connections, register metrics, initialise state.
- **`getRuntimeContext().getGlobalJobParameters()`** returns a `Map<String, String>`. On Flink 1.19+ this is the signature; older releases returned `ExecutionConfig.GlobalJobParameters`, which you had to cast:

  ```java
  // Pre-1.19 form, still seen everywhere in blog posts and older repos:
  ParameterTool p = (ParameterTool)
          getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
  ```
  If you are on 1.18 or earlier, use the cast form. On 1.19/1.20, use the `Map` form.
- **Parse in `open()`, not in `filter()`.** Doing the map lookup and `Integer.parseInt` per record is pure waste at a million events/sec.

### Which to use

| | Constructor injection | Global job parameters |
|---|---|---|
| Works in plain (non-Rich) functions | yes | no |
| Type safety | compile-time | you parse strings yourself |
| Testability | trivial — `new ThresholdFilter(5)` | needs a mocked RuntimeContext or a harness |
| Visible in Web UI | no | **yes** |
| Boilerplate for many values | grows | flat |

**Do both.** Constructor-inject for the logic, and `setGlobalJobParameters` anyway so the effective config is visible in the Web UI when you are debugging at 3am.

### A typed config object — the pattern to graduate to

Strings scattered through the code rot. Parse and validate once:

```java
package com.akash.flink.config;

import org.apache.flink.api.java.utils.ParameterTool;
import java.io.Serializable;

// Serializable so it can be constructor-injected into functions.
public class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;   // pins the serialization format

    private final String brokers;
    private final String topic;
    private final String groupId;
    private final int fraudThreshold;
    private final long checkpointIntervalMs;

    private JobConfig(String brokers, String topic, String groupId,
                      int fraudThreshold, long checkpointIntervalMs) {
        this.brokers = brokers;
        this.topic = topic;
        this.groupId = groupId;
        this.fraudThreshold = fraudThreshold;
        this.checkpointIntervalMs = checkpointIntervalMs;
    }

    // Static factory: one place that knows the key names and the defaults.
    public static JobConfig from(ParameterTool p) {
        JobConfig c = new JobConfig(
                p.getRequired("kafka.brokers"),
                p.getRequired("kafka.topic"),
                p.get("kafka.group.id", "flink-default"),
                p.getInt("fraud.threshold", 500),
                p.getLong("checkpoint.interval.ms", 60_000L));
        c.validate();
        return c;
    }

    // FAIL FAST on the client, before the job is submitted.
    private void validate() {
        if (fraudThreshold <= 0) {
            throw new IllegalArgumentException(
                "fraud.threshold must be > 0, got " + fraudThreshold);
        }
        if (checkpointIntervalMs < 1000) {
            throw new IllegalArgumentException(
                "checkpoint.interval.ms below 1000 will thrash the cluster");
        }
    }

    public String getBrokers() { return brokers; }
    public String getTopic() { return topic; }
    public String getGroupId() { return groupId; }
    public int getFraudThreshold() { return fraudThreshold; }
    public long getCheckpointIntervalMs() { return checkpointIntervalMs; }
}
```

The point of `validate()`: a typo in `--threshold 5OO` (letter O) should kill the submission in 200 milliseconds, not surface as wrong numbers three hours later.

---

## Part 3: Cluster configuration

### `config.yaml` (Flink 1.19+) vs `flink-conf.yaml` (1.18 and earlier)

Flink's cluster config file lives at `$FLINK_HOME/conf/`. **In Flink 1.19 it was renamed and its format changed.**

| | ≤ 1.18 | ≥ 1.19 |
|---|---|---|
| Filename | `flink-conf.yaml` | `config.yaml` |
| Format | flat `key: value`, **not** real YAML | **standard YAML**, nesting allowed |
| Status in 1.19/1.20 | still read if present, deprecated | the default |

The old file was YAML-*looking* but parsed line-by-line as `key: value`, so nesting silently broke. The new one is parsed by a real YAML parser.

```yaml
# ≤ 1.18 — conf/flink-conf.yaml (flat, dotted keys)
jobmanager.memory.process.size: 2048m
taskmanager.memory.process.size: 8192m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4
state.backend.type: rocksdb
execution.checkpointing.interval: 60s
```

```yaml
# ≥ 1.19 — conf/config.yaml (real YAML; nesting now works)
jobmanager:
  memory:
    process:
      size: 2048m
taskmanager:
  memory:
    process:
      size: 8192m
  numberOfTaskSlots: 4
parallelism:
  default: 4
state:
  backend:
    type: rocksdb
execution:
  checkpointing:
    interval: 60s
    min-pause: 30s
    externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
```

Flat dotted keys still parse fine in `config.yaml` as long as you quote nothing weird — most teams keep the flat style for readability. Both forms are valid YAML.

### The keys that matter most

```yaml
# ---------- Resources ----------
jobmanager.memory.process.size: 2048m
taskmanager.memory.process.size: 8192m        # total container size (ch. 59)
taskmanager.numberOfTaskSlots: 4              # subtasks per TM
taskmanager.memory.managed.fraction: 0.4      # RocksDB lives here (ch. 59)
parallelism.default: 4                        # used when the job sets none

# ---------- State & checkpointing ----------
state.backend.type: rocksdb                   # or 'hashmap'
                                              # (pre-1.19 key was `state.backend`)
state.backend.incremental: true               # RocksDB only. Turn it on.
execution.checkpointing.interval: 60s
execution.checkpointing.min-pause: 30s        # gap BETWEEN checkpoints
execution.checkpointing.timeout: 10min
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.mode: EXACTLY_ONCE    # or AT_LEAST_ONCE
execution.checkpointing.unaligned.enabled: false
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

# Storage locations. s3://, hdfs://, or file:// for local testing.
execution.checkpointing.dir: s3://my-bucket/flink/checkpoints
execution.checkpointing.savepoint-dir: s3://my-bucket/flink/savepoints
# ≤1.18 names: state.checkpoints.dir / state.savepoints.dir  (still accepted)

# ---------- Restart & failover ----------
restart-strategy.type: exponential-delay
restart-strategy.exponential-delay.initial-backoff: 10s
restart-strategy.exponential-delay.max-backoff: 5min
restart-strategy.exponential-delay.backoff-multiplier: 2.0
restart-strategy.exponential-delay.reset-backoff-threshold: 10min
restart-strategy.exponential-delay.jitter-factor: 0.1

# ---------- Networking / RPC ----------
rest.port: 8081
taskmanager.network.memory.fraction: 0.1

# ---------- Observability (ch. 56) ----------
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249
```

### Setting cluster config from code

Sometimes you need a setting the cluster does not have, or you want the job to be self-describing. `Configuration` + `getExecutionEnvironment(conf)`:

```java
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

Configuration conf = new Configuration();

// Typed option constants. Prefer these — a typo is a compile error,
// whereas a typo in a string key is silently ignored at runtime.
conf.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
conf.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
         "s3://my-bucket/flink/checkpoints");
conf.set(CheckpointingOptions.SAVEPOINT_DIRECTORY,
         "s3://my-bucket/flink/savepoints");
conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(60));
conf.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofSeconds(30));
conf.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
         org.apache.flink.configuration.ExternalizedCheckpointRetention
                 .RETAIN_ON_CANCELLATION);

conf.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");

// Untyped escape hatch for any key you know the string of:
conf.setString("taskmanager.memory.managed.fraction", "0.4");

// Hand the Configuration to the environment.
StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(conf);
```

**Precedence:** code beats `config.yaml`. `-D` flags on `flink run` sit between them (they beat the file, lose to explicit code). And a handful of keys — TaskManager memory sizes above all — are read when the **process starts**, so setting them in job code does nothing. Memory belongs in `config.yaml` or the pod spec.

---

## Part 4: Environment-specific config

### Pattern A — one properties file per environment

```
src/main/resources/
├── application.properties        ← shared defaults
├── application-dev.properties
├── application-staging.properties
└── application-prod.properties
```

```java
// --env prod  →  loads application.properties, then overlays application-prod.properties
ParameterTool cli = ParameterTool.fromArgs(args);
String envName = cli.get("env", "dev");

ParameterTool params =
        ParameterTool.fromPropertiesFile(
                        JobConfig.class.getResourceAsStream("/application.properties"))
        .mergeWith(ParameterTool.fromPropertiesFile(
                        JobConfig.class.getResourceAsStream(
                                "/application-" + envName + ".properties")))
        .mergeWith(cli);
```

Simple, and the whole config is version-controlled and reviewable. Downside: changing a value needs a rebuild.

### Pattern B — external file mounted at deploy time (preferred on Kubernetes)

```bash
flink run-application -t kubernetes-application \
  -Dkubernetes.cluster-id=fraud-detection \
  ... \
  local:///opt/flink/usrlib/fraud-detection-1.0.0.jar \
  --config-file /etc/flink-app/application.properties
```

```java
String path = cli.get("config-file");
ParameterTool params = (path != null
        ? ParameterTool.fromPropertiesFile(path)
        : ParameterTool.fromPropertiesFile(
                JobConfig.class.getResourceAsStream("/application.properties")))
        .mergeWith(cli);
```

The file comes from a Kubernetes **ConfigMap** mounted as a volume. Same jar, different config, no rebuild. This is the standard production shape.

**Careful:** `fromPropertiesFile(String)` reads a path on the machine running `main()`. In **Application mode** that is the JobManager pod, so the ConfigMap must be mounted there. In **Session mode** it is your laptop. See [ch. 57](57-deployment-modes.md).

---

## Part 5: Secrets

> **Key idea**
> A jar is a zip file that anyone with cluster access can `unzip`. It goes to artifact repositories, CI caches, and developer laptops. **Never put a credential in it.**

Never:

```java
// ❌ every one of these ends up in git, in the jar, and in the Web UI
props.setProperty("sasl.jaas.config",
    "... username=\"svc\" password=\"hunter2\";");
```

Never in `--args` either: `flink run job.jar --db-password hunter2` puts the password into the JobManager's process listing, the job's `Configuration` tab in the Web UI, and probably a log line.

### Environment variables

```java
// System.getenv reads the process environment of whichever JVM calls it.
// Call it inside open() so it reads the TASKMANAGER's environment.
public class KafkaAuthSink extends RichSinkFunction<Event> {

    private transient String password;

    @Override
    public void open(OpenContext ctx) {
        this.password = System.getenv("KAFKA_PASSWORD");
        if (password == null || password.isEmpty()) {
            // Fail loudly at startup, not on the first record.
            throw new IllegalStateException("KAFKA_PASSWORD env var is not set");
        }
    }
}
```

### Kubernetes secrets

```yaml
# Create once, out of band. NEVER commit this yaml.
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
type: Opaque
stringData:
  password: hunter2
```

Then in the `FlinkDeployment` (full CRD in [ch. 57](57-deployment-modes.md)):

```yaml
spec:
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - name: KAFKA_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: kafka-credentials
                  key: password
```

Kubernetes injects the value into both JobManager and TaskManager containers at start. `System.getenv("KAFKA_PASSWORD")` sees it; git never does.

### The other options

- **Cloud IAM roles** — the best answer when available. An IRSA role on the pod means S3 access with *no credential anywhere*, only a role binding.
- **Vault / AWS Secrets Manager** — fetch in `open()` with a short-lived token. Cache the result; do not call the secrets API per record.
- **Mounted secret files** — `Files.readString(Path.of("/etc/secrets/db-password"))` in `open()`.

### Do not leak them back out

```java
// ❌ dumps the password into the Web UI Configuration tab and probably the logs
env.getConfig().setGlobalJobParameters(paramsIncludingSecrets);

// ✅ register only the non-secret subset
Map<String, String> visible = new HashMap<>(params.toMap());
visible.keySet().removeIf(k -> k.contains("password")
                            || k.contains("secret")
                            || k.contains("token"));
env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(visible));
```

---

## Putting it together

```java
package com.akash.flink.jobs;

import com.akash.flink.config.JobConfig;
import com.akash.flink.functions.ThresholdFilter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {

        // ---- 1. layer the parameter sources ----
        ParameterTool cli = ParameterTool.fromArgs(args);
        String envName = cli.get("env", "dev");

        ParameterTool params = ParameterTool
                .fromPropertiesFile(FraudDetectionJob.class
                        .getResourceAsStream("/application.properties"))
                .mergeWith(ParameterTool.fromPropertiesFile(FraudDetectionJob.class
                        .getResourceAsStream("/application-" + envName + ".properties")))
                .mergeWith(ParameterTool.fromSystemProperties())
                .mergeWith(cli);

        // ---- 2. parse + validate on the CLIENT. Fail before submitting. ----
        JobConfig cfg = JobConfig.from(params);

        // ---- 3. cluster-level settings the job insists on ----
        Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL,
                 Duration.ofMillis(cfg.getCheckpointIntervalMs()));
        conf.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // ---- 4. publish the non-secret config to the Web UI ----
        Map<String, String> visible = new HashMap<>(params.toMap());
        visible.keySet().removeIf(k -> k.contains("password") || k.contains("secret"));
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(visible));

        // ---- 5. build the pipeline, injecting typed config ----
        // buildPipeline is a static method so tests can call it (ch. 60).
        // ... env.fromSource(...) ... .filter(new ThresholdFilter(cfg.getFraudThreshold())) ...

        env.execute("fraud-detection-" + envName);
    }
}
```

Notice the job **name** carries the environment. When you have twelve jobs in one Web UI, that pays for itself immediately.

---

## Remember

- Two config systems: **`ParameterTool`** for job parameters, **`Configuration` / `config.yaml`** for cluster settings.
- `ParameterTool.fromArgs / fromPropertiesFile / fromSystemProperties / fromMap`, layered with **`mergeWith` (later wins)**.
- **`main()` runs on the client.** Get values to TaskManagers by **constructor injection** (default) or `setGlobalJobParameters` + `getRuntimeContext().getGlobalJobParameters()` in a **Rich** function's `open()`.
- On Flink 1.19+ `getGlobalJobParameters()` returns `Map<String,String>`; pre-1.19 you cast `getExecutionConfig().getGlobalJobParameters()` to `ParameterTool`.
- **Parse config in `open()`, never per record.**
- **`flink-conf.yaml` → `config.yaml` in Flink 1.19+**, and the new file is real YAML that supports nesting. The old name still works but is deprecated.
- Precedence: **job code > `-D` CLI flags > `config.yaml`** — except TaskManager memory, which is fixed at process start.
- **Validate config on the client and fail fast.** A bad threshold should kill the submission, not the results.
- **Secrets never go in the jar, in git, or in `--args`.** Use env vars from Kubernetes secrets, or IAM roles. Strip secret-looking keys before `setGlobalJobParameters`.

**Interview one-liners**

- *"How do you pass config to a Flink job?"* → `ParameterTool` layered from a properties file, system properties and CLI args; parsed into a validated typed config object on the client and constructor-injected into functions, with the non-secret subset registered as global job parameters so it appears in the Web UI.
- *"Why can't I just use a field set in `main()`?"* → `main()` runs on the client; functions run on TaskManagers in a different JVM. Only what is serialized into the function object or the ExecutionConfig crosses that boundary.
- *"What changed in Flink 1.19 config?"* → `conf/flink-conf.yaml` was replaced by `conf/config.yaml`, parsed as standard YAML so nested keys work. The old file is still read but deprecated.
- *"Where do secrets go?"* → Never in the jar or the CLI args. Kubernetes secrets injected as env vars and read in `open()`, or better, cloud IAM roles so there is no credential at all.
- *"Why parse config in `open()` rather than in the function body?"* → `open()` runs once per subtask; the function body runs once per record. Parsing per record burns CPU at throughput.
