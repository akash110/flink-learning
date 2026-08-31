# 1. Why Flink Exists, and Your Setup

## The problem Flink was built to solve

Batch processing assumes the data is **finished**. You point a job at `/events/2026-08-28/`, it reads every file, computes an answer, and exits. That model breaks the moment somebody asks "what is happening *right now*".

The pre-Flink workaround was **micro-batching**: chop the infinite stream into small time slices and run a tiny batch job on each slice. That is exactly what Spark Streaming (DStreams) did, and what Structured Streaming still does by default.

```
MICRO-BATCH (Spark)
 time ──────────────────────────────────────────────►
 events   e e  e e e   e  e e e  e   e e   e e e
        └─ 500ms ─┘└─ 500ms ─┘└─ 500ms ─┘└─ 500ms ─┘
           batch1     batch2     batch3     batch4
              ↓          ↓          ↓          ↓
           schedule   schedule   schedule   schedule
           tasks      tasks      tasks      tasks

TRUE STREAMING (Flink)
 time ──────────────────────────────────────────────►
 events   e e  e e e   e  e e e  e   e e   e e e
          ↓ ↓  ↓ ↓ ↓   ↓  ↓ ↓ ↓  ↓   ↓ ↓   ↓ ↓ ↓
       operators are long-running; each record flows
       through immediately, no batch boundary at all
```

> **Key idea:** Flink treats **the stream as the primitive and batch as a special case** (a bounded stream). Spark treats **batch as the primitive and the stream as a sequence of small batches**. Almost every behavioural difference between them falls out of that one decision.

### What that buys you concretely

| | Micro-batch | True streaming (Flink) |
|---|---|---|
| Latency floor | one batch interval (100 ms – seconds) | sub-millisecond to low ms per record |
| Per-record state access | possible but batch-scoped | native, keyed state per record |
| Per-record timers | no | yes (`KeyedProcessFunction` timers) |
| Scheduling cost | tasks scheduled every batch | tasks scheduled once, run forever |
| Event-time windows | supported | supported, and the design centre |

The latency number is the least interesting difference. The important ones are **timers and per-record keyed state**, because those are what let you express things like "alert if a user does 3 failed logins and then no success within 5 minutes" — a rule that has no natural batch expression at all.

### What Flink is NOT better at

Say this in an interview and you sound like you have used both:

1. **Big periodic batch ETL.** Spark's ecosystem, tuning knobs, and file-format integration are more mature. Nobody should rewrite a nightly 40 TB Parquet job in Flink for fun.
2. **Ad-hoc interactive analytics.** That is Trino/Spark SQL territory.
3. **ML training.** Flink has no serious equivalent to MLlib.
4. **Teams with no JVM/ops appetite.** Flink is a stateful long-running distributed system. Checkpoints, state backends, and restarts are things you now own.

Flink wins when the workload is: **continuous, stateful, event-time-correct, and low-latency.** Fraud detection, real-time aggregation, sessionization, CEP, alerting, streaming joins, CDC pipelines.

---

## Java or PyFlink? (Honest answer)

Akash — you are new to Java, so this is the decision you actually care about. The honest version:

**Use Java for the DataStream API. Use PyFlink only if you are staying inside SQL/Table API.**

Reasons, in order of how much they matter:

1. **State and timers are first-class only in Java.** `KeyedProcessFunction`, `ValueState`, `ListState`, `MapState`, event-time and processing-time timers, side outputs — the whole toolkit that makes Flink worth using — is a Java/Scala API. PyFlink's DataStream API has grown some of this, but it lags and is thinly documented.
2. **PyFlink is a wrapper, not a port.** PyFlink drives the JVM through **Py4J**. Your Python `map` function runs in a separate Python process, and every record crosses a serialization boundary between the JVM and Python. That is a real per-record cost and a real debugging burden — a Python stack trace surfacing through a Java exception chain is not fun.
3. **Features land in Java first.** New connectors, new state APIs, new `WatermarkStrategy` features — Java first, Python later or never.
4. **Every serious Flink example, blog post, mailing-list answer, and StackOverflow reply is in Java.** When you are stuck at 2am, this matters more than you expect.

Where PyFlink is genuinely fine: **Table API and Flink SQL**. There, your Python code just builds a query plan; the execution is 100% JVM. No per-record Python boundary, no lag in features. `table_env.sql_query("SELECT ...")` in PyFlink is the same plan as in Java.

> **Key idea:** The Python cost is per-record only when Python is *in* the record path. SQL/Table API keeps Python at plan-build time, so it is free. DataStream API puts Python in the record path, so it is not.

You will learn just enough Java to be dangerous in [chapter 2](02-java-you-actually-need.md). It is a small subset.

---

## Setup: JDK 17

Flink 1.18 and 1.20 run on **Java 8, 11, or 17**. Use **17** — it is the modern default and 8 is effectively deprecated for new work.

```bash
# macOS, using Homebrew
brew install openjdk@17

# check it
java -version
# openjdk version "17.0.x" ...
```

Set `JAVA_HOME` so Maven and IntelliJ agree with your shell:

```bash
# add to ~/.zshrc
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

```bash
mvn -version   # should print "Java version: 17.0.x"
```

If `mvn` is missing: `brew install maven`.

---

## Directory layout

Maven has a fixed convention. Fight it and nothing works; follow it and everything is automatic.

```
flink-playground/
├── pom.xml                      ← build file: dependencies, java version, packaging
└── src/
    ├── main/
    │   ├── java/
    │   │   └── com/akash/flink/
    │   │       ├── FirstJob.java        ← your job classes
    │   │       └── model/
    │   │           └── Event.java       ← the POJO from ch. 9
    │   └── resources/
    │       └── log4j2.properties        ← controls console log noise
    └── test/
        └── java/                        ← tests (Phase 8)
```

Rules Maven enforces:
- Java source **must** live under `src/main/java`.
- The package declaration at the top of a file **must** match its directory path. `package com.akash.flink;` → file at `src/main/java/com/akash/flink/`.
- Anything under `src/main/resources` is copied onto the classpath.

---

## The full `pom.xml`

Create this at the project root. Read the comments — they are the lesson.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <!-- Coordinates: these three identify your artifact. groupId is your
       namespace (reverse domain), artifactId is the project name. -->
  <groupId>com.akash</groupId>
  <artifactId>flink-playground</artifactId>
  <version>1.0-SNAPSHOT</version>
  <packaging>jar</packaging>

  <properties>
    <!-- Central place to pin versions so every dependency below agrees. -->
    <flink.version>1.20.0</flink.version>
    <java.version>17</java.version>
    <maven.compiler.source>${java.version}</maven.compiler.source>
    <maven.compiler.target>${java.version}</maven.compiler.target>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>

  <dependencies>

    <!-- The DataStream API. This is the one you actually write against:
         DataStream, KeyedStream, WatermarkStrategy, ProcessFunction, etc.
         It transitively pulls in flink-core and flink-runtime. -->
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>

    <!-- Needed to (a) run a job from your IDE via a local MiniCluster and
         (b) submit jobs from code. Without it, running main() in IntelliJ
         fails with a ClassNotFoundException. -->
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-clients</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>

    <!-- Logging. Flink uses SLF4J; without a binding you get the famous
         "SLF4J: Failed to load class StaticLoggerBinder" and see no logs. -->
    <dependency>
      <groupId>org.apache.logging.log4j</groupId>
      <artifactId>log4j-slf4j-impl</artifactId>
      <version>2.17.1</version>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.logging.log4j</groupId>
      <artifactId>log4j-api</artifactId>
      <version>2.17.1</version>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.logging.log4j</groupId>
      <artifactId>log4j-core</artifactId>
      <version>2.17.1</version>
      <scope>runtime</scope>
    </dependency>

  </dependencies>

  <build>
    <plugins>

      <!-- Compile with Java 17. -->
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.13.0</version>
        <configuration>
          <source>${java.version}</source>
          <target>${java.version}</target>
        </configuration>
      </plugin>

      <!-- Builds the "fat jar" you submit to a cluster: your classes plus
           every non-provided dependency (connectors, JSON libs, ...).
           Flink's own classes are excluded because the cluster already
           has them - shipping them again causes classloader conflicts. -->
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.1</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <artifactSet>
                <excludes>
                  <exclude>org.apache.flink:flink-shaded-force-shading</exclude>
                  <exclude>com.google.code.findbugs:jsr305</exclude>
                  <exclude>org.slf4j:*</exclude>
                  <exclude>org.apache.logging.log4j:*</exclude>
                </excludes>
              </artifactSet>
              <filters>
                <filter>
                  <!-- Strip signature files from dependency jars; leaving
                       them in makes the JVM reject the shaded jar. -->
                  <artifact>*:*</artifact>
                  <excludes>
                    <exclude>META-INF/*.SF</exclude>
                    <exclude>META-INF/*.DSA</exclude>
                    <exclude>META-INF/*.RSA</exclude>
                  </excludes>
                </filter>
              </filters>
              <transformers>
                <transformer implementation=
                  "org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>com.akash.flink.FirstJob</mainClass>
                </transformer>
                <transformer implementation=
                  "org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>

    </plugins>
  </build>
</project>
```

### The `provided` scope — the one thing that bites everyone

Maven scopes control *when* a jar is on the classpath:

| Scope | Compile | Test | Packaged into your jar | Runtime |
|---|---|---|---|---|
| `compile` (default) | yes | yes | **yes** | yes |
| `provided` | yes | yes | **no** | expected from environment |
| `runtime` | no | yes | yes | yes |

Flink core is `provided` because **a real Flink cluster already contains flink-streaming-java**. If you also bundle it, you get two copies of the same classes loaded by different classloaders, and the symptom is a baffling `LinkageError` or `ClassCastException: X cannot be cast to X`.

**But `provided` jars are not on the classpath when you hit Run in IntelliJ**, so `main()` dies with `NoClassDefFoundError: .../StreamExecutionEnvironment`. Two fixes, pick one:

- **IntelliJ (preferred):** Run → Edit Configurations → your run config → check **"Add dependencies with 'provided' scope to classpath"**.
- **Maven profile:** keep a `dev` profile that overrides those two dependencies to `compile` scope, and activate it only locally.

> **Key idea:** `provided` = "I need this to compile, the cluster will supply it at runtime." It is correct for Flink core, wrong for connectors (Kafka, JDBC) which you *do* want in your fat jar.

---

## Running from IntelliJ

1. `File → Open` → select the folder containing `pom.xml`. IntelliJ imports it as a Maven project.
2. Wait for the Maven sync (bottom-right progress bar) to finish downloading jars.
3. `File → Project Structure → Project → SDK` → set to **17**.
4. Open `FirstJob.java`, click the green ▶ next to `main`.
5. If you get `NoClassDefFoundError`, apply the "provided scope" checkbox fix above.

When you run from the IDE, Flink starts an in-process **MiniCluster** — a JobManager and TaskManager inside your JVM. There is no cluster to install. This is how you will do 95% of your learning.

```
IntelliJ JVM
┌──────────────────────────────────────────┐
│  main()                                  │
│    └─ env.execute()                      │
│         └─ MiniCluster                   │
│              ├─ JobManager (in-process)  │
│              └─ TaskManager (in-process) │
│                   └─ your operators      │
└──────────────────────────────────────────┘
```

---

## Building and submitting to a real cluster

```bash
mvn clean package
```

- `clean` deletes `target/`.
- `package` compiles, runs tests, then runs the shade plugin.
- Output: `target/flink-playground-1.0-SNAPSHOT.jar` — your fat jar.

Sanity check what is inside:

```bash
jar tf target/flink-playground-1.0-SNAPSHOT.jar | grep -c "org/apache/flink/streaming"
# should be 0 — Flink core must NOT be in there (provided scope working)
```

Start a local standalone cluster (download the Flink binary distribution once):

```bash
# from the unpacked flink-1.20.0/ directory
./bin/start-cluster.sh          # Web UI at http://localhost:8081
```

Submit:

```bash
./bin/flink run \
  -c com.akash.flink.FirstJob \
  ~/flink-playground/target/flink-playground-1.0-SNAPSHOT.jar
```

- `-c` names the class whose `main()` to invoke. You can omit it if the manifest `mainClass` is right (the shade plugin set it).

Useful CLI:

```bash
./bin/flink list                     # running jobs + their JobIDs
./bin/flink cancel <jobId>           # stop a job
./bin/stop-cluster.sh                # shut it all down
```

The Web UI at `localhost:8081` shows the job graph, per-operator record counts, backpressure, and checkpoint history. You will live in it from Phase 5 onward.

---

## Sanity-check file

Put this at `src/main/resources/log4j2.properties` or your console will be unreadable:

```properties
rootLogger.level = INFO
rootLogger.appenderRef.console.ref = ConsoleAppender

appender.console.name = ConsoleAppender
appender.console.type = CONSOLE
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{HH:mm:ss} %-5p %c{1} - %m%n

# Flink's startup chatter is very loud at INFO; quiet the noisiest parts.
logger.netty.name = org.apache.flink.shaded.netty4
logger.netty.level = WARN
logger.akka.name = org.apache.pekko
logger.akka.level = WARN
```

---

## Remember

- Flink = **stream is the primitive, batch is a bounded stream**. Spark = the reverse.
- Micro-batch's real cost is not latency, it is the **absence of per-record state and timers**.
- **Java for DataStream API. PyFlink is fine for SQL/Table API**, because there Python never touches a record.
- PyFlink DataStream = Py4J + a Python process per subtask = per-record serialization boundary.
- `provided` scope keeps Flink core out of your fat jar; tick the IntelliJ checkbox so it still runs locally.
- `mvn clean package` → fat jar → `./bin/flink run -c <MainClass> <jar>`.
- Running `main()` in the IDE spins up a **MiniCluster**; you do not need to install anything to learn.

**Interview one-liners**

- *"Flink vs Spark Streaming?"* → Flink is a continuous-operator true-streaming engine with per-record keyed state and timers; Spark Structured Streaming is micro-batch by default, so its unit of work is a batch, not a record.
- *"Why is Flink's event-time support considered better?"* → It was designed around watermarks and event-time timers from the start, not retrofitted; and `ProcessFunction` gives you direct access to both.
- *"When would you not pick Flink?"* → Large periodic batch ETL, interactive SQL, ML training, or a team with no appetite to operate a stateful long-running system.
