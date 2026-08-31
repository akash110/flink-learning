# 54. Project Structure and Maven

Everything so far you could run from the IDE. This chapter is about turning a folder of `.java` files into **one jar you can hand to a cluster**. If you are new to Java, Maven is the part that feels like magic and then bites you. It is not magic.

> **Key idea**
> Maven does three jobs: **downloads your dependencies**, **compiles** your code, and **packages** it. A Flink job needs a *fat jar* that contains your code and your connectors, but **not** Flink itself — because Flink is already on the cluster.

---

## What Maven actually is

Java has no built-in package manager. Maven fills that gap. You describe your project once in an XML file called `pom.xml` ("Project Object Model"), and Maven derives everything else from convention.

```
your-project/
├── pom.xml               ← the ONLY file you configure
├── src/
│   ├── main/
│   │   ├── java/         ← your code. Maven compiles everything here.
│   │   └── resources/    ← non-code files (log4j2.properties, app.properties)
│   └── test/
│       ├── java/         ← your tests. NOT included in the jar.
│       └── resources/    ← test-only config (log4j2-test.properties)
└── target/               ← everything Maven generates. Never commit this.
    ├── classes/          ← compiled .class files
    └── my-job-1.0.jar    ← the artifact
```

Those paths are **not configurable in practice** — they are Maven's "convention over configuration". Put a `.java` file anywhere else and Maven will not see it.

---

## Coordinates: groupId, artifactId, version

Every jar in the Java world is identified by three strings, together called the **GAV coordinate**.

```xml
<groupId>com.akash.flink</groupId>
<artifactId>fraud-detection</artifactId>
<version>1.0.0</version>
```

- **`groupId`** — who owns it. Convention is a reverse domain name: you own `akash.com` → `com.akash`. It also becomes your Java **package** name by convention. It is a namespace, nothing more.
- **`artifactId`** — the project's own name. Lowercase, hyphens. This becomes the jar filename.
- **`version`** — `1.0.0`. A version ending in `-SNAPSHOT` (e.g. `1.0.0-SNAPSHOT`) means "in development"; Maven will re-download it from remote repos rather than trusting a cached copy. Release builds drop the suffix.

Together they name a file on disk:

```
~/.m2/repository/com/akash/flink/fraud-detection/1.0.0/fraud-detection-1.0.0.jar
                 └──── groupId ────┘ └── artifactId ─┘ └ ver ┘
```

`~/.m2/repository` is your **local repository** — Maven's download cache. Every dependency you have ever pulled lives there. Deleting it forces a full re-download (a genuine fix for "corrupt jar" errors).

---

## Dependencies

A dependency is another GAV coordinate you want on your classpath.

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>1.20.0</version>
    <scope>provided</scope>
</dependency>
```

Maven resolves **transitively**: `flink-streaming-java` itself depends on `flink-core`, which depends on more, and Maven pulls the whole tree. You declare the top of the tree only.

To see the tree:

```bash
mvn dependency:tree
```

This is your first tool when you hit `NoSuchMethodError` or `ClassNotFoundException` — the usual cause is two versions of the same library being pulled in by different paths.

---

## Scopes — and the `provided` rule that trips up everyone

`<scope>` answers: *when* is this jar needed?

| Scope | On compile classpath? | On test classpath? | **Packaged into the jar?** | Use for |
|---|---|---|---|---|
| `compile` (default) | yes | yes | **yes** | your connectors, your business libraries |
| `provided` | yes | yes | **no** | Flink core — the cluster already has it |
| `runtime` | no | yes | yes | JDBC drivers, log4j implementations |
| `test` | no | yes | no | JUnit, AssertJ, Testcontainers |

> **Key idea**
> Flink core dependencies are `provided` because **the cluster already has them on its classpath**, in `$FLINK_HOME/lib`. Packaging them into your jar produces duplicate classes loaded by two different classloaders, which surfaces as bizarre errors like `X cannot be cast to X`.

Which ones are `provided`:

```
flink-streaming-java     ← provided
flink-clients            ← provided
flink-table-api-java-bridge, flink-table-runtime, flink-table-planner-loader  ← provided
flink-statebackend-rocksdb  ← provided (it ships in the distribution)
```

Which ones are **not** (`compile`, so they get packaged):

```
flink-connector-kafka      ← NOT on the cluster. You must ship it.
flink-connector-base
flink-json / flink-avro
your JSON library, your HTTP client, your business code
```

### The `provided` problem in the IDE

`provided` means "not at runtime". When you hit Run in IntelliJ, IntelliJ takes that literally, excludes the Flink jars, and your job dies instantly:

```
java.lang.NoClassDefFoundError:
  org/apache/flink/streaming/api/environment/StreamExecutionEnvironment
```

Two fixes. Pick one.

**Fix 1 — IntelliJ checkbox (simplest).**
`Run → Edit Configurations… → your main class → Modify options → ☑ Add dependencies with "provided" scope to classpath`.

**Fix 2 — a Maven profile (works for everyone on the team, and in CI).**

```xml
<profiles>
    <profile>
        <id>add-dependencies-for-IDEA</id>
        <activation>
            <property>
                <name>idea.version</name>   <!-- IntelliJ sets this system property -->
            </property>
        </activation>
        <dependencies>
            <dependency>
                <groupId>org.apache.flink</groupId>
                <artifactId>flink-streaming-java</artifactId>
                <version>${flink.version}</version>
                <scope>compile</scope>      <!-- overrides provided, IDE-only -->
            </dependency>
            <dependency>
                <groupId>org.apache.flink</groupId>
                <artifactId>flink-clients</artifactId>
                <version>${flink.version}</version>
                <scope>compile</scope>
            </dependency>
        </dependencies>
    </profile>
</profiles>
```

A **profile** is a conditionally-activated chunk of pom. This one activates only when the `idea.version` system property exists — i.e. only inside IntelliJ. Your `mvn package` on the command line never sees it, so the shipped jar stays clean. This is the exact pattern the official Flink Maven archetype uses.

---

## The fat jar and `maven-shade-plugin`

`mvn package` on its own produces a "thin jar": your classes only. Submit that to a cluster and you get:

```
java.lang.ClassNotFoundException:
  org.apache.flink.connector.kafka.source.KafkaSource
```

Because the Kafka connector is not in `$FLINK_HOME/lib` and it is not in your jar either.

**`maven-shade-plugin`** fixes this. It unzips every `compile`-scope dependency and merges the class files into one jar — an "uber jar" / "fat jar".

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.5.1</version>
    <executions>
        <execution>
            <phase>package</phase>      <!-- run during `mvn package` -->
            <goals><goal>shade</goal></goals>
            <configuration>
                <transformers>
                    <transformer implementation=
                        "org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                    <transformer implementation=
                        "org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                        <mainClass>com.akash.flink.jobs.FraudDetectionJob</mainClass>
                    </transformer>
                </transformers>
            </configuration>
        </execution>
    </executions>
</plugin>
```

### `ServicesResourceTransformer` — the one that silently breaks connectors

Java has a plugin mechanism called **ServiceLoader**. A library says "I implement interface `X`" by shipping a text file at:

```
META-INF/services/org.apache.flink.table.factories.Factory
```

whose contents are a list of implementing class names. Flink SQL, formats (`json`, `avro`, `csv`), and file systems all discover themselves this way.

**The problem:** three different jars can each ship a file with the *same path*. When shade merges them, the last one wins and overwrites the other two — you lose all but one connector.

`ServicesResourceTransformer` **concatenates** them instead of overwriting. Omit it and you get:

```
org.apache.flink.table.api.ValidationException:
  Could not find any factory for identifier 'kafka' in the classpath.
```

with a jar that visibly contains the Kafka classes. Maddening. Always include it.

`ManifestResourceTransformer` writes `Main-Class:` into `META-INF/MANIFEST.MF`, so `flink run` can find your entry point without a `-c` flag.

### Relocation — renaming packages to dodge conflicts

Suppose Flink ships Guava 31 in `lib/` and your code needs Guava 32. Same class names, different behaviour, one classloader — whichever loads first wins, and the other breaks.

```xml
<relocations>
    <relocation>
        <pattern>com.google.common</pattern>
        <shadedPattern>com.akash.shaded.com.google.common</shadedPattern>
    </relocation>
</relocations>
```

Shade physically **rewrites the bytecode**: every reference to `com.google.common.Foo` in your jar becomes `com.akash.shaded.com.google.common.Foo`, and the class file is renamed to match. Your copy is now invisible to everyone else's, and theirs to yours. This is why the Flink project itself ships `flink-shaded-*` artifacts.

Only relocate when you have an actual conflict. It bloats the jar and makes stack traces uglier.

### Excluding signature files

```xml
<filters>
    <filter>
        <artifact>*:*</artifact>
        <excludes>
            <exclude>META-INF/*.SF</exclude>
            <exclude>META-INF/*.DSA</exclude>
            <exclude>META-INF/*.RSA</exclude>
        </excludes>
    </filter>
</filters>
```

Signed jars carry cryptographic digests of their own contents. Merge them into a new jar and the digests no longer match, so the JVM refuses to load anything with `SecurityException: Invalid signature file digest`. Stripping the signature files is standard and safe here.

---

## Source layout

```
src/main/java/com/akash/flink/
├── jobs/
│   ├── FraudDetectionJob.java     ← has main(); wires source → logic → sink
│   └── SessionMetricsJob.java
├── functions/
│   ├── FraudDetector.java         ← KeyedProcessFunction, no main(), testable
│   └── EventDeserializer.java
├── model/
│   └── Event.java                 ← the POJO. No Flink imports if you can help it.
├── config/
│   └── JobConfig.java             ← typed wrapper over ParameterTool (ch. 55)
└── sinks/
    └── AlertSink.java
```

Why split this way:

- **`jobs/`** — one class per deployable job, each with a `main()`. Thin: it should only wire things together.
- **`functions/`** — where the logic lives. These are what you unit-test with the harnesses in [ch. 60](60-testing-flink-jobs.md). A function class that does not know about Kafka or S3 is a function class you can test in 5 milliseconds.
- **`model/`** — your POJOs. Keeping them Flink-free means you can reuse them in a REST service.
- **`config/`** — parsing and validating parameters in one place.

**The Java rule you must obey:** the package declaration must match the directory path. `package com.akash.flink.model;` **must** live at `src/main/java/com/akash/flink/model/Event.java`. Maven will not compile it otherwise.

---

## The complete production pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             https://maven.apache.org/xsd/maven-4.0.0.xsd">

    <!-- Always 4.0.0. It is the POM format version, not your project's. -->
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.akash.flink</groupId>
    <artifactId>fraud-detection</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>
    <name>Fraud Detection Flink Job</name>

    <!-- Variables. Referenced elsewhere as ${flink.version}. Change once, applies everywhere. -->
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <flink.version>1.20.0</flink.version>
        <java.version>11</java.version>
        <maven.compiler.source>${java.version}</maven.compiler.source>
        <maven.compiler.target>${java.version}</maven.compiler.target>
        <log4j.version>2.24.1</log4j.version>
        <junit.version>5.10.2</junit.version>
    </properties>

    <dependencies>

        <!-- ================= FLINK CORE — provided ================= -->
        <!-- Present in $FLINK_HOME/lib on every cluster. Do NOT package. -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <!-- Needed to submit jobs and to run locally (MiniCluster). -->
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Table/SQL API. Drop these three if you use DataStream only. -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java-bridge</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-runtime</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <!-- The "loader" variant is classloader-isolated. Prefer it over
                 flink-table-planner_2.12 — the plain planner is for internal use. -->
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner-loader</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- RocksDB state backend: ships in the distribution → provided. -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-statebackend-rocksdb</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- ============ CONNECTORS & FORMATS — compile (shipped) ============ -->
        <!-- Kafka connector versions are decoupled from Flink since 1.17.
             The 3.x line uses a "<connector>-<flink>" version suffix. -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka</artifactId>
            <version>3.2.0-1.19</version>
        </dependency>
        <dependency>
            <!-- JSON (de)serialization schemas. Not on the cluster. -->
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-json</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <!-- ================= LOGGING ================= -->
        <!-- The cluster provides log4j2 in lib/. Mark provided so you do not
             ship a second copy that fights the cluster's. -->
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-slf4j-impl</artifactId>
            <version>${log4j.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-api</artifactId>
            <version>${log4j.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-core</artifactId>
            <version>${log4j.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- ================= TEST ================= -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <!-- Test harnesses: OneInputStreamOperatorTestHarness etc. See ch. 60. -->
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
            <!-- MiniClusterWithClientResource / MiniClusterExtension -->
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-test-utils</artifactId>
            <version>${flink.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>3.25.3</version>
            <scope>test</scope>
        </dependency>
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
    </dependencies>

    <build>
        <plugins>

            <!-- Compiler: which Java language level to target. -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.13.0</version>
                <configuration>
                    <source>${java.version}</source>
                    <target>${java.version}</target>
                </configuration>
            </plugin>

            <!-- Surefire runs src/test/java during `mvn test` / `mvn package`. -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.2.5</version>
                <configuration>
                    <!-- Flink tests need headroom; the default heap is often too small. -->
                    <argLine>-Xmx2048m</argLine>
                </configuration>
            </plugin>

            <!-- THE FAT JAR -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.1</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals><goal>shade</goal></goals>
                        <configuration>
                            <!-- Belt and braces: never let Flink core into the jar
                                 even if something declares it compile-scope. -->
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
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                        <exclude>module-info.class</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <!-- CRITICAL: merges META-INF/services/* instead of
                                     overwriting. Without it, connectors are not found. -->
                                <transformer implementation=
                                  "org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                                <transformer implementation=
                                  "org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.akash.flink.jobs.FraudDetectionJob</mainClass>
                                </transformer>
                            </transformers>
                            <!-- Uncomment only when you have a real conflict:
                            <relocations>
                                <relocation>
                                    <pattern>com.google.common</pattern>
                                    <shadedPattern>com.akash.shaded.com.google.common</shadedPattern>
                                </relocation>
                            </relocations>
                            -->
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

    <!-- IDE-only: puts provided-scope Flink back on the classpath in IntelliJ. -->
    <profiles>
        <profile>
            <id>add-dependencies-for-IDEA</id>
            <activation>
                <property><name>idea.version</name></property>
            </activation>
            <dependencies>
                <dependency>
                    <groupId>org.apache.flink</groupId>
                    <artifactId>flink-streaming-java</artifactId>
                    <version>${flink.version}</version>
                    <scope>compile</scope>
                </dependency>
                <dependency>
                    <groupId>org.apache.flink</groupId>
                    <artifactId>flink-clients</artifactId>
                    <version>${flink.version}</version>
                    <scope>compile</scope>
                </dependency>
                <dependency>
                    <groupId>org.apache.flink</groupId>
                    <artifactId>flink-table-planner-loader</artifactId>
                    <version>${flink.version}</version>
                    <scope>compile</scope>
                </dependency>
                <dependency>
                    <groupId>org.apache.flink</groupId>
                    <artifactId>flink-table-runtime</artifactId>
                    <version>${flink.version}</version>
                    <scope>compile</scope>
                </dependency>
                <dependency>
                    <groupId>org.apache.flink</groupId>
                    <artifactId>flink-statebackend-rocksdb</artifactId>
                    <version>${flink.version}</version>
                    <scope>compile</scope>
                </dependency>
            </dependencies>
        </profile>
    </profiles>
</project>
```

Two notes on versions: **check the actual latest patch** on Maven Central before copying. And `flink-connector-kafka` version `3.2.0-1.19` reads as "connector release 3.2.0, built against Flink 1.19" — it is compatible with 1.20 too, because connectors follow their own release cycle since Flink 1.17.

---

## Building and inspecting

```bash
# The workhorse. clean = delete target/. package = compile, test, jar, shade.
mvn clean package

# Skip tests when you are iterating on the jar itself (never in CI).
mvn clean package -DskipTests

# Offline build from the local ~/.m2 cache. Fast, and proves you have no missing deps.
mvn -o clean package
```

The build lifecycle in order: `validate → compile → test → package → verify → install → deploy`. Running `package` runs everything before it. `mvn install` additionally copies the jar into `~/.m2` so other local projects can depend on it.

You get **two** jars in `target/`:

```
target/fraud-detection-1.0.0.jar             ← the shaded fat jar. SUBMIT THIS.
target/original-fraud-detection-1.0.0.jar    ← the thin pre-shade jar. Ignore.
```

Shade renames the thin jar to `original-*` and puts the fat one at the normal name.

### Verify the jar before you deploy

A jar is a zip. Look inside.

```bash
# Is the Kafka connector actually in there?
jar tf target/fraud-detection-1.0.0.jar | grep -c "connector/kafka"
# → a few hundred. If 0, your dependency is provided/test scope by mistake.

# Did ServicesResourceTransformer do its job?
unzip -p target/fraud-detection-1.0.0.jar \
      META-INF/services/org.apache.flink.table.factories.Factory
# → a list of class names, one per line, from MULTIPLE jars.

# Is Flink core wrongly bundled? Should print 0.
jar tf target/fraud-detection-1.0.0.jar \
  | grep -c "org/apache/flink/streaming/api/environment/StreamExecutionEnvironment"

# Is the Main-Class set?
unzip -p target/fraud-detection-1.0.0.jar META-INF/MANIFEST.MF

# Size sanity check. 10-60 MB is normal. 300 MB means Flink leaked in.
ls -lh target/fraud-detection-1.0.0.jar
```

Run these once and the "why doesn't my job start on the cluster" class of problem mostly disappears.

---

## Remember

- Maven conventions are fixed: `src/main/java`, `src/main/resources`, `src/test/java`, `target/`. Package name must match directory path.
- **GAV** = groupId (namespace) + artifactId (name) + version. Together they locate a jar in `~/.m2`.
- **`provided`** = on the compile classpath, not in the jar. Use it for everything Flink ships in `lib/`.
- IntelliJ honours `provided` literally → `NoClassDefFoundError`. Fix with the "include provided scope" checkbox or the `idea.version` profile.
- **Connectors are NOT on the cluster.** They must be `compile` scope and inside the fat jar.
- `maven-shade-plugin` builds the fat jar. **`ServicesResourceTransformer` is mandatory** — without it, merged `META-INF/services` files overwrite each other and connectors "disappear".
- **Relocation** rewrites package names in bytecode to isolate conflicting library versions. Use only on a real conflict.
- Strip `META-INF/*.SF|DSA|RSA` or the JVM rejects the jar's broken signatures.
- `mvn clean package` → `target/<artifactId>-<version>.jar` (submit this) and `original-*.jar` (ignore).
- `jar tf` and `unzip -p` before you deploy. Two minutes saves two hours.

**Interview one-liners**

- *"Why are Flink dependencies `provided`?"* → They are already in `$FLINK_HOME/lib` on the cluster. Bundling them creates duplicate classes across classloaders, causing `ClassCastException` on identically-named classes.
- *"Why do you need a fat jar if Flink is on the cluster?"* → Connectors and formats are not part of the distribution. Only Flink core is provided; everything else must ship with the job.
- *"What does `ServicesResourceTransformer` do?"* → It concatenates `META-INF/services/*` files from all shaded jars instead of letting the last one overwrite the rest, so Flink's ServiceLoader-based factory discovery still finds every connector and format.
- *"When do you relocate classes?"* → When your jar and the cluster need incompatible versions of the same library, e.g. Guava. Shade rewrites the bytecode to a private package so the two copies cannot collide.
- *"`compile` vs `provided` vs `runtime` vs `test`?"* → compile: everywhere and packaged; provided: compile and test only, not packaged; runtime: not needed to compile but packaged; test: test classpath only.
