# Flink Mastery — Phase 1: Foundations

Java DataStream API, Flink 1.18/1.20. Written for someone new to Java: every snippet is explained line by line, and Java syntax is explained inline where it appears.

Work through these in order. Each chapter assumes the one before it.

## Chapters

| # | Chapter | Key idea |
|---|---|---|
| 1 | [Why Flink, and setup](01-why-flink-and-setup.md) | Stream is the primitive, batch is a bounded stream; micro-batch's real cost is no per-record state or timers; full `pom.xml` and the `provided`-scope trap |
| 2 | [The Java you actually need](02-java-you-actually-need.md) | POJO rules, generics, functional interfaces, type erasure, and why "Task not serializable" happens |
| 3 | [First job and the execution model](03-first-job-and-execution-model.md) | Building the graph vs running it — `env.execute()` is the only line that executes anything |
| 4 | [map, filter, flatMap](04-map-filter-flatmap.md) | Output cardinality 1 / 0-or-1 / 0-to-N; `Collector`; when a lambda needs `.returns()` |
| 5 | [keyBy and partitioning](05-keyby-and-partitioning.md) | Same key → same subtask, always. The gate to all keyed state, and the source of all skew |
| 6 | [Tuples and aggregations](06-tuples-and-aggregations.md) | **`max` mutates one field; `maxBy` keeps the whole record.** Rolling aggregates emit on every record |
| 7 | [Event time vs processing time](07-event-time-vs-processing-time.md) | Event time makes results a pure function of the data — replay determinism is the argument |
| 8 | [Watermarks](08-watermarks-concept.md) | "No more events with timestamp < T"; min across input channels; the idle-partition stall |
| 9 | [The `Event` class](09-the-event-class.md) | The POJO contract, and what Kryo silently costs you: throughput and state schema evolution |

## The seven things that matter most from Phase 1

1. **`env.execute()` is the only line that runs anything.** Everything above it builds a graph on the client.
2. **`main()` runs once, on the client. Function bodies run on TaskManagers, once per record.** `println` in the two places behaves completely differently.
3. **`keyBy` is a network shuffle and the gate to all state.** Same key → same subtask. Keys must be immutable, value-based, deterministic.
4. **`max` ≠ `maxBy`.** `max` updates only the aggregated field and leaves the rest frozen at the first record's values, producing records that never existed.
5. **A watermark `T` asserts nothing below `T` is still coming**, and propagates as the **minimum across all inputs** — so one idle partition freezes the entire job. Set `withIdleness`.
6. **A valid POJO needs a public no-arg constructor**, which Java silently removes the moment you add any other constructor. Otherwise: Kryo, 2–5x slower, and no state schema evolution.
7. **Lambda + generic output type → `InvalidTypesException` → `.returns(Types...)`.** Type erasure, not a Flink bug.

## Setup checklist

- [ ] JDK 17 installed, `JAVA_HOME` set, `mvn -version` reports 17
- [ ] Project created with the `pom.xml` from [ch. 1](01-why-flink-and-setup.md)
- [ ] IntelliJ: "Add dependencies with 'provided' scope to classpath" is **checked**
- [ ] `log4j2.properties` in `src/main/resources`
- [ ] `FirstJob` runs from the IDE and prints output
- [ ] `mvn clean package` produces a fat jar with **no** `org/apache/flink/streaming` inside it
- [ ] `Event.java` in place, and `TypeInformation.of(Event.class)` prints `PojoTypeInfo`

## What comes next

Phase 2 (windows) picks up exactly where chapter 8 stops: tumbling, sliding and session windows, triggers, evictors, `allowedLateness` and side outputs in code, and `ProcessWindowFunction` vs `AggregateFunction`.
