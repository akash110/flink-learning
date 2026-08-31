# Phase 4 — Real-World Flink: Kafka, Execution, and Backpressure

Phases 1–3 taught you the language: operators, event time, watermarks, windows, keyed state, timers. Every one of those examples started with `env.fromElements(...)`.

This phase replaces that toy source with a real one. By the end you have a **Kafka → Flink → Kafka** job running on your laptop, and you understand the execution model well enough to explain why it is slow when it is slow.

## Chapters

| # | Chapter | Key idea |
|---|---|---|
| 25 | [KafkaSource](25-kafka-source.md) | Flink assigns partitions itself — the group id is only for offset commits and lag dashboards. Watermarks go **inside** `fromSource` for per-split generation. |
| 26 | [KafkaSink](26-kafka-sink.md) | Exactly-once = Kafka transactions + two-phase commit. Visibility latency becomes your checkpoint interval, and the consumer must be `read_committed`. |
| 27 | [JSON serde](27-json-serde.md) | `getProducedType()` exists because generics are erased. `ObjectMapper` is `transient` + built in `open()`. Bad records go to a dead-letter side output, never to an exception. |
| 28 | [Parallelism, slots, subtasks](28-parallelism-slots-subtasks.md) | A slot slices **memory**, not CPU. Slot sharing means required slots = **max** parallelism, not the sum. `maxParallelism` is a one-way decision. |
| 29 | [Partitioning & rebalance](29-partitioning-and-rebalance.md) | Same parallelism = free forward edge. `rebalance()` fixes subtask imbalance; nothing fixes key skew except changing the key. |
| 30 | [**Backpressure**](30-backpressure.md) | **Credit-based flow control. The first NON-backpressured operator downstream is the bottleneck.** Backpressure → slow barriers → failed checkpoints → the death spiral. |
| 31 | [**The complete job**](31-kafka-to-flink-to-kafka.md) | **Everything above, assembled, commented, and runnable.** pom, docker-compose (KRaft), console commands, production checklist. |

## Reading order

Straight through. Chapter 31 assembles 25–30 and will not make sense out of order.

If you are short on time: **25, 26, 30, 31**. Chapters 28 and 29 are the ones interviewers dig into once they know you have shipped something.

## The seven things that matter most in this phase

1. **`FlinkKafkaConsumer` / `FlinkKafkaProducer` are removed.** `KafkaSource` + `env.fromSource`, `KafkaSink` + `stream.sinkTo`.
2. **Flink does not use Kafka consumer groups for partition assignment.** The JobManager's enumerator assigns splits. The group id is a label.
3. **Pass the `WatermarkStrategy` to `fromSource`.** Per-split watermarks. Applying it downstream silently drops data from lagging partitions.
4. **Required slots = the maximum operator parallelism**, because of slot sharing. Not the sum.
5. **The first non-backpressured operator downstream is the bottleneck.** Everything upstream of it is a symptom.
6. **Exactly-once output latency = checkpoint interval.** And the consumer must set `isolation.level=read_committed`.
7. **`uid()` on every operator, `maxParallelism` set on day one.** Both are unfixable later.

## Prerequisites from earlier phases

- The `Event` POJO (`userId`, `type`, `amount`, `timestamp`) — Phase 1
- `WatermarkStrategy`, event time, allowed lateness — Phase 2
- Tumbling windows, `AggregateFunction`, `ProcessWindowFunction` — Phase 2
- `keyBy`, keyed state, `KeyedProcessFunction` — Phase 3

## What comes next

Phase 5 (**Reliability**) picks up the threads this phase deliberately left dangling:

- Checkpointing internals, barrier alignment, and **unaligned checkpoints** (forward-referenced in ch. 30)
- Savepoints, state migration, and rescaling — the payoff for `uid()` and `maxParallelism` from ch. 28
- State backends: heap vs RocksDB, incremental checkpoints
- Restart strategies and failure recovery
