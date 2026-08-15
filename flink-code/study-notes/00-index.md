# Apache Flink — Study Notes

> **Target versions:** Flink **1.18.1**, **JDK 11** (matches your `pom.xml`).
> All code snippets use the `DataStream` API and `env.setParallelism(1)` like your existing examples.

These notes follow your 8-phase plan. Each phase file is self-contained: concept → why it matters → runnable code → gotchas.

---

## How to use these notes

1. Read the phase file top to bottom.
2. For each code block, create a class under `src/main/java/org/example/` (one class per topic, like you already do) and run it.
3. Watch the console output and compare against the "Expected output" notes.
4. When a concept clicks, tick it off in your plan.

**Golden rule while learning:** keep `env.setParallelism(1)`. It makes output deterministic and ordered so you can *see* what each operator does. You'll remove it in Phase 4 when we talk about parallelism.

---

## Roadmap

| Phase | File | Theme | Status |
|-------|------|-------|--------|
| 1 | [01-foundations.md](01-foundations.md) | Core Flink foundations (recap) | ✅ you've done this |
| 2 | [02-core-flink-apis.md](02-core-flink-apis.md) | Windows, aggregations, side outputs | ▶️ **you are here** |
| 3 | [03-state.md](03-state.md) | Keyed state, TTL, timers, `KeyedProcessFunction` | ⏳ |
| 4 | [04-realworld-streaming.md](04-realworld-streaming.md) | Kafka, JSON, parallelism, backpressure | ⏳ |
| 5 | [05-reliability.md](05-reliability.md) | Checkpoints, savepoints, exactly-once | ⏳ |
| 6 | [06-advanced-event-processing.md](06-advanced-event-processing.md) | Timers, CEP, sessionization, fraud | ⏳ |
| 7 | [07-sql-table-api.md](07-sql-table-api.md) | Flink SQL & Table API | ⏳ |
| 8 | [08-production.md](08-production.md) | Project structure, tuning, testing | ⏳ |

---

## The mental model (read this once, it ties everything together)

Flink is a **streaming dataflow engine**. You describe a *pipeline* of transformations; Flink runs it forever over an unbounded stream.

```
source  →  transform (map/filter/flatMap)  →  keyBy  →  window/state  →  sink
```

Four ideas do 90% of the work, and every phase deepens one of them:

1. **Records flow through operators** — `map`, `filter`, `flatMap` (Phase 1).
2. **`keyBy` splits the stream into independent per-key sub-streams** — everything stateful is *per key* (Phases 2–3).
3. **Time & windows** let you aggregate over bounded chunks of an unbounded stream (Phase 2), driven by **watermarks** (Phase 1 concept, Phase 2 in code).
4. **State + checkpoints** are what make Flink *reliable* — it can remember things per key and recover them after a crash (Phases 3 & 5).

Phases 4, 6, 7, 8 are about connecting this to the real world (Kafka), doing advanced patterns, using SQL instead, and running it in production.

---

## Quick glossary (forward references are fine — you'll meet each properly later)

- **DataStream** — the core handle to a stream of records.
- **Operator** — one transformation step (map, window, etc.).
- **Subtask** — one parallel instance of an operator (Phase 4).
- **Keyed stream** — a stream partitioned by key via `keyBy`; required for keyed state and keyed windows.
- **Watermark** — a marker in the stream meaning "I believe I've seen all events up to time T" (Phase 1/2).
- **State** — data an operator remembers across records, per key (Phase 3).
- **Checkpoint** — a periodic, consistent snapshot of all state for fault tolerance (Phase 5).
- **Savepoint** — a manually-triggered checkpoint you use for upgrades/migrations (Phase 5).
