# Flink Mastery — Stream Processing from Zero

A complete, example-driven course in Apache Flink. Every code snippet is explained
line by line. Written for someone who is **new to Java** — the Java you need is
taught inline, where it appears, not as a separate prerequisite.

All code is **Java DataStream API**, Flink 1.18+, with Phase 7 covering Flink SQL.

---

## Why Java and not PyFlink

You asked, and it's the right first question. **Use Java for this.**

| | Java | PyFlink |
|---|---|---|
| DataStream API | Complete | Partial — a Py4J wrapper over the JVM |
| Keyed state, timers, `ProcessFunction` | Full support | Limited, lags releases |
| New features | Land here first | Arrive later, sometimes never |
| Table API / SQL | Complete | **Genuinely first-class** |
| Debugging | Java stack traces | Java stack traces *through* a Python bridge |

Flink is written in Java. For Phases 2–6 — windows, state, timers, the parts that
make Flink worth learning — Java is the only comfortable option. PyFlink is a fine
choice for Phase 7 (SQL / Table API), and that's called out where relevant.

The Java subset you actually need is small: classes, generics, lambdas, interfaces.
[Chapter 2](phase1-foundations/02-java-you-actually-need.md) covers exactly that and
nothing more.

---

## How to use this

- **Starting from scratch?** Read Phase 1 in order, typing every example. Don't skim
  chapter 3 — the build-vs-execute distinction is the thing everyone gets wrong.
- **Phase 1 mostly done?** Skim ch. 7–9 (time, watermarks, the `Event` class — later
  phases all use it), then go to Phase 2.
- **Need it working at work?** Phases 2 → 3 → 4 → 5. That's the production core.
- **Interview soon?** [Cheatsheet](00-CHEATSHEET.md), then ch. 8 (watermarks),
  Phase 3 (state), ch. 37 (exactly-once), ch. 30 (backpressure), then the
  troubleshooting notes in the parent folder.
- **Debugging right now?** [Cheatsheet](00-CHEATSHEET.md) → "Diagnosing a sick job",
  then the parent-folder notes linked at the bottom of this page.

---

## Table of contents

### [Quick Reference Cheatsheet](00-CHEATSHEET.md)
Every API, config key, and diagnostic you'll look up repeatedly, on one page.

---

### 🟢 Phase 1 — Foundations
| # | Chapter | Key idea |
|---|---|---|
| 1 | [Why Flink & setup](phase1-foundations/01-why-flink-and-setup.md) | True streaming, not micro-batch; JDK + Maven + your first run |
| 2 | [The Java you actually need](phase1-foundations/02-java-you-actually-need.md) | Lambdas, generics, POJOs, and why `transient` matters |
| 3 | [First job & execution model](phase1-foundations/03-first-job-and-execution-model.md) | You build a graph; `execute()` runs it |
| 4 | [map, filter, flatMap](phase1-foundations/04-map-filter-flatmap.md) | 1→1, 1→0/1, 1→many; the `Collector` |
| 5 | [keyBy & partitioning](phase1-foundations/05-keyby-and-partitioning.md) | Logical partitioning; the gateway to all state |
| 6 | [Tuples & aggregations](phase1-foundations/06-tuples-and-aggregations.md) | `max()` vs `maxBy()` — the classic bug |
| 7 | [Event time vs processing time](phase1-foundations/07-event-time-vs-processing-time.md) | Determinism on replay is the whole point |
| 8 | [**Watermarks**](phase1-foundations/08-watermarks-concept.md) | **The minimum-across-inputs rule. Read twice.** |
| 9 | [The Event class](phase1-foundations/09-the-event-class.md) | The POJO contract, and what breaks without it |

### 🟡 Phase 2 — Windows & Time
| # | Chapter | Key idea |
|---|---|---|
| 10 | [Timestamps & watermarks in code](phase2-windows/10-timestamps-and-watermarks-in-code.md) | The full pipeline, traced |
| 11 | [Tumbling windows](phase2-windows/11-tumbling-windows.md) | Epoch alignment; `windowAll` kills parallelism |
| 12 | [sum, reduce, aggregate](phase2-windows/12-window-functions-sum-reduce-aggregate.md) | Incremental: one accumulator, not all elements |
| 13 | [ProcessWindowFunction](phase2-windows/13-processwindowfunction.md) | Combine with `aggregate()` for metadata *and* efficiency |
| 14 | [Sliding & session windows](phase2-windows/14-sliding-and-session-windows.md) | Overlap multiplies work; sessions merge |
| 15 | [Triggers & evictors](phase2-windows/15-triggers-and-evictors.md) | FIRE / PURGE / FIRE_AND_PURGE; early results |
| 16 | [Late events & side outputs](phase2-windows/16-late-events-and-side-outputs.md) | Allowed lateness re-fires windows — plan for it |
| 17 | [Capstone](phase2-windows/17-phase2-capstone.md) | One job using everything above |

### 🟠 Phase 3 — State
| # | Chapter | Key idea |
|---|---|---|
| 18 | [What state is](phase3-state/18-what-flink-state-is.md) | Local + checkpointed beats a remote DB call per record |
| 19 | [ValueState](phase3-state/19-valuestate.md) | Scoped to the current key automatically |
| 20 | [ListState & MapState](phase3-state/20-liststate-mapstate.md) | `MapState` ≫ `ValueState<HashMap>` on RocksDB |
| 21 | [Reducing & Aggregating state](phase3-state/21-reducing-aggregating-state.md) | Pre-aggregate on write |
| 22 | [**State TTL**](phase3-state/22-state-ttl.md) | **No TTL + unbounded keys = a dead job** |
| 23 | [KeyedProcessFunction & timers](phase3-state/23-keyedprocessfunction-and-timers.md) | Timers are keyed, checkpointed, deduplicated |
| 24 | [Fraud detection capstone](phase3-state/24-fraud-detection-capstone.md) | State + timers + side outputs together |

### 🔵 Phase 4 — Real-World Streaming
| # | Chapter | Key idea |
|---|---|---|
| 25 | [Kafka source](phase4-realworld/25-kafka-source.md) | Flink assigns splits itself; group id is only for offset commits |
| 26 | [Kafka sink](phase4-realworld/26-kafka-sink.md) | Transaction timeout vs checkpoint interval |
| 27 | [JSON serde](phase4-realworld/27-json-serde.md) | `transient ObjectMapper`; dead-letter the bad records |
| 28 | [Parallelism, slots, subtasks](phase4-realworld/28-parallelism-slots-subtasks.md) | Slots needed = max parallelism, not the sum |
| 29 | [Partitioning & rebalance](phase4-realworld/29-partitioning-and-rebalance.md) | `rescale` is the cheap `rebalance` |
| 30 | [Backpressure](phase4-realworld/30-backpressure.md) | The first non-backpressured operator is the bottleneck |
| 31 | [Kafka → Flink → Kafka](phase4-realworld/31-kafka-to-flink-to-kafka.md) | The complete production-shaped job |

### 🟣 Phase 5 — Reliability
| # | Chapter | Key idea |
|---|---|---|
| 32 | [How checkpoints work](phase5-reliability/32-checkpoints-how-they-work.md) | Barriers, alignment, and why unaligned exists |
| 33 | [Configuring checkpoints](phase5-reliability/33-enabling-and-configuring-checkpoints.md) | Min-pause matters more than interval |
| 34 | [State backends & storage](phase5-reliability/34-state-backends-and-storage.md) | Backend ≠ storage — the distinction everyone misses |
| 35 | [Savepoints](phase5-reliability/35-savepoints.md) | `uid()` on everything stateful, from day one |
| 36 | [Fault tolerance & restarts](phase5-reliability/36-fault-tolerance-and-restart-strategies.md) | Restore → rewind → reprocess |
| 37 | [**Exactly-once**](phase5-reliability/37-exactly-once-explained.md) | **State exactly-once ≠ delivery exactly-once** |
| 38 | [Recovery walkthrough](phase5-reliability/38-recovery-walkthrough.md) | Kill it, restore it, watch the counts continue |

### 🟤 Phase 6 — Advanced Event Processing
| # | Chapter | Key idea |
|---|---|---|
| 39 | [Timers deep dive](phase6-advanced-events/39-timers-deep-dive.md) | Coalescing by rounding = a real scale technique |
| 40 | [The ProcessFunction family](phase6-advanced-events/40-process-function-family.md) | Which one, and why; two-stream enrichment |
| 41 | [Broadcast state](phase6-advanced-events/41-broadcast-state.md) | Change rules at runtime without redeploying |
| 42 | [Stateful pattern detection](phase6-advanced-events/42-stateful-pattern-detection.md) | Failed-login detection, hand-rolled |
| 43 | [The CEP library](phase6-advanced-events/43-flink-cep-library.md) | `next` vs `followedBy`; same pattern, declaratively |
| 44 | [Sessionization](phase6-advanced-events/44-sessionization.md) | Session windows vs full manual control |
| 45 | [Joins & enrichment](phase6-advanced-events/45-joins-and-enrichment.md) | Interval joins; never block in `asyncInvoke` |

### 🔴 Phase 7 — Flink SQL & Table API
| # | Chapter | Key idea |
|---|---|---|
| 46 | [Table API & SQL basics](phase7-sql-table/46-table-api-and-sql-basics.md) | SQL compiles down to the same DataStream job |
| 47 | [**Dynamic tables & changelogs**](phase7-sql-table/47-dynamic-tables-and-changelogs.md) | **Append vs retract vs upsert — the central concept** |
| 48 | [Time attributes in SQL](phase7-sql-table/48-time-attributes-and-watermarks-in-sql.md) | `WATERMARK FOR ts AS ...` in the DDL |
| 49 | [SQL windows & aggregations](phase7-sql-table/49-sql-windows-and-aggregations.md) | Window TVFs; `CUMULATE` for running daily totals |
| 50 | [SQL joins](phase7-sql-table/50-sql-joins.md) | Regular joins keep state forever — use interval or temporal |
| 51 | [Connectors & formats](phase7-sql-table/51-connectors-and-formats-in-sql.md) | `upsert-kafka`; `datagen` for instant experiments |
| 52 | [SQL vs DataStream](phase7-sql-table/52-sql-vs-datastream.md) | Mix them; mind the savepoint-compatibility caveat |
| 53 | [SQL capstone](phase7-sql-table/53-sql-capstone.md) | A full pipeline in SQL, runnable with no Kafka |

### ⚫ Phase 8 — Production
| # | Chapter | Key idea |
|---|---|---|
| 54 | [Project structure & Maven](phase8-production/54-project-structure-and-maven.md) | `provided` scope, and the shade plugin transformer you must not omit |
| 55 | [Configuration & parameters](phase8-production/55-configuration-and-parameters.md) | Nothing hardcoded; no secrets in the jar |
| 56 | [Logging & metrics](phase8-production/56-logging-and-metrics.md) | Consumer lag is the one metric to watch |
| 57 | [Deployment modes](phase8-production/57-deployment-modes.md) | Application mode + the Kubernetes Operator |
| 58 | [Web UI & monitoring](phase8-production/58-web-ui-and-monitoring.md) | Reading the checkpoint and backpressure tabs |
| 59 | [Tuning](phase8-production/59-tuning-parallelism-memory-checkpoints.md) | RocksDB lives in *managed* memory |
| 60 | [Testing Flink jobs](phase8-production/60-testing-flink-jobs.md) | Test harnesses let you advance watermarks by hand |
| 61 | [**Production checklist**](phase8-production/61-production-checklist.md) | **Run through this before you ship** |

---

## The five ideas that matter most

If you remember nothing else:

1. **You build a graph; `execute()` runs it.** Nothing before that line processes data.
2. **A watermark of T asserts no more events before T** — and an operator takes the
   **minimum** across its inputs, so one idle partition stalls everything.
3. **State is scoped to the current key**, automatically. That's why `keyBy()` is required.
4. **Unbounded keyspace without TTL kills jobs.** Every `ValueState` needs an
   answer to "when does this get deleted?"
5. **Exactly-once means state is affected once**, not that records are processed once.
   End-to-end needs a replayable source *and* a transactional sink.

---

## Related notes in this project

The parent folder holds interview-style troubleshooting write-ups. This course
builds the model; those drill the failures.

- [Checkpointing slow](../01-checkpointing-slow.md) — pairs with Phase 5
- [Backpressure](../02-backpressure.md) — pairs with ch. 30
- [State and skew](../03-state-and-skew.md) — pairs with Phase 3 and ch. 29
- [Exactly-once](../04-exactly-once.md) — pairs with ch. 37
- [Watermarks and time](../05-watermarks-and-time.md) — pairs with ch. 8 and 10
- [Scale arithmetic](../06-scale-arithmetic.md) — pairs with ch. 59

And [`../spark-mastery/`](../spark-mastery/README.md) for the batch side.
