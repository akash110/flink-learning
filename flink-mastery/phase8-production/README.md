# Phase 8 — Production

Phases 1–7 taught you to *write* a Flink job: DataStream API, event time and watermarks, windows, state, timers, Kafka, parallelism and slots, checkpoints and savepoints, CEP, joins, and SQL.

This phase is everything between "it works in IntelliJ" and "it has been running in production for six months and nobody has been paged."

All code is **Java**, Flink **1.18–1.20** era. Every snippet is explained line by line. Every chapter ends with **Remember** and **Interview one-liners**.

## How to use this

- **Shipping a job this week?** Read ch. 54 (build it), 55 (configure it), 57 (deploy it), then work through ch. 61 as an actual checklist.
- **Job already in production and misbehaving?** Ch. 58 (read the UI), then the parent troubleshooting notes below.
- **Interview prep?** Ch. 61's "top 10 mistakes" and every chapter's interview one-liners, then the parent notes.
- **Building the habit properly?** Straight through. Ch. 60 pays for itself the first time a savepoint restore does not lose your state.

## Table of contents

| # | Chapter | Key idea |
|---|---|---|
| 54 | [Project structure and Maven](54-project-structure-and-maven.md) | Flink core is `provided` because the cluster has it; **connectors are not**, so you need a fat jar — and `ServicesResourceTransformer` or they silently vanish |
| 55 | [Configuration and parameters](55-configuration-and-parameters.md) | `ParameterTool` for job params, `config.yaml` for the cluster; `main()` runs on the client, so values reach TaskManagers only by serialization; secrets never go in the jar |
| 56 | [Logging and metrics](56-logging-and-metrics.md) | `static final Logger` (it isn't serializable), never log per record, and `records-lag-max` is the single most important metric you have |
| 57 | [Deployment modes](57-deployment-modes.md) | Application mode for production — one cluster per job, `main()` on the JobManager; the Kubernetes Operator's `upgradeMode: savepoint` is what makes upgrades safe |
| 58 | [Web UI and monitoring](58-web-ui-and-monitoring.md) | Overview → graph → backpressure → checkpoints → TaskManagers, in that order; plus the five alerts that catch real incidents |
| 59 | [Tuning: parallelism, memory, checkpoints](59-tuning-parallelism-memory-checkpoints.md) | `maxParallelism` is permanent; **RocksDB uses MANAGED memory, not heap**; min-pause is what stops the checkpoint death spiral |
| 60 | [Testing Flink jobs](60-testing-flink-jobs.md) | Extract `buildPipeline(env, source, sink)` out of `main()`, then test event time deterministically with `processWatermark` — no sleeping, ever |
| 61 | [**Production checklist**](61-production-checklist.md) | **The go-live list, plus the top 10 mistakes.** The dangerous failures are the silent ones |

---

## The eight things that matter most

If you remember nothing else from this phase:

1. **`uid()` on every stateful operator, from day one.** Without it, adding one `filter()` orphans all downstream state and the job restarts cleanly with nothing in it.
2. **TTL or timer cleanup on every unbounded keyspace.** This is the "it was perfect for three weeks" bug.
3. **Exactly-once is a property of the sink**, not of the checkpointing mode.
4. **Flink core is `provided`; connectors are not.** Verify with `jar tf` before you deploy, not after.
5. **RocksDB lives in managed memory.** `managed.fraction` is everything with RocksDB and irrelevant with `HashMapStateBackend`.
6. **`taskmanager.memory.process.size` must equal the container limit**, or you get `OOMKilled` with no Java exception and no stack trace.
7. **Never log per record.** It converts a stream processor into a log-shipping service.
8. **Alert on `records-lag-max` rising**, not on its absolute value. The slope is what tells you the job cannot recover on its own.

## Related notes in this repo

The parent folder holds the **operational / interview** view of the same systems. This phase is how you build and deploy; those are what you do when it breaks.

| File | Covers |
|---|---|
| [`../../01-checkpointing-slow.md`] | The full causal tree for slow or failing checkpoints, with the fix for each branch |
| [`../../02-backpressure.md`] | Finding the real bottleneck: the "first non-backpressured operator" rule, and the four causes |
| [`../../03-state-and-skew.md`] | Key groups, skew, slow recovery, rescaling, savepoints vs checkpoints |
| [`../../04-exactly-once.md`] | Delivery guarantees and matching your sink to them |
| [`../../05-watermarks-and-time.md`] | Event time, late data, and why windows don't fire |
| [`../../06-scale-arithmetic.md`] | Making "1M events/sec" concrete: the five numbers, the throughput table, the constants |

Sibling course: [`../../spark-mastery/README.md`] — the same treatment for Spark.
