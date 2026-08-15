# Phase 5 — Reliability (why Flink can survive a crash)

This is the phase that explains the promise you keep hearing: *"Flink recovers state after a crash and processes each event exactly once."* Now you'll understand the machinery.

The whole phase rests on one idea from Phase 3: **your state is managed by Flink.** Because Flink owns it, Flink can snapshot it and restore it.

---

## 1. Checkpoints — the core mechanism

A **checkpoint** is a periodic, **globally consistent** snapshot of *all* state in the job (every operator's keyed state, plus source offsets and sink info), written to durable storage.

```java
env.enableCheckpointing(60_000);   // checkpoint every 60 seconds
```

"Globally consistent" is the hard part. Flink uses the **Chandy–Lamport algorithm** with **barriers**:

1. The source injects a numbered **checkpoint barrier** into the stream, in-line with the data.
2. Barriers flow downstream with the records. When an operator receives a barrier on *all* its inputs (**barrier alignment**), it snapshots its state and forwards the barrier.
3. When the barrier reaches all sinks, checkpoint N is **complete** and acknowledged.

The result is a snapshot that represents the exact state *as if* the job had consumed a clean prefix of the input — even though different operators snapshotted at slightly different wall-clock moments.

**Config you'll actually set:**
```java
CheckpointConfig cp = env.getCheckpointConfig();
cp.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);   // default
cp.setMinPauseBetweenCheckpoints(30_000);   // don't start a new one until 30s after the last finished
cp.setCheckpointTimeout(120_000);           // fail a checkpoint that takes > 2 min
cp.setMaxConcurrentCheckpoints(1);
cp.setExternalizedCheckpointCleanup(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);  // keep for manual recovery
```

**Unaligned checkpoints** (`cp.enableUnalignedCheckpoints()`): under heavy backpressure, barrier alignment can stall. Unaligned checkpoints snapshot in-flight buffers instead of waiting for alignment — faster under backpressure, larger snapshots. Turn on if checkpoints time out under load.

---

## 2. Savepoints — checkpoints you own

A **savepoint** is a manually-triggered, self-contained snapshot meant for **operational** actions: upgrading your job, changing parallelism, migrating state, A/B'ing a new version.

| | Checkpoint | Savepoint |
|---|-----------|-----------|
| Triggered by | Flink, automatically | You, manually (CLI/REST) |
| Purpose | crash recovery | upgrades, migrations, rescaling |
| Lifecycle | Flink owns/cleans up | you own it forever |
| Format | optimized, may be incremental | portable, stable |

```bash
# take a savepoint (job keeps running)
flink savepoint <jobId> s3://bucket/savepoints

# stop the job WITH a savepoint (clean shutdown)
flink stop --savepointPath s3://bucket/savepoints <jobId>

# start a (possibly new version of the) job FROM a savepoint
flink run -s s3://bucket/savepoints/savepoint-abc123 my-job.jar
```

**Why this matters:** you can deploy a new version of your streaming job without losing the running balances/counts/fraud-state you built in Phase 3. Savepoint → stop → deploy new jar → resume from savepoint.

> Because state layout must survive across versions, give your stateful operators stable **UIDs**: `.uid("fraud-detector")`. Without UIDs, Flink can't match old state to new operators after a code change. **Set a `.uid()` on every stateful operator — it's a production must.**

---

## 3. Fault tolerance in action — recovery after a failure

When a subtask fails (machine dies, exception, OOM):

1. Flink **cancels** the whole job (or the affected region — see §5).
2. It **restarts** the operators.
3. Every operator **restores its state** from the last *completed* checkpoint.
4. Sources **rewind** to the offsets recorded in that checkpoint (Kafka `KafkaSource` stores offsets in the checkpoint).
5. Processing resumes from that consistent point.

Because sources rewind and state is restored to the matching point, **no data is lost** and (with exactly-once) **no effect is duplicated**.

---

## 4. Exactly-once vs at-least-once

This is the interview question. Be precise: it's about **effects**, not "each event is literally read once."

- **At-least-once** — after recovery, some records may be **reprocessed**, so downstream effects can be **duplicated**. Cheaper, lower latency. Fine when your sink is idempotent (upsert by key) or duplicates are tolerable.
- **Exactly-once** — the observable *effect* on state and (transactional) sinks is as if each event were processed once, even across failures. Achieved by:
  - **Internal state:** guaranteed by checkpointing + rewind (barrier alignment).
  - **Sources:** must be **replayable** (Kafka offsets in checkpoint ✅).
  - **Sinks:** must support **transactions / idempotency** — the **two-phase commit** protocol. The sink pre-commits on each checkpoint and commits when the checkpoint completes. Kafka sink with `DeliveryGuarantee.EXACTLY_ONCE` does exactly this (using Kafka transactions).

```java
// exactly-once end-to-end requires all three:
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // engine
// replayable source: KafkaSource ✅
KafkaSink.<String>builder()
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)     // transactional sink
    .setTransactionalIdPrefix("fraud-")                       // required for EOS
    .build();
```

**Gotcha:** exactly-once end-to-end adds latency (results are only visible after the checkpoint that commits them completes). If you need low latency and can dedupe downstream, at-least-once is often the pragmatic choice.

---

## 5. Restart strategies

What Flink does *when* a job fails. Configure in code or `flink-conf.yaml`.

```java
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;

// fixed delay: retry N times, waiting between attempts
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
        3,                       // attempts
        Time.seconds(10)));      // delay between them

// failure rate: allow up to N failures per interval, else give up
env.setRestartStrategy(RestartStrategies.failureRateRestart(
        3, Time.minutes(5), Time.seconds(10)));

// no restart: fail immediately (good for tests)
env.setRestartStrategy(RestartStrategies.noRestart());
```

- Default (when checkpointing is on) is a fixed-delay/exponential strategy.
- **Failover regions:** Flink only restarts the connected "region" of the graph that failed, not necessarily the whole job — cheaper recovery for embarrassingly-parallel pipelines. `keyBy`/rebalance edges define region boundaries.

---

## 6. Checkpoint storage & state backends

Two independent choices: **where running state lives** and **where snapshots are written**.

**State backend (where working state lives at runtime):**
- **HashMapStateBackend** — state as objects on the JVM heap. Fast, but bounded by memory. Good for small/medium state.
- **EmbeddedRocksDBStateBackend** — state in an on-disk RocksDB (with memory cache). Supports state **larger than memory** and **incremental checkpoints**. The production default for large state.

```java
env.setStateBackend(new EmbeddedRocksDBStateBackend(true));  // true = incremental checkpoints
```

**Checkpoint storage (where snapshots are durably written):**
```java
env.getCheckpointConfig().setCheckpointStorage("s3://bucket/checkpoints");
// local/dev: "file:///tmp/flink-checkpoints"
```
- Must be **durable & shared** across all TaskManagers (S3, HDFS, GCS, NFS) — a local disk path only works for single-node dev.
- **Incremental checkpoints** (RocksDB) upload only changed state — essential when state is large.

---

## 7. Mental model to keep

```
enableCheckpointing  →  periodic consistent snapshots  →  durable storage
        ↑                                                        │
   (barriers + alignment)                                        │ on failure
        │                                                        ▼
   sources rewind  ←────────  restore all operator state  ←──────┘
```

Exactly-once = this loop + replayable sources + transactional sinks.

---

### ✅ Phase 5 checklist

- [ ] Checkpoints: barriers, alignment, `enableCheckpointing`
- [ ] Savepoints + why every stateful op needs a `.uid()`
- [ ] Recovery flow (restore state + rewind sources)
- [ ] Exactly-once vs at-least-once (effects, 2-phase commit sinks)
- [ ] Restart strategies + failover regions
- [ ] State backends (HashMap vs RocksDB) & checkpoint storage
- [ ] Incremental + unaligned checkpoints (when to use)

⬅️ [Phase 4](04-realworld-streaming.md)  ·  ➡️ [Phase 6 — Advanced event processing](06-advanced-event-processing.md)
