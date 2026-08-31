# 30. Backpressure

Backpressure is what happens when a downstream operator cannot keep up and the slowdown propagates **upstream**, eventually all the way to the source. It is not a failure — it is the system protecting itself. But it is the single most common symptom you will chase in production, and the skill is not spotting it, it is **finding what caused it**.

## The problem it solves

```
 source produces 100k rec/s  ──►  sink writes 10k rec/s

 WITHOUT backpressure:
   90k records/second pile up somewhere.
   Queues grow → memory grows → OOM → job dies.

 WITH backpressure:
   The sink's slowness travels back up the chain until the SOURCE
   itself slows to 10k rec/s. Nothing accumulates in memory.
   Records accumulate in KAFKA instead — which is durable, on disk,
   and exactly what Kafka is for.
```

> **Key idea:** Backpressure moves the backlog from your **JVM heap** (fatal) to **Kafka's disk** (fine). A job under backpressure is running correctly and too slowly. A job without backpressure that is too slow just crashes.

---

## How Flink implements it: credit-based flow control

Flink does not use a timer, a rate limiter, or a sampling heuristic. The mechanism is **credit-based flow control**, built into the network stack since Flink 1.5, and you should be able to explain it.

Each producing subtask writes into **network buffers**; each consuming subtask has a pool of **exclusive buffers** per input channel plus a **floating buffer** pool shared across its channels.

```
      SENDER (upstream subtask)              RECEIVER (downstream subtask)
 ┌──────────────────────────────┐      ┌────────────────────────────────────┐
 │ result subpartition per      │      │ input channel per upstream subtask │
 │ downstream channel           │      │                                    │
 │                              │      │  exclusive buffers: [ ][ ]         │
 │  ch0 [B][B][B]  ──────────►  │      │  floating pool:     [ ][ ][ ][ ]   │
 │  ch1 [B]                     │      │                                    │
 │  ch2 [B][B]                  │  ◄───┤  CREDIT announcement:              │
 └──────────────────────────────┘      │  "channel 0 has 5 free buffers"    │
                                       └────────────────────────────────────┘

 A sender may transmit AT MOST `credit` buffers to that channel.
 Credit 0 → the sender transmits nothing to that channel. It stops.
```

The loop:

1. The receiver announces how many **free buffers** it has for each channel. That number is the **credit**.
2. The sender may send at most that many buffers to that channel.
3. Each buffer sent decrements the credit; each buffer consumed by the receiving task returns credit.
4. If a receiver is slow, it stops freeing buffers → credit hits **0** → the sender cannot send.
5. With nowhere to send, the sender's own output buffers fill up → its `requestBuffer()` call **blocks** → the sender's task thread stalls → its own credit to *its* upstream hits 0.

```
 sink slow
   → sink's buffers full
     → credit to window = 0 → window blocks on write
       → window's buffers full
         → credit to map = 0 → map blocks
           → map's buffers full
             → credit to source = 0 → source blocks
               → source stops polling Kafka
                 → Kafka consumer lag grows (durable, on disk)
```

Why *credit-based*, rather than just blocking on TCP as pre-1.5 Flink did:

- **One TCP connection is multiplexed across many logical channels.** Blocking the TCP connection would block *every* channel over it — including channels whose receivers are perfectly healthy. Credit-based flow control blocks only the affected channel.
- **Checkpoint barriers keep flowing** on the unblocked channels, so checkpointing degrades instead of dying.
- The receiver's credit announcement is a precise signal, not a guess.

---

## Detecting it in the Web UI

Since Flink 1.13 the UI shows per-task colour-coded ratios, sampled from the task threads:

| Metric | Meaning |
|---|---|
| **Busy** (red-ish) | Fraction of time the task is executing your code |
| **Backpressured** (black) | Fraction of time blocked waiting for an output buffer |
| **Idle** (blue) | Fraction of time waiting for input — nothing to do |

Underlying metrics if you scrape them:

```
backPressuredTimeMsPerSecond    0..1000 ms per second blocked on output
busyTimeMsPerSecond             0..1000 ms per second doing work
idleTimeMsPerSecond             0..1000 ms per second waiting for input
isBackPressured                 boolean, per subtask
```

Rules of thumb: `backPressuredTimeMsPerSecond` above ~500 (50%) is a real problem; sustained above 900 means you are almost entirely stalled. Brief spikes during a checkpoint or a rescale are normal.

There is also a **BackPressure** tab per operator with per-subtask detail — that is where you catch the case of *one* skewed subtask backpressuring everything, which the aggregated view hides.

---

## Finding the ROOT CAUSE — the one skill that matters

Backpressure propagates upstream, so **almost every operator shows it**. Reading the UI and saying "the source is backpressured" is useless — the source is the last victim, not the cause.

> **Key idea:** Walk the chain downstream. The **first operator that is NOT backpressured** is the bottleneck. It is not backpressured because nothing downstream is holding *it* back — its slowness is its own.

```
 Source ──► Parse ──► Enrich ──► Window ──► Sink
   BP        BP         BP        NOT BP    NOT BP
   100%      98%        95%       busy 100%  idle 60%
                                     ▲
                          ┌──────────┘
                          │ FIRST non-backpressured operator.
                          │ Busy 100% → it is CPU-bound on its own work.
                          │ THIS is the bottleneck. Everything upstream
                          │ is just reporting its symptom.

 The Sink is idle 60% → it is starved, waiting for the Window.
 Definitely not the problem.
```

The full diagnostic procedure:

```
1. Open the Web UI job graph. Find the operators showing backpressure.
2. Walk DOWNSTREAM until you find the first NOT-backpressured operator.
3. Look at ITS metrics:

   Busy ~100%, backpressured ~0%
        → CPU-bound in your code, or GC.
        → Check the TaskManager GC metrics before blaming the code.

   Busy LOW, backpressured ~0%, idle LOW
        → It is BLOCKING on something external and not counted as busy:
          a synchronous HTTP/DB lookup, a slow sink write, disk I/O.
        → This is the most common real cause.

   Only SOME subtasks busy, the rest idle
        → DATA SKEW. See chapter 29.

4. Check the per-subtask view before concluding anything. One hot
   subtask out of 32 backpressures the entire job.
```

Then, if it is the very last operator (the sink) that is backpressured, the bottleneck is **outside Flink**: the database, the Kafka cluster, the object store.

---

## The six causes, and what to do about each

### 1. Slow sink (the most common)

Symptom: the sink is the first non-backpressured operator, and it is not busy — it is waiting on the network or the external system.

Fixes:

- **Batch the writes.** One round trip per record is fatal. JDBC sinks take a batch size and interval; the Kafka sink already batches via `linger.ms` / `batch.size`.
- **Increase sink parallelism** — but check the external system can take the concurrency (connection pool limits, database write locks).
- **Check the target.** A saturated Elasticsearch cluster or an under-provisioned RDS is not a Flink problem.
- For Kafka: `linger.ms=50`, `batch.size=65536`, `compression.type=lz4` typically multiplies throughput.

### 2. A synchronous external lookup per record

```java
// BAD: a blocking HTTP call in the record path. At 5 ms per call and
// one thread per subtask, your ceiling is 200 records/second/subtask.
.map(e -> {
    UserProfile p = httpClient.get("/users/" + e.userId);  // blocks
    return enrich(e, p);
})
```

That is 200 rec/s per subtask regardless of CPU. The thread spends its life waiting.

Fixes, in order of preference:

- **`AsyncDataStream.unorderedWait(...)`** with an async client. Many requests are in flight concurrently on one thread; the ceiling becomes the external system's, not the thread's. This is the correct answer and it is worth a chapter of its own (Phase 6).
- **Cache** the lookups — an in-memory LRU, or `MapState` keyed by the entity.
- **Turn the lookup into a stream.** If the reference data is in a database, CDC it into Kafka and do a `connect()` + broadcast or a keyed join instead. No network call at all.

### 3. Data skew

Symptom: in the per-subtask view, one or two subtasks at 100% busy and the rest idle.

Fix: chapter 29 — salted keys, two-phase aggregation, or a better key. `rebalance()` if the skew is subtask-level rather than key-level.

### 4. GC pressure

Symptom: busy time is high but throughput is low; latency is spiky rather than steady; the TaskManager's GC metrics show long or frequent collections.

Fixes:

- **Reduce allocation in the hot path.** Do not allocate a new object per record if you can reuse one. `ObjectMapper` per record (chapter 27) is a classic.
- **Move state off-heap:** switch the state backend to RocksDB (`EmbeddedRocksDBStateBackend`), which stores state outside the JVM heap.
- **Fix Kryo fallbacks.** `GenericType<...>` in the logs means Kryo, which allocates far more than the POJO serializer.
- **Increase TaskManager memory**, or reduce slots per TaskManager so each task has more headroom.

### 5. Insufficient parallelism

Symptom: everything is uniformly busy at ~100%, no skew, no GC problem, the sink is fine. You are simply asking too few threads to do too much.

Fix: raise parallelism — bounded by `maxParallelism` (chapter 28) and by your Kafka partition count on the source side. If the topic has 12 partitions, source parallelism above 12 buys nothing; rebalance after the source and raise parallelism downstream instead.

### 6. RocksDB / disk

Symptom: a keyed stateful operator is the bottleneck; state size is large; the TaskManager shows high disk I/O and iowait.

Every RocksDB state access is a potential disk read plus serialization of the key and value. Fixes:

- **Use local SSD/NVMe**, never a network volume, for `state.backend.rocksdb.localdir`.
- **Increase the managed memory** given to RocksDB block cache and write buffers (`taskmanager.memory.managed.fraction`).
- **Reduce state access per record.** One `ValueState` read is far cheaper than iterating a `MapState`. Combine several small states into one object where possible.
- **Enable incremental checkpoints** — huge win for large RocksDB state.
- Reconsider whether the state is needed at all: a shorter TTL, a coarser window, a sketch instead of an exact set.

---

## Buffer debloating

Historically Flink sized network buffers for throughput, which meant a lot of data sitting in buffers. Under backpressure a checkpoint barrier had to travel through all of it, so checkpoints took forever.

**Buffer debloating** (Flink 1.14+) makes the buffer size adaptive: Flink measures throughput and shrinks the in-flight data to roughly one "target time" worth.

```yaml
# flink-conf.yaml
taskmanager.network.memory.buffer-debloat.enabled: true
taskmanager.network.memory.buffer-debloat.target: 1s   # default 1s
taskmanager.network.memory.buffer-debloat.period: 200ms
taskmanager.network.memory.buffer-debloat.samples: 20
```

The trade-off: less in-flight data means faster barrier propagation and shorter checkpoints, but slightly less absorption of short throughput bursts.

Turn it on when: you have backpressure **and** slow checkpoints. It does not fix the backpressure — it limits the damage.

### Manual buffer tuning

```yaml
# Total memory for network buffers
taskmanager.memory.network.fraction: 0.1     # of total TM memory
taskmanager.memory.network.min: 64mb
taskmanager.memory.network.max: 1gb

# Buffers per channel (exclusive) and per gate (floating)
taskmanager.network.memory.buffers-per-channel: 2      # default
taskmanager.network.memory.floating-buffers-per-gate: 8

# Size of one buffer
taskmanager.memory.segment-size: 32kb        # default
```

```java
// Max time a buffer waits before being flushed even if not full.
env.setBufferTimeout(100);   // ms, default 100
env.setBufferTimeout(0);     // flush immediately: lowest latency, worst throughput
env.setBufferTimeout(-1);    // only flush when full: max throughput, high latency
```

The classic failure is running out of network buffers entirely:

```
java.io.IOException: Insufficient number of network buffers:
  required 128, but only 64 available.
```

Required buffers scale with `parallelism²` on shuffle edges, so a high-parallelism job with many shuffles needs a bigger network memory fraction. That error is a sizing problem, not a bug.

---

## Backpressure and checkpoint duration — the classic incident

This connection is the one that turns "the job is a bit slow" into a 2am page.

A checkpoint works by injecting **barriers** at the sources; they flow with the records, and an operator snapshots when it has received a barrier on every input.

```
 NORMAL:
   source ──[barrier]──► map ──[barrier]──► window ──[barrier]──► sink
   barrier travels at record speed. Checkpoint: 2 seconds.

 UNDER BACKPRESSURE:
   source ──[b]─ [1M records queued] ─────► map ─── [queued] ───► ...
                       ▲
             The barrier is BEHIND a million buffered records and
             cannot overtake them (aligned checkpointing preserves order).
   Checkpoint: 4 minutes. Or it times out and FAILS.
```

The death spiral:

```
 1. Backpressure appears.
 2. Barriers crawl. Checkpoint duration goes from 2 s to 4 min.
 3. Checkpoints start timing out (execution.checkpointing.timeout,
    default 10 min) and FAIL.
 4. Enough consecutive failures → the job restarts
    (execution.checkpointing.tolerable-failed-checkpoints).
 5. On restart the job replays from the LAST SUCCESSFUL checkpoint —
    which is now old — so it must catch up on a big backlog.
 6. Catching up means running at max throughput, which causes MORE
    backpressure than steady state.
 7. Go to 2.
```

The job never recovers on its own. Every symptom points at checkpointing; the actual cause is a slow sink or a hot key.

Diagnostic tell: in the Checkpoints tab, look at **Alignment Duration** and **Start Delay** per subtask.

- **High Start Delay** = the barrier took a long time to arrive = backpressure upstream.
- **High Alignment Duration** = this operator waited a long time for barriers on its other inputs = skew across inputs.
- High **Sync/Async duration** instead = the snapshot itself is slow = a state backend / storage problem, *not* backpressure.

That distinction — Start Delay vs Async Duration — is exactly how you tell "slow checkpoints because of backpressure" from "slow checkpoints because of the state backend".

**Unaligned checkpoints** (Flink 1.11+) attack this directly: the barrier is allowed to **overtake** buffered records, and the overtaken in-flight data is written into the checkpoint instead. Checkpoint duration stops depending on backpressure.

```java
env.getCheckpointConfig().enableUnalignedCheckpoints();
```

The cost is a larger checkpoint (it now contains in-flight data) and some restrictions. This is a **Phase 5** topic — checkpointing, alignment, savepoints, and recovery get proper treatment there. For now, know that it exists and that it is the standard mitigation for backpressure-induced checkpoint failure.

> **Key idea:** Backpressure delays **barriers**, and delayed barriers mean long checkpoints, then failed checkpoints, then a restart with a stale checkpoint and a bigger backlog. Fixing the checkpoint timeout does not fix this. Fix the bottleneck.

---

## A worked diagnosis

```
Alert: "checkpoint duration > 5 minutes, 3 consecutive failures"

1. Web UI job graph:
     Source(BP 97%) → Parse(BP 95%) → Enrich(BP 91%) → Window(BP 88%)
       → JdbcSink(BP 0%, busy 12%, idle 3%)

2. First non-backpressured operator: JdbcSink.
   Busy is only 12%, idle only 3% → it is neither computing nor starved.
   It is BLOCKED on something not counted as busy → the database.

3. Per-subtask view: all 8 sink subtasks look identical → not skew.

4. Checkpoints tab: Start Delay is 4m50s, Async Duration is 900ms.
   → the barrier is stuck behind buffered records. Backpressure, not
     a state backend problem. Consistent with the diagnosis.

5. Database metrics: p99 insert latency 45 ms, and the sink is
   writing ONE ROW PER RECORD.
     8 subtasks × (1000/45) ≈ 178 rows/s.  Input is 20,000/s.

FIX
  a) Batch the JDBC writes (batch size 500, interval 1s)   → ~50x
  b) Raise sink parallelism 8 → 16 (pool has room)         → 2x
  c) Turn on buffer debloating + unaligned checkpoints so a
     future backlog doesn't kill checkpoints again
  d) Alert on backPressuredTimeMsPerSecond > 500, not just on
     checkpoint failures — catch it before the spiral
```

---

## Remember

- Backpressure = downstream slowness propagating upstream to the source. It is **protection**, not a bug: the backlog moves from your heap to Kafka's disk.
- Mechanism: **credit-based flow control**. The receiver announces free buffers as credit; zero credit stops the sender. Per-channel, so one slow channel does not block a shared TCP connection.
- UI signals: **Busy / Backpressured / Idle**, and `backPressuredTimeMsPerSecond`. Over 50% sustained is a problem.
- **Find the root cause by walking downstream: the first NON-backpressured operator is the bottleneck.**
- Busy ~100% → CPU or GC. Busy low and not idle → blocked on something external. Some subtasks busy → skew.
- Causes: slow sink, per-record synchronous lookup, skew, GC, too little parallelism, RocksDB on slow disk.
- Fixes: batch writes, `AsyncDataStream`, salted keys, reduce allocation, more parallelism, local SSD + incremental checkpoints.
- **Buffer debloating** (`taskmanager.network.memory.buffer-debloat.enabled`) caps in-flight data so barriers travel faster.
- Backpressure → slow barriers → long checkpoints → failed checkpoints → restart with a stale checkpoint → a bigger backlog → worse backpressure. The death spiral.
- **Start Delay high = backpressure. Async Duration high = state backend.** Different problems.
- **Unaligned checkpoints** let barriers overtake buffered data. Phase 5.

**Interview one-liners**

- *"How does Flink implement backpressure?"* → Credit-based flow control in the network stack: the receiver advertises free buffers as credit, and a sender with zero credit blocks. It is per-channel, so one blocked channel doesn't stall others sharing the TCP connection.
- *"Everything is backpressured — where's the problem?"* → The first operator downstream that is *not* backpressured. Backpressure propagates upstream, so everything before the bottleneck shows the symptom.
- *"Operator not backpressured, not busy, not idle — what's it doing?"* → Blocking on an external system. A synchronous DB or HTTP call, or a slow sink write.
- *"Why did backpressure fail my checkpoints?"* → Barriers travel with the records, so they queue behind the backlog. Start Delay explodes, the checkpoint times out, repeated failures restart the job, and the restart has an even bigger backlog.
- *"Start Delay vs Async Duration?"* → Start Delay means the barrier arrived late (backpressure upstream). Async Duration means writing the snapshot is slow (state backend/storage).
- *"What does buffer debloating do?"* → Sizes network buffers to about one second of throughput instead of a fixed amount, so less in-flight data and much faster barrier propagation under backpressure.
- *"How do you fix a per-record HTTP lookup?"* → `AsyncDataStream.unorderedWait` with an async client, plus caching, or replace the lookup entirely with a broadcast/CDC stream.
