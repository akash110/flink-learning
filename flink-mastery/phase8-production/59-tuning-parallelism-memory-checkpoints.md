# 59. Tuning: Parallelism, Memory, Checkpoints

Three dials. Turning them without understanding the mechanism is how people spend a week making a job slower.

> **Key idea**
> **Parallelism** decides how much work happens at once. **Memory** decides whether that work fits. **Checkpoint config** decides how much of your throughput you spend on durability.
> They interact: more parallelism means more TaskManagers means smaller per-slot managed memory means RocksDB spills more.

---

## Part 1: Parallelism

### The four constraints, in order

```
                  ┌─────────────────────────────────────┐
   1. SOURCE      │ parallelism ≤ Kafka partitions      │  hard ceiling for the source
                  └─────────────────────────────────────┘
                  ┌─────────────────────────────────────┐
   2. THROUGHPUT  │ parallelism ≥ rate ÷ per-subtask    │  hard floor
                  └─────────────────────────────────────┘
                  ┌─────────────────────────────────────┐
   3. KEY GROUPS  │ parallelism ≤ maxParallelism        │  permanent ceiling
                  │ and ideally DIVIDES maxParallelism  │
                  └─────────────────────────────────────┘
                  ┌─────────────────────────────────────┐
   4. SLOTS       │ total slots ≥ max parallelism       │  or the job never schedules
                  └─────────────────────────────────────┘
```

### Constraint 1: Kafka partitions bound the source

A Kafka partition is consumed by exactly one subtask. Set source parallelism to 20 against a 12-partition topic and 8 subtasks get nothing — forever.

```
12 partitions, source parallelism 20:

  subtask 0..11  → 1 partition each
  subtask 12..19 → NOTHING
                   ↑ idle forever, and each one holds its watermark at
                     Long.MIN_VALUE, which freezes event time for the
                     whole job unless you set withIdleness. See ch. 58.
```

The rule: **source parallelism ≤ partition count**, and ideally it **divides** the partition count evenly. 12 partitions with parallelism 5 gives some subtasks 3 partitions and some 2 — a permanent 50% imbalance. Parallelism 6 or 12 is even.

This bounds only the source. You can `rebalance()` after it and run the rest of the job wider:

```java
// Source at 12 (= partitions), heavy processing at 48.
// The rebalance() costs a full network shuffle - only worth it when the
// downstream work genuinely dominates the serialization cost.
env.fromSource(kafkaSource, watermarkStrategy, "kafka").setParallelism(12)
   .rebalance()
   .map(new ExpensiveEnrichment()).setParallelism(48);
```

If the source itself is your bottleneck, **add Kafka partitions** — but note you cannot reduce them later, and adding them changes key→partition mapping for new records.

### Constraint 2: throughput per subtask

```
required parallelism = target throughput ÷ per-subtask throughput
```

Per-subtask throughput depends entirely on what the operator does. The table in [`../../06-scale-arithmetic.md`] has the numbers; the two that matter most:

```
simple map/filter, POJO serde  →  100k – 1M records/sec/subtask
blocking I/O at 20ms           →  50 records/sec/subtask       ← four orders of magnitude
```

Worked: 500k events/sec through a RocksDB-backed keyed process function on local NVMe (~50k/subtask) → 500k ÷ 50k = **10 subtasks minimum**. Then **add headroom**: a job running at exactly 100% of the incoming rate can never drain a backlog. Size for ~2× peak, so **20**.

That headroom rule is the one people skip and then cannot explain why a 3-hour outage takes 3 days to recover from.

### Constraint 3: key groups and maxParallelism

```
key → hash(key) → key group (0 .. maxParallelism-1) → subtask
```

`maxParallelism` is **fixed at first run and cannot be changed without discarding state.** Defaults: 128 for parallelism ≤ 128, otherwise roughly `min(nextPowerOfTwo(parallelism * 1.5), 32768)`.

```java
// Set this DELIBERATELY, at job creation, before the first run.
env.setMaxParallelism(1024);
```

Two consequences:

- **You can never scale beyond `maxParallelism`.** Leaving it at the default 128 permanently caps you at 128 subtasks.
- **Parallelism should divide `maxParallelism`.** With `maxParallelism=128` and parallelism 10, subtasks get 12 or 13 key groups — a built-in ~8% imbalance you get for free, forever.

Pick a power of two you will never exceed and that your parallelisms divide: **1024** or **4096** are sensible. Absurdly high values add metadata overhead and slow recovery. Full treatment in [`../../03-state-and-skew.md`].

**And the one that surprises everyone: more parallelism does not fix skew.** A hot key lives in exactly one key group forever. Doubling parallelism gives the hot key's subtask the same hot key.

### Constraint 4: slots

```
slots needed = max operator parallelism        (thanks to slot sharing)
TaskManagers = slots needed ÷ taskmanager.numberOfTaskSlots
```

Slot sharing means one slot holds one subtask of *each* operator in a job, so a `source → map → sink` job at parallelism 8 needs 8 slots, not 24. Too few slots and the job sits in `CREATED` with `NoResourceAvailableException`.

### Setting it

```java
env.setParallelism(8);                     // job default
stream.map(...).setParallelism(16);        // one operator (creates a shuffle!)
sink.setParallelism(1);                    // force a single output file
env.setMaxParallelism(1024);               // key groups. SET THIS ONCE, FOREVER.
```

Precedence: **operator-level > `env.setParallelism()` > `-p` on the CLI > `parallelism.default`**. If `-p` "does nothing", someone hardcoded `env.setParallelism()`.

**Watch for the accidental shuffle:** changing parallelism between two adjacent operators inserts a `REBALANCE` edge, breaking the chain and adding serialize/network/deserialize cost you did not ask for.

### The rescale procedure

You cannot change the parallelism of a running job. You stop it, restart it wider or narrower.

```bash
# 1. Confirm the new parallelism ≤ maxParallelism. If not, STOP -
#    you cannot rescale and must migrate state (a much bigger job).
curl -s $JM/jobs/$JID | jq '.vertices[] | {name, maxParallelism}'

# 2. Stop with a savepoint (graceful; no drain, so windows don't fire early)
./bin/flink stop --no-drain \
    --savepointPath s3://bucket/savepoints/fraud <jobId>
#    → prints s3://bucket/savepoints/fraud/savepoint-4a3f00-1c9e2f

# 3. Make sure enough slots exist for the NEW parallelism, or the restore
#    will sit in CREATED. Scale the TaskManager count first.

# 4. Restart at the new parallelism from that savepoint.
./bin/flink run -d -p 24 \
    -s s3://bucket/savepoints/fraud/savepoint-4a3f00-1c9e2f \
    ./fraud-detection-1.0.0.jar --env prod

# 5. Verify: restarts 0, first checkpoint completes, lag falling,
#    lastCheckpointFullSize comparable to before. (ch. 57 checklist)
```

On the Kubernetes Operator this is `spec.job.parallelism: 24` plus `kubectl apply`, with `upgradeMode: savepoint` doing steps 2–4.

**What actually happens on restore:** Flink redistributes whole **key groups** across the new subtask count. Each new subtask reads the key groups it now owns from the savepoint. This works only because of the key-group indirection — hashing keys directly to subtasks would make rescaling impossible.

Restore is not free: each subtask downloads its share of state from S3 and rebuilds RocksDB. 200 GB across 24 subtasks is ~8 GB each. Test this in staging before you need it.

---

## Part 2: Memory

### The TaskManager memory model

The single most confusing thing in Flink, and the reason for most `Container killed by YARN`/`OOMKilled` incidents. Learn this diagram.

```
┌──────────────────────────────────────────────────────────────────────┐
│ TOTAL PROCESS MEMORY          taskmanager.memory.process.size        │
│ = what the container/cgroup limit must be                            │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ TOTAL FLINK MEMORY        taskmanager.memory.flink.size        │  │
│  │                                                                │  │
│  │  ┌──────────────────────────┐  ┌──────────────────────────┐    │  │
│  │  │ JVM HEAP                 │  │ OFF-HEAP                 │    │  │
│  │  │                          │  │                          │    │  │
│  │  │ ┌──────────────────────┐ │  │ ┌──────────────────────┐ │    │  │
│  │  │ │ Framework Heap       │ │  │ │ Framework Off-Heap   │ │    │  │
│  │  │ │  .framework.heap.size│ │  │ │ .framework.off-heap. │ │    │  │
│  │  │ │  default 128m        │ │  │ │  size, default 128m  │ │    │  │
│  │  │ └──────────────────────┘ │  │ └──────────────────────┘ │    │  │
│  │  │ ┌──────────────────────┐ │  │ ┌──────────────────────┐ │    │  │
│  │  │ │ TASK HEAP            │ │  │ │ Task Off-Heap        │ │    │  │
│  │  │ │  .task.heap.size     │ │  │ │  .task.off-heap.size │ │    │  │
│  │  │ │  ← YOUR objects,     │ │  │ │  default 0           │ │    │  │
│  │  │ │    HashMapStateBackend│ │ │ └──────────────────────┘ │    │  │
│  │  │ └──────────────────────┘ │  │ ┌──────────────────────┐ │    │  │
│  │  └──────────────────────────┘  │ │ NETWORK              │ │    │  │
│  │                                │ │  .network.fraction   │ │    │  │
│  │  ┌──────────────────────────┐  │ │  /.min /.max         │ │    │  │
│  │  │ MANAGED MEMORY (off-heap)│  │ │  ← shuffle buffers   │ │    │  │
│  │  │  .managed.size /         │  │ └──────────────────────┘ │    │  │
│  │  │  .managed.fraction       │  └──────────────────────────┘    │  │
│  │  │  ← ROCKSDB LIVES HERE    │                                  │  │
│  │  └──────────────────────────┘                                  │  │
│  └────────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────┐  ┌──────────────────────────────────────┐  │
│  │ JVM METASPACE        │  │ JVM OVERHEAD                         │  │
│  │ .jvm-metaspace.size  │  │ .jvm-overhead.fraction /.min /.max    │ │
│  │ default 256m         │  │ default 0.1, min 192m, max 1g        │  │
│  │ ← CLASS metadata     │  │ ← thread stacks, code cache, GC data │  │
│  └──────────────────────┘  └──────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

The arithmetic:

```
Total Process = Total Flink Memory + JVM Metaspace + JVM Overhead

Total Flink   = Framework Heap + Task Heap          (the JVM heap, -Xmx)
              + Framework Off-Heap + Task Off-Heap
              + Managed Memory
              + Network Memory                       (all off-heap / direct)
```

> **Key idea**
> **`taskmanager.memory.process.size` must equal your container memory limit.** Everything else Flink derives from it. If the container limit is 8 GB and process.size is 8 GB, Flink sizes the heap so the *total* stays under 8 GB — including the off-heap parts the JVM does not count. Set process.size larger than the container limit and Kubernetes kills the pod with `OOMKilled`, which looks like a Flink bug and is not.

### The keys you actually set

```yaml
# ---- Set exactly ONE of these. process.size is the right one on K8s/YARN. ----
taskmanager.memory.process.size: 8192m     # = the container limit
# taskmanager.memory.flink.size: 6g        # standalone alternative:
                                           # excludes metaspace + overhead

# ---- Managed memory: the RocksDB dial ----
taskmanager.memory.managed.fraction: 0.4   # default 0.4 = 40% of Total Flink Memory
# taskmanager.memory.managed.size: 3g      # absolute; overrides the fraction

# ---- Network buffers ----
taskmanager.memory.network.fraction: 0.1   # default 0.1 of Total Flink Memory
taskmanager.memory.network.min: 64mb
taskmanager.memory.network.max: 1gb

# ---- JVM areas ----
taskmanager.memory.jvm-metaspace.size: 256m
taskmanager.memory.jvm-overhead.fraction: 0.1
taskmanager.memory.jvm-overhead.min: 192m
taskmanager.memory.jvm-overhead.max: 1gb

# ---- Slots ----
taskmanager.numberOfTaskSlots: 4           # managed memory is split EVENLY per slot
```

That last line is important: **managed memory is divided evenly among slots.** 4 GB managed with 4 slots gives each subtask 1 GB of RocksDB budget. Doubling slots halves each subtask's RocksDB memory.

### The rule that changes everything: RocksDB uses MANAGED memory

> **Key idea**
> **RocksDB's block cache and write buffers are allocated from Flink's MANAGED memory, not the JVM heap.**
> Therefore: `taskmanager.memory.managed.fraction` matters **enormously** with RocksDB and **not at all** with `HashMapStateBackend`.

| | `HashMapStateBackend` | `EmbeddedRocksDBStateBackend` |
|---|---|---|
| State lives in | **JVM heap** (Task Heap) | **Managed memory** (off-heap, native) + local disk |
| Dial that matters | `taskmanager.memory.task.heap.size` | `taskmanager.memory.managed.fraction` |
| `managed.fraction` | **wasted memory** — set it low (0.0–0.1) | **critical** — 0.4–0.7 |
| Fails with | `OutOfMemoryError: Java heap space` | spilling to disk, then slow; or native OOM |
| GC impact | large state = huge heap = long GC pauses | almost none — off-heap |
| Scale limit | a few GB per subtask before GC ruins you | limited by local disk |

```yaml
# HashMapStateBackend: give the heap everything, reclaim managed memory.
state.backend.type: hashmap
taskmanager.memory.managed.fraction: 0.05

# RocksDB: managed memory is the performance dial. This is the common case.
state.backend.type: rocksdb
state.backend.incremental: true
taskmanager.memory.managed.fraction: 0.5

# ⚠️ THE CLASSIC PRODUCTION LANDMINE: the default RocksDB dir is /tmp,
#    which is often a small tmpfs (in RAM!) or a slow root volume.
#    Pointing this at real local NVMe is frequently the single biggest
#    performance fix on a struggling job.
state.backend.rocksdb.localdir: /mnt/nvme/rocksdb
```

Flink's `RocksDBMemoryControllerUtils` caps total RocksDB memory at the managed budget across all its column families, so RocksDB cannot silently overrun the container. Leave `state.backend.rocksdb.memory.managed: true` (the default) alone — turning it off means you must size every RocksDB knob by hand.

### Inspecting what Flink actually chose

The TaskManager log at startup prints the full breakdown. This is the fastest way to check a config.

```bash
grep -A 20 "Final TaskExecutor Memory configuration" \
    log/flink-*-taskexecutor-*.log
```

Or `bin/taskmanager.sh start-foreground` locally, or the **TaskManagers → Metrics** tab in the Web UI.

### Diagnosing OOM — four different errors, four different fixes

```
"OutOfMemoryError: Java heap space"
  └─► TASK HEAP. Your objects, or HashMapStateBackend state.
      Fixes: switch to RocksDB; find the accumulating collection;
             raise taskmanager.memory.task.heap.size;
             stop buffering whole windows with process() (use aggregate()).

"OutOfMemoryError: Metaspace"
  └─► CLASS metadata, not data. Almost always a CLASSLOADER LEAK from
      repeatedly submitting jobs into a SESSION cluster - each submission
      loads a new copy of your classes and something (a static field, a
      JDBC driver, a thread-local) pins the old classloader.
      Fixes: raise taskmanager.memory.jvm-metaspace.size (256m → 512m);
             use APPLICATION mode so the JVM dies with the job (ch. 57);
             check classloader.parent-first-patterns for leaky libraries.

"OutOfMemoryError: Direct buffer memory"
  └─► NETWORK BUFFERS or a native library using direct ByteBuffers.
      Fixes: raise taskmanager.memory.network.fraction/max;
             reduce parallelism (buffers scale with the number of channels,
             which is roughly parallelism², so a 200-way shuffle is expensive);
             lower taskmanager.network.memory.buffers-per-channel.

"Container killed by YARN / OOMKilled by Kubernetes"   ← NO Java exception
  └─► Total process exceeded the CONTAINER limit. The JVM heap was fine;
      something off-heap grew: RocksDB native memory, direct buffers,
      thread stacks, or a JNI library.
      Fixes: make taskmanager.memory.process.size EQUAL the container limit;
             raise jvm-overhead.fraction if you have many threads;
             verify RocksDB managed memory is enabled (default true).
```

The last one is the most confusing: **there is no Java exception and no stack trace**, because the OS killed the process. If you see a pod restart with exit code 137 and a clean log, this is it.

### A worked sizing example

Target: 500k events/sec, 1 KB events, 100M keys × 1 KB state = **100 GB total state**.

```
State size 100 GB              → must be RocksDB. Will not fit on any sane heap.
Per-subtask throughput ~50k    → 500k ÷ 50k = 10 subtasks, ×2 headroom = 20
20 subtasks ÷ 4 slots/TM       → 5 TaskManagers
100 GB ÷ 20 subtasks           → 5 GB of state per subtask (on local disk)

Per TaskManager:
  taskmanager.memory.process.size: 16384m       # = container limit
  taskmanager.numberOfTaskSlots: 4              # ~1 CPU core each → 4+ cores
  taskmanager.memory.managed.fraction: 0.5      # ≈ 7 GB managed
                                                #  ÷ 4 slots ≈ 1.75 GB RocksDB each
  Local NVMe: 4 slots × 5 GB × 2.5 (compaction headroom) ≈ 50 GB, round to 100 GB

  Heap left over ≈ 6 GB per TM → 1.5 GB per slot for your objects. Plenty,
  because RocksDB keeps state OFF the heap.
```

Note the **2.5× disk headroom**: RocksDB compaction rewrites SST files and needs room for both copies. Sizing local disk at exactly your state size guarantees a disk-full incident during compaction. The rest of this arithmetic is in [`../../06-scale-arithmetic.md`].

**And keep heaps small.** 4–8 GB per TaskManager, not 32. A full GC on a 32 GB heap is 1–10 seconds and looks exactly like backpressure in the UI.

---

## Part 3: Checkpoint tuning

### Interval vs min-pause — the distinction people get wrong

```java
env.enableCheckpointing(60_000);   // start a checkpoint every 60s

CheckpointConfig cp = env.getCheckpointConfig();
cp.setMinPauseBetweenCheckpoints(30_000);
```

```
INTERVAL is measured from START to START.
MIN PAUSE is measured from END to START.

interval=60s, min-pause=30s, checkpoint takes 10s:
  |--CP1(10s)--|.........50s.........|--CP2(10s)--|
  ↑ start                            ↑ start = 60s later. Interval governs.

interval=60s, min-pause=30s, checkpoint takes 55s:
  |--------CP1 (55s)--------|--30s--|--------CP2--------|
                                    ↑ start = 85s later.
                            MIN PAUSE governs: the job gets a guaranteed
                            30 seconds of undisturbed processing.
```

> **Key idea**
> **`minPauseBetweenCheckpoints` is your protection against the checkpoint death spiral.** Without it, a job whose checkpoints take longer than the interval starts a new checkpoint the instant the old one finishes, permanently. It spends all its time checkpointing, falls further behind, checkpoints get slower still. Set min-pause to roughly **half your interval** and the job can always make progress.

```java
cp.setCheckpointTimeout(600_000);              // 10 min. Fail rather than hang forever.
cp.setMaxConcurrentCheckpoints(1);             // keep at 1 unless you know why not
cp.setTolerableCheckpointFailureNumber(3);     // don't fail the JOB on one bad checkpoint
cp.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// Keep the checkpoint when the job is cancelled — otherwise cancelling
// destroys your only recovery point. This should be on in every prod job.
cp.setExternalizedCheckpointRetention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
```

Note `maxConcurrentCheckpoints > 1` and `minPauseBetweenCheckpoints > 0` are mutually exclusive — min-pause forces effectively one at a time.

### Incremental checkpoints — turn them on with RocksDB

```yaml
state.backend.type: rocksdb
state.backend.incremental: true
```

```java
// programmatic equivalent
conf.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
```

RocksDB SST files are **immutable once written**, so a checkpoint only needs to upload the SST files created since the last one. 200 GB of state with a 2 GB delta uploads 2 GB, not 200 GB.

The trade: **restore gets slower.** A long incremental chain means downloading many small SST files, and at 50,000 files S3 request latency dominates — this is the "restart takes 40 minutes" case in [`../../03-state-and-skew.md`]. Mitigate with `state.backend.local-recovery: true`, which keeps a copy on local disk and skips the S3 download when the TaskManager comes back on the same machine.

Incremental is **RocksDB only**. `HashMapStateBackend` always writes a full snapshot.

### Unaligned checkpoints

```java
cp.enableUnalignedCheckpoints();

// Better: the HYBRID. Start aligned; switch to unaligned only if
// alignment exceeds the timeout. Best of both.
cp.setAlignedCheckpointTimeout(Duration.ofSeconds(30));
```

Unaligned lets the barrier overtake queued records and snapshots the in-flight buffers instead of waiting for alignment. Duration stops depending on backpressure; checkpoints get bigger and restore gets slower. Requires `EXACTLY_ONCE` mode. The mechanism is in `phase5-reliability/32-checkpoints-how-they-work.md`.

**It is not a fix for an overloaded job.** It stops backpressure from *breaking* checkpointing; it does not stop the backpressure, and your lag still grows.

### The decision tree

```
Are checkpoints failing or slow?
│
├─ NO ──► Are you paying too much for durability?
│         (checkpoint duration > ~10% of the interval)
│         └─► lengthen the interval, enable incremental, keep min-pause.
│
└─ YES ─► Open Checkpoints → History → expand a row (ch. 58).
          Which column is large?
          │
          ├─ START DELAY large ──────► BACKPRESSURE.
          │   The barrier is queued behind data.
          │   ✗ Do NOT tune checkpoint config. It is the victim.
          │   ✓ Fix the bottleneck: ../../02-backpressure.md
          │   ✓ Band-aid: setAlignedCheckpointTimeout(30s)
          │
          ├─ ALIGNMENT large ────────► SKEW or one slow path.
          │   ✓ Fix the skew: ../../03-state-and-skew.md
          │   ✓ Band-aid: unaligned / hybrid checkpoints
          │
          ├─ SYNC large ─────────────► STATE BACKEND or TIMER COUNT.
          │   ✓ HashMapStateBackend with a big heap → move to RocksDB
          │   ✓ Millions of timers → coalesce them to second granularity
          │
          └─ ASYNC large ────────────► STATE SIZE or STORAGE THROUGHPUT.
              ✓ state.backend.incremental: true
              ✓ Is the state supposed to be this big? TTL? ../../01-checkpointing-slow.md
              ✓ Object store throttling → check for 503s / SlowDown in the TM log
              ✓ Too many tiny files → raise state.storage.fs.memory-threshold (default 20kb)
```

The single most important branch: **Start Delay large means you have a backpressure problem, not a checkpointing problem.** Tuning checkpoint config there makes things worse. Full causal tree in [`../../01-checkpointing-slow.md`].

### Interval: how to pick one

Trade-offs in both directions.

**Shorter interval** → less data reprocessed after a failure, but more checkpointing overhead, and with a 2PC exactly-once sink, **lower end-to-end latency** because output is only visible at commit time.

**Longer interval** → cheaper, but more replay on recovery, and higher latency to a transactional sink.

```
Reasonable starting points:

  small state (<1 GB), latency-sensitive    →  interval 10s,  min-pause 5s
  medium state (1–50 GB)                    →  interval 60s,  min-pause 30s
  large state (>50 GB), throughput-oriented →  interval 5min, min-pause 2min

Then measure lastCheckpointDuration and adjust so that
    duration < 10% of interval.
Duration approaching the interval means you are on the death spiral.
```

> **Key idea**
> With a transactional exactly-once sink, **your checkpoint interval is a hard floor on end-to-end latency**, because output is not visible until the checkpoint commits the transaction. You cannot have 100 ms latency and 60 s checkpoints. Those requirements are mutually exclusive — say so rather than trying to satisfy both.

### The restart strategy is part of checkpoint tuning

```yaml
restart-strategy.type: exponential-delay
restart-strategy.exponential-delay.initial-backoff: 10s
restart-strategy.exponential-delay.max-backoff: 5min
restart-strategy.exponential-delay.backoff-multiplier: 2.0
restart-strategy.exponential-delay.reset-backoff-threshold: 10min
restart-strategy.exponential-delay.jitter-factor: 0.1
```

A fixed-delay strategy with a zero delay hammers S3 with restore requests and never recovers. Exponential backoff with jitter gives the underlying problem time to clear.

---

## Part 4: Autoscaling with the Kubernetes Operator

The Flink Kubernetes Operator (ch. 57) ships an **autoscaler** that reads the job's own metrics — true processing rate, busy time, backpressure — and rewrites per-vertex parallelism, then triggers a savepoint-based redeploy.

```yaml
spec:
  flinkConfiguration:
    job.autoscaler.enabled: "true"
    job.autoscaler.stabilization.interval: "1m"    # ignore metrics right after a scale
    job.autoscaler.metrics.window: "10m"           # averaging window for decisions
    job.autoscaler.target.utilization: "0.6"       # aim for 60% busy → 40% headroom
    job.autoscaler.target.utilization.boundary: "0.2"
    job.autoscaler.scale-up.grace-period: "1h"
    job.autoscaler.restart.time: "5m"              # expected redeploy cost
    job.autoscaler.vertex.max-parallelism: "200"

    # Required: the autoscaler needs per-vertex source metrics
    pipeline.max-parallelism: "1024"
```

Four caveats worth knowing before you turn it on:

1. **Every scaling decision is a restart** — stop-with-savepoint, redeploy, restore. On a large-state job that is minutes of downtime. `job.autoscaler.restart.time` tells the autoscaler how expensive that is so it does not thrash.
2. **`pipeline.max-parallelism` still caps everything.** The autoscaler cannot exceed your key groups, and it works best when it can pick divisors of maxParallelism.
3. **It cannot fix skew.** It sees an operator as slow and adds parallelism; a hot key stays on one subtask. You get more cost and the same lag.
4. Start in observe-only mode (`job.autoscaler.scaling.enabled: "false"`) and read its recommendations for a week before letting it act.

---

## Remember

- Parallelism is bounded by **Kafka partitions** (source), **maxParallelism** (permanently), and **slots**; and floored by **throughput ÷ per-subtask rate**, with **~2× headroom** so you can drain a backlog.
- **`maxParallelism` is fixed at first run.** Set it deliberately (1024/4096). Parallelism should **divide** it, or you get built-in skew.
- **More parallelism never fixes a hot key** — it lives in one key group forever.
- Rescale = **stop-with-savepoint → check slots → restart with `-p N` from the savepoint → verify**.
- Memory: **Total Process = JVM Overhead + Metaspace + Total Flink Memory**, and Total Flink = Framework/Task Heap + Framework/Task Off-Heap + **Managed** + **Network**.
- **`taskmanager.memory.process.size` must equal the container limit.** Larger and you get `OOMKilled` with no Java exception.
- **RocksDB uses MANAGED memory.** `managed.fraction` is critical with RocksDB (0.4–0.7) and pure waste with `HashMapStateBackend` (set ~0.05).
- **Managed memory is split evenly per slot.** More slots per TM = less RocksDB memory each.
- Point `state.backend.rocksdb.localdir` at real NVMe. The `/tmp` default is a classic landmine.
- Four OOMs, four fixes: **heap** (your objects/HashMap backend), **metaspace** (classloader leak, session mode), **direct** (network buffers), **container-killed** (off-heap overrun, no Java exception).
- **Keep heaps 4–8 GB.** A full GC on 32 GB is seconds and looks exactly like backpressure.
- **Interval = start→start; min-pause = end→start.** Min-pause is what prevents the checkpoint death spiral. Set it to ~half the interval.
- Turn on **incremental** with RocksDB; accept slower restores and mitigate with local recovery.
- **Start Delay large ⇒ it is a backpressure problem, not a checkpoint problem.**
- With a transactional exactly-once sink, **checkpoint interval is a hard floor on end-to-end latency**.
- Autoscaling restarts the job every time it scales, and it cannot fix skew.

**Interview one-liners**

- *"How do you pick parallelism?"* → Floor from throughput ÷ per-subtask rate with 2× headroom; ceiling from Kafka partitions for the source and `maxParallelism` for keyed state; then round to something that divides `maxParallelism` and fits available slots.
- *"Why can't I scale past a certain parallelism?"* → `maxParallelism` defines the number of key groups, is fixed at first run, and cannot change without discarding state.
- *"Where does RocksDB memory come from?"* → Flink's managed memory, off-heap, capped by `taskmanager.memory.managed.fraction` and split evenly across slots. It does not use the JVM heap, which is why RocksDB jobs avoid GC pressure.
- *"The pod is OOMKilled but there's no Java exception."* → Total process memory exceeded the container limit through off-heap growth. `taskmanager.memory.process.size` must equal the container limit so Flink can size everything under it.
- *"Interval vs min-pause?"* → Interval is start-to-start, min-pause is end-to-start. Min-pause guarantees processing time between checkpoints and prevents the death spiral when checkpoint duration approaches the interval.
- *"Checkpoints are slow — what do you change?"* → Nothing, until I decompose the duration. Start Delay means backpressure, alignment means skew, sync means the state backend, async means state size or storage. Only the last two are checkpoint-config problems.
- *"What's the cost of exactly-once with a transactional sink?"* → End-to-end latency has a hard floor at the checkpoint interval, because records are not visible downstream until the checkpoint commits the transaction.
