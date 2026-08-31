# 28. Parallelism, Task Slots, and Subtasks

Everything you have written so far ran in one JVM with an invisible parallelism. This chapter is the execution model: **who runs your code, where, and how many copies**.

## The two processes

```
┌──────────────────────────── FLINK CLUSTER ────────────────────────────┐
│                                                                       │
│  JobManager  (one active; HA gives you standbys)                      │
│  ├─ Dispatcher       accepts job submissions, spawns a JobMaster      │
│  ├─ JobMaster        one per job: schedules tasks, coordinates        │
│  │                   checkpoints, reacts to failures                  │
│  └─ ResourceManager  owns the slot inventory, asks for TMs            │
│                                                                       │
│  TaskManager 1              TaskManager 2              TaskManager 3  │
│  ┌────────────────┐         ┌────────────────┐         ┌───────────┐  │
│  │ slot0 slot1    │         │ slot0 slot1    │         │ slot0 ... │  │
│  │ slot2 slot3    │         │ slot2 slot3    │         │           │  │
│  │ network buffers│         │ network buffers│         │           │  │
│  └────────────────┘         └────────────────┘         └───────────┘  │
│         ▲                          ▲                                  │
│         └──────── data flows directly TM ↔ TM ────────────────────────┤
│                   (never through the JobManager)                      │
└───────────────────────────────────────────────────────────────────────┘
```

**JobManager** — the coordinator. It never touches your records. It builds the ExecutionGraph, assigns tasks to slots, triggers checkpoint barriers, and decides on restarts. It is a control plane. If it dies without HA, the job dies.

**TaskManager** — a worker JVM. It runs your operator code, holds your state, and holds the network buffers. Records move **directly between TaskManagers**; the JobManager is not in the data path.

Compared with Spark: JobManager ≈ driver-side scheduler, TaskManager ≈ executor. The big difference is that Flink's tasks are **long-running** — deployed once, then they process records forever. Spark re-schedules tasks every stage/batch.

---

## Task slots

```java
// flink-conf.yaml  (Flink 1.20+ also accepts config.yaml)
taskmanager.numberOfTaskSlots: 4
```

A **slot** is a unit of resource on a TaskManager. What it does and does not isolate is the exam question:

```
TaskManager JVM, 8 GB managed memory, 4 slots
┌───────────────────────────────────────────────┐
│  MEMORY: divided, isolated                    │
│  ┌────────┬────────┬────────┬────────┐        │
│  │ slot 0 │ slot 1 │ slot 2 │ slot 3 │        │
│  │  2 GB  │  2 GB  │  2 GB  │  2 GB  │        │
│  └────────┴────────┴────────┴────────┘        │
│                                               │
│  CPU: shared, NOT isolated                    │
│  ══════════ all cores, all slots ═══════════  │
│  A CPU-hungry task in slot 0 slows slots 1-3  │
│                                               │
│  Also shared: heap for user objects, GC,      │
│  network connections, the TM's threads        │
└───────────────────────────────────────────────┘
```

- **Memory is sliced.** Flink's *managed memory* (used by RocksDB, sorters, the batch hash tables) is divided evenly across slots. A slot cannot eat another slot's managed memory.
- **CPU is not sliced.** There is no cgroup per slot. Slots share the JVM's threads and all the cores the container has.
- **Heap is not sliced either.** One task allocating giant objects can OOM the whole TaskManager and take down every slot on it.

> **Key idea:** A slot isolates **memory**, not **CPU**. "How many slots per TaskManager?" is answered with "as many as the container has CPU cores", so each concurrently-running task has roughly a core's worth of CPU.

Rule of thumb: `taskmanager.numberOfTaskSlots` = number of CPU cores allocated to the TaskManager container.

Total capacity: `total slots = number of TaskManagers × slots per TaskManager`.

---

## Operators become subtasks

An **operator** is one node in your program (`map`, `keyBy` + `window`, the sink). With parallelism *p*, Flink runs *p* independent **subtasks** of that operator, each with its own state, its own thread of execution, and its own slice of the data.

```java
DataStream<Event> events = env.fromSource(...).setParallelism(3);
DataStream<Event> big = events.filter(e -> e.amount > 100).setParallelism(3);
big.keyBy(e -> e.userId).sum("amount").setParallelism(2);
```

```
  source            filter            sum
 ┌───────┐         ┌───────┐         ┌───────┐
 │ [0]   │────────►│ [0]   │────┐    │ [0]   │
 ├───────┤         ├───────┤    ├───►├───────┤
 │ [1]   │────────►│ [1]   │────┤    │ [1]   │
 ├───────┤         ├───────┤    ├───►└───────┘
 │ [2]   │────────►│ [2]   │────┘
 └───────┘         └───────┘
 parallelism 3     parallelism 3     parallelism 2
 (forward)         (hash shuffle by userId)
```

Vocabulary you must keep straight:

| Term | Meaning |
|---|---|
| Operator | One logical step in the program |
| Subtask | One parallel instance of an operator |
| Task | A chained group of subtasks running in one thread — the schedulable unit |
| Slot | The resource container a task runs in |

A subtask index is stable and visible: `getRuntimeContext().getIndexOfThisSubtask()`.

---

## Setting parallelism — four levels and their precedence

```java
// 1. OPERATOR level — highest priority
stream.map(...).setParallelism(8);

// 2. EXECUTION ENVIRONMENT level — applies to all operators in this job
env.setParallelism(4);
```

```bash
# 3. CLIENT level -- the -p flag at submit time
./bin/flink run -p 6 -c com.akash.flink.MyJob target/job.jar
```

```yaml
# 4. CLUSTER level -- flink-conf.yaml, the fallback for everything
parallelism.default: 2
```

Precedence, highest wins:

```
  1. setParallelism() on the operator
        ↓ (if not set)
  2. env.setParallelism()
        ↓ (if not set)
  3. flink run -p N
        ↓ (if not set)
  4. parallelism.default in the config
        ↓ (if not set)
  5. 1
```

Note the practical consequence: **`env.setParallelism(4)` hardcoded in your job silently ignores `-p`**. Every "why won't my -p flag work" ticket is this. In production, do not call `env.setParallelism()` at all — let `-p` control it — and use operator-level `setParallelism` only where you genuinely need a different number.

Legitimate reasons to override per operator:

```java
// A source can't usefully exceed its Kafka partition count.
// 12 partitions, parallelism 20 → 8 idle subtasks that also
// break watermarks unless withIdleness() is set.
env.fromSource(kafka, ws, "kafka").setParallelism(12);

// A sink writing to a database with a small connection pool.
stream.sinkTo(jdbcSink).setParallelism(4);

// print() at parallelism 1 so output is readable while learning.
stream.print().setParallelism(1);
```

Some operators are **forced to parallelism 1** and Flink will tell you: a non-keyed `windowAll`, a global `ProcessFunction`. That is a bottleneck by construction.

---

## Slot sharing — why 100 operators fit in one slot

This is the part that surprises everyone.

**By default, subtasks of *different* operators from the same job may share a slot, as long as they are subtasks with the same index.** So one slot holds one complete *slice* of the pipeline.

```
JOB: source(p=4) → map(p=4) → window(p=4) → sink(p=4)

WITHOUT slot sharing (not how Flink works):
   4 + 4 + 4 + 4 = 16 slots needed

WITH slot sharing (how Flink actually works):
 ┌── slot 0 ──┐ ┌── slot 1 ──┐ ┌── slot 2 ──┐ ┌── slot 3 ──┐
 │ source[0]  │ │ source[1]  │ │ source[2]  │ │ source[3]  │
 │ map[0]     │ │ map[1]     │ │ map[2]     │ │ map[3]     │
 │ window[0]  │ │ window[1]  │ │ window[2]  │ │ window[3]  │
 │ sink[0]    │ │ sink[1]    │ │ sink[2]    │ │ sink[3]    │
 └────────────┘ └────────────┘ └────────────┘ └────────────┘
   4 slots needed
```

> **Key idea:** **Required slots = the MAXIMUM parallelism of any operator, not the sum of all parallelisms.**

With mixed parallelism:

```
JOB: source(p=2) → map(p=8) → sink(p=4)
required slots = max(2, 8, 4) = 8

 slot0    slot1    slot2  slot3  slot4  slot5  slot6  slot7
 src[0]   src[1]
 map[0]   map[1]   map[2] map[3] map[4] map[5] map[6] map[7]
 sink[0]  sink[1]  sink[2] sink[3]
```

Slots 4–7 hold only a `map` subtask; slots 0–1 hold three subtasks each. Uneven, but correct.

Why this design is good:

1. **A job needs only as many slots as its highest parallelism.** Easy capacity arithmetic.
2. **Resource balance.** A slot holding one heavy operator and three light ones is better utilised than a slot with four heavy ones.
3. **Less network traffic.** `source[0] → map[0]` in the same slot is an in-memory handoff, not a network hop.

### Slot sharing groups

To *stop* sharing — usually to give a memory-hungry operator a slot of its own:

```java
stream
    .map(new LightweightParser())          // default group "default"
    .keyBy(e -> e.userId)
    .process(new HugeRocksDBStateFunction())
    .slotSharingGroup("heavy-state")       // this operator and everything
                                           // downstream of it move to "heavy-state"
    .map(new Formatter())
    .slotSharingGroup("default");          // explicitly back to default
```

The name is arbitrary; `"default"` is the implicit group everything starts in. Important: the group is **inherited downstream** until you change it again.

Cost: required slots becomes the **sum over groups** of each group's max parallelism.

```
group "default": max p = 4
group "heavy-state": max p = 4
required slots = 4 + 4 = 8   (not 4)
```

Use it sparingly and deliberately: a RocksDB-heavy operator, or a sink that must not compete for memory.

---

## Operator chaining

Independently of slots, Flink **fuses adjacent operators into a single task** so records pass by direct method call instead of going through a serializer, a buffer, and a network stack.

```
UNCHAINED: source → map → filter → sink
  record → serialize → buffer → deserialize → map
         → serialize → buffer → deserialize → filter → ...
  four thread handoffs, four serialization round trips

CHAINED: [source → map → filter → sink] = ONE task, ONE thread
  record → map() → filter() → sink()
  plain method calls. Often a 3-10x throughput difference.
```

Two operators are chained only if **all** of these hold:

1. They are in the **same slot sharing group**.
2. The connection is **FORWARD** (one-to-one), not a shuffle/rebalance/hash partition.
3. They have the **same parallelism**.
4. Chaining is enabled (globally and on both operators).
5. The upstream operator's chaining strategy allows it (`ALWAYS`/`HEAD`).

A `keyBy` **always breaks the chain** — it is a hash partition, so records must cross subtasks by definition.

You read the result straight off the Web UI: the job graph draws each chain as one box, with the operator names inside separated by arrows.

### Controlling chaining

```java
env.disableOperatorChaining();          // globally off. DEBUGGING ONLY.

stream.map(new A())
      .disableChaining()                // A is alone: no chain in, no chain out
      .map(new B())
      .startNewChain()                  // the chain BREAKS BEFORE B; B starts a
                                        // fresh chain with what follows it
      .map(new C());
```

- `disableChaining()` — this operator is isolated on both sides.
- `startNewChain()` — cut the link to the *previous* operator only; keep chaining forward.

When would you deliberately break a chain? Only three reasons:

1. **Profiling.** Chained operators share one thread, so the UI shows one combined busy-time. Splitting them tells you which one is actually slow. Put it back afterwards.
2. **An operator with a blocking call** (an HTTP lookup) that would stall everything else in the chain — better to isolate it, or better still use `AsyncDataStream` (Phase 6).
3. **Very different resource profiles** where you also want separate slot groups.

Otherwise: **leave chaining on**. It is one of Flink's biggest free performance wins.

---

## `setMaxParallelism` and key groups

```java
env.setMaxParallelism(256);         // job-wide
stream.setMaxParallelism(256);      // per operator
```

This is **not** an upper bound you set casually — it is a **structural constant baked into your state snapshots**.

Flink does not map a key directly to a subtask. It maps a key to a **key group**, and key groups to subtasks:

```
   key ──hash──► key group (0 .. maxParallelism-1) ──► subtask

 maxParallelism = 128, parallelism = 4:
   subtask 0 ← key groups   0..31
   subtask 1 ← key groups  32..63
   subtask 2 ← key groups  64..95
   subtask 3 ← key groups  96..127

 rescale to parallelism 8:
   subtask 0 ← key groups   0..15
   subtask 1 ← key groups  16..31
   ...
   Each subtask reads a CONTIGUOUS RANGE of key groups from the
   savepoint. No re-hashing of individual keys is needed.
```

The key group is the unit of state redistribution. That is what makes rescaling from a savepoint fast.

Consequences:

- **`maxParallelism` is the hard ceiling on parallelism.** You can never run at higher parallelism than there are key groups.
- **It cannot be changed on a savepoint restore.** Changing it invalidates every keyed state snapshot, because keys would hash to different groups. Changing it means starting from scratch.
- **Too low** → you can never scale past it. A job pinned at `maxParallelism=8` cannot use 16 slots, ever.
- **Too high** → metadata and checkpoint overhead per key group; also, with `maxParallelism` not a multiple of parallelism, key groups distribute unevenly, so some subtasks get one more group than others (mild artificial skew).

Flink's defaults if you say nothing:

```
parallelism <= 128            → maxParallelism = 128
parallelism  > 128            → maxParallelism = round up
                                 min(parallelism + parallelism/2, 32768)
                                 to the next power of two
absolute range                → 1 .. 32768
```

Practical advice: **set it explicitly, once, at job creation, to a power of two comfortably above any parallelism you will ever need.** 128 for small jobs, 512 or 1024 for anything expected to grow. It is cheap insurance against a migration you cannot do.

```java
env.setMaxParallelism(512);   // set on day one, never change it
```

> **Key idea:** `maxParallelism` decides the number of key groups, which decides how keyed state is partitioned in a savepoint. It is a **one-way decision** — pick it before your first production savepoint.

---

## Worked capacity example

```
Cluster:  5 TaskManagers × 4 slots  = 20 slots

Job: source(p=12)  →  map(p=12)  →  keyBy+window(p=20)  →  sink(p=6)
     all in the default slot sharing group

required slots = max(12, 12, 20, 6) = 20    ← fits exactly

Chains:
  [source → map]        FORWARD, same p=12, same group  → CHAINED
  map → window          keyBy = hash partition          → BREAKS
  window → sink         different parallelism (20 vs 6) → BREAKS

Tasks in the UI:
  1. "Source: kafka → Map"     12 subtasks
  2. "Window(...)"             20 subtasks
  3. "Sink: kafka"              6 subtasks

Now move the window to its own slot sharing group:
  required slots = max(12, 12, 6) + max(20) = 12 + 20 = 32 slots
  → the job no longer fits. It will sit in SCHEDULED, waiting for slots,
    and eventually fail with NoResourceAvailableException.
```

That failure mode — a job stuck pending with `NoResourceAvailableException` after someone added a `slotSharingGroup()` — is worth recognising on sight.

---

## Remember

- **JobManager** coordinates and never sees a record; **TaskManager** runs the code and holds state. Data flows TM↔TM directly.
- A **slot** isolates **memory**, not CPU. Set `taskmanager.numberOfTaskSlots` ≈ cores per TaskManager.
- Operator → *p* **subtasks**; a chained group of subtasks = one **task** = one thread.
- Parallelism precedence: **operator > env > `-p` > `parallelism.default` > 1**. Never hardcode `env.setParallelism()` in a production job.
- Slot sharing means **required slots = max parallelism, not the sum**.
- `slotSharingGroup(name)` breaks that and makes it a **sum over groups**; it also propagates downstream.
- Chaining fuses operators into one thread with plain method calls. Requires: same group, FORWARD connection, same parallelism. `keyBy` always breaks it.
- `disableChaining()` isolates an operator; `startNewChain()` cuts only the incoming link. Use for profiling, then revert.
- `maxParallelism` = number of **key groups** = the hard scaling ceiling. **Cannot be changed on savepoint restore.** Set it once, high, on day one.

**Interview one-liners**

- *"What is a task slot?"* → A fixed slice of a TaskManager's managed memory. CPU and heap are shared across slots in the same JVM.
- *"How many slots does my job need?"* → The maximum parallelism among its operators, because of slot sharing — not the sum.
- *"What is operator chaining?"* → Fusing adjacent one-to-one operators of equal parallelism into a single task/thread so records pass by method call instead of serialization + network. Usually a large throughput win.
- *"Why did my keyBy break the chain?"* → It is a hash partition, so records must move between subtasks; only FORWARD connections chain.
- *"Precedence of parallelism settings?"* → operator, then env, then `-p`, then `parallelism.default`.
- *"What is maxParallelism?"* → The key group count. It fixes how keyed state is partitioned in a savepoint, so it caps rescaling and cannot be changed on restore.
- *"Job stuck in SCHEDULED with NoResourceAvailableException?"* → It needs more slots than the cluster has — often because someone added a slot sharing group and turned a max into a sum.
