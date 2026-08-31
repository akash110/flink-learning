# 18. What Flink State Actually Is

Phase 1 gave you stateless operators: `map`, `filter`, `flatMap`. Each record went in, a record came out, and the operator remembered nothing. Phase 2 gave you windows — which secretly *did* keep state, but Flink managed it for you and you never touched it.

This phase is about **you managing memory across records**. That's it. That's all state is.

## The one-sentence definition

> **Key idea:** State is a variable that survives from one record to the next, is scoped to a single key, and is checkpointed by Flink so it survives a crash.

Three properties, and all three matter:

1. **Survives across records** — otherwise it's just a local variable.
2. **Scoped to a key** — user `alice`'s balance and user `bob`'s balance are separate storage cells, and Flink switches between them automatically.
3. **Fault tolerant** — if the machine dies, the state comes back exactly as it was.

## Why streaming needs it

Batch jobs (Spark) load everything, so "state" is just the DataFrame in front of you. A stream has no end. You see record 1, then record 2, and you must answer questions like:

```
"What is alice's running balance?"        -> needs the previous balance
"How many logins has bob had?"            -> needs the previous count
"When did carol last do anything?"        -> needs the previous timestamp
"Was this small txn followed by a big one?" -> needs the previous txn
```

None of those can be answered from the current record alone. You need memory.

## The naive approach: a plain Java field

Here's what a Java beginner writes first:

```java
// BROKEN. Do not do this.
public class CountFn extends RichFlatMapFunction<Event, String> {

    // `private` = only this class can see it.
    // `long` = a 64-bit integer primitive in Java (not an object).
    private long count = 0;

    @Override
    public void flatMap(Event e, Collector<String> out) {
        count++;                       // increments a plain field
        out.collect(e.userId + " -> " + count);
    }
}
```

Two fatal problems:

```
PROBLEM 1: NOT KEYED
  alice, bob, alice, carol  ->  count = 1, 2, 3, 4
  You wanted alice=2, bob=1, carol=1.
  One field is shared by every key that lands on this parallel subtask.

PROBLEM 2: NOT FAULT TOLERANT
  TaskManager crashes -> the JVM dies -> `count` is gone.
  Job restarts, count = 0. All history lost silently.
```

Flink's managed state fixes both. `ValueState<Long>` (chapter 19) looks almost identical in code, but Flink swaps the underlying storage cell per key, and snapshots it into checkpoints.

## Local state vs an external database

The obvious alternative: keep counters in Redis or DynamoDB.

```
EXTERNAL DB PER RECORD                    FLINK LOCAL STATE
─────────────────────────                 ──────────────────────────
record arrives                            record arrives
   |                                         |
   v                                         v
network call to Redis  ~1-5 ms            read local hash map  ~50-500 ns
   |                                         |
compute                                   compute
   |                                         |
network call to write  ~1-5 ms            write local hash map ~50-500 ns
   |                                         |
emit                                      emit

Throughput per subtask: ~200-500 rec/s     Throughput per subtask: ~1,000,000 rec/s
```

That is a **1000x–10,000x difference**, and it is not the only problem:

| | External DB | Flink local state |
|---|---|---|
| Latency per access | 1–5 ms network round trip | sub-microsecond (heap) / ~10 µs (RocksDB) |
| Throughput ceiling | The DB's QPS limit, shared by all subtasks | Scales linearly with parallelism |
| Consistency on failure | DB has writes the checkpoint doesn't → duplicates or loss | Checkpoint and state are one atomic unit → exactly-once |
| Extra operational cost | Another cluster to run, page for, and pay for | None; it ships with the job |
| Backpressure behaviour | DB slows → your whole job stalls | No external dependency to stall on |

The consistency point is the one people underrate. If you write to Redis and then the job fails before the checkpoint, on restart Flink replays those records and writes to Redis *again*. Your counter is now wrong and there is nothing you can do about it. With Flink state, the state and the input offsets are captured in the same checkpoint, so replay restores a consistent pair.

> **Key idea:** Local + checkpointed beats remote-per-record on latency, throughput, *and* correctness. Reach for an external store only when the state must be queried by systems outside Flink.

## Keyed state vs operator state

Flink has exactly two families of managed state.

| | **Keyed state** | **Operator state** |
|---|---|---|
| Available on | A `KeyedStream` only (after `.keyBy(...)`) | Any operator, keyed or not |
| Scoped to | One key, per operator | One parallel subtask |
| Number of instances | One per distinct key | One per parallel subtask |
| Types | `ValueState`, `ListState`, `MapState`, `ReducingState`, `AggregatingState` | `ListState`, `BroadcastState` |
| Accessed via | `getRuntimeContext().getState(descriptor)` | `CheckpointedFunction` interface / `initializeState()` |
| Redistribution on rescale | Automatic, by key group | You choose: even-split or union |
| Typical use | Per-user counters, balances, session data | Kafka source offsets, sink buffers, config broadcast |
| How often you write it | Constantly — 99% of application code | Rarely — mostly connector authors |

**You will use keyed state.** Operator state is what a Kafka source uses internally to remember its partition offsets, or what a sink uses to remember its in-flight buffer. Unless you are writing a connector, you can safely park it. Broadcast state (a special operator state for distributing a rules table to every subtask) shows up in Phase 6.

Everything in chapters 19–24 is keyed state.

## Where state physically lives: state backends

The **state backend** is the pluggable storage engine underneath your `ValueState`. Since Flink 1.13 there are two, and the naming changed — modern names below.

### `HashMapStateBackend`

State lives as **live Java objects on the JVM heap**. A `ValueState<Long>` really is a `Long` object sitting in a `HashMap` in memory.

```java
// In your job's main method:
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(new HashMapStateBackend());
```

- No serialization on read or write. Access is a hash map lookup.
- Bounded by heap size. Exceed it → `OutOfMemoryError` → job dies.
- GC pressure: millions of live objects make full GCs long, which stalls the whole subtask.

### `EmbeddedRocksDBStateBackend`

State lives in **RocksDB**, an embedded key-value store that keeps data off-heap and spills to local disk.

```java
env.setStateBackend(new EmbeddedRocksDBStateBackend());
```

- Every read deserializes bytes; every write serializes. That's the cost.
- Not bounded by heap — bounded by local disk. Terabytes are normal.
- Supports **incremental checkpoints**: only changed SST files are uploaded, so checkpointing 500 GB of state doesn't mean uploading 500 GB every time.

### The trade-off table

| | `HashMapStateBackend` | `EmbeddedRocksDBStateBackend` |
|---|---|---|
| Storage | JVM heap, live objects | Off-heap + local disk, serialized bytes |
| Read/write speed | Fastest (no serialization) | ~10x slower (serialize + disk/block cache) |
| Max state size | Bounded by heap (~GBs) | Bounded by disk (~TBs) |
| Serialization cost | Only at checkpoint time | On **every** access |
| GC impact | High — millions of live objects | Low — bytes are off-heap |
| Incremental checkpoints | No, always full | **Yes** |
| Latency profile | Very low, but GC pauses spike it | Slightly higher, but far more predictable |
| `MapState` per-key access | Cheap either way | **Critical advantage** (see ch. 20) |

### Which to pick

```
State per TaskManager < a few GB, and latency is critical?
   -> HashMapStateBackend

State is large (10s of GB to TB), or grows over time,
or you need incremental checkpoints, or GC pauses hurt?
   -> EmbeddedRocksDBStateBackend

Not sure? Production default is RocksDB.
   The 10x access cost buys you a job that doesn't fall over as state grows.
```

Interviewers love this one, so know the shape of the answer: *heap is faster but bounded and GC-heavy; RocksDB is slower per access but unbounded, off-heap, and incrementally checkpointable.*

## State is per-key AND per-operator

This is the diagram to memorize.

```
                stream.keyBy(e -> e.userId)

  ┌──────────────── Operator A (a KeyedProcessFunction) ────────────────┐
  │                                                                     │
  │   ValueState "balance"          ValueState "count"                  │
  │   ┌────────────────────┐        ┌────────────────────┐              │
  │   │ alice   ->  250.0  │        │ alice   ->   3     │              │
  │   │ bob     ->   80.5  │        │ bob     ->   1     │              │
  │   │ carol   ->    0.0  │        │ carol   ->   7     │              │
  │   └────────────────────┘        └────────────────────┘              │
  └─────────────────────────────────────────────────────────────────────┘
                                 |
                                 v
  ┌──────────────── Operator B (a different function) ──────────────────┐
  │                                                                     │
  │   ValueState "balance"   <-- SAME NAME, COMPLETELY SEPARATE STORAGE │
  │   ┌────────────────────┐                                            │
  │   │ alice   ->   12.0  │                                            │
  │   └────────────────────┘                                            │
  └─────────────────────────────────────────────────────────────────────┘
```

Read that carefully. Two rules fall out:

1. **Two different keys never see each other's state.** When a record for `bob` arrives, Flink points the `balance` handle at bob's cell before your code runs. You never pass a key to `.value()`.
2. **Two different operators with the same state name are unrelated.** State is identified by `(operator, state name, key)`. Naming a `ValueStateDescriptor` "count" in two operators does not share anything.

And the third rule, the one beginners trip on:

3. **State is only accessible from inside a keyed context.** Calling `.value()` in `open()`, or from a non-keyed operator, throws. In `open()` there is no "current key" yet.

## Key groups: why you can rescale

If state is per key and keys are spread across subtasks, how can you change parallelism from 4 to 8 and keep your state?

The answer is **key groups**. Flink doesn't assign keys to subtasks directly. It assigns them to a fixed number of key groups, and then assigns *ranges of key groups* to subtasks.

```
maxParallelism = 128   (the number of key groups; set at job creation, IMMUTABLE)

key "alice" -> hash -> key group 37
key "bob"   -> hash -> key group 91

parallelism = 4:
   subtask 0: key groups   0- 31
   subtask 1: key groups  32- 63   <- alice (37) lives here
   subtask 2: key groups  64- 95   <- bob (91) lives here
   subtask 3: key groups  96-127

rescale to parallelism = 8:
   subtask 0: key groups   0- 15
   subtask 1: key groups  16- 31
   subtask 2: key groups  32- 47   <- alice moves here, whole group 37 travels together
   ...
   subtask 5: key groups  80- 95   <- bob moves here
```

Because state is snapshotted **grouped by key group**, restoring at a new parallelism is just "hand key-group ranges to the new subtasks". No re-hashing of individual keys, no shuffle of individual state entries.

Two consequences you must know:

- **`maxParallelism` is fixed for the life of the job's state.** Default is 128 if parallelism ≤ 128, else roughly `1.5 × parallelism` rounded up to a power of two, capped at 32768. Changing it invalidates your savepoint. Set it explicitly for anything long-lived:
  ```java
  env.setMaxParallelism(1024);   // now you can rescale anywhere from 1 to 1024
  ```
- **Your parallelism can never exceed `maxParallelism`.** More subtasks than key groups means idle subtasks.

Pick `maxParallelism` a few multiples above the largest parallelism you can imagine needing. It costs almost nothing to set high (a little metadata), and it costs a full state migration to have set it too low.

## State is what checkpoints snapshot

Everything in this phase is the *content*. Phase 5 is the *mechanism*.

```
     your state              checkpoint                  durable storage
  ┌───────────────┐        ┌────────────┐             ┌──────────────────┐
  │ balance: {..} │  ───►  │ barrier    │  ───────►   │  s3://ckpt/1234/ │
  │ count:   {..} │        │ alignment  │             │  + source offsets│
  │ timers:  {..} │        │ + snapshot │             └──────────────────┘
  └───────────────┘        └────────────┘
```

Three things worth knowing now:

- **Timers are state too.** Chapter 23's event-time timers are checkpointed exactly like your `ValueState`. Registering a timer for 2 hours from now is a durable promise.
- **Checkpoint duration scales with state size.** A job with 500 GB of state and no incremental checkpointing will have miserable checkpoints. This is the direct link between "I never set a TTL" (chapter 22) and "my checkpoints take 9 minutes."
- **Checkpoints are automatic and for recovery; savepoints are manual and for upgrades.** Same underlying machinery, different lifecycle.

## Remember

- State = memory that survives records, is scoped to a key, and is checkpointed.
- A plain Java field is neither keyed nor fault tolerant. It is always a bug.
- Local state beats a remote DB call on latency, throughput, and consistency.
- Keyed state (per key) is what you write. Operator state (per subtask) is for connectors.
- `HashMapStateBackend` = heap, fast, bounded, GC-heavy, full checkpoints.
- `EmbeddedRocksDBStateBackend` = disk, serialized, unbounded, incremental checkpoints. Production default.
- State identity = `(operator, state name, key)`. Same name in two operators shares nothing.
- Key groups (`maxParallelism`) are why rescaling works. Set `maxParallelism` explicitly and generously.
- Unbounded keyspace + no TTL = state grows forever = the job eventually dies. See chapter 22.

## Interview one-liners

- *"What is Flink state?"* → Fault-tolerant, key-scoped memory that Flink snapshots into checkpoints alongside source offsets, so replay after failure is consistent.
- *"Keyed vs operator state?"* → Keyed is one instance per key on a `KeyedStream`; operator state is one instance per parallel subtask and is mostly used by connectors.
- *"Why not just use Redis?"* → Millisecond network round trip per record instead of sub-microsecond local access, plus you lose exactly-once because the external write isn't inside the checkpoint.
- *"HashMap vs RocksDB backend?"* → Heap objects, fastest, heap-bounded, full checkpoints vs off-heap serialized bytes, ~10x slower per access, disk-bounded, incremental checkpoints.
- *"When would you still pick HashMapStateBackend?"* → Small state, ultra-low latency, and you can tolerate full checkpoints and GC tuning.
- *"How does Flink rescale stateful jobs?"* → State is snapshotted per key group; rescaling reassigns key-group ranges to subtasks. `maxParallelism` fixes the key-group count and can't be changed later.
- *"Why can't I read state in `open()`?"* → There's no current key yet. Keyed state is only accessible inside `processElement` / `flatMap` / `onTimer`.
- *"What's the #1 way stateful jobs die?"* → Unbounded keyspace with no TTL: state grows without limit until checkpoints time out or disk fills.
