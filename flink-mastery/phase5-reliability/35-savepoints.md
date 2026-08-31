# 35. Savepoints

Checkpoints keep the job alive through failures. Savepoints let *you* move the job — upgrade the code, rescale, migrate clusters, roll back a bad deploy.

> **Key idea**
> A checkpoint is **the system's** recovery mechanism, owned by Flink and optimised for speed.
> A savepoint is **your** deliberate, portable, self-contained image of the job, owned by you and optimised for compatibility.
> Same underlying machinery. Completely different purpose and lifecycle.

---

## The comparison table

| | Checkpoint | Savepoint |
|---|---|---|
| **Triggered by** | Flink, automatically, on the configured interval | You, manually (CLI/REST), or by `stop`/`suspend` |
| **Purpose** | Automatic recovery from failure | Planned operations: upgrade, rescale, migrate, A/B, rollback |
| **Owned by** | Flink — it creates and deletes them | You — Flink never deletes a savepoint |
| **Lifecycle** | Deleted when superseded (`num-retained`) or when the job ends, unless retained | Lives until you delete it |
| **Format** | Backend-specific, optimised for **write speed** | **Canonical** by default — backend-independent and portable |
| **Incremental** | Yes, on RocksDB | No by default (canonical is always full); yes with **native** format on RocksDB |
| **Portability** | Restore into the same job, usually the same Flink version | Restore into a **modified job**, a different state backend, a newer Flink version, a different cluster |
| **Change parallelism on restore** | Yes (supported since 1.15 even for unaligned) | Yes — this is a primary use case |
| **Cost to take** | Cheap, especially incremental | Expensive — full state, canonical serialization |
| **Typical frequency** | Every 30s–5min | On deploys |
| **Restore CLI** | `flink run -s <path-to-chk-N>` | `flink run -s <path-to-savepoint>` — **same flag** |

The last row surprises people: **there is one restore flag.** `-s` (or `--fromSavepoint`) takes either. Flink figures out which it is from the metadata.

---

## Taking a savepoint

### While the job keeps running

```bash
# Basic: uses the cluster's configured savepoint directory
./bin/flink savepoint 7a1c9e2f4b8d6a0c3e5f1a9b7d2c4e6f

# Explicit directory (required if no default is configured)
./bin/flink savepoint 7a1c9e2f4b8d6a0c3e5f1a9b7d2c4e6f s3://my-bucket/flink/savepoints
```

Output:

```
Triggering savepoint for job 7a1c9e2f4b8d6a0c3e5f1a9b7d2c4e6f.
Waiting for response...
Savepoint completed. Path: s3://my-bucket/flink/savepoints/savepoint-7a1c9e-a3f8b2c1d4e5
You can resume your program from this savepoint with the run command.
```

**That path is the thing you need.** Capture it — from a deploy script, capture stdout. Losing it means hunting through the bucket.

The job keeps running afterwards. This is a "backup now" operation.

Getting the job ID:

```bash
./bin/flink list
# ------------------ Running/Restarting Jobs -------------------
# 29.08.2026 14:02:11 : 7a1c9e2f4b8d6a0c3e5f1a9b7d2c4e6f : fraud-detector (RUNNING)
```

### Stopping the job with a final savepoint — the one you'll actually use

```bash
./bin/flink stop \
  --savepointPath s3://my-bucket/flink/savepoints \
  7a1c9e2f4b8d6a0c3e5f1a9b7d2c4e6f
```

### Why `stop` and not `cancel`

```
flink cancel <jobId>
   → sends cancel, tasks are torn down immediately, mid-stream
   → NO savepoint (unless you took one separately, which is racy —
     records processed between your savepoint and the cancel are lost
     or must be reprocessed)
   → sources are not drained; in-flight records are dropped
   → transactional sinks may leave transactions dangling

flink stop --savepointPath <dir> <jobId>
   → 1. sources stop reading new records
     2. a MAX_WATERMARK is emitted, so event-time windows and timers FIRE
        and flush their results  ← "draining"
     3. a final savepoint is taken at a clean, consistent point
     4. transactional sinks commit their final transaction
     5. THEN the job finishes, gracefully
   → the savepoint provably contains everything up to the stop
```

> **Key idea**
> `stop` gives you a savepoint that is the *exact* end of the job's processing, with nothing in flight and nothing lost. `cancel` plus a separate savepoint has a gap between the two. **Use `stop` for every planned shutdown.**

Useful flags:

```bash
# Do NOT emit MAX_WATERMARK. Windows do not prematurely fire; they are
# saved half-full and resume correctly on restore. This is what you want
# for an UPGRADE (you're coming back), not for a permanent shutdown.
./bin/flink stop --savepointPath <dir> --no-drain <jobId>

# Native format (see below): faster, RocksDB-specific
./bin/flink stop --savepointPath <dir> --type native <jobId>
```

`--no-drain` is the default in recent versions and is almost always what you want for a redeploy. Draining fires windows *early*, producing a partial window result — correct for a permanent shutdown, wrong if you're restarting in 30 seconds and the window would then be counted twice or emitted incomplete.

### Disposing a savepoint

```bash
./bin/flink savepoint --dispose s3://my-bucket/flink/savepoints/savepoint-7a1c9e-a3f8b2c1d4e5
```

Because savepoints are yours, Flink never cleans them up. Either dispose them or set a bucket lifecycle policy. A year of daily deploy savepoints of a 200 GB job is 73 TB of storage nobody budgeted for.

---

## Restoring from a savepoint

```bash
./bin/flink run \
  -s s3://my-bucket/flink/savepoints/savepoint-7a1c9e-a3f8b2c1d4e5 \
  -c com.example.FraudDetector \
  target/my-job-1.0.jar
```

- `-s` / `--fromSavepoint` — the path. Works for savepoints and for retained checkpoints (`.../chk-42`).
- `-c` / `--class` — the main class, needed unless the jar's manifest declares one.
- Change parallelism on restore with `-p`:

```bash
./bin/flink run -s <savepoint> -p 16 -c com.example.FraudDetector target/my-job-1.0.jar
```

Restoring with different parallelism redistributes keyed state across the new subtasks. It works, subject to **max parallelism** — see the troubleshooting section in chapter 38.

### `--allowNonRestoredState`

```bash
./bin/flink run \
  -s <savepoint> \
  --allowNonRestoredState \
  -c com.example.FraudDetector \
  target/my-job-2.0.jar
```

**What it does:** by default, if the savepoint contains state for an operator that no longer exists in your job graph, Flink **refuses to start**:

```
java.lang.IllegalStateException: Failed to rollback to checkpoint/savepoint ...
Cannot map checkpoint/savepoint state for operator 8a4f2b1c9d3e5f7a
to the new program, because the operator is not available in the new program.
If you want to allow to skip this, you can set the --allowNonRestoredState option
on the CLI.
```

`--allowNonRestoredState` says "drop that state, start anyway".

**When it's correct:**
- You genuinely removed a stateful operator from the pipeline — deleted a deduplication stage, dropped a feature.
- You are certain that state is no longer needed.

**When it is dangerous:**
- You *renamed* or *reordered* an operator, or added one upstream, and the auto-generated UID changed. The state still exists and you still want it — but Flink can no longer match it, so this flag silently **throws away your state**. The job starts, counters read zero, and nobody notices until the numbers are wrong.

> **The rule:** `--allowNonRestoredState` is only safe when you can name exactly which operator's state you are discarding and why. If you're using it to make an error go away, you are almost certainly deleting production state.

The permanent fix for the second case is the next section.

---

## Native vs canonical savepoint format

Flink 1.15 introduced a choice.

| | Canonical (default) | Native |
|---|---|---|
| Format | Backend-independent, unified | The state backend's own format (RocksDB SST files) |
| Switch state backend on restore | ✅ yes | ❌ no — heap savepoint restores only to heap, RocksDB only to RocksDB |
| Speed to take | slower — everything re-serialized into the canonical layout | **much faster** — essentially a checkpoint |
| Incremental | ❌ never | ✅ possible on RocksDB |
| State schema evolution | ✅ fully supported | supported, with more constraints |
| Flink version portability | best | good |

```bash
./bin/flink savepoint --type canonical <jobId> <dir>   # default
./bin/flink savepoint --type native    <jobId> <dir>
./bin/flink stop --type native --savepointPath <dir> <jobId>
```

Rule: **canonical for anything you might need in six months or want to migrate**; **native for routine deploys of a large-state job**, where the savepoint is created and consumed minutes apart and taking a canonical one of 500 GB would take an hour.

---

## Operator UIDs — the single most important operational habit

Every operator in your job graph has an ID. That ID is the key under which its state is stored in a savepoint. On restore, Flink matches savepoint state to operators **by that ID**.

If you do not set one, Flink **generates it by hashing the operator's position in the job graph** — its type, its inputs, and the structure around it.

```
Job v1                            Job v2 (you added a filter)

source                            source
   ↓ hash = A                        ↓ hash = A
  map                             filter          ← NEW
   ↓ hash = B                        ↓ hash = ???
 keyBy+process                      map           ← same code, but its INPUT changed
   ↓ hash = C                        ↓ hash = B'  ← DIFFERENT HASH
  sink                            keyBy+process
                                     ↓ hash = C'  ← DIFFERENT HASH
                                    sink

Savepoint has state under B and C.
New job looks for state under B' and C'.
→ "Cannot map checkpoint/savepoint state for operator ... to the new program"
→ or, with --allowNonRestoredState, your state is SILENTLY DISCARDED.
```

The auto-generated ID is a function of the *shape of the graph*, not of what the operator does. Adding an operator anywhere upstream changes every downstream hash.

### The fix, in code

```java
DataStream<Event> events = env
        .fromSource(kafkaSource, watermarkStrategy, "kafka-events")
        .uid("kafka-source")                    // ← set on the source
        .name("Kafka: events topic");           // ← human label for the UI only

DataStream<Alert> alerts = events
        .keyBy(e -> e.userId)
        .process(new FraudDetector())
        .uid("fraud-detector")                  // ← THE important one: stateful
        .name("Fraud detection");

alerts.sinkTo(kafkaSink)
      .uid("alert-sink")                        // ← transactional sink = stateful
      .name("Kafka: alerts topic");
```

Line notes:

- `.uid(String)` sets the **operator UID**. It is what state is keyed by in the savepoint. Set it once and **never change it** — changing a UID is exactly equivalent to deleting the operator and adding a new one.
- `.name(String)` is cosmetic: the label in the Web UI. It has **no** effect on state. Do not confuse the two; `name` is the one people set and `uid` is the one that matters.
- `.uid()` must be called on the operator it belongs to, immediately after the transformation that creates it. `stream.keyBy(...).process(f).uid("x")` puts the uid on the `process` operator, which is right — `keyBy` is a partitioning instruction, not a stateful operator, and has no state of its own.
- Both return the stream, so they chain: `.uid("a").name("A")`.

### The rules

1. **Set `uid()` on every stateful operator, from day one.** Retro-fitting UIDs requires a savepoint-compatible migration and is genuinely painful.
2. Stateful means: any `process`/`ProcessFunction`/`KeyedProcessFunction`, any window, any aggregation, any join, **sources** (they store offsets), and **transactional sinks** (they store transaction handles).
3. Setting `uid()` on stateless operators (`map`, `filter`) is harmless and makes the habit automatic. Just do it everywhere.
4. UIDs must be **unique within the job** and **stable across versions**.
5. Name them after what they *are*, not where they are: `"fraud-detector"`, not `"operator-3"`.

Enforce it — this makes an unset UID a startup failure instead of a 3 a.m. discovery:

```java
env.getConfig().disableAutoGeneratedUIDs();
```

Any operator without an explicit `uid()` now throws at job submission:

```
java.lang.IllegalStateException: Auto generated UIDs have been disabled but no UID
or hash has been assigned to operator Map
```

Turn this on in every new project. It is one line and it prevents the single most common Flink operational failure.

> **Key idea**
> `uid()` costs nothing and is unrecoverable in hindsight. Set it on everything, before your first deploy, and never change one.

### `uidHash` — the escape hatch

If you already have a production job with no UIDs and need to migrate, you can pin the *existing* auto-generated hash:

```java
.setUidHash("8a4f2b1c9d3e5f7a2b6c8d0e1f3a5b7c")
```

Read the hashes out of the savepoint metadata (or the exception message), pin them, deploy, and from then on the operators are anchored. This is a migration tool, not something to use routinely — the Flink docs describe it as a last resort.

---

## State schema evolution

You set your UIDs correctly, and now you want to change the *shape* of the data in state — add a field to `Event`. What survives?

### POJOs

A class is treated as a POJO by Flink if it: is public, has a public no-arg constructor, and all fields are either public or have public getters/setters. (This is why chapter 1 insisted on that shape.)

Supported changes:

| Change | Supported? | What happens |
|---|---|---|
| **Add** a field | ✅ | New field gets its Java default: `null`, `0`, `false`. |
| **Remove** a field | ✅ | Old values for that field are dropped. |
| **Reorder** fields | ✅ | Fields are matched by **name**, not position. |
| **Rename** a field | ❌ | Treated as remove + add. Old data is lost, new field is default. |
| **Change a field's type** | ❌ | Not supported. Even `int` → `long`. |
| **Change the class name / package** | ❌ | It is a different type entirely. |
| **Change the key type** of keyed state | ❌ | Never supported, for any serializer. |

```java
// v1 — in production, state exists with this shape
public class SessionData {
    public long count;
    public double total;
    public SessionData() {}
}

// v2 — ✅ safe: added a field. Restored objects get lastSeen = 0.
public class SessionData {
    public long count;
    public double total;
    public long lastSeen;      // new; defaults to 0
    public SessionData() {}
}

// v3 — ❌ NOT safe: changed a type. Restore fails.
public class SessionData {
    public long count;
    public java.math.BigDecimal total;   // was double
    public long lastSeen;
    public SessionData() {}
}
```

For an unsupported change, migrate deliberately: add a new field with a new name alongside the old one, deploy, backfill in code, then remove the old field in a later deploy. Two deploys, no data loss.

### Avro

If your state type is an Avro `SpecificRecord` or `GenericRecord`, Flink applies **Avro's own schema resolution rules**, which are far more generous:

- Add a field **with a default value** → old records read fine.
- Remove a field that had a default → fine.
- Rename via `aliases` → fine.
- Widen a type (`int` → `long`, `float` → `double`) → fine.
- Union with `null` to make a field optional → fine.

```java
// Force Avro serialization for a class, rather than POJO serialization
env.getConfig().enableForceAvro();
```

If you expect state schema to evolve repeatedly — and you should — **use Avro for your state types**. It converts a hard problem into a solved one. The cost is a code-generation step and a slightly heavier serializer.

### Kryo

Anything Flink cannot treat as a POJO or Avro falls back to **Kryo**, which supports **no schema evolution at all**. A Kryo-serialized state type is frozen forever.

```java
// Fail loudly if any type falls back to Kryo, instead of finding out
// during a migration a year from now.
env.getConfig().disableGenericTypes();
```

Worth turning on early in development. The error message tells you exactly which class is not a valid POJO.

---

## Remember

- **Checkpoint = Flink's, automatic, for failures. Savepoint = yours, manual, for planned changes.** Same machinery, different lifecycle.
- One restore flag for both: `flink run -s <path>`.
- `flink stop --savepointPath <dir> <jobId>` beats `flink cancel` — it drains, produces a consistent final savepoint, and lets transactional sinks commit.
- `--no-drain` (the default) for upgrades; drain only for permanent shutdowns.
- Flink **never deletes savepoints**. Dispose them or set a lifecycle policy.
- **Canonical** format = portable, slow, backend-independent. **Native** = fast, backend-locked. Canonical for archives, native for routine large-state deploys.
- **`uid()` on every stateful operator, from day one.** Auto-generated IDs are hashes of the graph shape and change whenever you touch the graph.
- `.name()` is cosmetic. `.uid()` is the one that matters.
- `env.getConfig().disableAutoGeneratedUIDs()` makes a missing uid a startup error. Turn it on.
- `--allowNonRestoredState` is only safe when you can name the operator whose state you are deliberately dropping. Otherwise it silently deletes production state.
- POJO evolution: add/remove/reorder ✅; rename/retype/change class ❌. **Avro** for state types you expect to evolve. Kryo evolves not at all.

**Interview one-liners**

- *"Checkpoint vs savepoint?"* → Checkpoints are Flink-owned, automatic, backend-native and optimised for cheap recovery; savepoints are user-owned, manual, canonical-format and optimised for portability across code versions, parallelism, state backends, and Flink versions.
- *"Why `stop` instead of `cancel`?"* → `stop --savepointPath` stops the sources, optionally drains timers and windows, takes a final consistent savepoint, and lets transactional sinks commit. `cancel` tears down mid-stream and leaves a gap.
- *"What are operator UIDs for?"* → They key operator state in the snapshot. Without explicit UIDs, Flink hashes the graph structure, so any change to the topology breaks state mapping on restore.
- *"When do you need `--allowNonRestoredState`?"* → When you deliberately removed a stateful operator. Using it to silence a mapping error usually means you're throwing away state you actually wanted.
- *"Native vs canonical savepoints?"* → Canonical is backend-independent and lets you switch state backends, at the cost of a full re-serialization; native is the backend's own format — much faster and possibly incremental, but locked to that backend.
- *"How do you evolve state schema?"* → POJOs allow adding, removing and reordering fields but not renaming or retyping. Avro follows Avro's resolution rules and is the right choice for state that will evolve. Kryo supports no evolution.
