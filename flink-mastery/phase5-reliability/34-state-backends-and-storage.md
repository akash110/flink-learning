# 34. State Backends and Checkpoint Storage

Two concepts, two separate settings, constantly confused. Get this distinction right and half the configuration questions answer themselves.

> **Key idea**
> **State backend** = where your state lives *while the job is running*.
> **Checkpoint storage** = where the *snapshot* is written when a checkpoint happens.
> They are configured independently. RocksDB does not imply S3; S3 does not imply RocksDB.

```
                RUNNING                          CHECKPOINT TIME
        ┌───────────────────────┐          ┌──────────────────────────┐
        │    STATE BACKEND      │  ──────► │   CHECKPOINT STORAGE     │
        │  where state lives    │ snapshot │  where snapshots go      │
        │  now, for reads/writes│          │  durably                 │
        ├───────────────────────┤          ├──────────────────────────┤
        │ HashMapStateBackend   │          │ JobManagerCheckpoint     │
        │   → JVM heap          │          │   Storage → JM memory    │
        │                       │          │                          │
        │ EmbeddedRocksDBState  │          │ FileSystemCheckpoint     │
        │   Backend             │          │   Storage → S3/HDFS/file │
        │   → local disk        │          │                          │
        └───────────────────────┘          └──────────────────────────┘
              any of these       ×         any of these = valid combo
```

---

## Part 1: State backends

### `HashMapStateBackend`

```java
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

env.setStateBackend(new HashMapStateBackend());   // this is the default
```

State is stored as **regular Java objects in a `HashMap` on the JVM heap**. Your `ValueState<Long>` is literally a `Long` sitting in a map.

- **Read/write cost:** a hash lookup and a pointer dereference. Nanoseconds. **No serialization at all** while the job runs.
- **Size limit:** the TaskManager's heap, minus everything else. Practically, tens of GB per TM before GC becomes intolerable.
- **Snapshot:** copy-on-write of the maps in the sync phase, then serialize + upload in the async phase. Always a **full** snapshot.
- **The GC problem:** every state entry is a live heap object the garbage collector must trace. 100 GB of heap state means 100 GB of live objects; full GC pauses go from milliseconds to *minutes*. Those pauses look like "the operator is stuck" and "checkpoints are slow" — the same symptoms as a dozen other problems.

### `EmbeddedRocksDBStateBackend`

```java
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;

env.setStateBackend(new EmbeddedRocksDBStateBackend(true));   // true = incremental
```

State is stored in **RocksDB**, an embedded key-value store, running *inside the TaskManager process* but **off-heap**, spilling to the local disk.

- **Read/write cost:** every access serializes the key, does a RocksDB lookup (memtable → block cache → possibly a disk read of an SST file), and deserializes the value. **10–100× slower per access** than heap.
- **Size limit:** local disk. Terabytes per TaskManager is normal.
- **Snapshot:** sync phase creates a RocksDB checkpoint = **hard links to the current immutable SST files**. Nearly free. Async phase uploads them, and with incremental enabled, only the new ones.
- **The disk requirement:** RocksDB does real random I/O. On local NVMe it is fine. On network-attached storage (EBS gp2, a Kubernetes PVC on network storage, NFS) it is agonising and your job will be mysteriously slow. Point `io.tmp.dirs` at genuinely local SSD.

> **Naming note:** the class moved from `org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend` to `org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend` in 1.20. Both names appear in the wild. It also needs its own dependency:
>
> ```xml
> <dependency>
>   <groupId>org.apache.flink</groupId>
>   <artifactId>flink-statebackend-rocksdb</artifactId>
>   <version>${flink.version}</version>
> </dependency>
> ```

### The comparison table

| | `HashMapStateBackend` | `EmbeddedRocksDBStateBackend` |
|---|---|---|
| Where state lives | JVM heap, as Java objects | Local disk + off-heap memory, as bytes |
| Serialization per access | **none** | serialize key + deserialize value, **every access** |
| Access latency | ~100 ns | ~1–100 µs |
| Practical size ceiling | TM heap; tens of GB | local disk; terabytes |
| GC impact | **severe** — all state is live heap objects | **negligible** — state is off-heap |
| Incremental checkpoints | ❌ no, always full | ✅ yes |
| Checkpoint sync phase | copy-on-write of the maps | hard-link SST files (cheaper) |
| Async phase cost | full state, every time | delta only, if incremental |
| Restore speed | fast (one blob, straight to heap) | slower (rebuild the file chain, warm the cache) |
| Sensitive to disk quality | no | **yes** — needs local SSD/NVMe |
| Timers | on heap (fast) | configurable: heap (default) or RocksDB |
| Supports very large single values | limited by heap | yes, but `MapState` beats one giant `ValueState` |

### Pick this when

**`HashMapStateBackend` when:**
- Total state per TaskManager comfortably fits in a few GB of heap.
- You need the lowest possible per-record latency and state is accessed on every record.
- Development, tests, and local runs — always.
- Small keyed aggregations, short windows, dedup over a bounded key space.

**`EmbeddedRocksDBStateBackend` when:**
- State exceeds heap, or you cannot bound it — high-cardinality keys, long windows, session state, streaming joins with a large build side.
- State growth over time is a real possibility (it always is).
- You need incremental checkpoints, which above ~10 GB you do.
- You have local SSD.

The honest default for anything going to production with unbounded keys: **RocksDB with incremental checkpoints.** The per-access penalty is real but predictable; a heap-based job that outgrows its heap fails hard and unpredictably.

### A subtlety: state access patterns matter more on RocksDB

```java
// ❌ On RocksDB this is brutal: the ENTIRE list is deserialized on read
//    and re-serialized on write, for every single record.
private ListState<Event> allEvents;
List<Event> current = new ArrayList<>();
for (Event e : allEvents.get()) current.add(e);   // full deserialize
current.add(newEvent);
allEvents.update(current);                        // full re-serialize

// ✅ ListState.add() appends without reading — RocksDB merge operator
allEvents.add(newEvent);

// ✅ MapState gives per-key access; only the touched entry is (de)serialized
private MapState<String, Long> perItemCount;
perItemCount.put(itemId, perItemCount.get(itemId) + 1);
```

On `HashMapStateBackend` all three are roughly the same speed, because nothing is serialized. Moving a job from heap to RocksDB and finding it 50× slower is nearly always this: code written against heap semantics.

---

## Part 2: Checkpoint storage

This is the setting people forget exists, because on `HashMapStateBackend` with no configuration it silently defaults to something that works locally and explodes in production.

### `JobManagerCheckpointStorage`

```java
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;

env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
```

Snapshots are sent to the **JobManager and held in its heap memory**.

- Default max size: **5 MB per state item** (`state.storage.fs.memory-threshold` / the constructor's `maxStateSize` argument).
- The JobManager's heap becomes your state ceiling for the whole job.
- If the JobManager dies, everything is gone unless HA storage is configured.

**Use for:** local development, unit tests, tiny stateless-ish jobs. **Never** production. This is the default when nothing is configured, which is why an unconfigured job silently works on your laptop and fails at scale with `Size of the state is larger than the maximum permitted memory-backed state`.

### `FileSystemCheckpointStorage`

```java
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;

env.getCheckpointConfig().setCheckpointStorage(
        new FileSystemCheckpointStorage("s3://my-bucket/flink/checkpoints"));
```

Snapshots go to a distributed file system. This is the production answer.

Shorthand — the `String` overload constructs a `FileSystemCheckpointStorage` for you:

```java
env.getCheckpointConfig().setCheckpointStorage("s3://my-bucket/flink/checkpoints");
```

Path schemes:

```java
"file:///data/flink/checkpoints"          // local FS — only valid for a single-node
                                          // setup; on a cluster each TM writes to ITS OWN
                                          // disk and the JM cannot read it back. Broken.
"hdfs://namenode:8020/flink/checkpoints"  // HDFS
"s3://bucket/flink/checkpoints"           // S3 (needs flink-s3-fs-presto or -hadoop plugin)
"s3a://bucket/flink/checkpoints"          // S3 via the Hadoop implementation
"gs://bucket/flink/checkpoints"           // Google Cloud Storage
"abfs://container@acct.dfs.core.windows.net/cp"   // Azure Data Lake Gen2
```

> **The most common production mistake:** `file://` on a multi-node cluster. It appears to work — checkpoints "complete" — but each TaskManager wrote to its own local disk, so recovery on a different node finds nothing. **Checkpoint storage must be a path every node can read and write.**

For S3, the plugin must be *installed*, not just on the classpath:

```bash
mkdir -p ./plugins/s3-fs-presto
cp ./opt/flink-s3-fs-presto-1.20.0.jar ./plugins/s3-fs-presto/
```

Two implementations exist: `flink-s3-fs-presto` (faster, recommended for checkpointing) and `flink-s3-fs-hadoop` (needed for the `StreamingFileSink`/`FileSink` writing actual data files). You can install both.

### Config file equivalent

```yaml
state.backend.type: rocksdb
state.backend.incremental: true

execution.checkpointing.dir: s3://my-bucket/flink/checkpoints
execution.checkpointing.savepoint-dir: s3://my-bucket/flink/savepoints
execution.checkpointing.num-retained: 3

# Below this size, state is embedded directly in the _metadata file
# instead of getting its own file. Prevents a small-files explosion.
state.storage.fs.memory-threshold: 20kb
```

`execution.checkpointing.dir` and `execution.checkpointing.savepoint-dir` (1.19+) were previously `state.checkpoints.dir` and `state.savepoints.dir` — both still work. Setting the directory in the config is enough; you do not need `setCheckpointStorage` in code at all, and you shouldn't have it there, because code wins over config.

The four combinations, and whether each is sensible:

| State backend | Checkpoint storage | Verdict |
|---|---|---|
| HashMap | JobManager | ✅ local dev / tests |
| HashMap | FileSystem | ✅ small-state production, low latency |
| RocksDB | JobManager | ❌ pointless — large state, 5 MB storage cap |
| RocksDB | FileSystem | ✅ the standard production setup |

---

## Part 3: What's actually on disk

Look inside your checkpoint directory. This is worth doing once by hand; it makes the abstraction concrete.

```
s3://my-bucket/flink/checkpoints/
└── a1b2c3d4e5f6...            ← the JOB ID (32 hex chars). One dir per job.
    ├── chk-41/                ← one completed checkpoint. N = checkpoint id.
    │   └── _metadata          ← THE FILE THAT MATTERS. Points at everything else.
    ├── chk-42/
    │   └── _metadata
    ├── chk-43/
    │   └── _metadata
    ├── shared/                ← incremental checkpoints: SST files shared
    │   ├── 0a1f...-sst        ← ACROSS checkpoints. This is where the bulk lives.
    │   ├── 3b7c...-sst           A file stays until NO checkpoint references it.
    │   └── ...
    └── taskowned/             ← state whose lifecycle the TaskManager owns and
        └── ...                   the JobManager cannot safely delete (e.g. some
                                  unaligned-checkpoint / changelog artifacts)
```

Read that structure carefully:

- **`chk-N/_metadata`** is what you pass to `flink run -s`. It is small — a manifest of operator IDs, their state handles, and the file paths those handles point at. Restoring means reading this and then fetching what it names.
- **`chk-N/` is often nearly empty** with incremental checkpoints, because the actual state lives in `shared/`. Do not conclude your state is tiny.
- **`shared/` grows and does not shrink monotonically.** An SST file survives as long as any retained checkpoint references it. Delete a `chk-N` folder by hand and you may orphan files, or worse, delete a `shared/` file that a live checkpoint needs. **Never hand-delete inside a checkpoint directory of a running job.**
- **`execution.checkpointing.num-retained: 3`** means only `chk-41`, `chk-42`, `chk-43` exist; `chk-40` and its no-longer-referenced shared files were cleaned up by Flink.
- Small state (below `state.storage.fs.memory-threshold`) is inlined *into* `_metadata` rather than written as its own file. Without this, a job with 500 subtasks and tiny per-subtask state would create 500 tiny files per checkpoint and destroy your object store.

Inspecting it:

```bash
# local filesystem
ls -R /tmp/flink-checkpoints/

# S3
aws s3 ls --recursive s3://my-bucket/flink/checkpoints/<job-id>/

# how big is each checkpoint really?
du -sh /tmp/flink-checkpoints/<job-id>/chk-*
du -sh /tmp/flink-checkpoints/<job-id>/shared
```

---

## Part 4: RocksDB tuning pointers

You will not need these on day one, but you need to know they exist.

### Managed memory — the one that matters

RocksDB's memory (memtables + block cache) comes out of Flink's **managed memory**, not the JVM heap. Flink sizes it for you as a fraction of the TaskManager's total memory:

```yaml
taskmanager.memory.managed.fraction: 0.4       # default: 40% of TM total memory
state.backend.rocksdb.memory.managed: true     # default: let Flink manage it
```

This is the correct default and you should leave it alone in that shape. What you *do* control is how much total memory the TaskManager gets — more managed memory means a bigger block cache means fewer disk reads.

The one thing to watch: managed memory is shared across **all slots on the TaskManager**. Eight slots means each RocksDB instance gets an eighth. Fewer, fatter TaskManagers are usually better for RocksDB jobs than many thin ones.

### Predefined options

```java
EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
backend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
env.setStateBackend(backend);
```

```yaml
state.backend.rocksdb.predefined-options: SPINNING_DISK_OPTIMIZED_HIGH_MEM
```

| Option | For |
|---|---|
| `DEFAULT` | RocksDB's own defaults |
| `SPINNING_DISK_OPTIMIZED` | HDDs; fewer, larger I/Os |
| `SPINNING_DISK_OPTIMIZED_HIGH_MEM` | HDDs with memory to spare — bigger memtables and block cache |
| `FLASH_SSD_OPTIMIZED` | SSD/NVMe; more parallel compaction, smaller blocks |

Start with `FLASH_SSD_OPTIMIZED` on modern hardware, or leave it at `DEFAULT` with managed memory on.

### Block cache and write buffers

Only relevant when you have disabled managed memory and are sizing by hand:

```yaml
state.backend.rocksdb.memory.managed: false
state.backend.rocksdb.block.cache-size: 256mb   # cache of uncompressed data blocks
state.backend.rocksdb.writebuffer.size: 64mb    # memtable size before flushing to SST
state.backend.rocksdb.writebuffer.count: 4      # memtables per column family
state.backend.rocksdb.thread.num: 4             # background flush/compaction threads
```

The block cache is the lever for **read**-heavy state (large `MapState` lookups); write buffers are the lever for **write**-heavy state. A block cache hit rate you can see in the metrics is worth more than any of these numbers guessed.

### Timers

```yaml
state.backend.rocksdb.timer-service.factory: HEAP     # default
# or ROCKSDB
```

Timers default to the **heap**, even with RocksDB, because timer access is latency-critical. If you have tens of millions of timers this becomes your actual heap problem — move them to `ROCKSDB` and accept the slower firing.

### Enable metrics before you tune

```yaml
state.backend.rocksdb.metrics.block-cache-hit-count: true
state.backend.rocksdb.metrics.estimate-num-keys: true
state.backend.rocksdb.metrics.estimate-live-data-size: true
state.backend.rocksdb.metrics.num-running-compactions: true
```

These are off by default because they cost a little. Turn them on when you are actually tuning, not permanently.

---

## Remember

- **State backend ≠ checkpoint storage.** Backend = where state lives while running. Storage = where snapshots go. Configured separately.
- `HashMapStateBackend`: heap, no serialization, fastest access, **GC-bound**, full checkpoints only. The default.
- `EmbeddedRocksDBStateBackend`: off-heap + local disk, serde on every access, TB-scale, **incremental checkpoints**, needs local SSD.
- Code written for heap semantics (read-modify-write a whole `ListState`) becomes pathologically slow on RocksDB. Use `add()` and `MapState`.
- `JobManagerCheckpointStorage` = dev only, ~5 MB cap. `FileSystemCheckpointStorage(path)` = production.
- **`file://` on a multi-node cluster is broken** even though checkpoints appear to succeed. Storage must be shared and readable by every node.
- On disk: `<job-id>/chk-N/_metadata` is the manifest you restore from; `shared/` holds the incremental SST files; `taskowned/` holds TM-owned artifacts. Never hand-delete inside it.
- `execution.checkpointing.num-retained: 3` keeps a fallback if the newest checkpoint is bad.
- RocksDB memory comes from **managed memory**, split across all slots on the TM. Prefer fewer, fatter TaskManagers.
- Timers default to heap even under RocksDB.

**Interview one-liners**

- *"State backend vs checkpoint storage?"* → The backend is where working state lives during execution; the storage is where the snapshot is durably written. Independent settings — RocksDB with S3 is the usual production pair.
- *"HashMap vs RocksDB?"* → Heap objects, nanosecond access, heap-bounded, GC-heavy, full checkpoints only — versus off-heap bytes on local disk, serde per access, terabyte scale, incremental checkpoints, needs local SSD.
- *"Why is incremental only on RocksDB?"* → RocksDB state is already immutable SST files, so a checkpoint can reference previously uploaded files and upload only new ones. Heap state has no such structure.
- *"What's in a checkpoint directory?"* → A per-job directory with `chk-N` folders each containing `_metadata`, plus a `shared` directory holding SST files referenced across incremental checkpoints and a `taskowned` directory.
- *"Why did checkpoints succeed but recovery fail?"* → Checkpoint storage was a local `file://` path on a multi-node cluster, so each TaskManager wrote where no one else could read.
- *"Where does RocksDB's memory come from?"* → Flink's managed memory (default 40% of TM memory), shared across slots — not the JVM heap.
