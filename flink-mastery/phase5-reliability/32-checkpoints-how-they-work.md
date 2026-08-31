# 32. Checkpoints — How They Actually Work

Phases 1–4 gave you state: `ValueState`, `ListState`, windows, timers. All of that lives in memory on a TaskManager. TaskManagers die. This chapter is the mechanism that makes that survivable.

> **Key idea**
> A checkpoint is a **globally consistent snapshot** of every operator's state, taken *without stopping the stream*.
> "Consistent" means: every operator's saved state reflects **exactly the same prefix of the input**. Not "roughly the same moment in wall-clock time" — the same set of records.

---

## The problem, stated precisely

You have a job with parallelism 3. Each subtask holds a counter in `ValueState`. You want to save all of them so that after a crash you can resume.

The naive approach — "at 10:00:00, everyone write your state to S3" — is **wrong**:

```
Wall-clock 10:00:00.000

Source subtask  : has emitted records 1..1000
Map subtask     : has processed  1..990   (10 still in the network buffer)
Aggregate subtask: has processed 1..950   (40 in flight)

Snapshot taken now says:
   source offset = 1000
   aggregate counted 950 records

On restore: replay from offset 1000. Records 951..1000 are NEVER counted.
                                       ^^^^^^^^^^^^^^^^ silent data loss
```

The snapshot is inconsistent because different operators are at different points in the stream. To be consistent, the source's saved offset must be exactly the point up to which every downstream operator has processed.

The naive fix — stop the world, drain all in-flight records, then snapshot — is consistent but pauses the job for the whole snapshot duration. At 1M events/sec with 200 GB of state that is minutes of downtime, every minute.

Flink's answer is neither.

---

## Chandy–Lamport, adapted: asynchronous barrier snapshotting

Flink uses a variant of the 1985 **Chandy–Lamport distributed snapshot algorithm**. The whole idea is one sentence:

> Inject a **marker** into the stream. When an operator sees the marker, it snapshots its state and forwards the marker. The marker itself defines the consistent cut.

In Flink the marker is called a **checkpoint barrier**. It is a special record that flows in the data stream, in order, alongside your `Event` objects.

```
Checkpoint barrier for checkpoint N, drawn as ▮

BEFORE the barrier: records that belong to checkpoint N
AFTER  the barrier: records that belong to checkpoint N+1

   ─── e e e e ▮ e e e e ───►
       └─ in N ─┘ └ in N+1 ┘
```

Because the barrier travels *with* the records and never overtakes them (in the aligned case), every operator that snapshots on the barrier has, by construction, processed exactly the records that came before it. That is the consistent cut. No clock is involved anywhere.

---

## The full lifecycle, step by step

```
                       ┌──────────────────┐
                       │   JobManager     │
                       │ CheckpointCoord. │
                       └────────┬─────────┘
   (1) "start checkpoint N"     │
       every `interval` ms      ▼
                       ┌──────────────────┐
                       │  Source subtask  │  (2) records its offset,
                       │                  │      injects barrier N
                       └────────┬─────────┘
                                │  ─ e e ▮ e e ─►
                                ▼
                       ┌──────────────────┐
                       │   Map subtask    │  (3) sees ▮, snapshots its state,
                       │                  │      forwards ▮ downstream
                       └────────┬─────────┘
                                │  ─ e e ▮ e e ─►
                                ▼
                       ┌──────────────────┐
                       │  Window subtask  │  (4) same
                       └────────┬─────────┘
                                ▼
                       ┌──────────────────┐
                       │      Sink        │  (5) sees ▮, snapshots,
                       │                  │      ACKs to JobManager
                       └────────┬─────────┘
                                │
   (6) when EVERY subtask has   ▼
       ACKed: checkpoint N is  ┌──────────────────┐
       COMPLETE. JM writes     │   JobManager     │
       _metadata and calls     │ notifyCheckpoint │
       notifyCheckpointComplete│    Complete      │
                               └──────────────────┘
```

Two things to fix in your head:

1. **The JobManager only talks to the sources.** It does not tell the Map operator when to snapshot — the barrier does that. This is what makes it scale.
2. **Step 6 is what makes exactly-once sinks possible.** `notifyCheckpointComplete` is the signal a transactional sink uses to commit. Chapter 37.

---

## Barrier alignment — the part everyone gets wrong

An operator with **one** input is easy: see barrier, snapshot, forward.

An operator with **multiple** input channels — after any `keyBy`, any `union`, any connected stream, or simply a parallelism change — receives one barrier per channel, and they do not arrive at the same time.

If it snapshotted on the *first* barrier, it would then keep consuming records from the slower channel that logically belong *after* the cut, and those records would be counted twice on restore. So it must wait.

```
ALIGNMENT

t=0    barrier N arrives on channel 1
       ┌──────────────────────────────────────────┐
ch 1:  │ e e e ▮ | x x x x x x x x x x            │  channel BLOCKED after ▮;
       └──────────────────────────────────────────┘  x's are BUFFERED, not processed
       ┌──────────────────────────────────────────┐
ch 2:  │ e e e e e e e e e e e e e e e e e e ▮    │  still streaming N's records
       └──────────────────────────────────────────┘
                                              ↑
t=30s                            barrier N arrives on channel 2
                                 → NOW snapshot, forward ▮,
                                   unblock ch 1 and drain the buffered x's

ALIGNMENT DURATION = 30 seconds
```

The rule:

> On receiving barrier N on a channel, **stop reading that channel**. Buffer whatever arrives on it. Keep processing the other channels normally. When barrier N has arrived on **all** channels, snapshot, emit barrier N downstream, and resume all channels.

### Why alignment is expensive under backpressure

Alignment duration ≈ **the skew between your fastest and slowest input channel**. Two things inflate it:

- **Data skew.** One subtask has a hot key, runs slower, so its barrier is late. See [`../../03-state-and-skew.md`].
- **Backpressure.** The barrier is an ordinary record in an ordinary network buffer. It cannot skip the queue. If a channel has 10,000 queued records ahead of the barrier, the barrier arrives 10,000 records later.

```
Source ──[ e e e e e e e e e e ▮ e e ]──► Operator
                                ↑
                    barrier stuck behind 10 buffered records.
                    At 200k rec/s consumption and full buffers,
                    this is milliseconds. At 200 rec/s it is a minute.
```

And it compounds: while channel 1 is blocked, its upstream buffers fill, which backpressures *its* upstream, which slows everything further. This is the mechanism behind "checkpoints got slow but my state is tiny."

> **Key idea**
> Slow *aligned* checkpoints with small state are almost never a checkpointing problem. They are a **backpressure or skew problem** measured by the checkpointing subsystem.
> The full causal tree for that is in [`../../01-checkpointing-slow.md`] — read it after this chapter.

---

## Unaligned checkpoints

Flink 1.11 added an alternative: **let the barrier overtake the buffered records, and snapshot the buffers themselves.**

```
UNALIGNED

t=0    barrier N arrives on channel 1.
       Operator does NOT block. Instead:

       1. Immediately move barrier N to the FRONT of every output queue
          (it overtakes any records already queued for output).
       2. Snapshot operator state, AS OF NOW.
       3. ALSO snapshot every in-flight record currently sitting in the
          input buffers and output buffers.
       4. Keep processing. Done in milliseconds.

       ┌──────────────────────────────────────────┐
ch 1:  │ e e e ▮ x x x x                          │  x's are IN the snapshot
       └──────────────────────────────────────────┘
       ┌──────────────────────────────────────────┐
ch 2:  │ e e e e e e e e e e e e ▮                │  the e's still to come are
       └──────────────────────────────────────────┘  ALSO in the snapshot
```

The consistent cut is no longer "the operator has processed exactly the pre-barrier records". It is "the operator's state **plus the recorded in-flight data** together represent exactly the pre-barrier records". On restore, Flink injects the saved in-flight data back into the network stack before resuming.

### What it costs

| | Aligned | Unaligned |
|---|---|---|
| Checkpoint duration under backpressure | grows with backpressure, unbounded | roughly constant |
| Checkpoint **size** | state only | state **+ all in-flight buffer contents** |
| I/O per checkpoint | lower | higher (writing buffers too) |
| Restore time | faster | slower — buffers must be replayed into the network stack |
| Rescaling on restore | supported | supported since 1.15 (was a restriction before) |
| Works with EXACTLY_ONCE sinks | yes | **yes** — the "they're incompatible" claim is a myth |
| Requires | nothing | `EXACTLY_ONCE` mode (unaligned is meaningless in at-least-once) |

### When to enable it

- **Yes:** you have transient backpressure spikes, or unavoidable skew, and alignment duration is your dominant checkpoint cost.
- **No:** your job is comfortably keeping up. You would pay extra I/O for nothing.
- **Not a fix for:** a genuinely overloaded job. Unaligned checkpoints stop backpressure from *breaking* checkpointing; they do not stop the backpressure. Your lag still grows.

The practical setting is the hybrid, covered in chapter 33: start aligned, auto-switch to unaligned only if alignment exceeds a timeout.

---

## Asynchronous snapshotting: sync phase vs async phase

"Snapshot the state" is itself two phases, and the UI reports them separately.

```
Operator receives the last barrier
   │
   ├── SYNC PHASE   (operator thread is STOPPED — no records processed)
   │      • take an immutable, point-in-time view of the state
   │        - HashMapStateBackend  : copy-on-write snapshot of the hash maps
   │        - RocksDB              : create a RocksDB *checkpoint* = hard links
   │                                 to the current SST files. Nearly free.
   │      • hand that view to a background thread
   │      • RESUME PROCESSING
   │   ← target: single-digit to low tens of milliseconds
   │
   └── ASYNC PHASE  (background thread; operator is processing records again)
          • serialize / upload the snapshot view to durable storage
            (S3, HDFS, local FS)
          • when done, ACK to the JobManager
       ← this is where seconds or minutes go, and it does NOT stall the stream
```

> **Key idea**
> The only part of a checkpoint that blocks your data flow is the **sync phase**, and it is designed to be tiny. This is why Flink can checkpoint 200 GB of state without a 200 GB pause. If your *sync* duration is large, your state backend is doing real work in the wrong phase — usually `HashMapStateBackend` with a very large heap.

RocksDB's trick is worth internalising: RocksDB SST files are **immutable once written**. So "snapshot" = "record the list of SST files that exist right now and hard-link them so compaction can't delete them". That is O(number of files), not O(state size). The async phase then uploads them — and with incremental checkpoints, only the *new* ones (chapter 33).

---

## Reading the checkpoint metrics in the Web UI

Web UI → your job → **Checkpoints** tab → **History** → click a checkpoint → per-subtask table.

| Column | What it measures | What a big number means |
|---|---|---|
| **Start Delay** | Time from the checkpoint being triggered by the JM to the first barrier arriving at this subtask | The barrier is stuck in queues upstream → **backpressure**. Checkpointing is the victim, not the cause. |
| **Alignment Duration** | Time between the *first* and the *last* barrier arriving at this subtask | Channel skew: a hot key, one slow subtask, or backpressure on one path. Zero for single-input operators and for unaligned checkpoints. |
| **Sync Duration** | The blocking part of the snapshot | Huge heap state, or a very large number of timers. Move to RocksDB. |
| **Async Duration** | Uploading the snapshot to durable storage | State too large per checkpoint, slow/throttled object store, too many tiny files. Enable incremental checkpoints. |
| **End to End Duration** | Trigger → this subtask ACKed. Roughly the sum of the above. | The number people quote; useless on its own. Always decompose it. |
| **Checkpointed Data Size** | Bytes written *by this checkpoint*. With incremental, the delta, not the full state. | Growing steadily over days = state leak, no TTL. |
| **Full Checkpoint Data Size** | Total logical size of the state | The number that matters for restore time. |
| **Processed / Persisted (in-flight data)** | Unaligned only: bytes of buffer content persisted | Non-zero means unaligned kicked in. |

The diagnostic reflex, in order:

```
Checkpoint slow?
  ├─ Start Delay big?        → backpressure. Fix the pipeline, not the checkpoint config.
  ├─ Alignment big?          → skew, or backpressure on one path. Unaligned CP is a band-aid.
  ├─ Sync big?               → state backend choice / timer count.
  └─ Async big?              → state size, storage throughput. Go incremental.
```

The full decision tree with real code fixes for each branch lives in [`../../01-checkpointing-slow.md`]. This chapter is the mechanism; that file is the diagnosis.

---

## What is *in* a checkpoint

Everything Flink knows it needs to resume, and nothing else:

- **Keyed state** — all your `ValueState`, `ListState`, `MapState`, `ReducingState`, `AggregatingState`, for every key of every subtask.
- **Operator state** — non-keyed state, e.g. a Kafka source's assigned partitions.
- **Source offsets / positions** — the Kafka partition offsets, the file split positions. This is what makes rewind possible.
- **Timers** — every registered event-time and processing-time timer.
- **Window contents** — windows are just keyed state, so they come along automatically.
- **Sink transaction handles** — for two-phase-commit sinks (chapter 37).
- **In-flight network buffers** — *only* for unaligned checkpoints.

What is **not** in a checkpoint: anything you did outside Flink. A row you inserted into Postgres, an email you sent, a file you wrote with `FileWriter`. That is the entire subject of chapter 37.

---

## A minimal job to watch this happen

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

public class CheckpointVisible {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);          // one checkpoint every 5 seconds

        env.socketTextStream("localhost", 9999) // nc -lk 9999 to feed it
           .map(String::toUpperCase)            // method reference == (s) -> s.toUpperCase()
           .keyBy(s -> s)                       // creates a shuffle → multiple input channels
           .sum(0)                              // wrong for Strings, but we only care about the shape
           .print();

        env.execute("checkpoint visible");
    }
}
```

Line notes for a Java newcomer:

- `env.enableCheckpointing(5000)` — the only line strictly required to turn checkpointing on. The argument is the **interval in milliseconds**. Off by default; a job with no checkpointing loses all state on any failure.
- `String::toUpperCase` — a **method reference**, shorthand for the lambda `s -> s.toUpperCase()`. Java allows it whenever the lambda does nothing but call one method.
- `keyBy(s -> s)` — matters here because it forces a network shuffle, which is what gives downstream operators multiple input channels, which is what makes alignment observable.

Run it, open `http://localhost:8081`, click your job, then the **Checkpoints** tab. Watch the History table fill in every 5 seconds and read the columns from the table above.

---

## Remember

- A checkpoint is a **consistent snapshot**: every operator's state reflects the same prefix of the input.
- The mechanism is **Chandy–Lamport asynchronous barrier snapshotting**. Barriers are records in the stream, not clock events.
- The JobManager triggers **only the sources**; barriers propagate the rest.
- Records **before** the barrier belong to checkpoint N; records **after** belong to N+1.
- **Alignment**: an operator with multiple inputs blocks each channel after its barrier arrives and waits for the rest. Alignment duration = channel skew.
- Barriers travel **in-band**, so backpressure delays them. That is why backpressure shows up as slow checkpoints.
- **Unaligned checkpoints**: barrier overtakes buffered records; in-flight data is snapshotted instead. Constant duration under backpressure, bigger checkpoints, slower restore.
- Snapshotting splits into a short **sync** phase (blocks the operator) and a long **async** phase (does not).
- **Never quote End-to-End Duration alone.** Decompose into Start Delay / Alignment / Sync / Async — each has a different fix.

**Interview one-liners**

- *"How does Flink checkpoint without stopping the job?"* → Chandy–Lamport barriers injected at the sources flow in-band with records; each operator snapshots when it sees the barrier, so the barrier itself defines a consistent cut. Snapshotting is split into a short synchronous phase and a long asynchronous upload.
- *"What is barrier alignment?"* → An operator with multiple input channels blocks each channel once its barrier arrives and buffers it, until barriers have arrived on all channels. Alignment duration measures the skew between channels.
- *"Why do checkpoints get slow under backpressure?"* → Barriers are ordinary in-band records queued behind your data; full buffers mean the barrier crawls. Symptom is high Start Delay and Alignment with small state.
- *"What do unaligned checkpoints change?"* → The barrier overtakes queued records and the in-flight buffer contents become part of the snapshot, so duration stops depending on backpressure — at the cost of larger checkpoints and slower restore.
- *"Sync vs async phase?"* → Sync takes an immutable point-in-time view (copy-on-write for heap, hard-linked SST files for RocksDB) and blocks the operator briefly; async uploads it to durable storage while processing continues.
- *"What's in a checkpoint?"* → Keyed state, operator state, source offsets, timers, window contents, sink transaction handles — plus in-flight buffers if unaligned.
