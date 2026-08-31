# Checkpointing is Slow — The Full Causal Tree

> The interview question: *"You have 1M events/sec. Checkpointing is slow / timing out.
> What's the reason?"*

This is the single most-asked Flink production question. The interviewer is NOT looking for
one answer. They want to see you **decompose the checkpoint lifecycle** and name which
stage is slow. Anyone can say "increase the timeout." That's the wrong answer.

---

## Part 0: You cannot debug this without knowing the 4 phases

A Flink checkpoint has four distinct phases. The Flink UI reports each one **separately**.
Your first move in an interview (and in prod) is always: *"which phase is slow?"*

```
Checkpoint N triggered by JobManager
   │
   ├── (1) TRIGGER DELAY  ──── barrier injected at source, travels through DAG
   │                            UI column: "Checkpoint Duration (Async)" / start delay
   │
   ├── (2) ALIGNMENT     ──── operator waits for barriers from ALL input channels
   │                            UI column: "Alignment Duration"     ← usually the killer
   │
   ├── (3) SYNC PHASE    ──── operator pauses, snapshots state to memory/disk
   │                            UI column: "Sync Duration"
   │
   └── (4) ASYNC PHASE   ──── state uploaded to durable store (S3/HDFS)
                                UI column: "Async Duration"          ← 2nd most common
```

**The whole interview answer hinges on this**: each phase has a *completely different*
root cause and a *completely different* fix. Naming the phase is 80% of the answer.

---

## Part 1: The Decision Table (memorize this)

| Slow phase | What it means | Most likely root cause | Fix |
|---|---|---|---|
| **Start delay** high | Barrier took long to even reach the operator | Backpressure upstream — source/operator queues full | Fix the backpressure first; checkpointing is a *symptom* |
| **Alignment** high | Waiting on the slowest input channel | **Data skew** or one slow subtask | Unaligned checkpoints, or fix the skew |
| **Sync** high | Snapshotting the state itself is slow | Huge state, heap state backend, timers | RocksDB + incremental |
| **Async** high | Uploading to S3/HDFS is slow | State too big, network, S3 throttling | Incremental checkpoints, fewer/larger files |

Say this table out loud in the interview. It shows you've actually operated a job.

---

## Part 2: Cause #1 — Backpressure (the trap answer)

This is the #1 real-world cause and the one most candidates miss.

**The key insight, stated as an interviewer wants to hear it:**
> "Slow checkpointing is usually not a checkpointing problem. It's a backpressure problem
> wearing a checkpointing costume."

### Why: the barrier is just another record in the queue

Checkpoint barriers flow **in-band** with your data. They sit in the same network buffers
as your events. They do not get to skip the line.

```
Source ──[ e e e e e e e e B e e ]──> Operator
                            ↑
                     barrier is STUCK behind
                     10,000 queued events
```

If your operator is processing 200k events/sec but receiving 1M events/sec, the queues are
permanently full. The barrier crawls. Checkpoint duration explodes — but your *state* might
be tiny. You're measuring the queue, not the snapshot.

### The code that causes it

```java
// ❌ THE CLASSIC: blocking I/O inside a hot-path operator
public class EnrichWithProfile extends RichMapFunction<Event, Enriched> {

    private transient JdbcClient db;

    @Override
    public Enriched map(Event e) throws Exception {
        // 20ms blocking DB call.
        // Max throughput of THIS SUBTASK = 1000/0.020 = 50 events/sec.
        // With 200 subtasks -> 10,000 events/sec ceiling.
        // You need 1,000,000/sec. You are 100x short.
        Profile p = db.lookup(e.getUserId());
        return new Enriched(e, p);
    }
}
```

Do the arithmetic out loud in an interview. `1 / latency = per-subtask throughput`. That
single division is the whole diagnosis. At 1M/sec you cannot afford *any* synchronous
blocking call in the hot path.

### The fix

```java
// ✅ AsyncIO — decouples in-flight requests from the operator thread
public class AsyncEnrich extends RichAsyncFunction<Event, Enriched> {

    private transient AsyncDbClient db;

    @Override
    public void asyncInvoke(Event e, ResultFuture<Enriched> out) {
        db.lookupAsync(e.getUserId())
          .thenAccept(p -> out.complete(
                  Collections.singleton(new Enriched(e, p))));
        // returns immediately -> operator thread never blocks
    }

    @Override
    public void timeout(Event e, ResultFuture<Enriched> out) {
        // ALWAYS implement this. Default behaviour throws and kills the job.
        out.complete(Collections.singleton(Enriched.withoutProfile(e)));
    }
}

AsyncDataStream.unorderedWait(          // unordered = higher throughput
        stream,
        new AsyncEnrich(),
        100, TimeUnit.MILLISECONDS,     // timeout
        1000);                          // capacity = max in-flight
```

**Gotcha worth mentioning unprompted (interviewers love this):**
`capacity` is a backpressure valve. Once 1000 requests are in flight, `asyncInvoke` blocks
again — and you are right back to backpressure. Also: **in-flight async requests are part
of the checkpointed state**, so a huge capacity makes checkpoints bigger, not just slower.
There is a real tension between the two knobs.

`unorderedWait` vs `orderedWait`: ordered must buffer completed results until earlier ones
finish, so a single slow lookup head-of-line blocks everything. Use unordered unless you
genuinely need order.

---

## Part 3: Cause #2 — Alignment / Data Skew

If **Alignment Duration** is the big number, you have skew.

### The mechanism

An operator with 2 inputs must wait for the barrier on *both* before it can snapshot.
It buffers records from the fast channel while waiting for the slow one.

```
input 1: ──e─e─B──────────────────  barrier arrived at t=0  (then BLOCKED, buffering)
input 2: ──e─e─e─e─e─e─e─e─e─e─B──  barrier arrives at t=30s
                                    ↑
                       alignment duration = 30 seconds
```

Alignment time ≈ **skew between your slowest and fastest channel**. It measures imbalance.

### The code that causes it

```java
// ❌ keyBy on a field with a massive hot key
events.keyBy(e -> e.getTenantId())    // tenant "ACME_CORP" = 40% of all traffic
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .aggregate(new CountAgg());
```

One subtask gets 400k events/sec while its 199 siblings get ~3k each. That one subtask is
the slow channel. It stalls alignment for the entire job. **Adding parallelism does not
help** — the hot key still hashes to exactly one subtask. This is an important thing to say:
scaling out is the intuitive fix and it is *useless* here.

### Fix A: two-phase aggregation (key salting)

```java
// ✅ Stage 1: split the hot key across N buckets
DataStream<Partial> partial = events
    .keyBy(e -> e.getTenantId() + "#" + (e.hashCode() % 64))   // salt
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(new PartialCountAgg());

// ✅ Stage 2: combine the 64 partials — tiny volume, skew no longer matters
DataStream<Result> result = partial
    .keyBy(Partial::getTenantId)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(new MergeCountAgg());
```

Works for **associative/commutative** aggregations (count, sum, min, max, HLL sketches).
Does **not** work for median/exact-distinct/"last value" without more care — a good thing
to flag, since it shows you know the limits of your own answer.

### Fix B: unaligned checkpoints

```java
env.getCheckpointConfig().enableUnalignedCheckpoints();
env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofSeconds(30));
// ^ starts aligned, auto-switches to unaligned if alignment exceeds 30s.
//   Best of both: cheap in the normal case, resilient under backpressure.
```

Unaligned checkpoints snapshot the **in-flight buffer contents** as part of state instead of
waiting for barriers to drain.

**The trade-offs (name these — it's the difference between memorized and understood):**
- Checkpoint *size* grows — you're now storing in-flight data too
- Recovery gets slower — that buffered data must be replayed back into the network stack
- **Incompatible with `EXACTLY_ONCE` sinks? No — that's a myth.** They work fine together.
- Real restriction: does not work with pointwise (rescale/forward) connections in some
  versions, and doesn't help at all if the *source* itself is slow

Unaligned checkpoints are a **band-aid for backpressure, not a cure for skew.** If your
alignment is slow because of a hot key, you still have a hot key — you've just stopped it
from breaking checkpoints. Say this.

---

## Part 4: Cause #3 — State Size (slow Sync/Async phase)

### The mechanism

```
Full checkpoint at 1M events/sec, 200GB state, every 60s:
  200 GB / 60 s = 3.4 GB/sec sustained upload to S3
  → physically impossible on most clusters
  → checkpoint N+1 triggers before N finishes
  → pile-up → timeout → job restart → replay → MORE backpressure → death spiral
```

That death spiral is the money detail. Slow checkpoints *cause* restarts, restarts cause
replay, replay causes backpressure, backpressure causes slower checkpoints. Naming the
feedback loop is what separates a strong answer.

### The fix: RocksDB + incremental

```java
// ✅ Incremental: upload only the NEW RocksDB SST files since last checkpoint
env.setStateBackend(new EmbeddedRocksDBStateBackend(true));   // true = incremental
env.getCheckpointConfig().setCheckpointStorage("s3://bucket/checkpoints");
```

200GB state → maybe 2GB of new SSTables per checkpoint. 100x reduction.

**Interview gotcha — incremental checkpoints have a nasty edge:**
Because they're a chain of deltas, an old SST file can be referenced for a very long time.
Your checkpoint *storage* grows unboundedly even though each checkpoint is small, and
**restore time gets worse**, not better — you must reassemble the chain. Restore duration is
the metric people forget to monitor, and it's exactly what bites you at 3am during a real
incident.

### Heap vs RocksDB — the real trade

```java
// HashMapStateBackend  — state on JVM heap
//   + fastest access (no serialization on read)
//   − limited by heap; snapshot is a full stop-the-world copy
//   − ⚠️ 200GB of state = 200GB of heap = GC pauses measured in MINUTES
//     Those GC pauses show up as slow checkpoints AND as "operator is stuck".

// EmbeddedRocksDBStateBackend — state off-heap on local disk
//   + state limited by disk, not RAM. TB-scale is fine.
//   + incremental checkpointing available
//   − every access is a serde + potential disk read (10-100x slower per access)
//   − ⚠️ needs FAST LOCAL SSD. On EBS gp2 or network storage, RocksDB is agony.
```

At 1M events/sec the answer is essentially always RocksDB + incremental + local NVMe.
Mentioning the **local NVMe requirement** is a strong signal — it's an infra detail you only
know if you've actually run this.

---

## Part 5: Cause #4 — Unbounded State (the silent killer)

Everything above is tuning. This one is a **bug**, and it's the most common real cause of
"checkpoints got slow over time."

```java
// ❌ State that NEVER gets cleaned up
public class SessionTracker extends KeyedProcessFunction<String, Event, Alert> {

    private ValueState<SessionData> session;

    @Override
    public void processElement(Event e, Context ctx, Collector<Alert> out) {
        SessionData s = session.value();
        if (s == null) s = new SessionData();
        s.add(e);
        session.update(s);
        // no timer, no TTL, no clear().
        // Every new userId adds a key FOREVER.
        // 1M events/sec with high-cardinality keys -> state grows without bound.
        // Checkpoints get slower every single day until the job dies.
    }
}
```

The signature symptom: **checkpoint duration climbing linearly over days/weeks.** If the
interviewer says "it got slow gradually," this is the answer, not tuning.

### Fix A: explicit timer-based cleanup

```java
// ✅
public class SessionTracker extends KeyedProcessFunction<String, Event, Alert> {

    private ValueState<SessionData> session;

    @Override
    public void processElement(Event e, Context ctx, Collector<Alert> out) throws Exception {
        SessionData s = session.value();
        if (s == null) {
            s = new SessionData();
        } else {
            // delete the old timer, otherwise timers accumulate as fast as events
            ctx.timerService().deleteEventTimeTimer(s.getExpiryTimer());
        }
        s.add(e);

        long expiry = ctx.timestamp() + Duration.ofMinutes(30).toMillis();
        ctx.timerService().registerEventTimeTimer(expiry);
        s.setExpiryTimer(expiry);
        session.update(s);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        session.clear();     // ← the line whose absence kills the job
    }
}
```

**Sub-gotcha:** timers are themselves checkpointed state. Forgetting `deleteEventTimeTimer`
means you register a new timer per event and never remove the old ones — you fix the state
leak and create a *timer* leak. At 1M/sec that's 1M timers/sec accumulating.

### Fix B: State TTL (simpler, but read the caveat)

```java
// ✅
StateTtlConfig ttl = StateTtlConfig
    .newBuilder(Time.hours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupInRocksdbCompactFilter(1000)   // ← without this, cleanup is lazy
    .build();

ValueStateDescriptor<SessionData> d =
        new ValueStateDescriptor<>("session", SessionData.class);
d.enableTimeToLive(ttl);
```

**The caveat that matters:** by default, TTL state is only cleaned up **when it is read**.
Cold keys that are never touched again sit in state forever — the exact keys you wanted
removed. `cleanupInRocksdbCompactFilter` is what actually reclaims them during compaction.
Enabling TTL without it is a very common and very quiet mistake.

**Also:** TTL is wall-clock (processing time) only. It does not respect event time. If you
are replaying history through a job with TTL, state expires based on when the *job* runs,
not when the *events* happened.

---

## Part 6: Cause #5 — Configuration self-sabotage

```java
CheckpointConfig cfg = env.getCheckpointConfig();

// ❌ WRONG at high throughput
env.enableCheckpointing(1000);                   // every 1s — never finishes
cfg.setMaxConcurrentCheckpoints(3);              // 3 pile-ups eating your I/O
cfg.setCheckpointTimeout(600_000);               // hides the problem for 10 min

// ✅ RIGHT
env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
cfg.setMinPauseBetweenCheckpoints(30_000);       // ← the important one
cfg.setMaxConcurrentCheckpoints(1);
cfg.setCheckpointTimeout(300_000);
cfg.setTolerableCheckpointFailureNumber(3);      // don't restart on 1 blip
cfg.enableUnalignedCheckpoints();
cfg.setExternalizedCheckpointCleanup(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
```

**`setMinPauseBetweenCheckpoints` is the single highest-value knob** and it's the one people
don't know. It guarantees a gap between the *end* of one checkpoint and the *start* of the
next, so the job always gets real work time. Without it, if a checkpoint takes 55s on a 60s
interval, your job spends 90%+ of its life checkpointing and throughput collapses — which
causes backpressure — which makes checkpoints slower. Another feedback loop.

Note `setMinPauseBetweenCheckpoints` implicitly forces `maxConcurrentCheckpoints = 1`.

---

## Part 7: The answer to give in an interview

Compressed to ~60 seconds:

> "First I'd want to know *which phase* is slow, because the fix is completely different for
> each. I'd open the checkpoint tab and look at start delay, alignment, sync, and async
> duration separately.
>
> If **start delay** is high, the barrier can't even reach the operator — that's
> backpressure, and checkpointing is a symptom, not the disease. At 1M/sec I'd suspect a
> blocking call in the hot path; the arithmetic is one over the latency, so a 20ms DB lookup
> caps you at 50 events/sec per subtask. Fix is AsyncIO.
>
> If **alignment** is high, that's skew — one channel is much slower than the others,
> usually a hot key. Scaling out doesn't help because the hot key still hashes to one
> subtask; you need two-phase aggregation with a salted key. Unaligned checkpoints stop it
> from breaking checkpoints, but they don't fix the skew.
>
> If **sync or async** is high, it's state size. Full checkpoints of 200GB every 60s is 3.4
> GB/sec, which isn't physically achievable — so I'd move to RocksDB with incremental
> checkpointing.
>
> And if the slowness developed *gradually* over days, I'd stop tuning and go look for a
> state leak — a `KeyedProcessFunction` with no TTL and no timer cleanup. That's a bug, not
> a config problem.
>
> The thing I'd watch out for is the death spiral: slow checkpoints cause timeouts, timeouts
> cause restarts, restarts cause replay, replay causes backpressure, and backpressure makes
> checkpoints slower still. So I'd also set `minPauseBetweenCheckpoints` to guarantee the
> job gets real work time between snapshots."

---

## Quick reference

```
SYMPTOM                            → LOOK AT              → LIKELY CAUSE
Checkpoint slow, state small       → start delay          → backpressure
Checkpoint slow, one subtask slow  → alignment duration   → data skew / hot key
Checkpoint slow, state huge        → async duration       → full checkpoints, need incremental
Slow and getting worse daily       → state size over time → state leak, no TTL/timer cleanup
Slow only during traffic spikes    → alignment            → transient backpressure; unaligned CP
Job spends all its time in CP      → interval vs duration → missing minPauseBetweenCheckpoints
Restore is slow (not checkpoint)   → restore duration     → long incremental chain
```

See also: [[02-backpressure]], [[03-state-and-skew]], [[04-exactly-once]],
[[05-watermarks-and-time]], [[06-scale-arithmetic]]
