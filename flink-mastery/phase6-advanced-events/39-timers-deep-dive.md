# 39. Timers — Deep Dive

Phase 3 showed you `ctx.timerService().registerEventTimeTimer(...)` and `onTimer(...)`. That was enough to write a working job. This chapter is everything that bites you *after* the job works: when timers actually fire, why you get 40 million of them, and how to delete one.

> **Key idea**
> A timer is a durable, keyed, deduplicated promise: *"call `onTimer` for key K when the clock reaches time T."*
> Everything in this chapter follows from those four words — **durable, keyed, deduplicated, clock**.

---

## The two clocks

Flink has exactly two timer services, and they read two different clocks.

```
PROCESSING TIME                          EVENT TIME
───────────────                          ──────────
clock = System.currentTimeMillis()       clock = the current WATERMARK
         on the TaskManager                       of this operator

advances by itself, always               advances ONLY when a watermark arrives
wall-clock, non-deterministic            deterministic, replayable
never "catches up" on replay             fires instantly in a backlog replay
```

Registering them looks nearly identical:

```java
// `ctx` is the Context object Flink hands to processElement.
// `timerService()` returns the TimerService for this operator + this key.
ctx.timerService().registerProcessingTimeTimer(
        ctx.timerService().currentProcessingTime() + 60_000L);   // 60s from now, wall clock

ctx.timerService().registerEventTimeTimer(
        ctx.timestamp() + 60_000L);   // 60s after THIS EVENT's timestamp
```

Reading the clocks:

```java
long wallClock  = ctx.timerService().currentProcessingTime();  // System.currentTimeMillis()
long watermark  = ctx.timerService().currentWatermark();       // event-time "now"
Long eventTs    = ctx.timestamp();   // this record's timestamp. Boxed Long — can be null!
```

`ctx.timestamp()` returns `Long` (the object, capital L) not `long` (the primitive), because it is **null** when the stream has no timestamp assigner. In Java, unboxing a `null Long` into a `long` throws `NullPointerException`. Guard it if you are not sure a watermark strategy is attached.

---

## The single most misunderstood fact about event-time timers

> **Key idea**
> An event-time timer for T does **not** fire when an event with timestamp T arrives.
> It fires when the **watermark** passes T.

Trace it. Watermark strategy is `forBoundedOutOfOrderness(Duration.ofSeconds(5))`, so watermark = maxSeenTimestamp − 5s (approximately; Flink emits `max - outOfOrderness - 1ms`).

```
event ts    max seen    watermark    timer@10:00:00 registered earlier
────────    ────────    ─────────    ───────────────────────────────────
09:59:50    09:59:50    09:59:45     pending
10:00:00    10:00:00    09:59:55     pending   <-- the event AT 10:00:00 arrived.
                                                   Timer STILL does not fire.
10:00:02    10:00:02    09:59:57     pending
10:00:04    10:00:04    09:59:59     pending
10:00:06    10:00:06    10:00:01     FIRES  ✅  <-- watermark finally crossed 10:00:00
```

The timer fired **six seconds of event time late**, because the watermark lags by the out-of-orderness bound. This is correct and intended: firing at 10:00:00 exactly would mean acting before you had seen all events up to 10:00:00.

Two consequences people hit in production:

1. **An idle source stalls all your event-time timers.** No events → no watermark advance → no timers fire → alerts never go out. Fix with `.withIdleness(Duration.ofMinutes(1))` on the watermark strategy.
2. **In a Kafka backlog replay, thousands of timers fire in a burst**, in the same millisecond of wall time, because the watermark rockets forward. Your `onTimer` must be cheap and must not, for example, make a synchronous HTTP call.

Processing-time timers have the mirror-image behaviour: they fire on schedule in production, and in a replay of historical data they fire "60 seconds after the job started" — which is meaningless relative to the data.

---

## Timers are keyed state

You cannot register a timer outside a keyed context. `ctx.timerService().registerEventTimeTimer(t)` inside a `KeyedProcessFunction` registers a timer **for the current key**, and `onTimer` runs **with that key restored**.

```java
@Override
public void onTimer(long timestamp,
                    OnTimerContext ctx,
                    Collector<String> out) throws Exception {

    // Inside onTimer the keyed context is set to the key that registered the timer.
    // So this reads THAT key's state — no lookup needed:
    Long count = countState.value();

    // And you can ask which key it was:
    String key = ctx.getCurrentKey();   // returns Object in ProcessFunction; typed here
}
```

Three properties follow from "timers are state":

| Property | What it means |
|---|---|
| **Checkpointed** | A timer registered for 6 hours from now survives a crash, a restart from a savepoint, and a rescale. |
| **Redistributed on rescale** | Timers travel with their key group, exactly like `ValueState`. |
| **Counted against state size** | 50 million pending timers is 50 million state entries. This is a real cause of slow checkpoints. |

Where they physically live depends on the backend:

- `HashMapStateBackend` → timers on the JVM heap. Fast, but bounded by heap.
- `EmbeddedRocksDBStateBackend` → by default timers **also** go to RocksDB (`state.backend.rocksdb.timer-service.factory: rocksdb`). You can force them onto the heap with `heap` if you have few timers and want speed, but then a timer explosion becomes an OOM instead of a disk problem.

---

## Timer coalescing — and how to exploit it

> **Key idea**
> A timer is identified by `(key, namespace, timestamp)`. Registering the *same* key and the *same* timestamp twice creates **one** timer, and `onTimer` fires **once**.

```java
ctx.timerService().registerEventTimeTimer(60_000L);
ctx.timerService().registerEventTimeTimer(60_000L);
ctx.timerService().registerEventTimeTimer(60_000L);
// -> ONE timer. onTimer called ONCE at watermark >= 60_000.
```

This is not a nicety, it is the main scale technique for timers. Consider a "flush this user's buffer once a minute" requirement:

### The naive version — one timer per event

```java
// BAD at scale.
@Override
public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
    buffer.add(e);
    // Each event registers a distinct timestamp -> a distinct timer.
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60_000L);
}
```

1000 events for one user → 1000 distinct timestamps → **1000 timers**, and `onTimer` runs 1000 times. Multiply by 10 million users.

### The fix — round the timestamp to a bucket

```java
@Override
public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
    buffer.add(e);

    long now = ctx.timestamp();

    // Round DOWN to the start of the current minute, then add one minute.
    // `%` is Java's remainder operator. 61_234 % 60_000 = 1_234,
    // so 61_234 - 1_234 = 60_000, the minute boundary.
    long minute = 60_000L;
    long fireAt = now - (now % minute) + minute;

    // Every event inside the same minute computes the SAME fireAt,
    // so coalescing collapses them into ONE timer.
    ctx.timerService().registerEventTimeTimer(fireAt);
}
```

1000 events spread over 5 minutes → **5 timers**, not 1000.

```
events:   |x x xx  x| x   xx x |xx    x  x|
minute:   [--- 0 ---][--- 1 ---][--- 2 ---]
timers:             ▲          ▲          ▲
                    1 timer    1 timer    1 timer
```

The cost: your callback fires on a grid, not exactly `X` after each event. For "flush", "clean up", "emit a heartbeat", and "expire old state", a one-minute grid is almost always fine. For "alert exactly 30s after this specific event", it is not — use an exact timer there.

Choose the bucket size by the precision you actually need. Rounding to 1 second cuts timer count ~by the event rate per key per second; rounding to 1 minute cuts it far more.

---

## Deleting timers — and the state you must keep to do it

Deletion is by exact timestamp:

```java
ctx.timerService().deleteEventTimeTimer(1_700_000_060_000L);
ctx.timerService().deleteProcessingTimeTimer(1_700_000_060_000L);
```

There is **no** `deleteAllTimersForThisKey()`. There is no way to list a key's timers. So:

> **Key idea**
> If you will ever need to delete a timer, you must store its timestamp in `ValueState` when you register it. Otherwise it is unreachable — you cannot delete what you cannot name.

Deleting a timestamp that has no timer is a harmless no-op, so you don't need to be defensive about that case.

### The canonical "sliding timeout" pattern

Requirement: fire 30 minutes after a user's **last** event. Every new event should push the deadline out.

```java
public class SlidingTimeout extends KeyedProcessFunction<String, Event, String> {

    // Holds the timestamp of the timer we currently have registered, or null.
    private transient ValueState<Long> timerTs;

    @Override
    public void open(OpenContext ctx) {
        // `transient` above tells Java not to serialize this field — Flink
        // rebuilds the handle here on every subtask at startup.
        timerTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timerTs", Long.class));
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {

        Long previous = timerTs.value();          // null on the very first event for this key

        // 1. Cancel the old deadline, if there is one.
        if (previous != null) {
            ctx.timerService().deleteEventTimeTimer(previous);
        }

        // 2. Register the new deadline.
        long next = ctx.timestamp() + 30 * 60_000L;
        ctx.timerService().registerEventTimeTimer(next);

        // 3. REMEMBER it, so the next event can delete it.
        timerTs.update(next);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect("user " + ctx.getCurrentKey() + " idle for 30 minutes");
        timerTs.clear();        // the timer has fired; there is nothing left to delete
    }
}
```

Skipping step 1 gives you the timer-explosion bug: every event leaves a stale timer behind, `onTimer` fires spuriously at every old deadline, and state grows linearly with event count.

### The alternative to deleting: fire and check

Deletion in RocksDB is not free (it's a write). A common high-throughput alternative is to leave the timer and validate on fire:

```java
@Override
public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
    lastSeen.update(ctx.timestamp());                   // just record the time
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 30 * 60_000L);
}

@Override
public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
    Long last = lastSeen.value();
    if (last == null) return;

    // Is this the timer for the LAST event, or a stale one from an earlier event?
    if (ts == last + 30 * 60_000L) {
        out.collect("idle: " + ctx.getCurrentKey());
        lastSeen.clear();
    }
    // else: stale timer, do nothing. It cost one no-op callback.
}
```

Trade-off: **delete** = fewer timers in state, more writes per event. **fire-and-check** = more timers in state, cheaper per event. Combine fire-and-check with coalescing (round `fireAt`) and you get the best of both.

---

## End of stream: MAX_WATERMARK

When a bounded source finishes (a file, `fromElements`, or a Kafka source in bounded mode), Flink emits a final watermark of `Long.MAX_VALUE`, called **MAX_WATERMARK**.

```
  bounded source drains
          │
          ▼
  Watermark(Long.MAX_VALUE) flows through the pipeline
          │
          ▼
  every pending EVENT-TIME timer is < MAX_VALUE  ->  ALL FIRE
  every pending PROCESSING-TIME timer            ->  DOES NOT FIRE
```

| | Event-time timers | Processing-time timers |
|---|---|---|
| Fire at end of a bounded job? | **Yes** — MAX_WATERMARK passes them all | **No** — the job shuts down first |
| Fire on graceful shutdown (`--drain` savepoint)? | **Yes** — `--drain` sends MAX_WATERMARK | No |
| Fire on a plain cancel / stop-with-savepoint (no drain) | No | No |

Practical consequences:

- **Unit tests behave differently from production.** A test with `env.fromElements(...)` sees every event-time timer fire at the end, so an idle-session emit "works". In an unbounded Kafka job, the last session for each user sits pending forever until new data arrives. Your test passes and production silently drops the tail.
- **`stop-with-savepoint --drain` is a one-way door.** It advances event time to infinity; the resulting savepoint should not be used to resume a live job, because all windows and timers have already been flushed.
- **If you rely on processing-time timers for final flushes, add a shutdown path**, or move to event time.

---

## The timer explosion problem

The failure mode, stated once:

```
   one timer registered per event
 × millions of events
 = millions of pending timers
 = huge timer state
 = long checkpoints, big savepoints, slow rescale, RocksDB pressure
 = eventually, checkpoint timeouts and a job that cannot recover
```

Symptoms in the Flink UI: checkpoint size grows steadily with no corresponding growth in your `ValueState`; checkpoint duration climbs; `onTimer` shows up hot in a flame graph.

The four fixes, in order of how often you should reach for them:

1. **Coalesce** — round `fireAt` to a bucket so many events share one timer. Biggest win, cheapest change.
2. **Delete on re-register** — keep the timestamp in `ValueState` and delete the old one. Turns N timers per key into 1.
3. **Fire-and-check** — leave stale timers but make the callback a no-op. Cheap per event, still bounded if combined with coalescing.
4. **Use state TTL instead of a timer** — if all you want is "delete this state after 24h", `StateTtlConfig` does it with no timers at all. It's the right tool when you don't need to *emit* anything at expiry.

```java
// No timers needed for pure expiry:
StateTtlConfig ttl = StateTtlConfig
        .newBuilder(Duration.ofHours(24))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build();

ValueStateDescriptor<Long> d = new ValueStateDescriptor<>("count", Long.class);
d.enableTimeToLive(ttl);
```

The catch: TTL is **processing-time only** and it does not call you back — state just disappears. If you need to emit an alert at expiry, you need a timer.

---

## Timers and the ProcessFunction family

> **Key idea**
> Timers require a **keyed** stream. No `keyBy`, no timers.

Why: a timer must be stored somewhere that survives rescaling, and the only per-record storage unit Flink can redistribute is a key group. Without a key, there is no key group.

```java
// ProcessFunction on a NON-keyed stream:
stream.process(new ProcessFunction<Event, String>() {
    @Override
    public void processElement(Event e, Context ctx, Collector<String> out) {
        ctx.timerService().currentProcessingTime();          // ✅ reading is fine
        ctx.timerService().registerEventTimeTimer(12345L);   // ❌ throws UnsupportedOperationException
    }
});
```

`ProcessFunction` *has* the `registerXTimer` methods on its `TimerService` — the interface is shared — but calling them on a non-keyed stream throws at runtime, not compile time. That's a common first-day surprise.

If you apply a `ProcessFunction` to a `KeyedStream`, Flink wraps it and timers do work — but you lose the typed key and typed keyed state, so **always use `KeyedProcessFunction` on a keyed stream**.

### The three you will meet in this phase

| Function | Input | Keyed? | Timers? | Use it for |
|---|---|---|---|---|
| `ProcessFunction<I, O>` | one stream | no | **no** | side outputs and per-record access to timestamps on an un-keyed stream |
| `KeyedProcessFunction<K, I, O>` | one stream | yes | **yes** | 95% of your stateful logic |
| `CoProcessFunction<I1, I2, O>` | two streams | no | **no** | merging two un-keyed streams |
| `KeyedCoProcessFunction<K, I1, I2, O>` | two streams | yes | **yes** | joining/enriching two keyed streams (chapter 40) |

The two-stream variants give you `processElement1` (for the first stream) and `processElement2` (for the second), plus one shared `onTimer`. Their state is shared: `processElement1` and `processElement2` see the same `ValueState` for the same key. That is the whole point of them.

```
      stream A ──┐
                 ├──► KeyedCoProcessFunction ──► out
      stream B ──┘        │
                          ├─ processElement1(a, ctx, out)
                          ├─ processElement2(b, ctx, out)
                          └─ onTimer(ts, ctx, out)     <- ONE shared timer callback
                          shared keyed state across all three
```

One important detail on `onTimer` in a Co function: you get **one** `onTimer`, so if both sides register timers you must be able to tell them apart. Store a marker in state, or encode intent by using distinct timestamp buckets (e.g. always round side-A timers to even seconds). There is no "which registration was this" parameter.

---

## Remember

- Two clocks: processing time = wall clock; event time = **the watermark**.
- An event-time timer for T fires when the **watermark** passes T, not when an event with timestamp T arrives. Expect a delay equal to your out-of-orderness bound.
- No events → no watermark → no event-time timers. Use `.withIdleness(...)`.
- Timers are keyed state: checkpointed, rescaled with key groups, and counted in checkpoint size.
- Timers coalesce on `(key, timestamp)`. Round `fireAt` to a bucket to collapse thousands of timers into one.
- You can only delete a timer by its exact timestamp, so store that timestamp in `ValueState` when you register it.
- Alternative to deleting: leave the timer and validate against `lastSeen` in `onTimer`.
- At end of a bounded stream, MAX_WATERMARK fires **all** pending event-time timers; processing-time timers never fire.
- Timer explosion fixes, in order: coalesce, delete-on-re-register, fire-and-check, state TTL.
- Timers need a keyed stream. `ProcessFunction` and `CoProcessFunction` cannot register them.

## Interview one-liners

- *"When does an event-time timer fire?"* → When the operator's watermark advances past the timer's timestamp — never on the arrival of an event with that timestamp.
- *"My event-time timers never fire in production."* → A source partition is idle so the watermark is stuck at the minimum across inputs; add `withIdleness`.
- *"Are timers checkpointed?"* → Yes, they are keyed state, redistributed by key group on rescale, and they count toward checkpoint size.
- *"How do you avoid millions of timers?"* → Coalesce by rounding the fire time to a bucket, and delete the previous timer when re-registering; both exploit the `(key, timestamp)` dedup.
- *"How do you delete a timer?"* → `deleteEventTimeTimer(ts)` with the exact timestamp, which means you had to store that timestamp in `ValueState` at registration.
- *"What happens to pending timers when a bounded job ends?"* → Event-time timers all fire on MAX_WATERMARK; processing-time timers do not fire at all.
- *"Why can't I use timers in a `ProcessFunction`?"* → Timers live in key groups, so they need a `KeyedStream`; on a non-keyed stream registration throws `UnsupportedOperationException`.
- *"`KeyedProcessFunction` vs `KeyedCoProcessFunction`?"* → Same state and timer model, but the Co version has `processElement1`/`processElement2` over two connected streams sharing one keyed state and one `onTimer`.
