# 40. The ProcessFunction Family

`map`, `filter`, `window` are the polite API. The `ProcessFunction` family is the **low-level API**: you get the raw record, the timestamp, the watermark, keyed state, timers, and side outputs. Everything else in Flink's DataStream API is built on top of it.

> **Key idea**
> A `ProcessFunction` is "give me the record, plus a `Context` that exposes time and state, and let me decide what comes out."
> The eight variants differ only in **how many streams** come in, **whether the stream is keyed**, and **what extra thing** the `Context` gives you.

---

## What every member of the family gives you

```
          ┌──────────────────────────────────────────────┐
          │  processElement(value, ctx, out)             │
          │                                              │
  record ─┤   ctx.timestamp()          the event's ts    ├─► out.collect(x)   main output
          │   ctx.timerService()       clocks + timers   │
          │   ctx.output(tag, x)       side output       ├─► side output(s)
          │   ctx.getCurrentKey()      keyed variants    │
          │   getRuntimeContext()      keyed state       │
          └──────────────────────────────────────────────┘
                          │
                          ▼
          ┌──────────────────────────────────────────────┐
          │  onTimer(timestamp, ctx, out)                │  (keyed variants only)
          └──────────────────────────────────────────────┘
```

All of them are `RichFunction`s, so you also get `open(OpenContext)` / `close()` for setup and teardown — that's where you build state handles and open connection pools.

---

## The eight members

### 1. `ProcessFunction<I, O>`

One stream, **not keyed**.

```java
DataStream<Event> in = ...;

SingleOutputStreamOperator<Event> out = in.process(new ProcessFunction<Event, Event>() {
    @Override
    public void processElement(Event e, Context ctx, Collector<Event> out) {
        if (e.amount < 0) {
            ctx.output(BAD, e);      // side output for garbage
        } else {
            out.collect(e);
        }
    }
});
```

No keyed state, no timers. Use it for routing, side outputs, and cheap validation before a `keyBy`.

### 2. `KeyedProcessFunction<K, I, O>`

One stream, keyed. **The workhorse.** Keyed state + timers + side outputs. Chapters 42 and 44 are built entirely on it.

```java
in.keyBy(e -> e.userId)
  .process(new KeyedProcessFunction<String, Event, Alert>() { ... });
```

The three type parameters are `<KeyType, InputType, OutputType>`. Getting `K` wrong is the most common compile error here — it must match what your `keyBy` lambda returns (`String` for `e -> e.userId`).

### 3. `CoProcessFunction<I1, I2, O>`

Two streams via `connect()`, **not keyed**. `processElement1` / `processElement2`, no timers, no keyed state.

```java
streamA.connect(streamB).process(new CoProcessFunction<A, B, O>() {
    public void processElement1(A a, Context ctx, Collector<O> out) { ... }
    public void processElement2(B b, Context ctx, Collector<O> out) { ... }
});
```

Genuinely rare. Without keyed state there is little you can remember, so it's mostly stream merging with different logic per side.

### 4. `KeyedCoProcessFunction<K, I1, I2, O>`

Two streams, both keyed on the same key, sharing keyed state and one `onTimer`. **This is how you hand-roll a stream-to-stream join or an enrichment.** Worked example at the bottom of this chapter.

### 5. `ProcessWindowFunction<IN, OUT, KEY, W extends Window>`

Applied inside `.window(...).process(...)`. You get the whole window's elements as an `Iterable`, plus a `Context` with the window's start/end and per-window state.

```java
keyed.window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
     .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
         @Override
         public void process(String key, Context ctx, Iterable<Event> events, Collector<String> out) {
             long n = 0;
             for (Event e : events) n++;                  // Java enhanced-for loop
             out.collect(key + " " + ctx.window().getStart() + " count=" + n);
         }
     });
```

The cost: Flink must **buffer every element** of the window. Combine it with a `ReduceFunction`/`AggregateFunction` (`.reduce(agg, windowFn)`) to get incremental aggregation *and* window metadata, buffering only the accumulator.

### 6. `ProcessJoinFunction<I1, I2, O>`

Only used as the argument to `intervalJoin(...).process(...)`. You get one matched pair at a time. Covered in chapter 45.

```java
public void processElement(Event left, Event right, Context ctx, Collector<O> out)
```

No state, no timers — Flink manages the join buffers for you.

### 7. `BroadcastProcessFunction<IN, BC, OUT>`

A **non-keyed** stream connected to a broadcast stream. `processElement` gets read-only broadcast state; `processBroadcastElement` gets read-write. Chapter 41.

### 8. `KeyedBroadcastProcessFunction<K, IN, BC, OUT>`

Same, but the main stream is keyed — so you also get keyed state, timers, and `applyToKeyedState`. **The dynamic-rules pattern.** Chapter 41.

---

## Decision table

| I need to… | Use |
|---|---|
| Route records / emit side outputs, no memory needed | `ProcessFunction` |
| Remember something per key, or fire a timer | `KeyedProcessFunction` |
| Combine two streams that share a key (enrich, join, correlate) | `KeyedCoProcessFunction` |
| Combine two un-keyed streams with different logic per side | `CoProcessFunction` |
| See all elements of a window, or need window start/end metadata | `ProcessWindowFunction` |
| Join two streams within a time interval, with Flink managing the buffers | `intervalJoin` + `ProcessJoinFunction` |
| Push config/rules to **every** parallel instance at runtime | `KeyedBroadcastProcessFunction` (or the non-keyed `BroadcastProcessFunction`) |
| Call an external service per record | `RichAsyncFunction` — not a ProcessFunction at all (ch. 45) |

And the negative rules, which matter as much:

| Function | Keyed state | Timers | Side outputs |
|---|---|---|---|
| `ProcessFunction` | ✗ | ✗ | ✓ |
| `KeyedProcessFunction` | ✓ | ✓ | ✓ |
| `CoProcessFunction` | ✗ | ✗ | ✓ |
| `KeyedCoProcessFunction` | ✓ | ✓ | ✓ |
| `ProcessWindowFunction` | ✓ (+ per-window state) | ✗ (window fires it) | ✓ |
| `ProcessJoinFunction` | ✗ | ✗ | ✓ |
| `BroadcastProcessFunction` | ✗ (broadcast state only) | ✗ | ✓ |
| `KeyedBroadcastProcessFunction` | ✓ | ✓ (in `processElement` and `processBroadcastElement`) | ✓ |

---

## Worked example: enriching transactions with a slowly-changing profile

The most common real requirement in the family. Two streams:

- **Transactions** — high volume, `Event` records, keyed by `userId`.
- **Profiles** — low volume, one record whenever a user's tier/country changes, also keyed by `userId`.

Goal: emit each transaction annotated with the user's current tier.

### The shape

```java
DataStream<Event>   txns     = ...;      // high volume
DataStream<Profile> profiles = ...;      // low volume, changes rarely

DataStream<Enriched> enriched =
    txns.keyBy(e -> e.userId)                      // both sides keyed the SAME way
        .connect(profiles.keyBy(p -> p.userId))    // connect() unions two DIFFERENT types
        .process(new EnrichWithProfile());
```

`connect()` is not `union()`. `union()` requires identical types and merges into one stream. `connect()` keeps the two types distinct and gives you two callbacks. That's why the profiles can be a different class.

### The naive version — and its bug

```java
public class NaiveEnrich extends KeyedCoProcessFunction<String, Event, Profile, Enriched> {

    private transient ValueState<Profile> profileState;

    @Override
    public void open(OpenContext ctx) {
        profileState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("profile", Profile.class));
    }

    // Stream 1 = transactions
    @Override
    public void processElement1(Event txn, Context ctx, Collector<Enriched> out) throws Exception {
        Profile p = profileState.value();
        if (p != null) {
            out.collect(new Enriched(txn, p.tier));
        }
        // BUG: if p == null the transaction is silently DROPPED.
    }

    // Stream 2 = profiles
    @Override
    public void processElement2(Profile p, Context ctx, Collector<Enriched> out) throws Exception {
        profileState.update(p);       // overwrite with the newest profile
    }
}
```

### The ordering problem

Flink gives you **no ordering guarantee between the two inputs of a connected stream**. Both sides are read as fast as they arrive; Flink does not hold back stream 1 until stream 2 catches up.

```
what you assumed:                    what actually happens:

 profile(alice, GOLD) ──►             txn(alice, $50)   ──►  state empty → DROPPED ❌
 txn(alice, $50)      ──►  GOLD       txn(alice, $30)   ──►  state empty → DROPPED ❌
                                      profile(alice, GOLD) ─► state = GOLD
                                      txn(alice, $10)   ──►  GOLD ✅
```

This is guaranteed to happen at **job startup**, when the profile topic is being replayed from the beginning while transactions stream in live. Not an edge case — the normal case.

### The fix: buffer with `ListState` + a timer

Hold unmatched transactions for a bounded time, then flush them when the profile shows up (or give up).

```java
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class EnrichWithProfile
        extends KeyedCoProcessFunction<String, Event, Profile, Enriched> {
        //                              ^key    ^in1   ^in2     ^out

    /** How long we are willing to wait for a profile before giving up. */
    private static final long WAIT_MS = 60_000L;   // `_` is just a digit separator in Java

    /** The user's current profile, or null if we've never seen one. */
    private transient ValueState<Profile> profileState;

    /** Transactions that arrived before the profile did. */
    private transient ListState<Event> pending;

    /** Timestamp of the flush timer we registered, so we can delete it (ch. 39). */
    private transient ValueState<Long> flushTimer;

    @Override
    public void open(OpenContext ctx) {
        profileState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("profile", Profile.class));

        // ListState = an append-only list per key. add() is cheap; get() returns an Iterable.
        pending = getRuntimeContext().getListState(
                new ListStateDescriptor<>("pendingTxns", Event.class));

        flushTimer = getRuntimeContext().getState(
                new ValueStateDescriptor<>("flushTimer", Long.class));
    }

    // ── stream 1: transactions ────────────────────────────────────────────
    @Override
    public void processElement1(Event txn, Context ctx, Collector<Enriched> out) throws Exception {

        Profile p = profileState.value();

        if (p != null) {
            // Happy path: profile already known, enrich immediately.
            out.collect(new Enriched(txn, p.tier));
            return;
        }

        // Sad path: no profile yet. Buffer instead of dropping.
        pending.add(txn);

        // Register ONE give-up timer for this key (see ch.39 coalescing/delete rules).
        if (flushTimer.value() == null) {
            long fireAt = ctx.timestamp() + WAIT_MS;
            ctx.timerService().registerEventTimeTimer(fireAt);
            flushTimer.update(fireAt);
        }
    }

    // ── stream 2: profiles ────────────────────────────────────────────────
    @Override
    public void processElement2(Profile p, Context ctx, Collector<Enriched> out) throws Exception {

        profileState.update(p);        // newest profile wins

        // Flush anything that was waiting on exactly this.
        Iterable<Event> buffered = pending.get();     // never null; may be empty
        boolean any = false;
        for (Event txn : buffered) {
            out.collect(new Enriched(txn, p.tier));
            any = true;
        }

        if (any) {
            pending.clear();                           // free the buffer

            Long t = flushTimer.value();
            if (t != null) {
                ctx.timerService().deleteEventTimeTimer(t);   // no longer needed
                flushTimer.clear();
            }
        }
    }

    // ── the give-up path ──────────────────────────────────────────────────
    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Enriched> out) throws Exception {

        // We waited WAIT_MS of event time and no profile arrived.
        // Emit with a default rather than losing the data.
        for (Event txn : pending.get()) {
            out.collect(new Enriched(txn, "UNKNOWN"));
        }
        pending.clear();
        flushTimer.clear();
    }
}
```

### Trace

```
WAIT_MS = 60s.  Key = alice.

t=100  txn($50)              profile=null  pending=[$50]        timer@60100 registered
t=105  txn($30)              profile=null  pending=[$50,$30]    timer already set (no 2nd timer)
t=110  profile(alice,GOLD)   profile=GOLD  pending=[]           emits $50/GOLD, $30/GOLD
                                                                timer@60100 DELETED
t=200  txn($10)              profile=GOLD  pending=[]           emits $10/GOLD immediately
```

And the give-up path:

```
t=100  txn($50)              pending=[$50]   timer@60100
       ... no profile ever arrives ...
wm>60100  onTimer            emits $50/UNKNOWN, pending cleared
```

### The three design decisions in that code

1. **Buffer, don't drop.** Dropping is silent data loss and the hardest bug class to notice.
2. **Bound the buffer with a timer.** Without the timer, a user who never gets a profile accumulates transactions in `ListState` forever.
3. **One timer per key, deleted on success.** Straight out of chapter 39. Registering a timer per buffered transaction would be the explosion bug.

### What this still doesn't do

- **It has no notion of "the profile as of the transaction's event time."** If a profile changes at t=500 and a transaction with t=400 arrives late, it gets the *new* tier. Proper temporal correctness needs a versioned profile history in `MapState<Long, Profile>` keyed by validity timestamp — or Flink SQL's temporal join, which does exactly this (Phase 7).
- **It doesn't bootstrap.** On job start, the profile topic must be replayed from `earliest` or every key waits `WAIT_MS`. In production you either replay the compacted profile topic from the beginning, or you use **broadcast state** — chapter 41 — when the reference data is small enough to fit in every subtask.

> **Key idea**
> `keyBy` + `connect` + `KeyedCoProcessFunction` = reference data is **partitioned** by key (big data, each subtask holds a slice).
> Broadcast state = reference data is **replicated** to every subtask (small data, no `keyBy` needed on the reference side).
> Choosing between them is a size question.

---

## Remember

- The `ProcessFunction` family is the low-level API: raw record + `Context` (time, state, timers, side outputs).
- Two axes decide which one: how many input streams, and keyed or not. Keyed = state + timers.
- `KeyedProcessFunction` is the one you'll write 95% of the time.
- `connect()` joins two streams of **different types** and gives you two callbacks; `union()` needs identical types.
- Both sides of a connected stream must be keyed the same way to share keyed state.
- There is **no ordering guarantee between the two inputs**. Assume the "wrong" side arrives first, especially at startup.
- The fix is always the same shape: buffer in `ListState`, bound it with one timer, delete the timer when the match arrives.
- `ProcessWindowFunction` buffers the entire window; pair it with `reduce`/`aggregate` to keep only the accumulator.
- Partitioned reference data → `KeyedCoProcessFunction`. Small replicated reference data → broadcast state.

## Interview one-liners

- *"What's a `ProcessFunction`?"* → The low-level DataStream API giving direct access to timestamps, watermarks, keyed state, timers, and side outputs; every higher-level operator is built on it.
- *"`connect` vs `union`?"* → `union` merges same-typed streams into one; `connect` keeps two types with separate `processElement1`/`processElement2` callbacks and shared keyed state.
- *"How do you enrich a stream with slowly-changing data?"* → `keyBy` both sides, `connect`, `KeyedCoProcessFunction` with the reference record in `ValueState` — or broadcast state if the reference set is small enough to replicate.
- *"What's the classic bug in that pattern?"* → No ordering guarantee between inputs, so transactions arriving before their profile get dropped; buffer them in `ListState` with a bounded timer instead.
- *"Why one timer per key rather than per buffered record?"* → Timer explosion: N records would mean N timers in checkpointed state; one give-up timer per key is enough.
- *"When is `ProcessWindowFunction` a bad idea?"* → When the window is large, because it buffers every element; use `reduce`/`aggregate` with it so only the accumulator is retained.
- *"Which functions can register timers?"* → Only the keyed ones: `KeyedProcessFunction`, `KeyedCoProcessFunction`, `KeyedBroadcastProcessFunction`.
