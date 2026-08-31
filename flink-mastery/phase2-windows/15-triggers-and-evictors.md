# 15. Triggers and Evictors

The window assigner decides **which** window an element goes in. The trigger decides **when** that window produces a result. They are separate, pluggable pieces.

```
element ──▶ WindowAssigner ──▶ window state ──▶ Trigger ──▶ (Evictor) ──▶ window function ──▶ output
            "which window?"                     "fire now?"  "drop some?"   "compute what?"
```

> **Key idea**
> Every window assigner ships with a default trigger, which is why you have never had to write one. `TumblingEventTimeWindows` defaults to `EventTimeTrigger`, which fires exactly once when the watermark passes the window end. A custom trigger lets you fire early, fire repeatedly, or fire on something other than time.

---

## The `Trigger` interface

```java
public abstract class Trigger<T, W extends Window> implements Serializable {

    public abstract TriggerResult onElement(
            T element, long timestamp, W window, TriggerContext ctx) throws Exception;

    public abstract TriggerResult onEventTime(
            long time, W window, TriggerContext ctx) throws Exception;

    public abstract TriggerResult onProcessingTime(
            long time, W window, TriggerContext ctx) throws Exception;

    public abstract void clear(W window, TriggerContext ctx) throws Exception;

    public boolean canMerge() { return false; }
    public void onMerge(W window, OnMergeContext ctx) throws Exception { ... }
}
```

| method | called when |
|---|---|
| `onElement` | every single element assigned to this window |
| `onEventTime` | an **event-time timer** you registered fires (i.e. the watermark passed it) |
| `onProcessingTime` | a **processing-time timer** you registered fires (wall clock reached it) |
| `clear` | the window is being destroyed — delete your timers and state here |
| `onMerge` | two windows merge (session windows); `canMerge()` must return `true` |

The trigger does not receive the window's *contents*. It sees each element as it arrives and can keep its own small state.

## `TriggerResult`

```java
public enum TriggerResult {
    CONTINUE,          // do nothing
    FIRE,              // run the window function; KEEP the window contents
    PURGE,             // discard the window contents; do NOT run the window function
    FIRE_AND_PURGE     // run the window function, then discard the contents
}
```

`enum` is a Java type with a fixed set of named constants.

The `FIRE` vs `FIRE_AND_PURGE` distinction is the crux:

```
FIRE:            emit a result, keep the state
                 → the next fire includes everything again (cumulative)
                 → state grows

FIRE_AND_PURGE:  emit a result, then clear the state
                 → the next fire only sees new elements (incremental/disjoint)
                 → state stays bounded
```

For early firings you almost always want `FIRE` (each partial result is a better estimate of the same window's total), and `FIRE_AND_PURGE` only at the very end.

---

## The `TriggerContext`

```java
public interface TriggerContext {
    long getCurrentProcessingTime();
    long getCurrentWatermark();

    void registerProcessingTimeTimer(long time);
    void registerEventTimeTimer(long time);
    void deleteProcessingTimeTimer(long time);
    void deleteEventTimeTimer(long time);

    <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor);
}
```

`getPartitionedState` gives you state scoped to **this key and this window**, which is how a trigger remembers "I have seen 12 elements so far".

---

## The default: `EventTimeTrigger`

Simplified to its essence:

```java
public class EventTimeTrigger extends Trigger<Object, TimeWindow> {

    @Override
    public TriggerResult onElement(Object element, long timestamp,
                                   TimeWindow window, TriggerContext ctx) {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // watermark has already passed this window's end -> fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;    // processing time is irrelevant here
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
```

Read the flow:
1. First element arrives → register an event-time timer at `windowEnd - 1`.
2. Watermark advances past that → `onEventTime` fires → `FIRE`.
3. Note it returns `FIRE`, **not** `FIRE_AND_PURGE`. The contents are cleaned up separately by the window operator, after `allowedLateness` expires (chapter 16). That is exactly how late-arriving elements can re-fire a window.

`ProcessingTimeTrigger` is the mirror image using processing-time timers.

---

## A practical custom trigger: early firing

**The problem.** A 1-hour tumbling window emits nothing for an hour. A dashboard needs a partial number now.

**The solution.** Fire every N elements *and* every N seconds of processing time, then fire finally at the watermark.

```java
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class EarlyFiringTrigger extends Trigger<Object, TimeWindow> {

    private final long maxCount;        // fire after this many elements
    private final long intervalMs;      // ...or after this much processing time

    public EarlyFiringTrigger(long maxCount, long intervalMs) {
        this.maxCount = maxCount;
        this.intervalMs = intervalMs;
    }

    // count of elements seen in this window, kept as trigger state
    private final ReducingStateDescriptor<Long> countDesc =
            new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

    // marker: the processing-time timer we have currently scheduled
    private final ReducingStateDescriptor<Long> nextTimerDesc =
            new ReducingStateDescriptor<>("nextTimer", new Max(), LongSerializer.INSTANCE);

    @Override
    public TriggerResult onElement(Object element, long timestamp,
                                   TimeWindow window, TriggerContext ctx) throws Exception {

        // 1. always make sure the FINAL event-time firing is scheduled
        ctx.registerEventTimeTimer(window.maxTimestamp());

        // 2. count this element
        ReducingState<Long> count = ctx.getPartitionedState(countDesc);
        count.add(1L);

        if (count.get() >= maxCount) {
            count.clear();                       // reset the counter, keep the data
            return TriggerResult.FIRE;           // FIRE, not FIRE_AND_PURGE
        }

        // 3. make sure a periodic processing-time timer is scheduled
        ReducingState<Long> nextTimer = ctx.getPartitionedState(nextTimerDesc);
        if (nextTimer.get() == null) {
            long fireAt = ctx.getCurrentProcessingTime() + intervalMs;
            ctx.registerProcessingTimeTimer(fireAt);
            nextTimer.add(fireAt);
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        // the real, final firing
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {

        ReducingState<Long> nextTimer = ctx.getPartitionedState(nextTimerDesc);
        nextTimer.clear();

        // schedule the next periodic firing
        long fireAt = ctx.getCurrentProcessingTime() + intervalMs;
        ctx.registerProcessingTimeTimer(fireAt);
        nextTimer.add(fireAt);

        ctx.getPartitionedState(countDesc).clear();
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        ReducingState<Long> nextTimer = ctx.getPartitionedState(nextTimerDesc);
        Long t = nextTimer.get();
        if (t != null) {
            ctx.deleteProcessingTimeTimer(t);
        }
        nextTimer.clear();
        ctx.getPartitionedState(countDesc).clear();
    }

    // small helper reduce functions for the ReducingStates
    private static class Sum implements ReduceFunction<Long> {
        @Override public Long reduce(Long a, Long b) { return a + b; }
    }
    private static class Max implements ReduceFunction<Long> {
        @Override public Long reduce(Long a, Long b) { return Math.max(a, b); }
    }
}
```

### Java notes

- `private final long maxCount;` — `final` means assign once, in the constructor. Trigger fields are serialized and shipped to every TaskManager, so they must be `Serializable` (primitives and Strings are fine).
- `ReducingState<Long>` folds every `add()` through a `ReduceFunction`, so `count.add(1L)` is a "+= 1". It exists so the trigger doesn't need to read-modify-write.
- `private static class Sum implements ...` — a **static nested class**. `static` matters: a non-static inner class holds a hidden reference to its enclosing instance and often breaks serialization.
- `LongSerializer.INSTANCE` — Flink's built-in serializer for `Long`; `INSTANCE` is a singleton constant.
- `count.get()` returns `Long` (nullable), not `long`. Compare carefully; `null >= maxCount` would throw.
- Always mirror every `register*Timer` with a `delete*Timer` in `clear()`. Leaked timers are a real memory leak.

### Using it

```java
stream
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
    .trigger(new EarlyFiringTrigger(1000, 10_000L))   // every 1000 elements or 10s
    .aggregate(new StatsAgg(), new AddWindowInfo());
```

Output for one key in one window now looks like:

```
user=u1 window=[0,3600000) count=1000  sum=2431.00 avg=2.43    <- early, at element 1000
user=u1 window=[0,3600000) count=1873  sum=4502.00 avg=2.40    <- early, at the 10s timer
user=u1 window=[0,3600000) count=2000  sum=4811.00 avg=2.41    <- early, at element 2000
...
user=u1 window=[0,3600000) count=94211 sum=... avg=...          <- FINAL, at the watermark
```

> **Key idea**
> Early firing emits **the same window multiple times with growing counts**. Your sink must handle that — upsert by `(key, windowStart)`, not append. Emitting to an append-only sink here produces duplicated, wrong totals.

Because we used `FIRE` (not `FIRE_AND_PURGE`), each emission is cumulative over the window so far, and the last one is the true total. If you'd used `FIRE_AND_PURGE`, each emission would be a disjoint chunk that a downstream consumer must sum itself.

---

## Built-in triggers

```java
import org.apache.flink.streaming.api.windowing.triggers.*;
```

| trigger | behaviour |
|---|---|
| `EventTimeTrigger.create()` | default for event-time assigners: fire once when watermark ≥ window end |
| `ProcessingTimeTrigger.create()` | default for processing-time assigners |
| `CountTrigger.of(n)` | `FIRE` every `n` elements. No time component. |
| `ContinuousEventTimeTrigger.of(Duration)` | `FIRE` every `d` of **event time**, plus at the window end |
| `ContinuousProcessingTimeTrigger.of(Duration)` | `FIRE` every `d` of **wall clock** |
| `DeltaTrigger.of(threshold, deltaFn, serializer)` | `FIRE` when a value drifts by more than `threshold` from the last fired one |
| `PurgingTrigger.of(other)` | wrapper: turns any `FIRE` from `other` into `FIRE_AND_PURGE` |
| `NeverTrigger` | default for `GlobalWindows`; never fires |

`ContinuousEventTimeTrigger` gets you most of the way to the custom trigger above with one line:

```java
stream
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
    .trigger(ContinuousEventTimeTrigger.of(Duration.ofMinutes(5)))
    .aggregate(new StatsAgg(), new AddWindowInfo());
```
Fires every 5 minutes of *event time* — so it advances with the data, and on a replay of historical data it fires as fast as the data flows. Use `ContinuousProcessingTimeTrigger` if you want "every 5 real minutes" regardless.

`PurgingTrigger` composition:

```java
.trigger(PurgingTrigger.of(CountTrigger.of(100)))
// = fire every 100 elements AND clear the window each time
```

**Setting `.trigger(...)` replaces the default trigger entirely.** With `TumblingEventTimeWindows` plus `.trigger(CountTrigger.of(100))`, the window will *never* fire on the watermark — only on counts. A key with 99 elements emits nothing, ever. If you want both, write a trigger that does both (like `EarlyFiringTrigger` above) or use a `Continuous*Trigger`, which keeps the end-of-window firing.

---

## Evictors

An `Evictor` removes elements from the buffer around the window function call.

```java
public interface Evictor<T, W extends Window> extends Serializable {
    void evictBefore(Iterable<TimestampedValue<T>> elements, int size,
                     W window, EvictorContext ctx);
    void evictAfter(Iterable<TimestampedValue<T>> elements, int size,
                    W window, EvictorContext ctx);
}
```

Built-ins:

```java
import org.apache.flink.streaming.api.windowing.evictors.*;

.evictor(CountEvictor.of(100))                    // keep only the last 100 elements
.evictor(TimeEvictor.of(Duration.ofSeconds(30)))  // keep only the last 30s of elements
.evictor(DeltaEvictor.of(threshold, deltaFunction))
```

Each takes an optional `boolean doEvictAfter` second argument to evict after the window function instead of before.

### Why evictors are usually a mistake

> **Key idea**
> **Attaching an evictor disables incremental aggregation.** The evictor needs to inspect and remove individual elements, so Flink must keep every element in the buffer — even if your window function is a `reduce` or `aggregate`.

```
NO evictor:      reduce/aggregate → 1 accumulator in state.
WITH evictor:    every element buffered, the aggregate is recomputed
                 from scratch on each firing.
```

You pay the `ProcessWindowFunction` memory cost plus O(n) CPU per firing, for a feature you can usually get another way. `CountEvictor.of(100)` on a global window is exactly `countWindow(100, slide)`; a `TimeEvictor` is usually better expressed as a shorter window.

Legitimate uses are narrow: dropping a warm-up record from a sensor window, or keeping a bounded trailing buffer where you truly need the raw elements. If you find yourself reaching for an evictor, first check whether a different assigner or a trigger solves it.

---

## Trigger vs Evictor vs Assigner

| | decides | typical customization |
|---|---|---|
| Assigner | which window(s) an element belongs to | almost never — use the built-ins |
| Trigger | when the window function runs | occasionally — early/continuous firing |
| Evictor | which elements the function sees | rarely — and it costs you incrementality |

---

## Remember

- Every assigner has a default trigger. `.trigger(...)` **replaces** it, it does not add to it.
- `TriggerResult`: `CONTINUE`, `FIRE` (emit, keep state), `PURGE` (drop state, no emit), `FIRE_AND_PURGE`.
- `onElement` / `onEventTime` / `onProcessingTime` / `clear`. Register timers; delete them in `clear`.
- `EventTimeTrigger` registers a timer at `window.maxTimestamp()` (= end − 1) and returns `FIRE`, not `FIRE_AND_PURGE` — that's what makes late re-firing possible.
- Early firing emits the same window many times. The sink must upsert on `(key, windowStart)`.
- `ContinuousEventTimeTrigger` / `ContinuousProcessingTimeTrigger` are the one-line versions of a periodic early trigger.
- `PurgingTrigger.of(t)` wraps any trigger to make its firings purge.
- Evictors force full buffering and kill incremental aggregation. Avoid unless you need raw elements.

**Interview one-liners**

- *"What is a trigger?"* → The pluggable policy deciding when a window's function runs; it sees each element and can register event-time and processing-time timers.
- *"FIRE vs FIRE_AND_PURGE?"* → `FIRE` emits and keeps contents (cumulative re-firings); `FIRE_AND_PURGE` emits and clears (disjoint chunks, bounded state).
- *"How do you get partial results from a long window?"* → A custom trigger firing on count/time, or `ContinuousEventTimeTrigger`. Downstream must dedupe by window key.
- *"Why does my custom trigger never fire at the window end?"* → You replaced the default `EventTimeTrigger`; nothing registers the end-of-window timer any more.
- *"Why avoid evictors?"* → They disable incremental aggregation: all elements must be buffered and the aggregate recomputed per firing.
- *"How does countWindow work?"* → `GlobalWindows` + `PurgingTrigger.of(CountTrigger.of(n))`.
