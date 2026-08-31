# 14. Sliding, Session, and Global Windows

Tumbling windows are one of four assigner families. Here are the other three.

```
TUMBLING   fixed size, no overlap        [0,5) [5,10) [10,15)
SLIDING    fixed size, WITH overlap      [0,10) [5,15) [10,20)   size=10 slide=5
SESSION    variable size, gap-driven     activity ... gap ... activity
GLOBAL     one window, forever           needs a custom trigger to ever fire
```

---

## 1. Sliding windows

`SlidingEventTimeWindows.of(size, slide)`:
- **size** — how much history each window covers.
- **slide** — how often a new window starts.

```java
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import java.time.Duration;

stream
    .keyBy(e -> e.userId)
    .window(SlidingEventTimeWindows.of(
            Duration.ofSeconds(10),      // size
            Duration.ofSeconds(5)))      // slide
    .sum("amount");
```

"Every 5 seconds, report the total over the last 10 seconds."

`SlidingProcessingTimeWindows.of(size, slide)` is the processing-time twin. There is also a three-argument overload with an `offset`, same meaning as chapter 11.

### The picture

size = 10s, slide = 5s:

```
time (s)   0    5    10   15   20   25
           |----|----|----|----|----|

W1  [ 0,10)  ██████████
W2  [ 5,15)       ██████████
W3  [10,20)            ██████████
W4  [15,25)                 ██████████
W5  [20,30)                      ██████████

event at t=7  ───▶ belongs to W1 and W2       (2 windows)
event at t=12 ───▶ belongs to W2 and W3       (2 windows)
event at t=0  ───▶ belongs to W1 and also to  [-5, 5)
```

### The multiplication cost

> **Key idea**
> Every element is **copied into `ceil(size / slide)` windows**. That factor multiplies your state, your CPU, and your output volume.

```
size=10s, slide=5s     → 10/5   =    2 copies of each element
size=1h,  slide=1min   → 60/1   =   60 copies
size=24h, slide=1min   → 1440/1 = 1440 copies
size=1h,  slide=1s     → 3600   = 3600 copies   ← this will not work
```

At 1440x, a 100-byte event becomes 144 KB of window state, per key. With 100k keys and 10k events/sec you are asking for terabytes. This is the single most common way people accidentally blow up a Flink job.

Also note the **output** rate: with slide = 1s you emit a result per key per second, forever.

**Mitigations:**
1. Increase the slide. `size=1h, slide=5min` is 12 copies instead of 60. Usually nobody actually needs 1-minute resolution on an hourly metric.
2. Use `aggregate` / `reduce`, never a bare `ProcessWindowFunction`. Incremental keeps one small accumulator per window instead of the elements. 60 accumulators ≪ 60 copies of the raw data.
3. If your aggregate is a simple sum/count over a rolling period, consider tumbling sub-windows plus a downstream rolling sum in `KeyedProcessFunction` (Phase 3) — same result, linear state.

### Sliding boundaries

The first window containing timestamp `t` starts at `t - (t % slide)`, and windows extend backwards from there, so an element at `t=0` also lands in `[-5000, 5000)`. Negative-start windows are real and will appear in your output at the very beginning of a stream. That's not a bug.

Setting `size == slide` makes a sliding window degenerate into a tumbling window. Prefer the tumbling assigner in that case; it's cheaper and clearer.

---

## 2. Session windows

A session window has **no fixed size**. It groups activity separated by gaps of inactivity.

```java
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;

stream
    .keyBy(e -> e.userId)
    .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(30)))
    .aggregate(new StatsAgg(), new AddWindowInfo());
```

"A user's session ends after 30 seconds of no events from that user."

`ProcessingTimeSessionWindows.withGap(...)` exists too, but sessions on processing time are rarely what you want — a lag spike would split one real session into two.

### How merging works

Flink's implementation is counter-intuitive and worth knowing:

**For every arriving element, Flink creates a brand new window `[ts, ts + gap)`. Then it merges all overlapping windows.**

Trace, gap = 30s, user `u1`:

```
event ts   window created       merge result
--------   ------------------   -----------------------------------------
  0        [  0,  30)           sessions: [0,30)
 10        [ 10,  40)           overlaps [0,30)  → MERGE → [0,40)
 25        [ 25,  55)           overlaps [0,40)  → MERGE → [0,55)
100        [100, 130)           no overlap       → separate: [0,55), [100,130)
110        [110, 140)           overlaps [100,130) → MERGE → [100,140)
```

Timeline:

```
ts:   0   10   25            100  110
      ●    ●    ●             ●    ●
      └────┬────┘             └─┬──┘
     session A [0,55)      session B [100,140)
                  <── 45s gap > 30s ──>
```

**Merges can join windows retroactively.** If an event arrives at ts=60 (after the 100 and 110 events, but out of order), it creates `[60,90)`, which overlaps neither — no merge. But an event at ts=70 creates `[70,100)`, which touches `[100,140)`; depending on exact overlap rules two previously separate sessions can be stitched into one. A single late event can therefore **merge two existing sessions**, and their two accumulators must become one.

### That is why sessions need `merge()`

This is the answer to "when is `AggregateFunction.merge` called" from chapter 12:

```
window [0,40) acc = (count=2, sum=13.0)
window [25,55) acc = (count=1, sum=4.0)
                       │
                       ▼  merge(a, b)
window [0,55)  acc = (count=3, sum=17.0)
```

Without a correct `merge`, session aggregates are wrong or the job throws `UnsupportedOperationException` at the first merge.

`ReduceFunction` works with sessions too — its `reduce(a,b)` doubles as the merge, which is another reason it must be associative and commutative.

`ProcessWindowFunction` also works: Flink merges the buffered element lists by concatenation.

### Dynamic gaps

The gap can depend on the element:

```java
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;

stream
    .keyBy(e -> e.userId)
    .window(EventTimeSessionWindows.withDynamicGap(
        (SessionWindowTimeGapExtractor<Event>) element -> {
            // premium users get a longer session before we consider them gone
            return "premium".equals(element.type) ? 300_000L : 30_000L;
        }))
    .aggregate(new StatsAgg(), new AddWindowInfo());
```

- `SessionWindowTimeGapExtractor<T>` is a functional interface: `long extract(T element)`. The return value is **milliseconds**.
- The `(SessionWindowTimeGapExtractor<Event>)` cast tells the compiler which interface this lambda implements — needed because `withDynamicGap` can't infer it here.
- `"premium".equals(element.type)` — string literal first, so a null `type` returns false instead of throwing `NullPointerException`. Always write it this way in Java.
- `300_000L` — underscores in numeric literals are legal Java and purely for readability.

### Session window caveats

- **Sessions never fire while the user is active.** A user clicking once a second for an hour produces one session that emits after they stop. Do not use session windows for anything needing periodic output — you need an early trigger (chapter 15).
- **Session state grows with session length**, not with a fixed window size. One pathological key can hold a session open indefinitely.
- **`getStart()`/`getEnd()` are only final at fire time.** Mid-stream a session's bounds move as it merges.

---

## 3. Global windows

```java
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

stream
    .keyBy(e -> e.userId)
    .window(GlobalWindows.create())
    .sum("amount");            // <-- this NEVER emits anything
```

`GlobalWindows` puts **every element of a key into one window that never ends**. Its `maxTimestamp()` is `Long.MAX_VALUE`, so no watermark can ever pass it.

Its default trigger is `NeverTrigger`. The job runs, state grows forever, nothing is ever emitted.

```
key u1:  ┌───────────────────────────────────────────────────▶ (no end)
         │ e1 e2 e3 e4 e5 e6 e7 e8 e9 ...
         └───────────────────────────────────────────────────▶
                    never fires without a custom trigger
```

It is a **building block**, not something you use bare. You attach a trigger that defines "done":

```java
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

stream
    .keyBy(e -> e.userId)
    .window(GlobalWindows.create())
    .trigger(PurgingTrigger.of(CountTrigger.of(100)))
    .sum("amount");
```

`CountTrigger.of(100)` fires every 100 elements. `PurgingTrigger.of(...)` wraps it so the window contents are cleared after each fire — otherwise the 200th element's result would include all 200, and state would grow without bound.

### `countWindow()` is exactly this

Flink ships the shorthand:

```java
stream.keyBy(e -> e.userId).countWindow(100);          // tumbling count window
stream.keyBy(e -> e.userId).countWindow(100, 10);      // sliding: size 100, slide 10
```

`countWindow(100)` is literally implemented as `GlobalWindows` + `PurgingTrigger.of(CountTrigger.of(100))`. `countWindow(100, 10)` uses a `GlobalWindows` with an evictor that keeps the last 100 and a trigger firing every 10.

**The trap:** count windows have no time component. A key that reaches 97 events and then goes quiet holds those 97 in state **forever** and never emits. Use count windows only where you are confident every key keeps producing, or pair them with a time-based fallback in a `KeyedProcessFunction`.

---

## Choosing an assigner

| question | assigner |
|---|---|
| "revenue per 5-minute bucket" | tumbling |
| "revenue over the last hour, refreshed every minute" | sliding (watch the 60x cost) |
| "how long was each user's visit" | session |
| "every 100 events" / custom firing logic | global + trigger |

Rules of thumb:
- Reach for **tumbling** first. It is the cheapest and it is what most requirements actually mean.
- Reach for **sliding** only when the requirement genuinely says "moving/rolling", and compute `size/slide` before you commit.
- Reach for **session** for user-behaviour analytics, and always implement `merge()`.
- Reach for **global** only when you're writing a custom trigger anyway.

---

## Cost comparison

Per key, holding `n` events, with an incremental aggregate:

| assigner | open windows per key | state per key |
|---|---|---|
| tumbling 10s | 1 | 1 accumulator |
| sliding size=1h slide=1min | 60 | 60 accumulators |
| session gap=30s | 1 (usually; more mid-merge) | 1 accumulator, unbounded duration |
| global | 1 | 1 accumulator, forever |

With a `ProcessWindowFunction` instead, replace "accumulator" with "every element", and the sliding row becomes 60 full copies.

---

## Remember

- Sliding: `SlidingEventTimeWindows.of(size, slide)`. Each element goes into `ceil(size/slide)` windows — compute that number before deploying.
- `size == slide` is a tumbling window; use the tumbling assigner.
- Session: `EventTimeSessionWindows.withGap(d)`. One window created per element, then overlapping windows merge.
- Sessions require a correct `merge()` in your `AggregateFunction`; that is the only place `merge` is called.
- A session doesn't fire until `gap` of silence — no periodic output.
- `withDynamicGap(extractor)` returns the gap in **milliseconds** per element.
- `GlobalWindows.create()` never fires on its own (`NeverTrigger`). It needs a trigger.
- `countWindow(n)` = `GlobalWindows` + `PurgingTrigger.of(CountTrigger.of(n))`. A key that stalls below `n` never emits.

**Interview one-liners**

- *"Tumbling vs sliding?"* → Tumbling: disjoint, one window per element. Sliding: overlapping, `size/slide` windows per element, with proportional state and output cost.
- *"What's the cost of a 1-hour window sliding every minute?"* → 60 copies of every element (or 60 accumulators) per key. Often the reason a job OOMs.
- *"How do session windows work internally?"* → Each element opens a `[ts, ts+gap)` window; overlapping windows are merged, so sessions grow and can be joined retroactively by a late event.
- *"Why do session windows need merge()?"* → Merging two windows must merge their accumulators; tumbling/sliding never merge so `merge` is never invoked there.
- *"Why doesn't my GlobalWindows job emit anything?"* → Its default trigger is `NeverTrigger`; a global window has no end, so you must supply a trigger.
- *"What is countWindow built on?"* → `GlobalWindows` with a purging `CountTrigger`; it has no time dimension, so a stalled key never fires.
