# 16. Late Events and Side Outputs

Watermarks are a **guess**. When the guess is wrong, an event arrives after Flink has already decided its window is done. This chapter is about what happens then, and how to not lose the data.

---

## What "late" precisely means

> **Key idea**
> An element is **late** if `element.timestamp < currentWatermark` at the moment the window operator receives it.
> That is the whole definition. It has nothing to do with wall-clock time, network delay, or how long the record sat in Kafka.

```
watermark = 10:05:00
   event with ts = 10:04:59  ──▶ LATE
   event with ts = 10:05:00  ──▶ LATE     (watermark T means "nothing more <= T")
   event with ts = 10:05:01  ──▶ on time
```

Concretely, with `forBoundedOutOfOrderness(Duration.ofSeconds(5))` and a 10-second tumbling window:

```
timeline (event time)
0        10000     20000
|---------|---------|
|   W1    |   W2    |

max ts seen = 15000  →  watermark = 15000 - 5000 - 1 = 9999
   W1 [0,10000) has NOT fired (9999 < 10000).
   An event at ts=8000 arriving now is technically late (8000 <= 9999),
   but W1 is still open, so the element is simply added and nothing is lost.

max ts seen = 16000  →  watermark = 10999
   10999 >= 10000, so W1 FIRES.
   An event at ts=8000 arriving now belongs to a window that has produced
   its result. What happens next depends entirely on allowedLateness.
```

The nuance to hold onto: **being "late" is not the same as being lost.** Lateness only costs you data once the window's state has been cleaned up. Everything below is about controlling that moment.

---

## Default behaviour: silently dropped

```java
stream
    .assignTimestampsAndWatermarks(strategy)
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
    .sum("amount");
```

A late element here is **discarded**. No exception. No log line at INFO. No metric you'd notice by default. Your sums are quietly wrong.

There is a metric — `numLateRecordsDropped` on the window operator — but nobody looks at it until after the incident. **Always route late data somewhere in production.**

---

## `allowedLateness(Duration)`

```java
stream
    .assignTimestampsAndWatermarks(strategy)
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
    .allowedLateness(Duration.ofSeconds(30))
    .sum("amount");
```

### What it actually does

Three things, and the third one is the one that surprises people:

1. **The window fires normally** when `watermark >= windowEnd`. Allowed lateness does **not** delay the first result.
2. **The window's state is kept** until `watermark > windowEnd + lateness`, instead of being cleaned up immediately.
3. **Each late element that arrives in that grace period triggers the window function again**, producing a new, updated output record.

```
window [0, 10000), allowedLateness = 30s

watermark
 9999  ────────────────────────────  nothing
10000  ── FIRE ──▶ emit (u1, 10.0)   state KEPT
                   late event ts=7000, amount=3.0 arrives
       ── FIRE ──▶ emit (u1, 13.0)   ← same window, updated result
                   late event ts=9000, amount=1.0 arrives
       ── FIRE ──▶ emit (u1, 14.0)   ← again
40000  ── state destroyed ──         any later element is now dropped/side-outputted
```

### The consequence you must design for

Your sink now receives:

```
(u1, [0,10000), 10.0)
(u1, [0,10000), 13.0)
(u1, [0,10000), 14.0)
```

Three records for one window. Whether this is "a correction" or "a duplicate that triples your revenue" depends entirely on the sink:

| sink | result |
|---|---|
| Kafka topic, append | 3 messages. Consumers must dedupe/take-latest by `(key, windowStart)`. |
| JDBC `INSERT` | 3 rows. Your dashboard sums to 37.0 instead of 14.0. **Broken.** |
| JDBC `UPSERT` on `(key, windowStart)` | 1 row, correct final value 14.0. Correct. |
| Elasticsearch with `_id = key + windowStart` | 1 doc, overwritten. Correct. |
| A downstream stateful Flink operator | Must handle retractions itself. |

> **Key idea**
> `allowedLateness` turns your output from an **append stream** into an **update stream**. If your sink cannot upsert by `(key, windowStart)`, allowed lateness will corrupt your numbers rather than correct them. Decide the sink semantics before you enable it.

Note also: with `ProcessWindowFunction` alone, the re-firing runs over the *whole buffered list including the new late element*, so the emitted value is the correct new total (not a delta). With `aggregate`, the late element is folded into the retained accumulator and `getResult` is called again. Both give cumulative, not incremental, results.

### State cost

```
open windows per key = 1 + ceil(allowedLateness / windowSize)
```

10-second windows with 1 hour of allowed lateness = 361 windows held per key. Allowed lateness is not free; it is a direct multiplier on window state.

### Where lateness applies

`allowedLateness` is available on `WindowedStream` and `AllWindowedStream`. It defaults to `Duration.ZERO`. It works with all four assigner families.

---

## `sideOutputLateData(OutputTag)` — capture what you'd otherwise lose

```java
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

// 1. Declare the tag
final OutputTag<Event> lateTag = new OutputTag<Event>("late-events") {};

// 2. Attach it to the window
SingleOutputStreamOperator<Event> result = stream
        .assignTimestampsAndWatermarks(strategy)
        .keyBy(e -> e.userId)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
        .allowedLateness(Duration.ofSeconds(30))
        .sideOutputLateData(lateTag)
        .sum("amount");

// 3. Retrieve the side stream
DataStream<Event> lateStream = result.getSideOutput(lateTag);

result.print("ON-TIME");
lateStream.print("LATE");
```

### The `{}` at the end — why it's mandatory

```java
new OutputTag<Event>("late-events") {}
//                                  ^^ this
```

Java uses **type erasure**: generic type arguments are removed at compile time. At runtime `new OutputTag<Event>("x")` and `new OutputTag<String>("x")` would be indistinguishable — the `Event` is gone, and Flink cannot work out which serializer to use.

The trailing `{}` makes this an **anonymous subclass** of `OutputTag`. Subclasses record their superclass's type arguments in the class file (in the `Signature` attribute), and that survives erasure. Flink reads `getClass().getGenericSuperclass()` to recover `Event`. This trick has a name: the **super type token** (or "Gafter's gadget").

Omit the `{}` and you get a runtime exception at job submission:

```
InvalidTypesException: Could not determine TypeInformation for the OutputTag type.
The most common reason is forgetting to make the OutputTag an anonymous inner class.
```

The error message literally tells you. Now you know why.

Other rules for `OutputTag`:
- The `String` id must be **unique** within the job. Two tags with the same id and different types will collide.
- Declare it `final` (or `static final`) — Flink needs to serialize it into the operator, and it must be effectively immutable.
- The type parameter is the type of the **side output stream**, which need not match the main stream's type.

### Ordering: lateness first, then side output

```java
.allowedLateness(Duration.ofSeconds(30))
.sideOutputLateData(lateTag)
```

An element goes to the side output only if it is late **beyond the allowed lateness**. Within the lateness window it re-fires the window instead. So the two features compose:

```
element arrives, ts < watermark
        │
        ├── watermark <= windowEnd + lateness  →  re-fire the window, updated output
        │
        └── watermark >  windowEnd + lateness  →  window state is gone
                                                   → side output (if a tag is set)
                                                   → otherwise DROPPED
```

### What to do with the late stream

```java
lateStream
    .map(e -> "LATE: user=" + e.userId + " ts=" + e.timestamp + " amount=" + e.amount)
    .sinkTo(someKafkaSink);
```

Real options, in rough order of usefulness:
1. **Write to a "late" table/topic** and reprocess in batch. The standard answer.
2. **Alert on the rate.** A rising late-record rate means your out-of-orderness bound is too tight or an upstream producer is misbehaving. This is a genuinely useful SLO.
3. **Feed a correction job** that upserts into the same sink.
4. Never: silently drop, unless you have measured that the volume is negligible and written that down.

---

## General side outputs from a `ProcessFunction`

Side outputs are not just for late data — they're the general way to split one stream into several **without re-reading the input**.

```java
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitByAmount {

    static final OutputTag<Event> BIG   = new OutputTag<Event>("big") {};
    static final OutputTag<String> BAD  = new OutputTag<String>("malformed") {};

    public static void run(DataStream<Event> stream) {

        SingleOutputStreamOperator<Event> normal = stream.process(
            new ProcessFunction<Event, Event>() {
                @Override
                public void processElement(Event e, Context ctx, Collector<Event> out) {
                    if (e.userId == null || e.userId.isEmpty()) {
                        ctx.output(BAD, "missing userId at ts=" + e.timestamp);
                    } else if (e.amount > 10_000.0) {
                        ctx.output(BIG, e);          // side output
                    } else {
                        out.collect(e);              // main output
                    }
                }
            });

        DataStream<Event>  big = normal.getSideOutput(BIG);
        DataStream<String> bad = normal.getSideOutput(BAD);

        normal.print("NORMAL");
        big.print("BIG");
        bad.print("BAD");
    }
}
```

Points:
- `ctx.output(tag, value)` writes to a side output. `out.collect(value)` writes to the main output. A single call to `processElement` can do both, or neither, any number of times.
- Note `BAD` is `OutputTag<String>` while the main stream is `Event`. **Side outputs can have different types from the main stream.** This is a real advantage over `filter`.
- `getSideOutput` must be called on the `SingleOutputStreamOperator` returned by the operator that produced the tag — not on a downstream stream. This is the #1 mistake.
- Calling `getSideOutput` with a tag nobody ever emitted to gives an empty stream, not an error. A typo in the tag id therefore fails silently.

### Side outputs vs `filter`

```java
// filter: reads the stream twice, both operators see every record
DataStream<Event> big   = stream.filter(e -> e.amount > 10_000);
DataStream<Event> small = stream.filter(e -> e.amount <= 10_000);

// side output: one pass, one operator, types may differ
```

Use side outputs for n-way splits, for error/DLQ routing, and whenever the branches have different types. Use `filter` when there are two branches, the predicate is cheap, and readability wins.

Side outputs are also available from `KeyedProcessFunction`, `CoProcessFunction`, `ProcessWindowFunction` (via its `Context.output`), and `ProcessAllWindowFunction`.

---

## The trade-off table

Given a fixed workload, these three move against each other:

| knob | latency | completeness | state size |
|---|---|---|---|
| ↑ out-of-orderness bound | **worse** — every window delayed by the bound | better — fewer records are late | same |
| ↑ `allowedLateness` | unchanged for the first result | better — late records are incorporated | **worse** — `1 + lateness/windowSize` open windows per key |
| `sideOutputLateData` | unchanged | recovers data, but out-of-band | +1 small stream |
| no lateness handling | best | worst — silent data loss | best |

Worked comparison for a 1-minute tumbling window, p99 event delay of 12 seconds, p99.99 delay of 4 minutes:

| config | first result at | records lost | open windows/key |
|---|---|---|---|
| `bound=0` | windowEnd | ~everything out of order | 1 |
| `bound=15s` | windowEnd + 15s | ~0.01% | 1 |
| `bound=5min` | windowEnd + 5min | ~0 | 1 |
| `bound=15s, lateness=5min` | windowEnd + 15s | ~0 (as corrections) | 6 |
| `bound=15s, lateness=5min, sideOutput` | windowEnd + 15s | 0 (rest recoverable offline) | 6 |

**The recommended production shape** is the last row: a small out-of-orderness bound for low latency, a moderate allowed lateness to auto-correct the common case, and a side output so nothing is ever lost. It costs 6x window state and an upsert-capable sink.

---

## Complete example

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class LateDataDemo {

    static final OutputTag<Event> LATE = new OutputTag<Event>("late") {};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> events = env.fromElements(
                new Event("u1", "click", 1.0,  1000L),
                new Event("u1", "click", 2.0,  4000L),
                new Event("u1", "click", 3.0, 12000L),   // pushes watermark to 9999... not yet
                new Event("u1", "click", 4.0, 16000L),   // watermark 13999 -> [0,10000) FIRES
                new Event("u1", "click", 5.0,  3000L),   // LATE, within lateness -> re-fire
                new Event("u1", "click", 6.0, 60000L),   // watermark 57999 -> lateness expired
                new Event("u1", "click", 7.0,  2000L)    // LATE, too late -> side output
        );

        SingleOutputStreamOperator<Event> windowed = events
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                         .withTimestampAssigner((e, ts) -> e.timestamp))
                .keyBy(e -> e.userId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .allowedLateness(Duration.ofSeconds(20))
                .sideOutputLateData(LATE)
                .sum("amount");

        windowed.print("RESULT");
        windowed.getSideOutput(LATE).print("LATE  ");

        env.execute("late data demo");
    }
}
```

### Expected console output

```
RESULT> Event{userId='u1', type='click', amount=3.0, timestamp=1000}
RESULT> Event{userId='u1', type='click', amount=8.0, timestamp=1000}
RESULT> Event{userId='u1', type='click', amount=7.0, timestamp=12000}
LATE  > Event{userId='u1', type='click', amount=7.0, timestamp=2000}
RESULT> Event{userId='u1', type='click', amount=6.0, timestamp=60000}
```

### Why each line appeared

Watermark after each input event (`maxTs - 2000 - 1`):

```
event ts   watermark   window          effect
--------   ---------   -------------   ---------------------------------------
 1000        -1001     [0,10000)       buffered
 4000         1999     [0,10000)       buffered
12000         9999     [10000,20000)   buffered  (9999 < 10000, nothing fires)
16000        13999     [10000,20000)   buffered; 13999 >= 10000 → [0,10000) FIRES
 3000        13999     [0,10000)       late, but within lateness → RE-FIRES
60000        57999     [60000,70000)   57999 >= 20000 → [10000,20000) FIRES
                                        and 57999 > 10000+20000 → [0,10000) state destroyed
 2000        57999     [0,10000)        window gone → SIDE OUTPUT
<EOF>    MAX_VALUE                      [60000,70000) flushed
```

1. `amount=3.0` — window `[0,10000)` = 1.0 + 2.0. Fired when the ts=16000 event pushed the watermark to 13999 ≥ 10000.
2. `amount=8.0` — the ts=3000 event arrived with watermark at 13999. It is late (3000 < 13999) but `13999 <= 10000 + 20000`, so it is **within allowed lateness**: it is folded into the still-retained window state and the window **re-fires**. 3.0 + 5.0 = 8.0. Two RESULT records now exist for the same window — this is the update-stream behaviour described above.
3. `amount=7.0` at ts=12000 — window `[10000,20000)` = 3.0 + 4.0, fired when the ts=60000 event pushed the watermark to 57999. The record carries `timestamp=12000` because `sum()` keeps the *first* record's other fields and only replaces `amount`.
4. `LATE > amount=7.0` at ts=2000 — arriving with watermark 57999. `57999 > 10000 + 20000 = 30000`, so window `[0,10000)`'s state was already destroyed. Nowhere to put it → side output.
5. `amount=6.0` at ts=60000 — window `[60000,70000)`, single event, flushed by the end-of-stream `Watermark(Long.MAX_VALUE)`.

Note how confusing line 3 is: the output says `timestamp=12000` and `amount=7.0`, which is not a real event that ever existed. That is the `sum()` passthrough behaviour from chapter 12. **In real jobs, emit the window bounds explicitly** using the `aggregate(AggFn, ProcessWindowFunction)` pattern from chapter 13 — the capstone does exactly that.

---

## Remember

- Late = `element.timestamp < currentWatermark` when the window operator sees it.
- Default is **silent drop**. Check the `numLateRecordsDropped` metric; better, always set a side output.
- `allowedLateness(d)` does not delay the first firing. It keeps window state for `d` longer and **re-fires per late element**.
- Re-firing makes the output an **update stream**. The sink must upsert on `(key, windowStart)` or your numbers will be wrong.
- Open windows per key ≈ `1 + lateness / windowSize`.
- `new OutputTag<Event>("late") {}` — the `{}` is mandatory; it's an anonymous subclass that preserves the generic type through erasure.
- `getSideOutput(tag)` must be called on the operator that produced it.
- Side outputs from `ProcessFunction` via `ctx.output(tag, value)` split streams in one pass and may use different types per branch.

**Interview one-liners**

- *"What is a late event?"* → One whose timestamp is below the current watermark; its window may already have fired and been cleaned up.
- *"What happens to late events by default?"* → Dropped silently; only the `numLateRecordsDropped` metric records it.
- *"What does allowedLateness do?"* → Retains window state past the firing watermark and re-fires the window for each late element, emitting updated results.
- *"What's the danger of allowedLateness?"* → Duplicate output records for the same window. Append-only sinks double-count; you need upsert semantics on `(key, windowStart)`.
- *"Why does OutputTag need the trailing braces?"* → Type erasure. The anonymous subclass records the generic argument in its class signature so Flink can infer `TypeInformation`.
- *"Side output vs filter?"* → Side output is one pass, supports n branches with different types, and is the standard DLQ pattern; filter re-reads the stream per branch.
- *"How do you tune completeness vs latency?"* → Small out-of-orderness for latency, moderate allowed lateness for auto-correction, side output as the safety net — and an upsert sink to make it consistent.
