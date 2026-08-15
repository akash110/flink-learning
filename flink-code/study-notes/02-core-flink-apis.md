# Phase 2 — Core Flink APIs (deep dive)

This is your **Next** phase, so it's the most detailed. Goal: go from "I made one tumbling window" to "I understand every kind of window, every way to aggregate them, and how late data is handled in real code."

**Reused POJO for this whole file:**

```java
public static class Event {
    public String user;
    public int amount;
    public long timestamp;               // event time in millis

    public Event() {}
    public Event(String user, int amount, long timestamp) {
        this.user = user; this.amount = amount; this.timestamp = timestamp;
    }
    public String getUser()     { return user; }
    public int    getAmount()   { return amount; }
    public long   getTimestamp(){ return timestamp; }

    @Override public String toString() {
        return "Event{user='" + user + "', amount=" + amount + ", ts=" + timestamp + "}";
    }
}
```

---

## 0. The complete timestamp + watermark pipeline (build this once, reuse forever)

Every event-time job has the same front end. Memorize this shape:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

DataStream<Event> events = env.fromElements(
        new Event("alice", 100, 1000),
        new Event("alice", 200, 2000),
        new Event("bob",   50,  2500),
        new Event("alice", 300, 8000),
        new Event("bob",   70,  9000)
    )
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))   // out-of-order budget
            .withTimestampAssigner((e, recordTs) -> e.getTimestamp()) // where the event time lives
    );
```

Two jobs done by this block:
1. **Timestamp assigner** — tells Flink which field is event time.
2. **Watermark generator** — emits watermarks = `maxSeenTs - 2s`, which drive when windows fire.

> ⚠️ If you *don't* assign timestamps & watermarks, event-time windows **never fire** and you get no output. This is the single most common Phase 2 confusion. When a windowed job prints nothing, check this first.

---

## 1. Window types — the big picture

A window assigner decides **which window(s) each event belongs to**.

| Assigner | Shape | Overlap? | Typical use |
|----------|-------|----------|-------------|
| **Tumbling** | fixed size, back-to-back | no | "sum per 1-minute bucket" |
| **Sliding** | fixed size, step < size | yes | "sum over last 10 min, updated every 1 min" |
| **Session** | gap-based, variable size | no | "group activity until user idle for N min" |
| **Global** | one window, all events | n/a | custom triggering / count windows |

All of these have event-time and processing-time variants (`TumblingEventTimeWindows` vs `TumblingProcessingTimeWindows`). Prefer **event time**.

---

## 2. Tumbling windows (you know these — quick reinforce)

```java
events.keyBy(Event::getUser)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))   // [0,5s), [5s,10s), ...
      .sum("amount")
      .print();
```

Windows are **per key**. `alice`'s `[0,5s)` window is completely separate from `bob`'s.

**Expected output** (with the data above): alice's [0,5s) = 100+200 = 300; bob's [0,5s) = 50; alice's [5s,10s) = 300; bob's [5s,10s) = 70. Each prints once the watermark passes the window end.

---

## 3. The four ways to aggregate a window

This is the heart of Phase 2. Same window, four APIs, increasing power.

### 3a. `sum(field)` / `max` / `min` / `maxBy` / `minBy`

Simplest. One field, built-in reduction.

```java
.window(TumblingEventTimeWindows.of(Time.seconds(5))).sum("amount");
```

- `max("amount")` returns the max value but **keeps the first record's other fields**.
- `maxBy("amount")` returns the **whole record** that had the max — usually what you actually want.

### 3b. `reduce(...)` — combine two records of the *same* type

Incremental: Flink applies it as records arrive, storing only the running result (memory-cheap). Input type == output type.

```java
events.keyBy(Event::getUser)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .reduce((a, b) -> new Event(a.user, a.amount + b.amount, b.timestamp))
      .print();
```

Use when the aggregate is the same shape as the input (running total, running max).

### 3c. `aggregate(AggregateFunction)` — input, accumulator, output can all differ

The most flexible incremental aggregation. You define 4 methods:

```java
public static class AvgAgg
        implements AggregateFunction<Event, Tuple2<Long,Long>, Double> {
    //                                input,  accumulator(sum,count), output(avg)

    @Override public Tuple2<Long,Long> createAccumulator() { return Tuple2.of(0L, 0L); }

    @Override public Tuple2<Long,Long> add(Event e, Tuple2<Long,Long> acc) {
        return Tuple2.of(acc.f0 + e.amount, acc.f1 + 1);
    }

    @Override public Double getResult(Tuple2<Long,Long> acc) {
        return acc.f1 == 0 ? 0.0 : (double) acc.f0 / acc.f1;
    }

    @Override public Tuple2<Long,Long> merge(Tuple2<Long,Long> a, Tuple2<Long,Long> b) {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);   // needed for session windows
    }
}

// usage:
events.keyBy(Event::getUser)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(new AvgAgg())
      .print();   // prints the average amount per user per window
```

**Why it's better than reduce:** the accumulator can be a different type than input/output — compute averages, distinct counts, histograms, etc. Still incremental (memory-cheap).

### 3d. `count()` — convenience for "how many in this window"

There's no literal `.count()` on windows in the DataStream API; you get counts by aggregating. Two idioms:

```java
// idiom A: map each event to 1, then sum
events.map(e -> Tuple2.of(e.user, 1L))
      .returns(Types.TUPLE(Types.STRING, Types.LONG))
      .keyBy(t -> t.f0)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sum(1)
      .print();

// idiom B: an AggregateFunction whose accumulator is just a counter (cleaner for real code)
```

> Note: **count *windows*** (fire every N elements) are a different thing — see §6 Global windows.

### Incremental vs full-buffer — the key trade-off

- `reduce` / `aggregate` = **incremental**. Flink keeps only the running accumulator. Scales to huge windows.
- `ProcessWindowFunction` (next) = **buffers all events** in the window, then hands you the whole `Iterable`. Powerful, but memory grows with window size.
- **Best of both:** combine them (§5).

---

## 4. `ProcessWindowFunction` — full control over a window

When you need **all events together**, or window **metadata** (start/end time, the key), use `process(...)`.

```java
public static class MyWindowFn
        extends ProcessWindowFunction<Event, String, String, TimeWindow> {
    //                                 in,    out,    key,    window

    @Override
    public void process(String user,
                        Context ctx,
                        Iterable<Event> events,
                        Collector<String> out) {
        long count = 0, sum = 0;
        for (Event e : events) { count++; sum += e.amount; }

        long start = ctx.window().getStart();
        long end   = ctx.window().getEnd();
        out.collect("user=" + user + " window=[" + start + "," + end + ") "
                    + "count=" + count + " sum=" + sum);
    }
}

// usage:
events.keyBy(Event::getUser)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new MyWindowFn())
      .print();
```

You get:
- the **key** (`user`),
- an **`Iterable`** of all events in the window,
- `ctx.window()` → start/end,
- `ctx.currentWatermark()`, and per-window/per-key state (advanced).

**Cost:** Flink buffers every event until the window fires. Fine for small windows, dangerous for large ones.

---

## 5. The production pattern: incremental aggregate + `ProcessWindowFunction`

You almost never want to choose between "cheap" and "has metadata." Combine them: the aggregate runs incrementally, and the process function receives the *single* pre-aggregated result plus window metadata.

```java
events.keyBy(Event::getUser)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(new AvgAgg(), new EnrichWithWindowInfo());
```

```java
// second arg: gets the aggregate's OUTPUT as a single-element Iterable
public static class EnrichWithWindowInfo
        extends ProcessWindowFunction<Double, String, String, TimeWindow> {
    @Override
    public void process(String user, Context ctx,
                        Iterable<Double> avgs, Collector<String> out) {
        Double avg = avgs.iterator().next();          // exactly one pre-aggregated value
        out.collect("user=" + user
                + " windowEnd=" + ctx.window().getEnd()
                + " avgAmount=" + avg);
    }
}
```

**This is the pattern to reach for by default** in real jobs: memory-cheap *and* you get window boundaries/key. Remember it.

---

## 6. Sliding, Session, and Global windows

### Sliding windows — overlapping

Size + slide. Each event can land in multiple windows.

```java
events.keyBy(Event::getUser)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      //                                  size=10s,        slide=5s
      .sum("amount");
```

"Sum over the last 10s, recomputed every 5s." If size/slide = 10/5, each event belongs to 2 windows. Use for moving averages / rolling metrics.

### Session windows — gap-based

No fixed size. A window stays open while events keep coming; it closes after a **gap of inactivity**.

```java
events.keyBy(Event::getUser)
      .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
      .sum("amount");
```

"Group a user's activity; end the session after 30s of silence." Perfect for user sessions, clickstreams. (This is also §Phase 6 "sessionization".)

### Global windows — one window, you control firing

All events for a key go into a single, never-ending window. Useless without a **trigger** (and usually an **evictor**). This is how you build **count windows**:

```java
events.keyBy(Event::getUser)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(3))     // fire every 3 elements
      .sum("amount");
```

There's also a shortcut: `keyedStream.countWindow(3)` for tumbling count windows and `countWindow(size, slide)` for sliding count windows.

---

## 7. Window triggers — *when* does a window fire?

A **`Trigger`** decides when the window's function runs and results are emitted. Defaults:

- Event-time windows → `EventTimeTrigger` (fires when watermark passes window end).
- Processing-time windows → `ProcessingTimeTrigger`.
- Global windows → **no default** (you must set one — that's why the example above needs `CountTrigger`).

Custom triggers let you do things like "fire early every 1s while the window is still open, then fire finally at the end":

```java
.window(TumblingEventTimeWindows.of(Time.minutes(1)))
.trigger(/* custom or ContinuousEventTimeTrigger.of(Time.seconds(10)) */)
```

`ContinuousEventTimeTrigger.of(Time.seconds(10))` = emit partial results every 10s for a 1-minute window (early results). You rarely write a trigger from scratch early on — but know the concept: **assigner decides membership, trigger decides firing.**

(**Evictor** — optional — removes elements before/after the function runs. Advanced; skip for now.)

---

## 8. Late events in actual code + `allowedLateness`

Recall: watermark = `maxTs - outOfOrderness`. An event whose timestamp is **before the current watermark** is **late**.

By default, late events are **silently dropped**. Two tools to handle them:

### 8a. `allowedLateness` — keep the window around after it fires

```java
events.keyBy(Event::getUser)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .allowedLateness(Time.seconds(5))     // keep window state 5s past its end
      .sum("amount");
```

Effect: the window fires when the watermark passes its end, but stays in state for 5 more seconds. Each late event that still fits triggers a **re-fire** with an updated result. After the lateness expires, the window state is purged and further stragglers are dropped.

Trade-off: longer lateness = more correct, but state is held longer (memory) and downstream sees updates/retractions.

### 8b. Side outputs — capture the dropped stragglers

The clean way to *not lose* data that's too late: route it to a **side output** and handle it separately (log it, write to a "late" table, alert).

---

## 9. Side outputs for late data (full example)

A **side output** is a secondary stream tagged by an `OutputTag`. You define the tag, attach it to the window, then pull the side stream off the result.

```java
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

// 1) define the tag (anonymous subclass captures the generic type — the {} matters!)
final OutputTag<Event> lateTag = new OutputTag<Event>("late-events") {};

// 2) attach .sideOutputLateData(tag) to the window
SingleOutputStreamOperator<Event> result = events
        .keyBy(Event::getUser)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .allowedLateness(Time.seconds(2))
        .sideOutputLateData(lateTag)          // stragglers past lateness go here
        .sum("amount");

// 3) main (on-time) results
result.print();

// 4) pull the late stream and handle it separately
DataStream<Event> lateEvents = result.getSideOutput(lateTag);
lateEvents.print();      // in real life: write to a dead-letter Kafka topic / log / metric
```

**Why side outputs matter beyond late data:** they're the general mechanism for **splitting a stream** — e.g. a `ProcessFunction` can emit "normal" records to the main output and "suspicious" records to a side output. You'll use them again in Phase 6 (fraud) and Phase 3.

Side outputs from a `ProcessFunction` (preview of Phase 3/6):

```java
public class Splitter extends ProcessFunction<Event, Event> {
    static final OutputTag<Event> big = new OutputTag<Event>("big") {};
    @Override public void processElement(Event e, Context ctx, Collector<Event> out) {
        if (e.amount > 1000) ctx.output(big, e);   // side output
        else                 out.collect(e);       // main output
    }
}
```

---

## 10. Common Phase 2 pitfalls (save yourself hours)

1. **Windowed job prints nothing** → you forgot `assignTimestampsAndWatermarks`, or the watermark never advances because you have too few events. With `fromElements`, the stream ends and Flink emits a final "max" watermark that flushes everything — good for demos.
2. **`OutputTag` without the trailing `{}`** → type erasure again. Always `new OutputTag<Event>("x") {}`.
3. **Using `ProcessWindowFunction` on a huge window** → OOM from buffering. Use the aggregate+process combo (§5).
4. **`max` vs `maxBy`** → `max` mixes fields from different records. Use `maxBy` if you want a coherent record.
5. **`keyBy` before window is mandatory** for keyed windows. `windowAll` (non-keyed) runs at parallelism 1 and doesn't scale — avoid except for tiny global aggregates.
6. **Session windows need `merge()`** in your `AggregateFunction` (sessions can merge when a bridging event arrives).

---

## 11. Suggested exercises (do these, don't just read)

1. Take the pipeline in §0 and print results for **tumbling(5s)**, then switch to **sliding(10s,5s)**, then **session(3s gap)**. Watch how the same data groups differently.
2. Rewrite a `sum("amount")` window as (a) `reduce`, (b) an `AggregateFunction`. Confirm identical output.
3. Compute the **average** amount per user per 5s window using the aggregate+`ProcessWindowFunction` combo, and print the window `[start,end)`.
4. Add a deliberately-late event (`new Event("alice", 999, 500)` at the end) and:
   - first observe it's dropped,
   - then add `allowedLateness(3s)` and watch the window re-fire,
   - then add a **side output** and print the late stream separately.

---

### ✅ Phase 2 checklist

- [ ] Complete timestamp + watermark pipeline
- [ ] `TumblingEventTimeWindows`
- [ ] `sum` / `reduce` / `aggregate` / count idioms
- [ ] `ProcessWindowFunction` (+ the aggregate+process combo)
- [ ] Sliding windows
- [ ] Session windows
- [ ] Global windows + count windows
- [ ] Triggers (assigner vs trigger mental model)
- [ ] Late events in real code + `allowedLateness`
- [ ] Side outputs for late data

⬅️ [Phase 1](01-foundations.md)  ·  ➡️ [Phase 3 — State](03-state.md)
