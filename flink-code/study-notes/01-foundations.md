# Phase 1 — Core Flink Foundations (recap)

You've mostly done this phase. This file is a **fast recap + reference** so the later phases have a solid base to build on. Skim it; run anything that feels shaky.

---

## 1. The skeleton of every Flink job

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);                 // deterministic output while learning

DataStream<T> stream = env.fromElements(...);   // a source
// ... transformations ...
stream.print();                        // a sink

env.execute();                         // NOTHING runs until you call this
```

**Key insight:** the transformations you write only *describe* a graph. `env.execute()` ships that graph to the runtime and starts it. Forgetting `env.execute()` is the #1 beginner "why is there no output" bug.

---

## 2. Stateless transformations

| Operator | Input → Output | Use |
|----------|----------------|-----|
| `map` | 1 → 1 | transform each record |
| `filter` | 1 → 0 or 1 | keep records matching a predicate |
| `flatMap` | 1 → 0..N | split / expand / conditionally emit |

```java
DataStream<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6);

nums.map(x -> x * 10);                  // 10,20,30,...
nums.filter(x -> x > 3);                // 4,5,6
nums.flatMap((Integer x, Collector<Integer> out) -> {   // emit x twice
        out.collect(x);
        out.collect(x);
}).returns(Types.INT);
```

### The `.returns(...)` gotcha (you already hit this)

Java **erases generics** at runtime, so when you use a lambda that produces a generic type like `Tuple2<String,Integer>`, Flink can't infer the type. You must tell it:

```java
values.map(x -> new Tuple2<>(x, 1))
      .returns(new TypeHint<Tuple2<String, Integer>>(){});   // or Types.TUPLE(Types.STRING, Types.INT)
```

Rule of thumb: **plain types (String, Integer, your POJO) → fine. Tuples / generics from a lambda → add `.returns(...)`.**

---

## 3. `keyBy` — the most important line in stateful Flink

`keyBy` re-partitions the stream so all records with the same key go to the same place. It turns one stream into **many independent logical sub-streams, one per key.**

```java
DataStream<Tuple2<String,Integer>> counts = df
        .keyBy(x -> x.f0)   // key by the String
        .sum(1);            // running sum PER KEY
```

Everything stateful later (windows, `ValueState`, timers) operates **per key**. If you internalize "keyBy = group by key, forever" you're ahead of most beginners.

---

## 4. `Tuple2` and basic aggregation

`Tuple2<A,B>` is Flink's built-in pair. Fields are `.f0`, `.f1`. Handy for `(key, count)` style data before you graduate to POJOs.

Built-in rolling aggregations on a keyed stream: `sum(field)`, `min`, `max`, `minBy`, `maxBy`. These keep a **running** result and emit an updated value on every record (no windows involved yet).

---

## 5. Time: the concept that separates Flink from toy stream processors

- **Processing time** — the wall-clock time on the machine when the record is processed. Fast, but non-deterministic and wrong if events arrive late or you reprocess history.
- **Event time** — the timestamp *inside* the event (when it actually happened). Deterministic, correct, and what you almost always want.

### Out-of-order events

In reality, event #5 (t=8000) can arrive *before* event #4 (t=5000) — network delays, retries, multiple producers. Event-time processing must tolerate this.

### Watermarks (the mechanism)

A **watermark** is a special marker flowing with the stream that says: *"I don't expect any more events with timestamp ≤ T."* Watermarks are how Flink decides a window is complete and can fire.

```java
WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))   // tolerate 2s of lateness
    .withTimestampAssigner((event, ts) -> event.getTimestamp());
```

`forBoundedOutOfOrderness(2s)` means: watermark = (max event time seen) − 2s. That 2-second slack is your budget for out-of-order arrivals. Bigger = more tolerance but more latency.

### Allowed lateness (concept)

Even after the watermark passes, you can tell a window to *keep accepting* stragglers for a grace period via `.allowedLateness(...)`. Events later than that are dropped — or routed to a **side output** (Phase 2, in real code).

---

## 6. Tumbling windows (concept)

A **tumbling window** chops time into fixed, non-overlapping buckets: `[0–10s), [10–20s), ...`. Each event belongs to exactly one window. You aggregate per window, per key.

You already built this:

```java
eventStream.keyBy(e -> e.getName())
           .window(TumblingEventTimeWindows.of(Time.seconds(10)))
           .sum("count");
```

Phase 2 expands this into sliding/session/global windows, richer aggregations, custom triggers, and real late-data handling.

---

## 7. The `Event` POJO pattern (reuse this everywhere)

Flink needs POJOs to have a **public no-arg constructor** and public fields or getters/setters, or it falls back to slow generic serialization.

```java
public static class Event {
    public String value;
    public int count;
    public long timestamp;

    public Event() {}                                  // REQUIRED no-arg ctor
    public Event(String value, int count, long timestamp) {
        this.value = value; this.count = count; this.timestamp = timestamp;
    }
    public String getName()   { return value; }
    public int    getCount()  { return count; }
    public long   getTimestamp() { return timestamp; }

    @Override public String toString() {
        return "Event{value='" + value + "', count=" + count + ", timestamp=" + timestamp + "}";
    }
}
```

Keep a small set of demo POJOs like this — you'll reuse them in every later phase.

---

### ✅ Phase 1 checklist

- [x] Job skeleton + `env.execute()`
- [x] `map` / `filter` / `flatMap` + the `.returns()` rule
- [x] `keyBy` = per-key sub-streams
- [x] `Tuple2` + rolling `sum`
- [x] Event time vs processing time
- [x] Watermarks & out-of-order tolerance (concept)
- [x] Allowed lateness (concept)
- [x] Tumbling windows (concept + code)
- [x] The `Event` POJO pattern

➡️ Next: [Phase 2 — Core Flink APIs](02-core-flink-apis.md)
