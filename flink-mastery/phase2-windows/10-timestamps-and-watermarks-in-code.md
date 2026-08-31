# 10. Timestamps and Watermarks — in code

Phase 1 explained *what* a watermark is. This chapter is about the three lines of Java that actually create one, and how to read the output of a job so you can see the windows firing.

> **Key idea**
> A watermark is just a special record Flink injects into the stream that says
> *"I do not expect any more events with timestamp ≤ T."*
> Windows do not fire because time passed on your laptop. They fire because a watermark ≥ the window's end arrived.

---

## The shape of every event-time pipeline

```
source  →  assignTimestampsAndWatermarks(...)  →  keyBy(...)  →  window(...)  →  aggregate/process  →  sink
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
           put this as close to the source as you can
```

Everything upstream of `assignTimestampsAndWatermarks` has no notion of event time. Everything downstream does.

---

## The full runnable job

We reuse the `Event` POJO from Phase 1 (`userId : String`, `type : String`, `amount : double`, `timestamp : long`).

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class WatermarkDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> raw = env.fromElements(
                new Event("u1", "click",  1.0,  1000L),
                new Event("u1", "click",  2.0,  4000L),
                new Event("u1", "click",  3.0,  3000L),   // out of order
                new Event("u1", "click",  4.0,  9000L),
                new Event("u1", "click",  5.0, 12000L)
        );

        WatermarkStrategy<Event> strategy =
                WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, recordTimestamp) -> event.timestamp);

        DataStream<Event> withTime = raw.assignTimestampsAndWatermarks(strategy);

        withTime
                .keyBy(e -> e.userId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .sum("amount")
                .print();

        env.execute("watermark demo");
    }
}
```

### Line by line

```java
public class WatermarkDemo {
```
Java requires every top-level piece of code to live in a class. The file must be named `WatermarkDemo.java`.

```java
public static void main(String[] args) throws Exception {
```
The JVM entry point. `static` = belongs to the class, not to an instance, so it can be called without constructing anything. `throws Exception` = "I am not handling errors here, let them bubble up." `env.execute()` declares `throws Exception`, so main must too, otherwise the compiler rejects it.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```
The handle to the Flink runtime. `getExecutionEnvironment()` is a *static factory method*: run locally it returns a mini-cluster environment; submitted to a cluster it returns the cluster one. Same code, both places.

```java
env.setParallelism(1);
```
One subtask per operator. Do this while learning — with parallelism > 1 the print order is nondeterministic and watermarks get more complicated (see "Parallelism" below).

```java
DataStream<Event> raw = env.fromElements(...);
```
`DataStream<Event>` — the angle brackets are Java **generics**: "a DataStream whose elements are `Event`". `fromElements` is a test source that emits a fixed list, then ends.

```java
WatermarkStrategy<Event> strategy = WatermarkStrategy
        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
```
`WatermarkStrategy` is an interface with static factory methods. The `.<Event>` before the method name is an **explicit type witness** — you are telling the compiler "T is Event here". You need it because at this point in the chain there is no argument for Java to infer `T` from. If you write it as a separate variable with the type on the left (as above) you can often drop it; inside a long fluent chain you usually need it.

`forBoundedOutOfOrderness(Duration.ofSeconds(2))` = "events may arrive up to 2 seconds late relative to the newest event I have seen". The generated watermark is:

```
watermark = (max event timestamp seen so far) - 2000ms - 1ms
```

The extra `-1ms` exists because a watermark of `T` means "no more events with timestamp **≤ T**", and Flink wants an event exactly at `maxTs - 2000` to still be accepted.

```java
        .withTimestampAssigner((event, recordTimestamp) -> event.timestamp);
```
This tells Flink *where the timestamp lives inside your record*. Without it, Flink has no idea which field is time and will throw at runtime.

The `(a, b) -> expr` syntax is a **Java lambda** — an inline function. Here it implements `SerializableTimestampAssigner<Event>`, whose single method is `long extractTimestamp(Event element, long recordTimestamp)`.

**The two parameters:**
- `event` — your record.
- `recordTimestamp` — the timestamp the *source connector* already attached, or `Long.MIN_VALUE` if none. For Kafka this is the Kafka record timestamp. You usually ignore it and read your own field, but if you want to use the Kafka broker time you write `(event, recordTimestamp) -> recordTimestamp`.

**The return value must be milliseconds since the Unix epoch.** If your field is in seconds, multiply by 1000. This is the single most common beginner bug — a seconds-based timestamp puts every event in 1970 and windows appear never to fire.

```java
DataStream<Event> withTime = raw.assignTimestampsAndWatermarks(strategy);
```
Returns a *new* stream. `DataStream` is immutable — nothing is mutated in place; every operator returns a new stream.

```java
.keyBy(e -> e.userId)
```
Partitions by user. Another lambda, implementing `KeySelector<Event, String>`. Every window is per-key: `u1` and `u2` get separate windows.

```java
.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
```
Fixed, non-overlapping 5-second buckets, aligned to the epoch: `[0,5000)`, `[5000,10000)`, `[10000,15000)`, ...

> **Old tutorials will show** `TumblingEventTimeWindows.of(Time.seconds(5))` using `org.apache.flink.streaming.api.windowing.time.Time`. That class is **deprecated**. Use `java.time.Duration` — `Duration.ofSeconds(5)`, `Duration.ofMinutes(1)`, `Duration.ofMillis(500)`.

```java
.sum("amount")
```
Rolling sum of the field named `"amount"`, by reflection on the POJO. Output type is still `Event` — the first event of the window with its `amount` replaced by the sum. (Field-name aggregations are the reason Phase 1 insisted your POJO have a no-arg constructor and public fields.)

---

## Tracing exactly when windows fire

Windows: `[0,5000)`, `[5000,10000)`, `[10000,15000)`. Out-of-orderness = 2000ms.

```
event ts      max seen   watermark = max-2001    which window     what fires
--------      --------   --------------------    ------------     ----------
1000          1000       -1001                   [0,5000)         nothing
4000          4000        1999                   [0,5000)         nothing
3000          4000        1999                   [0,5000)         nothing   (late-ish but accepted:
                                                                   3000 > watermark 1999)
9000          9000        6999                   [5000,10000)     ** [0,5000) FIRES **
                                                                   (6999 >= 5000, its end)
12000         12000       9999                   [10000,15000)    ** [5000,10000) FIRES **
                                                                   (9999 >= 10000? NO — see below)
<end of stream>           Long.MAX_VALUE                          everything remaining fires
```

Careful with that second-to-last row: `9999 >= 10000` is **false**, so `[5000,10000)` does *not* fire on the 12000 event. It fires only when the stream ends.

### Console output

```
Event{userId='u1', type='click', amount=6.0, timestamp=1000}
Event{userId='u1', type='click', amount=4.0, timestamp=9000}
Event{userId='u1', type='click', amount=5.0, timestamp=12000}
```

Reading it:
- `6.0` = 1.0 + 2.0 + 3.0 — the three events in `[0,5000)`. It printed at the moment the 9000-event pushed the watermark to 6999.
- `4.0` = the lone event in `[5000,10000)`, flushed at end of stream.
- `5.0` = the lone event in `[10000,15000)`, flushed at end of stream.

> **Key idea**
> Nothing fires until a watermark **crosses the window end**. With a bounded source, Flink emits `Watermark(Long.MAX_VALUE)` when the source finishes, which flushes every open window. On an infinite Kafka stream that never happens — a window with no further data just sits there forever. This is why "my last window never printed" is such a common question.

---

## The four strategies you will actually use

### 1. `forBoundedOutOfOrderness(Duration)`

```java
WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                 .withTimestampAssigner((e, ts) -> e.timestamp);
```
The default choice. You pick the lateness bound. Bigger bound = fewer dropped events but every window result is delayed by that bound. This is the fundamental latency/completeness trade-off; there is no setting that avoids it.

### 2. `forMonotonousTimestamps()`

```java
WatermarkStrategy.<Event>forMonotonousTimestamps()
                 .withTimestampAssigner((e, ts) -> e.timestamp);
```
"Timestamps never go backwards." Watermark = `maxTs - 1`. Lowest possible latency. Only valid when the source truly is ordered — a single Kafka partition written by one producer in timestamp order, or a sorted file. If an out-of-order event slips in, it is late and dropped. Equivalent to `forBoundedOutOfOrderness(Duration.ZERO)`.

### 3. `noWatermarks()`

```java
WatermarkStrategy.<Event>noWatermarks()
                 .withTimestampAssigner((e, ts) -> e.timestamp);
```
Assigns timestamps but **never emits a watermark**. Event-time windows will never fire. Use it when downstream is processing-time only, or when a different operator supplies watermarks. If your event-time job produces nothing, check you didn't leave this in.

### 4. `withIdleness(Duration)`

```java
WatermarkStrategy
        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((e, ts) -> e.timestamp)
        .withIdleness(Duration.ofMinutes(1));
```

**The problem it solves.** A downstream operator's watermark is the **minimum** across all its input channels — it can only be as confident as its least-advanced input. If you read 3 Kafka partitions and partition 2 goes quiet, its watermark is frozen, so the minimum is frozen, so **no windows fire anywhere**, even though partitions 1 and 3 are streaming happily.

```
partition 0 ──▶ WM = 10:05:00 ┐
partition 1 ──▶ WM = 09:30:00 ├─▶ min = 09:30:00   ← everything stalls
partition 2 ──▶ WM = 10:05:00 ┘
```

`withIdleness(Duration.ofMinutes(1))` marks a source subtask idle after 1 minute of silence; idle channels are **excluded from the minimum**, so the watermark advances again. When data returns, the channel becomes active and rejoins.

Rule: if you have more Kafka partitions/subtasks than steady traffic, you need `withIdleness`. Otherwise a quiet partition silently freezes your job.

---

## Where to put the assignment

**Best: in the source.** Modern connectors take the strategy directly, so watermarks are generated per split and Flink can track per-partition watermarks correctly.

```java
KafkaSource<Event> source = KafkaSource.<Event>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("events")
        .setValueOnlyDeserializer(new EventDeserializer())
        .build();

DataStream<Event> stream = env.fromSource(
        source,
        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                         .withTimestampAssigner((e, ts) -> e.timestamp)
                         .withIdleness(Duration.ofMinutes(1)),
        "kafka-events");
```

`fromSource(source, watermarkStrategy, sourceName)` — three arguments, and the middle one is exactly the object you built above.

**Acceptable: immediately after the source**, via `assignTimestampsAndWatermarks`. Simple, and what every tutorial shows.

**Bad: after a `keyBy` or a shuffle.** Once records are redistributed, per-partition ordering is destroyed, so you need a much larger out-of-orderness bound for the same completeness.

**Bad: after a filter that drops most records.** If a key's events are filtered out, the watermark generator sees fewer timestamps and can stall.

> **Key idea**
> Assign timestamps and watermarks **as close to the source as possible**, before any shuffle. Every shuffle between the source and the assigner increases the out-of-orderness you must tolerate.

---

## Parallelism and watermarks

```
Parallelism 3, tumbling 5s windows

Source subtask 0 ─ WM 8000 ┐
Source subtask 1 ─ WM 6500 ├──▶ window operator's watermark = min = 6500
Source subtask 2 ─ WM 9000 ┘
```

Every operator's current watermark is the **minimum over its input channels**. Consequences:

- One slow subtask holds back the whole job. This is exactly what `withIdleness` fixes for the *empty* case; for the *slow* case you must fix the skew.
- Each parallel instance of a `WatermarkStrategy` computes its own max independently. Set parallelism to 1 while learning so the trace above is reproducible.

---

## Debugging: print the watermark

You cannot `print()` a watermark directly, but a `ProcessFunction` can read the current one.

```java
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

withTime.process(new ProcessFunction<Event, String>() {
    @Override
    public void processElement(Event e, Context ctx, Collector<String> out) {
        out.collect("event ts=" + e.timestamp
                  + "  currentWatermark=" + ctx.timerService().currentWatermark());
    }
}).print();
```

- `new ProcessFunction<Event, String>() { ... }` is an **anonymous inner class**: a class defined and instantiated in one expression. Used instead of a lambda because `ProcessFunction` is an abstract class with several methods, not a single-method interface.
- `@Override` tells the compiler "this must be overriding something" — a typo in the name then becomes a compile error instead of a silent no-op.
- `ctx.timerService().currentWatermark()` is the watermark the operator has received so far. Before the first watermark it is `Long.MIN_VALUE`.

Note that `currentWatermark()` is the watermark that arrived **before** this element, so you'll see it lagging by one — that's correct, not a bug.

---

## Remember

- Watermark = "no more events at or before T". It's a record in the stream, not a clock.
- `forBoundedOutOfOrderness(d)` → `watermark = maxSeenTs - d - 1ms`.
- `withTimestampAssigner` lambda takes `(event, recordTimestamp)` and must return **epoch millis**.
- A window fires when `watermark >= windowEnd`. Never before.
- Bounded sources emit `Watermark(Long.MAX_VALUE)` at the end, flushing all open windows. Unbounded sources do not.
- Downstream watermark = **min** over input channels. `withIdleness` removes silent channels from that min.
- Assign as close to the source as possible; `fromSource(source, strategy, name)` is the best place.
- `Time.seconds(5)` is deprecated. Use `Duration.ofSeconds(5)`.

**Interview one-liners**

- *"What is a watermark?"* → An in-band record asserting no further events with timestamp ≤ T will arrive; it drives event-time window firing and timers.
- *"How do you set out-of-orderness?"* → `WatermarkStrategy.forBoundedOutOfOrderness(Duration)` plus `withTimestampAssigner`. Larger bound = higher completeness, higher latency.
- *"Why did my job stop producing output?"* → An idle source partition froze the minimum watermark. Fix with `withIdleness`.
- *"forMonotonousTimestamps vs forBoundedOutOfOrderness?"* → Monotonous assumes ordered input, zero added latency, drops any out-of-order record. Bounded tolerates a fixed disorder at the cost of that much delay.
- *"Where should watermarks be assigned?"* → In the source (`fromSource`), before any shuffle. Every shuffle upstream of the assigner inflates required out-of-orderness.
- *"My last window never fires."* → No watermark ever passed its end. Bounded sources flush at EOF; unbounded ones need new data or an idleness setting.
