# 8. Watermarks

## The problem watermarks solve

You are computing a 5-minute event-time window `[12:00, 12:05)`. Events arrive out of order. At some point you must **emit the result**, which means deciding "no more events for this window will arrive".

On an infinite stream there is no end-of-input to tell you. You need something in the stream itself that says *how far along in event time we are*. That thing is a watermark.

> **Definition:** A watermark with timestamp `T` is an assertion flowing through the stream that says **"no further events with timestamp < T will arrive."**

That is the whole concept. Everything else is consequences.

```
 stream (left = older):

  ... E12:03   E12:01   W(12:00)   E12:04   E12:02   W(12:03)   E12:07 ...
                          ▲                             ▲
                    "nothing < 12:00                "nothing < 12:03
                     is still coming"                is still coming"
```

Watermarks are **special records** interleaved with your data. They are not visible to `map` or `filter`; they are consumed by operators that care about event time — windows, event-time timers, event-time joins.

When `W(12:05)` reaches the window operator, the window `[12:00, 12:05)` fires. Not before.

---

## The trade-off, stated once

The watermark is a **bet you make about how out-of-order your stream is**.

```
   aggressive watermark              conservative watermark
   (small out-of-orderness)          (large out-of-orderness)
        │                                     │
   low latency                          high latency
   more late/dropped events             fewer late events
```

There is no correct value. You pick a bound, you accept the events beyond it are late, and you handle lateness separately. Anyone who tells you watermarks give you both low latency and zero data loss is selling something.

---

## `WatermarkStrategy`

The modern API (Flink 1.11+, and the only one you should learn):

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import java.time.Duration;

DataStream<Event> timed = raw.assignTimestampsAndWatermarks(
    WatermarkStrategy
        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
);
```

Line by line:

**`WatermarkStrategy.<Event>forBoundedOutOfOrderness(...)`**
A `static` factory. `<Event>` is an **explicit type witness** — Java cannot infer the type parameter here because there is no argument of type `Event`, so you spell it out before the method name. Unusual syntax; you will only ever see it in this spot.

**`Duration.ofSeconds(5)`**
The out-of-orderness bound. The generated watermark is `maxTimestampSeenSoFar - 5s - 1ms`. You are asserting: an event is never more than 5 seconds behind the newest event you have seen.

**`.withTimestampAssigner((event, recordTimestamp) -> ...)`**
A `SerializableTimestampAssigner<Event>` — a functional interface, hence the lambda. It must return **epoch milliseconds**.
- `event` — your record.
- `recordTimestamp` — the timestamp the source already attached (e.g. the Kafka record timestamp), or `Long.MIN_VALUE` if none. Usually ignored; occasionally you want `return recordTimestamp;` to use Kafka's own time.

> **Note the deprecated API you may see in old blog posts:** `assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<>(Time.seconds(5)))` with `AssignerWithPeriodicWatermarks` / `AssignerWithPunctuatedWatermarks`. Those are removed. `WatermarkStrategy` replaced both, and it unified periodic and punctuated generation behind one interface.

### The built-in strategies

**`forBoundedOutOfOrderness(Duration)`** — the one you will use 95% of the time.

```java
WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
```

```
 max ts seen:  12:00:03   12:00:09   12:00:07   12:00:12
 watermark  :  11:59:58   12:00:04   12:00:04   12:00:07
                                        ▲
                          did NOT go backwards even though
                          the event's timestamp did
```

**`forMonotonousTimestamps()`** — asserts events arrive in strictly non-decreasing timestamp order; watermark = `maxTimestamp - 1ms`.

```java
WatermarkStrategy.<Event>forMonotonousTimestamps()
    .withTimestampAssigner((e, ts) -> e.getTimestamp());
```

Equivalent to `forBoundedOutOfOrderness(Duration.ZERO)`. Correct when a single Kafka partition is written by a single ordered producer and you read at parallelism matching the partitions. **If your assumption is wrong, every out-of-order event is immediately late.** Do not use it optimistically.

**`noWatermarks()`** — timestamps assigned, no watermarks generated. Event-time windows never fire. Use only when downstream is entirely processing-time.

**`forGenerator(...)`** — supply a custom `WatermarkGenerator` when you need punctuated watermarks (e.g. the producer emits explicit "end of batch" markers). Rare.

### Where to put it

```java
// preferred: on the source itself — Kafka's per-partition watermarking
env.fromSource(kafkaSource, watermarkStrategy, "kafka-source");

// alternative: as a separate operator right after the source
raw.assignTimestampsAndWatermarks(watermarkStrategy);
```

**Assign as early as possible.** Attaching the strategy to the source lets Flink track a watermark **per Kafka partition** and take the minimum, which is dramatically more accurate than a single watermark over an already-merged stream. If you assign after a `keyBy` or a `union`, the partitions are already interleaved and the per-partition information is lost.

Never assign twice — the second strategy overwrites the first and you lose the per-partition tracking.

---

## How watermarks propagate: the min-across-inputs rule

An operator with multiple input channels (which is every operator downstream of a shuffle) tracks a watermark **per input channel** and sets its own current watermark to the **minimum** across them.

```
    upstream subtask 0  ──W(12:05)──┐
    upstream subtask 1  ──W(12:03)──┼──►  operator
    upstream subtask 2  ──W(12:07)──┘     current watermark = min = 12:03
                                          │
                                          └──► emits W(12:03) downstream
```

The rule is forced by the definition. The operator can only claim "nothing earlier than T is coming" if **every** input has made that claim. Channel 1 is still only at 12:03, so an 12:04 event could still arrive from it.

Three consequences:

1. **The slowest input sets the pace for the entire job.** One lagging Kafka partition holds back every window in the pipeline.
2. **Watermarks are monotonic.** An operator never lowers its watermark; a watermark arriving with a smaller timestamp than the current one is ignored.
3. **Watermark lag is your key event-time health metric.** `currentProcessingTime - currentOutputWatermark`, visible in the Web UI and as the `currentOutputWatermark` metric per operator. Growing lag = something upstream is stuck.

### The idle-partition stall

This is the failure mode that catches everyone.

```
Kafka topic, 3 partitions:
  partition 0  ──►  busy, events flowing,  watermark 12:05
  partition 1  ──►  busy, events flowing,  watermark 12:06
  partition 2  ──►  NO TRAFFIC (a region with no night-time activity)
                    watermark stuck at 09:00

  downstream watermark = min(12:05, 12:06, 09:00) = 09:00
```

**Result: nothing fires. No windows close. No output at all.** Meanwhile records pile up in window state and memory grows. From the outside the job looks healthy — it is consuming, checkpointing, and not erroring. It is simply emitting nothing.

The symptom to recognize: *"my job runs fine but produces no output, and state keeps growing."* Check the `currentOutputWatermark` metric per subtask; one will be far behind.

### `withIdleness` — the fix

```java
WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((e, ts) -> e.getTimestamp())
    .withIdleness(Duration.ofMinutes(1));      // ← the fix
```

If a partition/subtask emits nothing for 1 minute, it is marked **idle** and **excluded from the min computation**. The remaining active channels drive the watermark forward.

```
  partition 0  ──►  12:05   active
  partition 1  ──►  12:06   active
  partition 2  ──►  IDLE    excluded
                    ▼
  downstream watermark = min(12:05, 12:06) = 12:05   ✓ windows fire again
```

When the idle partition produces a record again, it is immediately marked active and rejoins the minimum.

**The cost is real:** while a partition is idle-excluded, the watermark can advance past events that partition would later have produced. Those events arrive late. `withIdleness` trades a little correctness for liveness. Set the duration comfortably longer than your normal inter-record gap — too short and you will idle out a merely-slow partition.

> **Key idea:** Watermarks propagate as the **minimum across all inputs**, so the slowest or most idle source dictates the progress of the entire job. `withIdleness` is not optional in any real multi-partition deployment.

Also relevant: `env.setParallelism()` higher than your Kafka partition count leaves some source subtasks with **no partition assigned at all** — permanently idle, permanently stalling the watermark, unless `withIdleness` is set. This is a very common first production incident.

---

## Late events and allowed lateness

An event is **late** when its timestamp is below the current watermark at the moment it arrives.

```
 watermark now at 12:05
        │
   E12:03 arrives  →  LATE.  Its window [12:00,12:05) has already fired.
```

Default behaviour: **late events are silently dropped.** No log, no metric increment you would notice, no error. Correct by the definition of a watermark, and terrible to discover in production.

Three levers, in order of increasing cost:

**1. Increase the out-of-orderness bound.** Fewer events are late, at the price of every window firing later. This is the blunt instrument.

**2. Allowed lateness** — keep the window state alive after firing, and re-fire on each late arrival.

```java
stream.keyBy(Event::getUserId)
      .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
      .allowedLateness(Duration.ofMinutes(1))     // keep state 1 min past the watermark
      .sum("amount");
```

Semantics: the window fires normally at watermark ≥ window end; state is retained until `watermark > windowEnd + allowedLateness`; each late event within that grace period triggers an **additional, updated emission**. Your sink must therefore be able to handle updates (upsert), not just appends. State cost is proportional to the lateness you allow.

**3. Side output for late data** — capture the stragglers instead of dropping them.

```java
OutputTag<Event> lateTag = new OutputTag<Event>("late") {};   // note the {} — anonymous
                                                              // subclass, so the generic
                                                              // type survives erasure

SingleOutputStreamOperator<Result> main = stream
    .keyBy(Event::getUserId)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
    .sideOutputLateData(lateTag)
    .sum("amount");

DataStream<Event> late = main.getSideOutput(lateTag);
late.print("LATE");    // log them, alert on them, write them to a repair table
```

`new OutputTag<Event>("late") {}` — the trailing `{}` creates an anonymous subclass. Without it, type erasure loses `Event` and Flink throws at job build. This is the same erasure story as chapter 2, in a different disguise.

**The production default:** a modest out-of-orderness bound, a short allowed lateness, **and always a side output** so you can measure how much you are actually losing. Full window code is Phase 2 — for now, know these three levers exist and what each costs.

---

## Putting the strategy together

```java
package com.akash.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.akash.flink.model.Event;

import java.time.Duration;

public class WatermarkSetup {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> raw = env.fromElements(
            new Event("u1", "purchase", 10.0, 1000L),
            new Event("u1", "purchase", 20.0, 3000L),
            new Event("u2", "view",      0.0, 2000L),   // out of order
            new Event("u1", "purchase", 30.0, 9000L),
            new Event("u2", "purchase", 15.0, 6000L)    // out of order
        );

        // The strategy you will copy into almost every job you write.
        WatermarkStrategy<Event> strategy = WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
            .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
            .withIdleness(Duration.ofSeconds(30));

        DataStream<Event> timed = raw.assignTimestampsAndWatermarks(strategy)
                                     .name("assign-watermarks")
                                     .uid("assign-watermarks");

        timed.print();
        env.execute("watermark-setup");
    }
}
```

Trace the watermark as records flow (out-of-orderness = 2000 ms, so `WM = maxSeen - 2000 - 1`):

| record ts | max seen | watermark emitted |
|---|---|---|
| 1000 | 1000 | -1001 |
| 3000 | 3000 | 999 |
| 2000 | 3000 | 999 (unchanged — max did not move) |
| 9000 | 9000 | 6999 |
| 6000 | 9000 | 6999 |

Notice the last row: the `6000` event has a timestamp **below** the current watermark of 6999. It is **late**. There is no window here to drop it, so it flows on — but in a windowed job it would have been silently discarded.

That table is the mental model. Rerun it in your head for your own out-of-orderness values before you pick one.

### Watermark generation is periodic

Watermarks are not emitted on every record. Flink emits them on a timer, default **200 ms**:

```java
env.getConfig().setAutoWatermarkInterval(200);   // milliseconds; 0 disables
```

Lower = fresher watermarks, lower window latency, more watermark records in the stream. Higher = less overhead, more latency. The 200 ms default is fine until you are chasing single-digit-millisecond latency.

This is why watermark advancement is slightly lumpy, and why a 1-second window may fire up to ~200 ms after its true boundary.

---

## Remember

- **A watermark `T` asserts: no more events with timestamp `< T` will arrive.** It is a record in the stream, not a clock.
- Watermarks are what **trigger event-time windows and event-time timers**. Nothing fires without them.
- `WatermarkStrategy.forBoundedOutOfOrderness(Duration)` → `WM = maxTimestampSeen - bound - 1ms`. This is your default.
- `forMonotonousTimestamps()` = zero tolerance for disorder. Only when you can prove ordering.
- **`.withTimestampAssigner(...)` returns epoch milliseconds.** Seconds is the classic bug.
- **Assign watermarks at the source**, so Flink tracks them **per Kafka partition**. Assigning later loses that.
- **Propagation = minimum across all input channels.** The slowest input governs the whole job. Watermarks never move backwards.
- **An idle partition stalls everything** — no output, growing state, and the job looks healthy. Fix with **`.withIdleness(Duration)`**. Also required when parallelism > partition count.
- Events below the current watermark are **late and silently dropped by default**. Levers: bigger bound, `allowedLateness` (re-fires, needs an upsert sink), and `sideOutputLateData` (measure it).
- `new OutputTag<Event>("late") {}` — the trailing `{}` is mandatory, for the same type-erasure reason as chapter 2.
- Watermarks are generated periodically, default every 200 ms (`setAutoWatermarkInterval`).

**Interview one-liners**

- *"What is a watermark?"* → An in-stream assertion that no events with a timestamp below `T` will arrive, used to trigger event-time windows and timers.
- *"How does an operator with multiple inputs compute its watermark?"* → The minimum across all input channels, because it can only make the assertion every input has already made.
- *"My event-time job produces no output. Where do you look?"* → The `currentOutputWatermark` metric per subtask. Almost always an idle or empty source partition holding the minimum down — fix with `withIdleness`, and check parallelism against partition count.
- *"What happens to late events?"* → Dropped by default. `allowedLateness` keeps the window state alive and re-fires; `sideOutputLateData` routes them to a separate stream so you can measure the loss.
- *"How do you pick the out-of-orderness bound?"* → Measure the observed distribution of `arrivalTime - eventTime` in production and pick a high percentile; it is a latency-versus-completeness decision, not a correctness one.
- *"Why assign watermarks at the source rather than later?"* → Per-partition watermark tracking. After a merge you only have the interleaved stream and the watermark becomes needlessly conservative or wrong.
