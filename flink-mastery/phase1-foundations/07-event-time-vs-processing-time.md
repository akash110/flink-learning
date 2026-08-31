# 7. Event Time vs Processing Time

## The three notions of time

Every record in a streaming system has at least two timestamps, and Flink names a third.

```
   [phone]              [Kafka]              [Flink]
      │                    │                    │
  12:00:00              12:00:07             12:00:09
  user taps           broker writes        operator reads
  "purchase"          the record           the record
      │                    │                    │
  EVENT TIME          INGESTION TIME     PROCESSING TIME
```

**Event time** — when the event actually happened, in the real world. It is a field *inside the record*, written by whatever produced it. It never changes, no matter how many times you reprocess.

**Ingestion time** — when the record entered the Flink pipeline (assigned at the source). Deterministic within one run, but different on every rerun.

**Processing time** — the system clock of the machine running the operator, read at the moment the record is handled. Different for every operator, every subtask, every run.

Modern Flink has **no global time characteristic setting**. The old `env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)` is removed/deprecated — since Flink 1.12, **event time is the default**, and you choose per-operator by picking the window assigner and by whether you supply a `WatermarkStrategy`.

```java
// event time — you assigned timestamps + watermarks (ch. 8)
stream.keyBy(...).window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))

// processing time — no timestamps needed, uses the wall clock
stream.keyBy(...).window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(5)))
```

---

## Why event time matters: reprocessing determinism

This is the argument that wins the debate.

Suppose it is Monday and you compute "purchases per 5-minute window" using **processing time**. It works. On Thursday you find a bug, fix it, and replay the last 3 days from Kafka.

Replaying 3 days of data takes 20 minutes. So under processing time, **all 3 days of events arrive within a 20-minute wall-clock span**, and they land in roughly 4 windows instead of 864.

```
ORIGINAL RUN (processing time)          REPLAY (processing time)
  ┌────┬────┬────┬────┬────┐              ┌──────────────────┐
  │ 12 │ 47 │ 33 │ 51 │ 29 │              │      2170        │
  └────┴────┴────┴────┴────┘              └──────────────────┘
  5-min windows over 3 days               everything in ~4 windows

SAME CODE. SAME DATA. COMPLETELY DIFFERENT ANSWER.
```

Under **event time**, the replay produces byte-identical output to the original run, because the windows are defined by timestamps inside the records, not by when the machine happened to see them.

> **Key idea:** Event time makes your results **a pure function of your data**. Processing time makes them a function of your data *and* your infrastructure's timing — throughput, backpressure, restarts, GC pauses, and how long the replay took.

Everything downstream depends on this: reproducible backfills, testable pipelines, results that survive a job restart, results that match a batch job over the same data.

### The second reason: correctness under delay

Real event streams are late and out of order. Mobile clients buffer offline. Kafka partitions are consumed at different speeds. A network hiccup delays one region by 30 seconds.

Processing time silently attributes those events to the wrong window. Event time attributes them to the window they belong in — and gives you an explicit mechanism (watermarks, chapter 8) to decide how long to wait.

---

## Out-of-order events: a concrete timeline

Events, labelled `E<event-time>`, arriving in this **processing order**:

```
arrival order →   E12:01   E12:03   E12:02   E12:07   E12:04   E12:06
                                      ▲                 ▲
                              out of order        very out of order
```

Window: 5-minute tumbling, boundaries at `[12:00, 12:05)` and `[12:05, 12:10)`.

**Under processing time** — assignment depends purely on arrival:

```
processing clock:  12:04  12:04  12:04  12:04  12:09  12:09
                    │      │      │      │      │      │
 window [12:00,12:05):  E12:01, E12:03, E12:02, E12:07     ← E12:07 is WRONG here
 window [12:05,12:10):  E12:04, E12:06                     ← E12:04 is WRONG here

 counts: 4 and 2.  Both windows contain events that don't belong.
```

**Under event time** — assignment depends on each record's own timestamp:

```
 window [12:00,12:05):  E12:01, E12:03, E12:02, E12:04      ← correct, 4 events
 window [12:05,12:10):  E12:07, E12:06                      ← correct, 2 events
```

The counts happen to be 4 and 2 either way in this toy example — but the *contents* are wrong under processing time, and with any aggregation more interesting than a count (sum, max, distinct users) the numbers diverge too.

The remaining question is: **when does Flink close the `[12:00, 12:05)` window?** It cannot close at the instant `E12:07` arrives, because `E12:04` is still in flight. Something has to say "I believe all events before 12:05 have now arrived". That something is a **watermark**, and it is the whole of chapter 8.

```
                      the window must stay open until
                      a watermark says "12:05 has passed"
                                  │
 events   E12:01  E12:03  E12:02  E12:07  E12:04  E12:06
                                                    │
 watermark  ~11:59 ~12:01 ~12:01  ~12:05  ~12:05  ~12:04*
                                                    ▲
                                   watermarks never go backwards,
                                   so this stays at 12:05
```

---

## Where the timestamp comes from

Event time only exists if your record carries a timestamp. This is a **data modelling requirement**, not a Flink feature:

```java
public class Event {
    private String userId;
    private String type;
    private double amount;
    private long timestamp;    // ← epoch millis. Event time lives here.
}
```

You extract it in a `WatermarkStrategy` (ch. 8):

```java
WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp());
//                          ^^^^^  ^^^^^^^^^^^^^^^
//                          your    the timestamp already attached by the source
//                          record  (Kafka record time), -1 if none
```

Two practical warnings:

- **Milliseconds, always.** Flink's entire time API is epoch millis as a `long`. A timestamp in seconds makes every event look like 1970 and every window empty.
- **Clock skew on producers is real.** If a mobile client's clock is 3 hours fast, that event's timestamp advances your watermark by 3 hours and drops everything legitimate that follows. Producer-side clocks are the most common source of "my windows fire and are empty" in production. Prefer server-assigned timestamps when you can.

---

## Ingestion time

Ingestion time = "the source stamps `System.currentTimeMillis()` on each record as it reads it".

```java
// ingestion time is just an event-time strategy where the timestamp is the read time
WatermarkStrategy.<Event>forMonotonousTimestamps()
    .withTimestampAssigner((e, ts) -> System.currentTimeMillis());
```

It is a middle ground:

| | Determinism on replay | Handles out-of-order sources | Cost |
|---|---|---|---|
| Event time | yes | yes | needs timestamps + watermarks |
| Ingestion time | no | partially (fixed once at the source) | free |
| Processing time | no | no | free |

Its one real advantage over processing time: the timestamp is assigned **once, at the source**, so all downstream operators agree on it and it does not drift with backpressure between operators. Its disadvantage versus event time is total: replays are still non-deterministic.

Flink no longer has a dedicated "ingestion time" mode. It is just event time with a trivial timestamp assigner. In practice: **rarely the right answer.** If your records genuinely have no timestamp, ingestion time is better than processing time; if they do, use it.

---

## When processing time is actually correct

Event time is not automatically right. Reach for processing time when:

**1. Latency matters more than correctness.**
A "requests in the last 10 seconds" dashboard, a rate limiter, a liveness monitor. You want an answer *now* even if a few late events are missed. Processing time windows fire the instant the clock passes the boundary — no waiting for a watermark, no watermark stall risk.

**2. The records genuinely have no meaningful event time.**
Sensor readings with no clock, a synthetic load generator, a stream of "current state" snapshots.

**3. You are measuring the system, not the data.**
"How many records did this operator process per minute of wall clock" is by definition a processing-time question.

**4. Timeouts and heartbeats.**
"Alert if no heartbeat for 60 seconds" is about the real passage of time on your side, not the sender's. A processing-time timer is the correct tool; an event-time timer would never fire, because with no incoming events the watermark never advances.

**5. Simplicity while learning or prototyping.**
Processing time has no watermark configuration and no stalls. Nothing wrong with using it in phase-1 exercises.

> **Key idea:** Ask "if I replay this data tomorrow, must I get the same answer?" Yes → event time. No, and I care about latency → processing time.

### The honest trade-off table

| Dimension | Event time | Processing time |
|---|---|---|
| Replay determinism | identical every time | different every time |
| Handles late/out-of-order data | yes, explicitly | no, silently wrong |
| Latency | waits for the watermark (+ out-of-orderness) | fires immediately |
| Failure behaviour | results unchanged after restart | records reprocessed land in later windows |
| Idle sources | can **stall** all progress (ch. 8) | unaffected — clock always moves |
| Configuration burden | timestamps, watermarks, lateness, idleness | none |
| Matches an equivalent batch job | yes | no |

The "idle sources stall progress" row is the one people forget. Event time is not free — an event-time job with one idle Kafka partition emits nothing at all until you configure `withIdleness`. That is chapter 8.

---

## Seeing it yourself

```java
package com.akash.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;

public class TimeComparison {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // (name, eventTimeMillis) — deliberately out of order
        DataStream<Tuple2<String, Long>> raw = env.fromElements(
            Tuple2.of("a", 1000L),
            Tuple2.of("b", 3000L),
            Tuple2.of("c", 2000L),    // late relative to b
            Tuple2.of("d", 7000L),
            Tuple2.of("e", 4000L),    // very late relative to d
            Tuple2.of("f", 6000L)
        ).returns(Types.TUPLE(Types.STRING, Types.LONG));

        DataStream<Tuple2<String, Long>> withTime = raw.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(
                    Duration.ofMillis(3000))              // tolerate 3s of disorder
                .withTimestampAssigner((t, ts) -> t.f1)   // read the event time from f1
        );

        // EVENT TIME: 5-second windows over the timestamps in the records
        withTime.keyBy(t -> "all")
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .apply((key, window, input, out) -> {
                    StringBuilder sb = new StringBuilder("EVENT  [" + window.getStart()
                                                          + "," + window.getEnd() + ") : ");
                    input.forEach(t -> sb.append(t.f0).append(" "));
                    out.collect(sb.toString());
                })
                .returns(Types.STRING)
                .print();

        // PROCESSING TIME: 5-second windows over the wall clock.
        // All 6 records arrive within microseconds, so they land in ONE window.
        withTime.keyBy(t -> "all")
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .apply((key, window, input, out) -> {
                    StringBuilder sb = new StringBuilder("PROC   : ");
                    input.forEach(t -> sb.append(t.f0).append(" "));
                    out.collect(sb.toString());
                })
                .returns(Types.STRING)
                .print();

        env.execute("time-comparison");
    }
}
```

Expected output:

```
EVENT  [0,5000)  : a c b e
EVENT  [5000,10000) : f d
PROC   : a b c d e f
```

The event-time windows split the records by their **own** timestamps, and `c` and `e` were placed correctly despite arriving out of order. The processing-time window swept everything into one bucket because it all arrived at effectively the same instant. Replay this with different arrival timing and only the `EVENT` lines stay the same.

(Full window API details are Phase 2; this is here so you can see the difference run.)

---

## Remember

- Three times: **event time** (in the record, immutable), **ingestion time** (stamped at the source), **processing time** (the operator's wall clock).
- **Event time is Flink's default since 1.12.** `setStreamTimeCharacteristic` / `TimeCharacteristic` are gone — you choose per-operator via the window assigner.
- The killer argument for event time is **replay determinism**: results become a pure function of the data, not of infrastructure timing.
- Processing time silently puts out-of-order events in the wrong window. Event time puts them in the right one — but must **wait** for a watermark to know when to close it.
- Timestamps are **epoch milliseconds, always**. Seconds-vs-millis and skewed producer clocks are the top two real-world event-time bugs.
- Ingestion time is event time with a trivial assigner. It fixes operator drift, not replay determinism. Rarely the right answer.
- **Processing time is correct** for latency-first dashboards, timeouts/heartbeats, system self-metrics, and data with no real timestamp.
- Event time is not free: an idle input partition **stalls the watermark and freezes all output** until you set `withIdleness`.

**Interview one-liners**

- *"Why event time over processing time?"* → Results become deterministic under replay and correct under out-of-order delivery; processing time makes output depend on throughput, restarts, and backpressure.
- *"What's the cost of event time?"* → Latency equal to your out-of-orderness bound, plus watermark configuration, plus the idle-source stall failure mode.
- *"When is processing time right?"* → Timeouts and heartbeats, latency-critical monitoring, system metrics, and streams with no meaningful embedded timestamp.
- *"How do you set event time in Flink 1.18+?"* → You don't set a global mode; supply a `WatermarkStrategy` with a timestamp assigner and use event-time window assigners. `TimeCharacteristic` is removed.
- *"What is ingestion time?"* → A timestamp assigned once at the source. Consistent across operators, but non-deterministic on replay — so it buys little over processing time and much less than event time.
