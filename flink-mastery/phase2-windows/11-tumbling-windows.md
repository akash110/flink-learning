# 11. Tumbling Windows

The simplest window: fixed size, no overlap, every element lands in exactly one.

> **Key idea**
> A tumbling window of size `S` chops the timeline into `[0,S)`, `[S,2S)`, `[2S,3S)` … aligned to the **Unix epoch**, not to when your job started. `windowStart = timestamp - (timestamp % S)`.

---

## The picture

5-second tumbling windows, events shown by their event timestamp:

```
timestamp (ms)
0        5000      10000     15000     20000
|---------|---------|---------|---------|
|   W1    |   W2    |   W3    |   W4    |
|---------|---------|---------|---------|
  a  b c     d          e f      g
  ^                                ^
  1200,2500,4900       11000,13400  17000
              6500
```

- `W1 = [0, 5000)` contains a, b, c
- `W2 = [5000, 10000)` contains d
- `W3 = [10000, 15000)` contains e, f
- `W4 = [15000, 20000)` contains g

**Start inclusive, end exclusive.** An event with timestamp exactly `5000` belongs to `W2`, not `W1`. This off-by-one bites everybody once.

---

## Event-time tumbling

```java
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import java.time.Duration;

stream
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
    .sum("amount")
    .print();
```

- `TumblingEventTimeWindows.of(...)` — a static factory returning a `WindowAssigner`. The assigner's only job is: given an element and its timestamp, decide which window(s) it goes in.
- `.window(...)` is only available on a `KeyedStream` (the result of `keyBy`). On a plain `DataStream` you must use `.windowAll(...)` — see below.
- Requires a `WatermarkStrategy` upstream, or nothing will ever fire.

> Old code you will see: `TumblingEventTimeWindows.of(Time.seconds(5))`. `org.apache.flink.streaming.api.windowing.time.Time` is deprecated in recent Flink; use `java.time.Duration`.

## Processing-time tumbling

```java
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

stream
    .keyBy(e -> e.userId)
    .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
    .sum("amount")
    .print();
```

Buckets by **wall-clock time on the TaskManager**, ignoring `event.timestamp` entirely.

| | Event time | Processing time |
|---|---|---|
| Bucketed by | timestamp in the record | machine clock |
| Needs watermarks | yes | no |
| Fires when | watermark ≥ window end | system clock ≥ window end |
| Reprocessing gives | identical results | different results every run |
| Late data | a defined concept | does not exist (everything is "on time") |
| Latency | out-of-orderness bound | near zero |

Use processing time for monitoring/heartbeats where "roughly now" is fine. Use event time for anything whose result must be correct and reproducible.

---

## Epoch alignment and the `offset` parameter

`TumblingEventTimeWindows.of(Duration.ofHours(1))` gives hourly windows aligned to **UTC hour boundaries**: 00:00, 01:00, 02:00 … because alignment is to epoch 0, which is 1970-01-01T00:00:00Z.

If your business day is IST (UTC+5:30), your "daily" window would run 00:00 UTC → 05:30 IST. Wrong. Fix it with the two-argument overload:

```java
// Daily windows aligned to India Standard Time (UTC+5:30)
TumblingEventTimeWindows.of(
        Duration.ofDays(1),
        Duration.ofHours(-5).minusMinutes(-30));   // careful, see below
```

The arithmetic is easier read plainly:

```java
Duration size   = Duration.ofDays(1);
Duration offset = Duration.ofHours(-5).minus(Duration.ofMinutes(30));  // -5h30m
// windows now run [00:00 IST, 24:00 IST)
```

The formula the assigner uses:

```
windowStart = timestamp - ((timestamp - offset) % size + size) % size
```

The doubled `% size + size) % size` is Java's idiom for a **non-negative modulo**, because Java's `%` returns a negative result for negative operands (`-7 % 5 == -2`, not `3`).

**Rule of thumb:** to shift windows into a timezone `UTC+X`, pass `offset = -X`. UTC+5:30 → offset `-5h30m`. UTC-8 → offset `+8h`.

A second, unrelated use of offset: **de-synchronizing** windows across jobs so they don't all fire in the same millisecond and spike your sink.

```java
TumblingEventTimeWindows.of(Duration.ofMinutes(1), Duration.ofSeconds(17));
// windows are [00:17, 01:17), [01:17, 02:17), ...
```

---

## A complete traced example

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class TumblingTrace {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> events = env.fromElements(
                new Event("u1", "click", 1.0,  1000L),
                new Event("u2", "click", 5.0,  2000L),
                new Event("u1", "click", 2.0,  4999L),
                new Event("u1", "click", 3.0,  5000L),   // exactly on the boundary
                new Event("u2", "click", 7.0,  8000L),
                new Event("u1", "click", 4.0, 11000L)
        );

        events
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                                 .withTimestampAssigner((e, ts) -> e.timestamp))
            .keyBy(e -> e.userId)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
            .sum("amount")
            .map(e -> Tuple2.of(e.userId, e.amount))
            .print();

        env.execute("tumbling trace");
    }
}
```

`forMonotonousTimestamps()` is safe here because the input list is already in timestamp order. `Tuple2.of(a, b)` builds Flink's built-in 2-field tuple so the printout is short; the fields are `f0` and `f1`.

### Which window does each event land in?

```
event            ts      ts % 5000    window
--------------   -----   ----------   ----------------
u1 amount=1.0     1000     1000       u1 [    0,  5000)
u2 amount=5.0     2000     2000       u2 [    0,  5000)
u1 amount=2.0     4999     4999       u1 [    0,  5000)
u1 amount=3.0     5000        0       u1 [ 5000, 10000)   <- boundary is exclusive at the top
u2 amount=7.0     8000     3000       u2 [ 5000, 10000)
u1 amount=4.0    11000     1000       u1 [10000, 15000)
```

### When does each window fire?

`forMonotonousTimestamps` → `watermark = maxTs - 1`.

```
after event      watermark    windows with end <= watermark   fires
-------------    ---------    -----------------------------   ----------------------
ts=1000              999      none                            —
ts=2000             1999      none                            —
ts=4999             4998      none                            —
ts=5000             4999      none  (4999 < 5000)              —
ts=8000             7999      [0,5000)                        u1 -> 3.0,  u2 -> 5.0
ts=11000           10999      [5000,10000)                    u1 -> 3.0,  u2 -> 7.0
<stream end>   MAX_VALUE      [10000,15000)                   u1 -> 4.0
```

### Console output

```
(u1,3.0)
(u2,5.0)
(u1,3.0)
(u2,7.0)
(u1,4.0)
```

Walk through it:
1. `(u1,3.0)` — window `[0,5000)` for u1: 1.0 + 2.0. Fired when the 8000-event pushed the watermark to 7999.
2. `(u2,5.0)` — window `[0,5000)` for u2: the single 5.0 event. Same firing moment; per-key windows with the same bounds fire together.
3. `(u1,3.0)` — window `[5000,10000)` for u1: the boundary event, 3.0 alone. Fired at watermark 10999. Same value as line 1 by coincidence.
4. `(u2,7.0)` — window `[5000,10000)` for u2. Same moment.
5. `(u1,4.0)` — window `[10000,15000)`, flushed only by the end-of-stream `Watermark(Long.MAX_VALUE)`.

Note that nothing for u2 appears after that: u2 has no events past 8000, so u2 has no third window at all. **Flink does not create empty windows.** A window object exists only once at least one element is assigned to it. This is why "count of users active per window" never reports zero for a silent user — there is no window to report from.

---

## `windowAll()` — and why it kills parallelism

If you want a global aggregate with no key:

```java
events
    .assignTimestampsAndWatermarks(strategy)
    .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
    .sum("amount")
    .print();
```

`windowAll` is available on a plain `DataStream` and produces an `AllWindowedStream`. It works — and it runs at **parallelism 1**, always.

```
   parallelism 4 upstream               parallelism 1
 ┌────────┐
 │ src 0  │──┐
 ├────────┤  │
 │ src 1  │──┤        ┌───────────────────────┐
 ├────────┤  ├───────▶│  windowAll  (ONE task)│──▶ sink
 │ src 2  │──┤        └───────────────────────┘
 ├────────┤  │            every record in the
 │ src 3  │──┘            entire job funnels here
 └────────┘
```

Every record from every subtask is shipped to one machine. At high volume that single task is the bottleneck and eventually backpressures the whole job.

**The fix — two-phase aggregation.** Pre-aggregate on a synthetic key in parallel, then combine the (much smaller) partials:

```java
events
    .keyBy(e -> e.userId.hashCode() % 16)          // 16 parallel buckets
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
    .sum("amount")                                  // 16 partial sums per window
    .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
    .sum("amount")                                  // combine: tiny volume
    .print();
```

The second `windowAll` is still parallelism 1, but now it only sees 16 records per window instead of millions. Because tumbling windows are epoch-aligned and the partial results carry the window's own timestamp, the second stage's windows line up exactly with the first stage's.

Rule: `windowAll` is fine for low-volume streams (alerts, heartbeats, already-aggregated data). For anything high-volume, pre-aggregate first.

---

## Common mistakes

**Timestamps in seconds.** `1724900000` (seconds) interpreted as millis is 1970-01-20. Windows will appear to work but bucket everything absurdly. Multiply by 1000.

**No watermark strategy.** With `TumblingEventTimeWindows` and no strategy, the job runs and prints nothing. There is no error.

**Expecting a window per wall-clock interval.** With event time, if no data arrives for an hour, no windows are created for that hour and nothing fires.

**Assuming window start = job start.** It's epoch-aligned. Start a 1-hour job at 10:47 and your first window is `[10:00, 11:00)`, containing only 13 minutes of data.

---

## Remember

- Tumbling = fixed size, no overlap, one element → exactly one window.
- Aligned to epoch: `start = ts - (ts % size)`. Use the `offset` overload for timezones (`offset = -X` for UTC+X).
- `[start, end)` — start inclusive, end **exclusive**.
- Event-time windows fire on `watermark >= end`; processing-time windows fire on system clock ≥ end.
- Empty windows are never created.
- `.window()` needs a `KeyedStream`; `.windowAll()` works on `DataStream` but is parallelism 1 forever.

**Interview one-liners**

- *"What is a tumbling window?"* → Fixed-size, non-overlapping; every record belongs to exactly one.
- *"What are the window boundaries?"* → Aligned to the Unix epoch, `[start, start+size)`, start inclusive, end exclusive.
- *"How do you get windows aligned to a local timezone?"* → The `offset` argument on the assigner; for UTC+X pass `-X`.
- *"Event time vs processing time tumbling?"* → Event time is deterministic and reprocessable but requires watermarks and pays their latency; processing time is cheap and non-deterministic.
- *"Why is windowAll slow?"* → It forces parallelism 1. Pre-aggregate on a bucketed key, then combine.
- *"Does Flink emit a result for a window with no data?"* → No. The window object is only created when an element is assigned to it.
