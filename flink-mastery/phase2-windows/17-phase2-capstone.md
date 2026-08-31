# 17. Phase 2 Capstone — Everything Together

One job. Out-of-order events, bounded-out-of-orderness watermarks, keyed 10-second tumbling event-time windows, incremental `aggregate()` combined with a `ProcessWindowFunction` for window metadata, allowed lateness, and a side output for data that arrives too late to save.

Read the code, then the trace, then run it.

---

## The three supporting classes

### 1. `Event` — from Phase 1, reproduced for reference only

```java
public class Event {
    public String userId;
    public String type;
    public double amount;
    public long timestamp;

    public Event() {}                      // required no-arg constructor for POJO serialization

    public Event(String userId, String type, double amount, long timestamp) {
        this.userId = userId;
        this.type = type;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" + userId + "," + type + "," + amount + "," + timestamp + "}";
    }
}
```

### 2. `WindowResult` — what we emit

```java
public class WindowResult {
    public String userId;
    public long windowStart;
    public long windowEnd;
    public long count;
    public double sum;
    public double avg;

    public WindowResult() {}

    public WindowResult(String userId, long windowStart, long windowEnd,
                        long count, double sum, double avg) {
        this.userId = userId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.count = count;
        this.sum = sum;
        this.avg = avg;
    }

    @Override
    public String toString() {
        return String.format("user=%s window=[%d,%d) count=%d sum=%.1f avg=%.2f",
                userId, windowStart, windowEnd, count, sum, avg);
    }
}
```

`%.1f` / `%.2f` are printf format specifiers: a floating-point value with 1 or 2 decimal places.

### 3. `CountSum` — the accumulator

```java
public class CountSum {
    public long count = 0L;
    public double sum = 0.0;

    public CountSum() {}
}
```

Two mutable public fields plus a no-arg constructor. That is all Flink needs to treat it as a POJO and use its efficient serializer instead of Kryo.

---

## The job

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Phase2Capstone {

    /**
     * The side-output tag for events that arrive too late to be folded back
     * into their window.
     *
     * The trailing "{}" makes this an ANONYMOUS SUBCLASS of OutputTag. It is
     * mandatory: Java erases generic type arguments at runtime, but a subclass
     * records its superclass's type arguments in the class file, which is how
     * Flink recovers "Event" and picks a serializer. Without the braces the job
     * fails at submission with InvalidTypesException.
     */
    static final OutputTag<Event> LATE_EVENTS = new OutputTag<Event>("late-events") {};

    public static void main(String[] args) throws Exception {

        // ---------------------------------------------------------------
        // 1. Environment. Parallelism 1 so the output order is deterministic
        //    and the trace below is reproducible.
        // ---------------------------------------------------------------
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ---------------------------------------------------------------
        // 2. Source. Deliberately out of order. The comment on each line is
        //    the tumbling window it belongs to (10s windows, epoch-aligned).
        // ---------------------------------------------------------------
        DataStream<Event> raw = env.fromElements(
            //           user   type      amount  timestamp
            new Event("u1", "click",  10.0,  1_000L),   // W[0,10000)
            new Event("u2", "click",  50.0,  2_000L),   // W[0,10000)
            new Event("u1", "click",  20.0,  8_000L),   // W[0,10000)
            new Event("u1", "click",  30.0,  5_000L),   // W[0,10000)  out of order, still on time
            new Event("u2", "click",  60.0, 13_000L),   // W[10000,20000)
            new Event("u1", "click",  40.0, 15_000L),   // W[10000,20000)  wm -> 12999, W1 FIRES
            new Event("u1", "click", 999.0,  6_000L),   // W[0,10000)  LATE, within lateness
            new Event("u1", "click",  70.0, 26_000L),   // W[20000,30000)  wm -> 23999, W2 FIRES
            new Event("u2", "click", 888.0,  4_000L)    // W[0,10000)  TOO LATE -> side output
        );

        // ---------------------------------------------------------------
        // 3. Timestamps + watermarks.
        //    forBoundedOutOfOrderness(3s) => watermark = maxSeenTs - 3000 - 1
        //    withTimestampAssigner's lambda receives (event, recordTimestamp);
        //    recordTimestamp is whatever the source attached (Long.MIN_VALUE
        //    for fromElements) and we ignore it, returning our own field.
        //    The return value MUST be epoch milliseconds.
        // ---------------------------------------------------------------
        WatermarkStrategy<Event> watermarks = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, recordTimestamp) -> event.timestamp);

        // ---------------------------------------------------------------
        // 4. The pipeline.
        // ---------------------------------------------------------------
        SingleOutputStreamOperator<WindowResult> results = raw
                // assign as close to the source as possible, before any shuffle
                .assignTimestampsAndWatermarks(watermarks)

                // partition by user: every window below is per-user
                .keyBy(event -> event.userId)

                // fixed 10s buckets aligned to the epoch:
                // [0,10000) [10000,20000) [20000,30000) ...
                // Duration, not the deprecated Time.seconds(10)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))

                // keep window state for 10s of watermark progress past the
                // window end. Late elements arriving in that grace period
                // RE-FIRE the window with an updated result.
                .allowedLateness(Duration.ofSeconds(10))

                // anything later than that goes here instead of being dropped
                .sideOutputLateData(LATE_EVENTS)

                // THE combined form:
                //   AggregateFunction<Event, CountSum, CountSum>   -- incremental, O(1) state
                //   ProcessWindowFunction<CountSum, WindowResult, String, TimeWindow>
                //                        ^^^^^^^^ its IN is the AGGREGATE'S OUTPUT
                .aggregate(new CountSumAgg(), new AttachWindowInfo());

        // ---------------------------------------------------------------
        // 5. Two sinks.
        // ---------------------------------------------------------------
        results.print("RESULT");
        results.getSideOutput(LATE_EVENTS).print("LATE  ");
        // getSideOutput must be called on the operator that DECLARED the tag,
        // not on some downstream stream.

        env.execute("Phase 2 Capstone: windows, lateness, side outputs");
    }

    // ===================================================================
    // Incremental aggregation. IN=Event, ACC=CountSum, OUT=CountSum.
    // Holds ONE CountSum per key per window, never the elements themselves.
    // ===================================================================
    public static class CountSumAgg
            implements AggregateFunction<Event, CountSum, CountSum> {

        @Override
        public CountSum createAccumulator() {
            // called once, when a window sees its first element
            return new CountSum();
        }

        @Override
        public CountSum add(Event value, CountSum acc) {
            // called for EVERY element. Mutating the accumulator in place is
            // safe and idiomatic here (unlike in a ReduceFunction, where the
            // arguments may be stream records).
            acc.count += 1;
            acc.sum += value.amount;
            return acc;
        }

        @Override
        public CountSum getResult(CountSum acc) {
            // called when the window FIRES -- including every re-firing
            // caused by a late element within allowedLateness.
            return acc;
        }

        @Override
        public CountSum merge(CountSum a, CountSum b) {
            // Called ONLY when two windows merge, i.e. session windows.
            // Never invoked for tumbling windows -- but implement it anyway
            // so the code keeps working if the assigner is ever changed.
            CountSum out = new CountSum();
            out.count = a.count + b.count;
            out.sum = a.sum + b.sum;
            return out;
        }
    }

    // ===================================================================
    // Adds window metadata to the aggregate result.
    //   IN  = CountSum      <- the AggregateFunction's OUT, NOT Event
    //   OUT = WindowResult
    //   KEY = String        <- the type returned by keyBy
    //   W   = TimeWindow
    // ===================================================================
    public static class AttachWindowInfo
            extends ProcessWindowFunction<CountSum, WindowResult, String, TimeWindow> {

        @Override
        public void process(String userId,
                            Context context,
                            Iterable<CountSum> elements,
                            Collector<WindowResult> out) {

            // In the combined form the Iterable ALWAYS holds exactly one
            // element: the single value returned by getResult(). Nothing was
            // buffered -- this is the whole point of the pattern.
            CountSum acc = elements.iterator().next();

            double avg = acc.count == 0 ? 0.0 : acc.sum / acc.count;

            out.collect(new WindowResult(
                    userId,
                    context.window().getStart(),   // inclusive
                    context.window().getEnd(),     // EXCLUSIVE
                    acc.count,
                    acc.sum,
                    avg));
        }
    }
}
```

---

## Exact expected console output

```
RESULT> user=u1 window=[0,10000) count=3 sum=60.0 avg=20.00
RESULT> user=u1 window=[0,10000) count=4 sum=1059.0 avg=264.75
RESULT> user=u2 window=[0,10000) count=1 sum=50.0 avg=50.00
LATE  > Event{u2,click,888.0,4000}
RESULT> user=u2 window=[10000,20000) count=1 sum=60.0 avg=60.00
RESULT> user=u1 window=[10000,20000) count=1 sum=40.0 avg=40.00
RESULT> user=u1 window=[20000,30000) count=1 sum=70.0 avg=70.00
```

(With parallelism 1 the relative order of the two keys within one firing is an implementation detail; the set of lines and their values are what matter.)

---

## Line-by-line walkthrough of why each line appeared when it did

First, the watermark after each input event. `forBoundedOutOfOrderness(3s)` ⇒ `watermark = maxTsSeen - 3000 - 1`.

```
#  event                     maxTs    watermark   window        what this triggers
-- ------------------------  -------  ----------  ------------  --------------------------------
1  u1 10.0 @1000               1000      -2001    u1 [0,10k)    buffer
2  u2 50.0 @2000               2000      -1001    u2 [0,10k)    buffer
3  u1 20.0 @8000               8000       4999    u1 [0,10k)    buffer
4  u1 30.0 @5000               8000       4999    u1 [0,10k)    buffer (out of order, on time:
                                                                 5000 > 4999)
5  u2 60.0 @13000             13000       9999    u2 [10k,20k)  buffer (9999 < 10000, no fire)
6  u1 40.0 @15000             15000      12999    u1 [10k,20k)  12999 >= 10000 →
                                                                 u1 [0,10k) FIRES  (line 1)
                                                                 u2 [0,10k) FIRES  (line 3)
7  u1 999.0 @6000             15000      12999    u1 [0,10k)    LATE (6000 <= 12999) but
                                                                 12999 <= 10000+10000 = 20000
                                                                 → within lateness
                                                                 → u1 [0,10k) RE-FIRES (line 2)
8  u1 70.0 @26000             26000      22999    u1 [20k,30k)  22999 >= 20000 →
                                                                 u1 [10k,20k) FIRES (line 6)
                                                                 u2 [10k,20k) FIRES (line 5)
                                                                 22999 > 20000 → [0,10k) state
                                                                 destroyed for both keys
9  u2 888.0 @4000             26000      22999    u2 [0,10k)    window gone → SIDE OUTPUT (line 4)
EOF                                  MAX_VALUE                  u1 [20k,30k) FIRES (line 7)
```

Now each output line:

**Line 1 — `u1 window=[0,10000) count=3 sum=60.0 avg=20.00`**
Events 1, 3, 4 (10.0 + 20.0 + 30.0 = 60.0, count 3). Event 4 arrived out of order at ts=5000 when the watermark was 4999 — `5000 > 4999`, so it was **not late** and was accepted normally. This is exactly what the 3-second out-of-orderness bound bought us. Fired at input #6, when the watermark reached 12999 ≥ the window end of 10000.

**Line 2 — `u1 window=[0,10000) count=4 sum=1059.0 avg=264.75`**
Input #7, `999.0 @6000`, is late: `6000 <= 12999`. But `allowedLateness(10s)` means u1's `[0,10000)` state is retained until the watermark exceeds `10000 + 10000 = 20000`. The watermark is 12999, so the state is still there. The element is folded into the retained accumulator and the window **fires again**: count 3→4, sum 60.0→1059.0.

> **Key idea**
> Two RESULT lines now exist for `u1 [0,10000)`. This is the update-stream consequence of `allowedLateness`. A sink that appends will report 60.0 + 1059.0 = 1119.0. A sink that upserts on `(userId, windowStart)` will report the correct 1059.0. **The sink must upsert.**

**Line 3 — `u2 window=[0,10000) count=1 sum=50.0 avg=50.00`**
Only event 2 fell in that window for u2. Fired at the same moment as line 1 — windows for different keys with the same bounds fire on the same watermark. Note that u2 has no `[20000,30000)` window at all; **Flink never creates empty windows**, so a silent key simply produces no output rather than a zero.

**Line 4 — `LATE > Event{u2,click,888.0,4000}`**
Input #9, `888.0 @4000` for u2. It arrives when the watermark is 22999. Its window `[0,10000)` was destroyed at input #8, because `22999 > 10000 + 10000`. There is no state left to fold it into, so it is routed to `LATE_EVENTS` instead of being silently dropped. Without `sideOutputLateData` this record would have vanished with no error and no log — only the `numLateRecordsDropped` metric would have moved.

**Line 5 — `u2 window=[10000,20000) count=1 sum=60.0 avg=60.00`**
Event 5. Fired at input #8 when the watermark hit 22999 ≥ 20000.

**Line 6 — `u1 window=[10000,20000) count=1 sum=40.0 avg=40.00`**
Event 6. Same firing moment as line 5.

**Line 7 — `u1 window=[20000,30000) count=1 sum=70.0 avg=70.00`**
Event 8. No further event ever pushes the watermark past 30000. This window fires only because `fromElements` is a **bounded** source: when it finishes, Flink emits `Watermark(Long.MAX_VALUE)`, which flushes every remaining open window.

> **Key idea**
> On an unbounded source like Kafka this last line would **never appear** until more data arrived. "My final window never printed" is almost always this, not a bug.

---

## What each Phase 2 chapter contributed

| chapter | what it contributed to this job |
|---|---|
| 10 | `WatermarkStrategy.forBoundedOutOfOrderness` + `withTimestampAssigner`, placed right after the source |
| 11 | `TumblingEventTimeWindows.of(Duration.ofSeconds(10))`, epoch-aligned `[start,end)` buckets |
| 12 | `AggregateFunction<Event, CountSum, CountSum>` — four methods, `merge` unused here |
| 13 | `aggregate(AggFn, ProcessWindowFunction)` — O(1) state **and** `context.window().getStart()/getEnd()` |
| 14 | (why we chose tumbling over sliding: no `size/slide` multiplication) |
| 15 | (the default `EventTimeTrigger` is doing the firing; we didn't need a custom one) |
| 16 | `allowedLateness` + `sideOutputLateData(OutputTag)` + the `{}` type-erasure trick |

---

## Exercises

Change one thing and predict the output before you run it.

1. Set `forBoundedOutOfOrderness` to `Duration.ZERO`. Which events become late? (Answer: event 4 `@5000` — at that point maxTs is 8000, watermark 7999, and `5000 <= 7999`. It now re-fires the window instead of being counted on time.)
2. Remove `.allowedLateness(...)`. What happens to input #7? (It goes straight to the side output; line 2 disappears.)
3. Remove `.sideOutputLateData(LATE_EVENTS)` and add `.allowedLateness(Duration.ZERO)`. How do you now know that two events were lost? (Only the `numLateRecordsDropped` metric. This is the default and it is dangerous.)
4. Replace the assigner with `SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5))`. How many windows does each event land in? (2 — `ceil(10/5)`. Expect roughly double the output lines, and windows with negative start values at the beginning.)
5. Replace `.aggregate(new CountSumAgg(), new AttachWindowInfo())` with `.process(new SomeProcessWindowFunction())` computing the same numbers. Same output, O(n) state instead of O(1).
6. Add `.trigger(ContinuousEventTimeTrigger.of(Duration.ofSeconds(5)))`. Now each window emits partial results as event time advances. Confirm the sink-upsert requirement is now unavoidable.

---

## Remember

- The canonical production shape of an event-time windowed job:
  ```
  fromSource(src, WatermarkStrategy..., "name")
      .keyBy(...)
      .window(TumblingEventTimeWindows.of(Duration...))
      .allowedLateness(Duration...)
      .sideOutputLateData(tag)
      .aggregate(AggregateFunction, ProcessWindowFunction)
  ```
- Watermarks as close to the source as possible.
- `aggregate(AggFn, PWF)` is the default choice: incremental state plus window bounds. The PWF's `IN` is the aggregate's `OUT`, and its `Iterable` holds exactly one element.
- `allowedLateness` makes the output an update stream. Upsert on `(key, windowStart)`.
- Always set `sideOutputLateData`. The default is silent loss.
- `new OutputTag<Event>("late") {}` — the braces are mandatory.
- Bounded sources flush all windows at EOF. Unbounded sources do not.

**Interview one-liners**

- *"Walk me through a production event-time windowing job."* → Watermarks at the source with a bounded out-of-orderness sized from measured p99 delay; keyBy; tumbling event-time window; incremental `aggregate` combined with a `ProcessWindowFunction` for the window bounds; allowed lateness to auto-correct; side output as the safety net; an upsert sink keyed on `(key, windowStart)`.
- *"Why aggregate + ProcessWindowFunction rather than one or the other?"* → `aggregate` alone gives O(1) state but no window identity; `ProcessWindowFunction` alone gives identity but buffers everything. Combined you get both.
- *"How do you size out-of-orderness?"* → From the measured distribution of `ingestTime - eventTime`. Pick around p99 and let allowed lateness plus the side output handle the tail; every added second is added latency on every window.
- *"How do you guarantee no data loss?"* → Side-output the late data and either reprocess it in batch or feed a correction job; never rely on the default drop.
