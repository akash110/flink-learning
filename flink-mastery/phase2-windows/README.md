# Phase 2 — Windows and Time

Everything about slicing an unbounded stream into finite pieces you can aggregate. All code is **Java**, Flink 1.18/1.20-era API. Every chapter has per-line explanations, ASCII timelines, and interview one-liners at the end.

**Assumes Phase 1:** setup, Java basics, `map`/`filter`/`flatMap`, `keyBy`, `Tuple2`, rolling `sum`/`max`/`maxBy`, event vs processing time, the watermark *concept*, and the shared `Event` POJO (`userId`, `type`, `amount`, `timestamp`). Every example here reuses that `Event`.

## How to use this

- **Reading straight through?** 10 → 17 in order. Chapters 13 and 16 are the two that matter most.
- **Just need windows working today?** Read 10, 11, and the capstone (17).
- **Job is OOMing?** Chapter 14 (sliding-window multiplication) then chapter 13 (buffering cost).
- **Numbers look wrong?** Chapter 16 (late data), then re-read the watermark arithmetic in chapter 10.
- **Interview in two days?** The one-liners at the end of 10, 12, 13, 16, plus the capstone trace.

## Table of contents

| # | Chapter | Key idea |
|---|---|---|
| 10 | [Timestamps and watermarks in code](10-timestamps-and-watermarks-in-code.md) | `watermark = maxTs - bound - 1`; a window fires only when a watermark crosses its end |
| 11 | [Tumbling windows](11-tumbling-windows.md) | Fixed buckets aligned to the **epoch**, `[start,end)`; `offset` shifts them into your timezone |
| 12 | [`sum`, `reduce`, `aggregate`](12-window-functions-sum-reduce-aggregate.md) | `reduce` forces one type so it can't average; `AggregateFunction<IN,ACC,OUT>` can |
| 13 | [**ProcessWindowFunction**](13-processwindowfunction.md) | **`aggregate(AggFn, PWF)` — O(1) state *and* window metadata. The most useful API in windowing.** |
| 14 | [Sliding and session windows](14-sliding-and-session-windows.md) | Sliding copies each element into `ceil(size/slide)` windows; sessions merge, which is the only place `merge()` runs |
| 15 | [Triggers and evictors](15-triggers-and-evictors.md) | The trigger decides *when* a window fires; `.trigger()` **replaces** the default, it doesn't add to it |
| 16 | [**Late events and side outputs**](16-late-events-and-side-outputs.md) | **Default is silent data loss. `allowedLateness` turns output into an update stream.** |
| 17 | [Phase 2 capstone](17-phase2-capstone.md) | One runnable job with the full trace: why every output line appeared when it did |

## The nine things that matter most

1. **Windows fire on watermarks, not on clocks.** `watermark >= windowEnd`, nothing else.
2. **`watermark = maxTsSeen - outOfOrderness - 1ms`.** Memorize this; every trace in Phase 2 depends on it.
3. **Timestamps must be epoch milliseconds.** A seconds-based field puts everything in 1970.
4. **Assign watermarks as close to the source as possible** — ideally in `fromSource(src, strategy, name)`, before any shuffle.
5. **`aggregate(AggregateFunction, ProcessWindowFunction)`** is the default choice. Its PWF's `IN` is the *aggregate's output type*, and the `Iterable` always has exactly one element.
6. **`ProcessWindowFunction` alone buffers every element** — cost is elements × keys × simultaneously open windows.
7. **`size/slide` is a multiplier on everything.** `size=1h, slide=1min` = 60 copies of every element.
8. **`allowedLateness` produces duplicate records per window.** The sink must upsert on `(key, windowStart)` or your numbers get worse, not better.
9. **Always set `sideOutputLateData`.** The default behaviour is silent drop with no log line.

## Quick reference

```java
// watermarks
WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                 .withTimestampAssigner((event, recordTimestamp) -> event.timestamp)
                 .withIdleness(Duration.ofMinutes(1));      // for quiet partitions

WatermarkStrategy.<Event>forMonotonousTimestamps()          // ordered sources only
WatermarkStrategy.<Event>noWatermarks()                     // event-time windows never fire

// assigners  (use java.time.Duration; Time.seconds() is deprecated)
TumblingEventTimeWindows.of(Duration.ofSeconds(10))
TumblingEventTimeWindows.of(Duration.ofDays(1), Duration.ofHours(-5).minus(Duration.ofMinutes(30)))
TumblingProcessingTimeWindows.of(Duration.ofSeconds(10))
SlidingEventTimeWindows.of(Duration.ofMinutes(10), Duration.ofMinutes(1))
EventTimeSessionWindows.withGap(Duration.ofSeconds(30))
GlobalWindows.create()                                       // needs a trigger

// window functions
.sum("amount") .max("amount") .maxBy("amount")
.reduce((a, b) -> ...)                                       // IN = ACC = OUT
.aggregate(new MyAgg())                                      // AggregateFunction<IN,ACC,OUT>
.aggregate(new MyAgg(), new MyPWF())                         // <-- the important one
.process(new MyPWF())                                        // buffers everything

// triggers
.trigger(ContinuousEventTimeTrigger.of(Duration.ofMinutes(5)))
.trigger(PurgingTrigger.of(CountTrigger.of(100)))

// late data
static final OutputTag<Event> LATE = new OutputTag<Event>("late") {};   // braces required
.allowedLateness(Duration.ofSeconds(30))
.sideOutputLateData(LATE)
result.getSideOutput(LATE)
```

## What's next

Phase 3 (`../phase3-state/`) covers keyed state, `KeyedProcessFunction`, your own timers, and state TTL — the tools for logic that windows can't express.
