# Watermarks & Event Time — Why Your Windows Never Fire

> Typical questions: *"Your windows stopped emitting results. Why?"*
> *"You're dropping 30% of events as late. What happened?"*
> *"One Kafka partition is idle and the whole job stalls. Explain."*

Along with checkpointing, this is where the "trigger condition" style questions live.
Your interviewer's phrase *"then trigger condition"* is exactly this topic.

---

## The core rule

> **A watermark of time T asserts: "no more events with timestamp < T will arrive."**
> An event-time window `[start, end)` fires when the watermark passes `end`.

And the rule that causes every production incident:

> **The watermark of an operator = the MINIMUM watermark across all its input channels.**

One slow, stuck, or idle input holds back the watermark for the *entire job*. Everything
downstream freezes. This single sentence answers most watermark interview questions.

```
partition 0 watermark: 10:00:00
partition 1 watermark: 10:00:05
partition 2 watermark: 09:15:00  ← 🔴 stuck / idle
────────────────────────────────
operator watermark:    09:15:00  ← whole job is 45 minutes behind
                                    windows for 09:15–10:00 never fire
```

---

## Failure 1: The idle partition (most common)

```java
// ❌ no idleness handling
WatermarkStrategy<Event> ws = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((e, ts) -> e.getEventTime());
```

You have 32 Kafka partitions, parallelism 32. At 3am traffic drops and partition 17 gets no
events. It emits no watermark. The job-wide watermark **freezes**. No windows fire. No
results. Nothing in the logs looks wrong — the job is "healthy," just silently producing
nothing.

```java
// ✅
WatermarkStrategy<Event> ws = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((e, ts) -> e.getEventTime())
    .withIdleness(Duration.ofMinutes(1));   // ← mark idle, exclude from the min()
```

**The trade-off to state:** once a source is marked idle and excluded, if it suddenly wakes
up its events may be **late** relative to the now-advanced watermark and get dropped. Idleness
trades completeness for liveness. Set the duration comfortably longer than your normal
quiet gaps.

**Also:** parallelism > partition count means some subtasks have *no* partition assigned at
all and are permanently idle. Without `withIdleness` those subtasks stall the job from the
moment it starts. Symptom: the job never emits anything, ever.

---

## Failure 2: Watermark generated in the wrong place

```java
// ❌ watermark assigned AFTER a shuffle
env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka")
   .keyBy(Event::getUserId)                        // shuffle — mixes all partitions
   .assignTimestampsAndWatermarks(strategy)        // 🔴 too late
```

After the shuffle, each subtask sees events from every partition interleaved. Per-partition
ordering is destroyed, so out-of-orderness looks far worse than it is and you must set a huge
bound.

```java
// ✅ assign at the source — Flink tracks watermarks PER KAFKA PARTITION
env.fromSource(
    kafkaSource,
    WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((e, ts) -> e.getEventTime())
        .withIdleness(Duration.ofMinutes(1)),
    "kafka");
```

Per-partition watermarking is a genuine advantage and a good thing to name: Kafka guarantees
order *within* a partition, so Flink can generate a tight watermark per partition and take
the min. Assigning after a shuffle throws that away.

---

## Failure 3: Out-of-orderness bound too small → mass drops

```java
// ❌ 5 seconds, but a mobile client buffers offline for 2 hours
.forBoundedOutOfOrderness(Duration.ofSeconds(5))
```

Everything older than `watermark - 5s` is late and dropped **silently by default**. You lose
30% of events and nothing tells you.

```java
// ✅ 1. capture late data instead of dropping it
final OutputTag<Event> LATE = new OutputTag<>("late"){};   // note the {} — anon subclass

SingleOutputStreamOperator<Result> out = events
    .keyBy(Event::getUserId)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .sideOutputLateData(LATE)
    .aggregate(new CountAgg());

out.getSideOutput(LATE).sinkTo(lateEventSink);   // audit, reprocess, or alert

// ✅ 2. allow a grace period — window re-fires with updated results
    .allowedLateness(Time.minutes(10))
```

**Gotcha on `allowedLateness`:** window state is retained for the full lateness period. 10
minutes of lateness on 1-minute windows means ~10x the window state, and each late event
triggers a **re-emission** — your sink must handle updates (upsert), or you get duplicate
rows. This connects directly to [[04-exactly-once]].

**Always monitor the drop metric:**
```
numLateRecordsDropped
```
If nobody alerts on this, silent data loss is inevitable.

**The tension to articulate:** larger out-of-orderness bound = more correctness, more
latency, more state. There is no right answer, only a choice matched to the SLA. Saying that
is better than picking a number.

---

## Failure 4: Processing time used by accident

```java
// ❌ non-deterministic — replays give different answers
.window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
```

Reprocessing history gives completely different windows than the original run. Breaks
backfills, breaks idempotency ([[04-exactly-once]]), breaks tests.

```java
// ✅
.window(TumblingEventTimeWindows.of(Time.minutes(1)))
```

Rule of thumb: **event time for correctness, processing time only for liveness** (timeouts,
heartbeats, "alert if nothing seen in 5 min").

---

## Failure 5: Watermark held back by a slow operator

Watermarks propagate through the DAG. A backpressured operator delays them just like data
([[02-backpressure]]). So:

> **Slow windows can be a backpressure symptom, not a watermark bug.**

Diagnostic order: check whether the *source* watermark is advancing. If yes but the window
operator's isn't, it's backpressure. If the source watermark is also stuck, it's idleness or
a timestamp problem.

```
Flink UI → operator → Watermarks tab → per-subtask "Low Watermark"
one subtask far behind = your stalled channel
```

---

## Bonus: timestamps that are wrong

```java
// ❌ silent disaster — seconds vs milliseconds
.withTimestampAssigner((e, ts) -> e.getEventTimeSeconds())   // 1_700_000_000
// Flink expects epoch MILLIS. This is interpreted as Jan 1970.
// Watermark is ~55 years in the past → NO window EVER fires.
```

```java
// ❌ the other direction — a bad client sends a timestamp in the year 2085
// watermark jumps to 2085 → every subsequent real event is "late" → everything dropped
```

One malformed record can permanently poison the watermark, because watermarks only ever move
forward. Defensive assignment:

```java
// ✅ clamp and reject implausible timestamps
.withTimestampAssigner((e, ts) -> {
    long t = e.getEventTime();
    long now = System.currentTimeMillis();
    if (t < now - Duration.ofDays(7).toMillis() || t > now + Duration.ofMinutes(5).toMillis()) {
        malformedCounter.inc();
        return now;           // or route to a side output and drop from the main path
    }
    return t;
});
```

This "one poisoned record kills the job forever" scenario is a great answer to
*"what's the worst production bug you can imagine in a streaming job?"*

---

## Quick reference

```
SYMPTOM                                  → CAUSE
Windows never fire at all                → parallelism > partitions, no withIdleness
                                           OR timestamps in seconds not millis
Windows stopped firing at 3am            → idle partition, no withIdleness
Windows fire but results are incomplete  → out-of-orderness bound too small
30% of events dropped as late            → bound too small; check numLateRecordsDropped
Everything dropped after a bad deploy    → poisoned watermark (future timestamp)
Windows lag but watermark advances       → backpressure, see [[02-backpressure]]
Backfill gives different results         → processing-time windows used somewhere
```

See also: [[04-exactly-once]], [[02-backpressure]], [[06-scale-arithmetic]]
