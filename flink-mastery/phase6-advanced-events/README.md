# Phase 6 — Advanced Event Processing

Phases 1–5 gave you the building blocks: DataStream basics, event time and watermarks, windows, keyed state, `KeyedProcessFunction` with timers, Kafka, parallelism, and checkpointing. This phase is about **composing them into real detection systems**.

The thread running through it: *how do you recognise that something meaningful happened across many events, for one key, in event time, without your state growing forever?*

All code is Java on the Flink 1.18/1.20-era API. The shared POJO throughout is:

```java
public class Event {
    public String userId;
    public String type;      // "LOGIN", "FAILED_LOGIN", "PURCHASE", ...
    public double amount;
    public long   timestamp;
    public Event() {}        // Flink POJOs need a public no-arg constructor
}
```

## Table of contents

| # | Chapter | Key idea |
|---|---|---|
| 39 | [Timers — deep dive](39-timers-deep-dive.md) | Event-time timers fire when the **watermark** passes them, not when an event with that timestamp arrives. Coalescing on `(key, timestamp)` is the scale technique. |
| 40 | [The ProcessFunction family](40-process-function-family.md) | Two axes pick the function: how many streams, keyed or not. Connected streams have **no ordering guarantee** — buffer the early side. |
| 41 | [**Broadcast state**](41-broadcast-state.md) | **Change business rules at runtime with no redeploy.** Broadcast state is read-only on the main side because every instance must hold an identical copy. |
| 42 | [**Stateful pattern detection**](42-stateful-pattern-detection.md) | **Hand-rolled CEP.** 3 failed logins in 5 minutes, in full. `ListState<Long>` + prune + one cleanup timer + a side output. |
| 43 | [The Flink CEP library](43-flink-cep-library.md) | Declare the pattern instead of coding the state machine. `next`/`followedBy`/`followedByAny` is the concept that decides whether it works. |
| 44 | [Sessionization](44-sessionization.md) | Session windows emit nothing until the session closes and have no length cap. Hand-roll when you need early output or a cap. |
| 45 | [Joins and enrichment](45-joins-and-enrichment.md) | `intervalJoin` is inner-only and state-hungry. For external lookups, async I/O — and **never block in `asyncInvoke`**. |

## Reading order

Read **39 → 40** first; everything else builds on them. After that:

- **Building a fraud/alerting system?** 42 → 41 → 43, in that order. Hand-roll it (42), make it configurable (41), then decide whether CEP (43) buys you anything.
- **Building analytics on user behaviour?** 44 → 45.
- **Interview in two days?** 39 (timer semantics), 41 (broadcast state — the pattern interviewers love), 42 (be able to write the failed-login detector on a whiteboard), and the `intervalJoin` + async I/O sections of 45.

## The running example

Chapter 42 is the centrepiece and it detects exactly this:

```
LOGIN → FAILED_LOGIN → FAILED_LOGIN → FAILED_LOGIN → 🚨 suspicious activity
```

Chapter 43 then writes the same detector in five lines of CEP and compares the two honestly.

## The seven things that matter most in this phase

1. **An event-time timer fires on the watermark, not on the event.** Expect a delay equal to your out-of-orderness bound. No events → no watermark → no timers.
2. **Timers are keyed state.** They're checkpointed, they rescale with key groups, and a million of them is a million state entries. Coalesce by rounding the fire time; delete before re-registering.
3. **Every stateful detector needs an expiry path.** State that is only written and never cleaned up will kill the job — six months later, at 3am.
4. **Connected streams have no ordering guarantee between inputs.** Assume the wrong side arrives first. Buffer in `ListState`, bound the buffer with one timer.
5. **Broadcast state is read-only where records arrive** because every parallel instance must hold an identical copy — that's the whole invariant.
6. **Store the minimum that answers the question.** Timestamps, not events. An accumulator, not a buffer. This one habit prevents most state problems.
7. **CEP patterns cannot change without a redeploy.** If the risk team wants to tune thresholds hourly, hand-roll it and drive it from broadcast state.

## Where this goes next

- **Phase 7 (SQL / Table API)** does several of these declaratively: `MATCH_RECOGNIZE` is CEP in SQL, temporal joins solve the versioned-enrichment problem chapter 40 leaves open, and interval joins in SQL *do* support outer semantics.
- **Phase 8 (Production)** covers what happens when the state you built here gets large: state size monitoring, RocksDB tuning, savepoint migration, and rescaling.
