# 44. Sessionization

A **session** is a burst of activity by one user, ended by a period of inactivity. "Alice browsed for 12 minutes, then went quiet for half an hour" is one session.

Flink gives you a built-in session window. It's one line, and for many jobs it's the right answer. Then there's the version you write yourself, which is 100 lines and gives you everything the built-in one can't do.

> **Key idea**
> A session is defined by a **gap**, not by a fixed boundary.
> The session ends when nothing has happened for `gap` milliseconds of event time — which means you cannot know a session is over until `gap` has elapsed *after* its last event.

---

## The picture

```
gap = 30 min

user alice:
  events:  ●  ●   ●              ●  ●        ●
  time:   10:00 10:05 10:12    11:00 11:03  11:40
           └──── session 1 ────┘  └── s2 ──┘ └ s3 ...
                                ▲          ▲
                          48-min gap   37-min gap
                          > 30 → new   > 30 → new
```

Sessions are per key. Two users' sessions are completely independent, and their boundaries have nothing to do with each other. This is why sessions can never be modelled with tumbling windows.

---

## Approach 1 — `EventTimeSessionWindows.withGap` (the recap)

```java
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import java.time.Duration;

DataStream<SessionSummary> sessions =
    events
        .keyBy(e -> e.userId)
        .window(EventTimeSessionWindows.withGap(Duration.ofMinutes(30)))
        .process(new ProcessWindowFunction<Event, SessionSummary, String, TimeWindow>() {
            @Override
            public void process(String key,
                                Context ctx,
                                Iterable<Event> events,
                                Collector<SessionSummary> out) {
                long count = 0;
                double total = 0.0;
                for (Event e : events) {         // Flink buffered ALL of them for you
                    count++;
                    total += e.amount;
                }
                out.collect(new SessionSummary(
                        key,
                        ctx.window().getStart(),   // first event's timestamp
                        ctx.window().getEnd(),     // last event's timestamp + gap
                        count,
                        total));
            }
        });
```

### How it works internally (worth knowing)

Session windows are **merging windows**. Each arriving event creates a provisional window `[ts, ts + gap)`. Then Flink merges any windows that overlap.

```
event @10:00 -> window [10:00, 10:30)
event @10:05 -> window [10:05, 10:35)   overlaps -> merge -> [10:00, 10:35)
event @10:12 -> window [10:12, 10:42)   overlaps -> merge -> [10:00, 10:42)
event @11:00 -> window [11:00, 11:30)   no overlap -> separate window
```

An **out-of-order event can merge two existing sessions into one**, which is why the assigner must support merging and why `ProcessWindowFunction`'s per-window state is tricky here (it's discarded on merge unless you use a `MergingState`).

There is also `ProcessingTimeSessionWindows.withGap(...)` and dynamic-gap variants (`EventTimeSessionWindows.withDynamicGap(extractor)`) where the gap is computed per element.

### The two limitations that push you to approach 2

**Limitation 1: nothing is emitted until the session closes.**

The window fires when the watermark passes `lastEventTs + gap`. With a 30-minute gap, a session that started at 10:00 and is still active at 14:00 has produced **zero output for four hours**. If your dashboard needs "sessions currently in progress", or you want an incremental update every minute, session windows cannot give it to you without a custom `Trigger`.

**Limitation 2: unbounded session length.**

A user who clicks every 29 minutes for a week has **one session lasting a week**. With `ProcessWindowFunction` that means every event of that week is buffered in state. A bot doing exactly this is a state bomb. There is no `maxSessionDuration` option on the built-in assigner.

Two smaller ones:

- **You must use `reduce`/`aggregate` to avoid buffering.** `.process(...)` alone holds every element.
- **The window's end is `lastEvent + gap`**, not `lastEvent`. Your session "duration" is inflated by the gap unless you compute it from the events themselves.

---

## Approach 2 — `KeyedProcessFunction`

Full control. The design:

```
STATE per key:
   ValueState<SessionAcc> session    the running accumulator (count, sum, start, last)
   ValueState<Long>       timerTs    the timestamp of our one pending timer

ON EACH EVENT:
   1. load or create the accumulator
   2. fold this event into it
   3. delete the old timer, register a new one at ts + gap
   4. if the session has run longer than maxDuration -> emit and reset NOW

ON TIMER:
   emit the session, clear all state
```

That's the whole algorithm. Note step 3 — it is exactly the "sliding timeout" pattern from chapter 39.

### The accumulator POJO

```java
/**
 * A POJO Flink can serialize: public class, public no-arg constructor, public fields.
 * We keep an ACCUMULATOR, not the list of events — that's the whole memory win
 * over ProcessWindowFunction.
 */
public class SessionAcc {
    public String userId;
    public long   startTs;      // event time of the first event
    public long   lastTs;       // event time of the most recent event
    public long   count;
    public double total;

    public SessionAcc() {}      // REQUIRED

    public static SessionAcc start(Event e, long ts) {
        SessionAcc a = new SessionAcc();
        a.userId  = e.userId;
        a.startTs = ts;
        a.lastTs  = ts;
        a.count   = 1;
        a.total   = e.amount;
        return a;
    }

    public void add(Event e, long ts) {
        this.count++;
        this.total += e.amount;
        // Guard against out-of-order events moving lastTs backwards.
        if (ts > this.lastTs)  this.lastTs  = ts;
        if (ts < this.startTs) this.startTs = ts;
    }

    /** Duration measured from first to last EVENT, not to the window end. */
    public long durationMs() { return lastTs - startTs; }
}
```

State size per active session: one small object, regardless of whether the session has 5 events or 5 million. That's the point.

### The function

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Sessionizer extends KeyedProcessFunction<String, Event, SessionSummary> {

    /** Inactivity that ends a session. */
    private final long gapMs;

    /** Hard cap: cut the session even if the user is still active. 0 = no cap. */
    private final long maxDurationMs;

    public Sessionizer(long gapMs, long maxDurationMs) {
        this.gapMs = gapMs;
        this.maxDurationMs = maxDurationMs;
    }

    private transient ValueState<SessionAcc> session;
    private transient ValueState<Long>       timerTs;

    @Override
    public void open(OpenContext ctx) {
        session = getRuntimeContext().getState(
                new ValueStateDescriptor<>("session", SessionAcc.class));
        timerTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timerTs", Types.LONG));
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<SessionSummary> out)
            throws Exception {

        long ts = ctx.timestamp();

        // ── 1. load or start the session ─────────────────────────────────
        SessionAcc acc = session.value();     // null the first time for this key
        if (acc == null) {
            acc = SessionAcc.start(e, ts);
        } else {
            acc.add(e, ts);
        }

        // ── 2. the max-duration cap ──────────────────────────────────────
        // Checked BEFORE we re-arm, so the emitted session ends here and a
        // brand-new one starts with this same event.
        if (maxDurationMs > 0 && acc.durationMs() >= maxDurationMs) {
            out.collect(SessionSummary.of(acc, "MAX_DURATION"));
            clearTimer(ctx);
            // This event starts the next session.
            acc = SessionAcc.start(e, ts);
        }

        session.update(acc);

        // ── 3. slide the inactivity timer ────────────────────────────────
        long fireAt = acc.lastTs + gapMs;

        Long old = timerTs.value();
        if (old == null || old != fireAt) {
            if (old != null) {
                ctx.timerService().deleteEventTimeTimer(old);   // cancel the stale deadline
            }
            ctx.timerService().registerEventTimeTimer(fireAt);
            timerTs.update(fireAt);
        }
        // If old == fireAt (an out-of-order event that didn't move lastTs),
        // we do nothing — coalescing would have made it a no-op anyway.
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<SessionSummary> out)
            throws Exception {

        SessionAcc acc = session.value();
        if (acc == null) return;             // defensive: nothing to close

        // Sanity check: is this the CURRENT deadline, or a stale timer?
        Long expected = timerTs.value();
        if (expected == null || expected != ts) return;

        out.collect(SessionSummary.of(acc, "GAP"));

        // Release everything. This key now costs zero state until it's active again.
        session.clear();
        timerTs.clear();
    }

    private void clearTimer(Context ctx) throws Exception {
        Long t = timerTs.value();
        if (t != null) {
            ctx.timerService().deleteEventTimeTimer(t);
            timerTs.clear();
        }
    }
}
```

### The summary output

```java
public class SessionSummary {
    public String userId;
    public long   startTs;
    public long   endTs;
    public long   count;
    public double total;
    public String closedBy;      // "GAP" or "MAX_DURATION" — tells you WHY it ended

    public SessionSummary() {}

    public static SessionSummary of(SessionAcc a, String reason) {
        SessionSummary s = new SessionSummary();
        s.userId   = a.userId;
        s.startTs  = a.startTs;
        s.endTs    = a.lastTs;      // the LAST EVENT, not lastEvent + gap
        s.count    = a.count;
        s.total    = a.total;
        s.closedBy = reason;
        return s;
    }
}
```

### Wiring

```java
events
    .assignTimestampsAndWatermarks(
        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner((e, ts) -> e.timestamp)
            .withIdleness(Duration.ofMinutes(1)))   // or sessions never close on a quiet partition
    .keyBy(e -> e.userId)
    .process(new Sessionizer(30 * 60_000L, 4 * 60 * 60_000L))   // 30 min gap, 4 hour cap
    .print();
```

---

## Trace

`gap = 30 min = 1 800 000 ms`, `maxDuration = 4 h = 14 400 000 ms`. Key = alice. Times shown as `HH:MM` for readability; the code uses millis.

| # | event | ts | acc after | timerTs after | output |
|---|---|---|---|---|---|
| 1 | click | 10:00 | `start=10:00 last=10:00 n=1` | 10:30 | — |
| 2 | click | 10:05 | `start=10:00 last=10:05 n=2` | 10:35 (10:30 deleted) | — |
| 3 | click | 10:12 | `start=10:00 last=10:12 n=3` | 10:42 (10:35 deleted) | — |
| — | *watermark passes 10:42* | | `onTimer(10:42)`: expected==ts ✅ | cleared | **SessionSummary(10:00→10:12, n=3, GAP)** |
| 4 | click | 11:00 | `start=11:00 last=11:00 n=1` (fresh) | 11:30 | — |
| 5 | click | 11:03 | `start=11:00 last=11:03 n=2` | 11:33 | — |
| — | *watermark passes 11:33* | | onTimer | cleared | **SessionSummary(11:00→11:03, n=2, GAP)** |

Note the emitted `endTs` is **10:12**, the real last event — not 10:42, which is what a session window's `getEnd()` would have given you.

### The max-duration cap firing

A bot clicking every 20 minutes forever:

```
10:00 click   start=10:00 last=10:00                    timer@10:30
10:20 click   start=10:00 last=10:20  duration 20 min   timer@10:50
10:40 click   start=10:00 last=10:40  duration 40 min   timer@11:10
 ...
14:00 click   start=10:00 last=14:00  duration 240 min >= 240 min CAP
              -> EMIT SessionSummary(10:00→14:00, n=13, MAX_DURATION)
              -> acc reset: start=14:00 last=14:00 n=1
              -> timer@14:30
14:20 click   start=14:00 last=14:20                    timer@14:50
```

Without the cap, that accumulator would keep growing (well — the *accumulator* wouldn't, but a `ProcessWindowFunction` buffering the raw events absolutely would) and you'd never emit anything for that user.

### The out-of-order case

```
10:00 click  last=10:00  timer@10:30
10:20 click  last=10:20  timer@10:50 (10:30 deleted)
10:10 click  LATE but within the watermark bound.
             acc.add(): ts=10:10 is NOT > last=10:20, so last stays 10:20.
             startTs unchanged (10:10 > 10:00).
             count becomes 3, total updated.
             fireAt = 10:20 + 30m = 10:50 == old timer -> nothing to do.  ✅
```

The `if (ts > this.lastTs)` guard in `add()` is what makes this correct. Without it, `lastTs` would jump backwards to 10:10 and the timer would move *earlier*, closing the session prematurely.

Events later than the watermark are dropped by Flink before they reach you — they'd be assigned to a session that already fired. If you care, add a lateness side output upstream.

---

## Emitting early (the thing session windows can't do)

Because you own the code, adding a "heartbeat" is trivial — a second, coalesced timer (ch. 39):

```java
// In processElement, after updating the accumulator:
long minute = 60_000L;
long heartbeat = ts - (ts % minute) + minute;      // rounded -> coalesces
ctx.timerService().registerEventTimeTimer(heartbeat);

// In onTimer, distinguish the two kinds:
Long deadline = timerTs.value();
if (deadline != null && deadline == ts) {
    // the real session-close timer
} else {
    // a heartbeat: emit a PARTIAL summary, do NOT clear state
    SessionAcc acc = session.value();
    if (acc != null) out.collect(SessionSummary.of(acc, "IN_PROGRESS"));
}
```

Downstream then sees a live, updating view of open sessions. This is the single most common reason teams abandon session windows.

---

## Which to use

| Requirement | Session window | `KeyedProcessFunction` |
|---|---|---|
| Simple "summarize each session" | ✅ one line | overkill |
| Output only at session end is fine | ✅ | ✅ |
| Need in-progress / early output | ❌ (needs a custom `Trigger`) | ✅ trivially |
| Need a max session duration cap | ❌ | ✅ |
| Need the real last-event timestamp, not `last + gap` | awkward (compute from elements) | ✅ naturally |
| Need to know *why* the session ended | ❌ | ✅ (`closedBy`) |
| Need dynamic per-user gap | `withDynamicGap` (per element only) | ✅ any logic, incl. broadcast state (ch. 41) |
| Out-of-order events merging two sessions | ✅ automatic (merging windows) | ❌ you'd have to write it |
| Minimal state | use `.reduce`/`.aggregate` | ✅ accumulator only |
| Amount of code | 1 line | ~100 lines |

> **Key idea**
> Use the built-in session window until you need **early output**, a **duration cap**, or **custom close semantics**. Then switch to `KeyedProcessFunction` — you're rewriting maybe 100 lines and you get complete control.
> The one thing you lose is automatic **session merging** on out-of-order events. If that matters more than early output, stay with the window.

---

## Remember

- A session ends after `gap` of inactivity; you can only know it ended `gap` after the last event.
- `EventTimeSessionWindows.withGap(Duration)` works by creating a `[ts, ts+gap)` window per event and **merging** overlapping ones.
- Session windows emit **nothing** until the session closes, and have **no max-duration cap**.
- Session window `getEnd()` is `lastEvent + gap`, not the last event.
- The hand-rolled version: `ValueState<Acc>` + `ValueState<Long> timerTs`, delete-and-re-register the timer on every event, emit and clear in `onTimer`.
- Store an **accumulator**, not the events. State per session is then O(1).
- Guard `lastTs` against out-of-order events moving it backwards, or you'll close sessions early.
- Validate `ts == timerTs.value()` in `onTimer` so stale timers are no-ops.
- Add `withIdleness` or quiet partitions freeze the watermark and no session ever closes.
- A max-duration cap needs the hand-rolled version. So does early/heartbeat output.
- What you lose hand-rolling: automatic merging of two sessions by a late event.

## Interview one-liners

- *"What is a session window?"* → A per-key window defined by an inactivity gap; each event opens a `[ts, ts+gap)` window and overlapping ones merge.
- *"Why are session windows merging windows?"* → An out-of-order event landing between two sessions must be able to join them into one, so the assigner has to support merge.
- *"What's the problem with session windows in production?"* → No output until the session closes, and no cap on session length — one long-running key can hold state indefinitely.
- *"How do you sessionize with a `KeyedProcessFunction`?"* → Accumulator in `ValueState`, delete-and-re-register an event-time timer at `lastTs + gap` on every event, emit and clear the state when it fires.
- *"How do you cap session length?"* → Track `startTs` in the accumulator and, when `lastTs − startTs` exceeds the cap, emit immediately and start a fresh session from the current event.
- *"How do you emit in-progress sessions?"* → A second, coalesced heartbeat timer that emits a partial summary without clearing state.
- *"Why store an accumulator instead of the events?"* → State becomes O(1) per session instead of O(number of events); a `ProcessWindowFunction` buffers everything.
- *"Why won't my sessions close?"* → The watermark isn't advancing — usually an idle source partition; add `withIdleness`.
