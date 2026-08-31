# 23. KeyedProcessFunction and Timers

Everything so far was reactive: a record arrives, you do something. But some of the most valuable questions in streaming are about things that **didn't** happen.

```
"Alert me if a user is inactive for 30 minutes."
"Alert me if an order isn't paid within 15 minutes."
"Expire this fraud flag after 1 minute."
"Emit a session summary once the session has been quiet for 5 minutes."
```

No record triggers any of those. Silence triggers them. You need to schedule work for the future, and that's what timers do.

## `KeyedProcessFunction<K, I, O>`

```java
public abstract class KeyedProcessFunction<K, I, O> extends AbstractRichFunction {

    public abstract void processElement(I value, Context ctx, Collector<O> out)
            throws Exception;

    public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out)
            throws Exception { }   // default: does nothing; override to use timers
}
```

Three type parameters:

```
K = the key type produced by keyBy    e.g. String for keyBy(e -> e.userId)
I = the input record type             e.g. Event
O = the output record type            e.g. String or Alert
```

`extends AbstractRichFunction` means it's already rich — you get `open()`, `close()`, and `getRuntimeContext()` without a `Rich` prefix. `abstract` on `processElement` means you *must* implement it; `onTimer` has an empty default so you override only if you use timers.

Wire it with `.process()`:

```java
events.keyBy(e -> e.userId).process(new MyFunction());
```

## The `Context`

`processElement` receives a `Context`. Four things live on it:

```java
public abstract class Context {
    public abstract Long timestamp();          // this record's event-time timestamp
    public abstract TimerService timerService();
    public abstract <X> void output(OutputTag<X> tag, X value);   // side output
    public abstract K getCurrentKey();
}
```

### `ctx.timestamp()`

The current record's event-time timestamp, as assigned by your `WatermarkStrategy` in Phase 1.

**It returns `Long`, the object, and it can be `null`** — when the stream has no timestamp assigner, or when you're in processing-time mode. Null-check it before arithmetic, or you'll get a `NullPointerException` in a code path that only triggers in production.

### `ctx.getCurrentKey()`

The key Flink set for this record. Useful in log messages and alert payloads, and it's the correct way to get the key when it was derived (`keyBy(e -> e.userId + ":" + e.region)`) rather than copied off a field.

### `ctx.timerService()`

The timer API. Six methods:

```java
long currentProcessingTime();                // wall clock, right now
long currentWatermark();                     // the current watermark

void registerEventTimeTimer(long time);      // fire when watermark passes `time`
void registerProcessingTimeTimer(long time); // fire when wall clock passes `time`

void deleteEventTimeTimer(long time);        // cancel an event-time timer
void deleteProcessingTimeTimer(long time);   // cancel a processing-time timer
```

### `ctx.output(tag, value)`

Emits to a **side output** — a second, independently typed output stream. Phase 2 used these for late data; chapter 24 uses one for fraud alerts.

## `onTimer`

```java
public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out)
```

- `timestamp` is the time the timer was **registered for**, not the time it fired. Event-time timers fire when the watermark passes that time, which is usually later.
- `OnTimerContext` extends `Context`, so you get everything above, plus:
  ```java
  ctx.timeDomain()   // TimeDomain.EVENT_TIME or TimeDomain.PROCESSING_TIME
  ```
  Needed only if one function registers both kinds.
- **The key is still set.** Inside `onTimer` you can read and write state, and it's the state for the key that timer belongs to. This is what makes timers useful.
- You can `out.collect(...)` and `ctx.output(...)` from `onTimer` exactly as from `processElement`.

## THE key facts about timers

> **Key idea:** Timers are (1) scoped per key, (2) themselves checkpointed state, and (3) deduplicated on key + timestamp.

Take them one at a time, because each has a practical consequence.

### 1. Timers are scoped per key

`registerEventTimeTimer(t)` registers a timer for **the current key only**. Three users can each have a timer at the same timestamp; they're three separate timers, and `onTimer` runs three times, once per key, with that key's state active.

```
alice registers timer @ 10:30   ┐
bob   registers timer @ 10:30   ├── three DISTINCT timers
carol registers timer @ 10:30   ┘

watermark passes 10:30:
   onTimer(10:30) with key=alice   -> alice's state visible
   onTimer(10:30) with key=bob     -> bob's state visible
   onTimer(10:30) with key=carol   -> carol's state visible
```

Corollary: timers, like state, are per key. A million keys each holding a timer is a million timers, and that's a million entries of state.

### 2. Timers are checkpointed state

A registered timer survives a job failure. If you register a timer for two hours from now and the TaskManager dies in between, the restored job still has that timer and fires it. It's a durable promise.

Which also means:

- **Timers count toward your state size.** A million pending timers is real memory or disk.
- **They rescale with key groups**, exactly like keyed state.
- **They can be an unbounded-growth source of their own.** Register one per record and never delete, and you have the chapter-22 problem in a new outfit.

### 3. Duplicate timers are deduplicated

```java
ctx.timerService().registerEventTimeTimer(10_000L);
ctx.timerService().registerEventTimeTimer(10_000L);
ctx.timerService().registerEventTimeTimer(10_000L);
// ONE timer exists. onTimer fires ONCE.
```

Registration is idempotent per (key, time domain, timestamp). Calling it repeatedly is harmless.

This is genuinely useful — but it is also the source of the single most common timer bug, because the deduplication is on the **exact timestamp**:

```java
// BUG: this registers a NEW timer on every record.
ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 30 * 60_000L);

// alice event @ 10:00 -> timer @ 10:30
// alice event @ 10:01 -> timer @ 10:31   (different timestamp, NOT deduplicated!)
// alice event @ 10:02 -> timer @ 10:32
// ... 1000 events -> 1000 live timers, and 999 of them fire spuriously.
```

Which brings us to the pattern that fixes it.

## The pattern: store the timer timestamp in `ValueState`

> **Key idea:** To reset a timer, you must **delete the old one**, and to delete it you must **know its timestamp**. Keep it in `ValueState<Long>`.

`deleteEventTimeTimer(t)` needs the exact `t`. There's no "delete all timers for this key". So:

```java
private transient ValueState<Long> timerState;   // the pending timer's timestamp

// ... in processElement:

// 1. Cancel the previous timer, if any.
Long previousTimer = timerState.value();
if (previousTimer != null) {
    ctx.timerService().deleteEventTimeTimer(previousTimer);
}

// 2. Register the new one.
long newTimer = ctx.timestamp() + 30 * 60_000L;
ctx.timerService().registerEventTimeTimer(newTimer);

// 3. Remember it so we can cancel it next time.
timerState.update(newTimer);
```

Deleting a timer that doesn't exist is a harmless no-op, so the null check above is for clarity rather than safety.

And in `onTimer`, clear the handle — the timer has fired, so there's nothing left to cancel:

```java
@Override
public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
    timerState.clear();
    // ... do the work ...
}
```

## Event time vs processing time timers

| | `registerEventTimeTimer(t)` | `registerProcessingTimeTimer(t)` |
|---|---|---|
| Fires when | the **watermark** passes `t` | the **wall clock** passes `t` |
| `t` is measured in | event time (record timestamps) | epoch milliseconds, wall clock |
| Deterministic on replay | **yes** — same input, same output | no — depends on when you run it |
| Fires if the stream goes idle | **no** — no data means no watermark advance | **yes** — wall clock always advances |
| Fires during a backfill | yes, and fast — watermark jumps | no, only after real elapsed time |
| Requires | a `WatermarkStrategy` | nothing |
| Use for | business logic, session gaps, timeouts | wall-clock housekeeping, real-time alerting SLAs |

### The trap in each

**Event time:** if the source goes quiet, the watermark stops advancing and your timers never fire.

```
last event @ 10:00, watermark @ 09:59
source goes silent for 3 hours
   -> watermark stays at 09:59
   -> a timer at 10:30 does NOT fire, for three hours
   -> your "inactive for 30 minutes" alert never arrives
```

The fix is **idleness detection** on the watermark strategy:

```java
WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((e, ts) -> e.timestamp)
    .withIdleness(Duration.ofMinutes(1));   // mark a quiet partition idle so
                                            // the watermark can advance from
                                            // the remaining active partitions
```

`withIdleness` handles a *partition* going quiet while others still flow. If the **whole** stream stops, nothing can advance event time — that's inherent to event-time semantics, and it's the case where you genuinely need a processing-time timer.

**Processing time:** during a backfill, wall-clock timers are useless. Replaying 30 days in 40 minutes means your "30 minutes of inactivity" processing-time timer fires 40 minutes into the replay, having nothing to do with the data.

The general rule: **event time for business logic, processing time for operational concerns.**

## Worked example: detect a user inactive for 30 minutes

Emit an alert when a user has been silent for 30 minutes of event time.

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * KeyedProcessFunction<String, Event, String>
 *   K = String   userId
 *   I = Event    the Phase 1 POJO
 *   O = String   the alert text
 */
public class InactivityDetector extends KeyedProcessFunction<String, Event, String> {

    // 30 minutes in milliseconds. `static final` = a compile-time constant
    // shared by all instances; SCREAMING_CASE is the Java naming convention.
    private static final long INACTIVITY_MS = 30 * 60 * 1000L;

    // The timestamp of the currently-pending timer for this key.
    // We need it because deleteEventTimeTimer() requires the exact value.
    private transient ValueState<Long> timerState;

    // The last event's timestamp, so the alert can say when they went quiet.
    private transient ValueState<Long> lastSeenState;

    @Override
    public void open(OpenContext ctx) {
        timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("inactivity-timer", Long.class));
        lastSeenState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-seen", Long.class));
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<String> out)
            throws Exception {

        // ---- 1. Get this record's event time -----------------------------
        // ctx.timestamp() is a Long (object) and CAN be null if no timestamp
        // assigner is configured. Fall back to the POJO's own field.
        Long eventTime = ctx.timestamp();
        long ts = (eventTime != null) ? eventTime : event.timestamp;
        //         ^^^^^^^^^^^^^^^^^^^^^^^ Java's ternary operator:
        //         condition ? valueIfTrue : valueIfFalse

        // ---- 2. Cancel the previously scheduled timer --------------------
        // The user just did something, so the old "30 min from the LAST event"
        // deadline is obsolete. Without this we'd accumulate one live timer
        // per record and get a flood of spurious alerts.
        Long previousTimer = timerState.value();
        if (previousTimer != null) {
            ctx.timerService().deleteEventTimeTimer(previousTimer);
        }

        // ---- 3. Schedule a fresh deadline --------------------------------
        long newTimer = ts + INACTIVITY_MS;
        ctx.timerService().registerEventTimeTimer(newTimer);

        // ---- 4. Remember it so step 2 can find it next time --------------
        timerState.update(newTimer);
        lastSeenState.update(ts);

        // ---- 5. Pass the activity through --------------------------------
        out.collect(String.format("%s active @%d (deadline %d)",
                ctx.getCurrentKey(), ts, newTimer));
    }

    /**
     * Fires when the WATERMARK passes the registered timestamp. The key is
     * still bound, so timerState and lastSeenState are this user's.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
            throws Exception {

        // The timer has fired, so there is nothing left to cancel.
        // Failing to clear this leaves a stale timestamp that a later
        // deleteEventTimeTimer() call would waste time on.
        timerState.clear();

        Long lastSeen = lastSeenState.value();

        out.collect(String.format("*** INACTIVE: %s, last seen @%d, silent for %d min",
                ctx.getCurrentKey(),
                lastSeen,
                INACTIVITY_MS / 60000));

        // The user is gone. Free their state so it doesn't accumulate.
        // (A TTL from chapter 22 would be the belt to this braces.)
        lastSeenState.clear();
    }
}
```

The full job:

```java
public class InactivityJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> events = /* your source */ null;

        events
            // Event-time timers need a watermark strategy. withIdleness keeps
            // the watermark moving when one partition goes quiet.
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((e, recordTs) -> e.timestamp)
                    .withIdleness(Duration.ofMinutes(1)))
            .keyBy(e -> e.userId)
            .process(new InactivityDetector())
            .print();

        env.execute("inactivity detection");
    }
}
```

### Trace

30-minute threshold = 1,800,000 ms. Timestamps abbreviated to `10:00` style for readability.

```
INPUT (event time)                WATERMARK   ACTION
────────────────────────────────  ─────────   ─────────────────────────────────────────
alice LOGIN     @ 10:00           09:59:55    prev timer: none
                                              register alice@10:30
                                              timerState[alice] = 10:30
                                              emit "alice active @10:00 (deadline 10:30)"

bob   LOGIN     @ 10:05           10:04:55    prev timer: none
                                              register bob@10:35
                                              timerState[bob] = 10:35
                                              emit "bob active @10:05 (deadline 10:35)"

alice PURCHASE  @ 10:10           10:09:55    prev timer: 10:30 -> DELETE alice@10:30
                                              register alice@10:40
                                              timerState[alice] = 10:40
                                              emit "alice active @10:10 (deadline 10:40)"

              ── alice goes quiet; bob keeps going ──

bob   PURCHASE  @ 10:20           10:19:55    delete bob@10:35, register bob@10:50
bob   PURCHASE  @ 10:30           10:29:55    delete bob@10:50, register bob@11:00
bob   LOGOUT    @ 10:38           10:37:55    delete bob@11:00, register bob@11:08

              ── watermark climbs past 10:40 ──

(bob event   @ 10:41)             10:40:55    ***** WATERMARK PASSES alice@10:40 *****
                                              onTimer(10:40) with key = alice:
                                                timerState[alice].clear()
                                                emit "*** INACTIVE: alice, last seen
                                                      @10:10, silent for 30 min"
                                                lastSeenState[alice].clear()
                                              THEN processElement for bob's record.

STATE AND TIMERS AT THIS POINT:
   timerState[alice]     = (cleared)
   lastSeenState[alice]  = (cleared)
   timerState[bob]       = 11:11  (reset by the 10:41 event)
   pending timers        = { bob @ 11:11 }
```

Two things to notice:

1. **Alice's alert fired at watermark 10:40**, not at wall clock 10:40. If the source is 6 hours behind, the alert arrives 6 hours late in wall-clock terms but is *exactly right* in event-time terms — and identical on a replay.
2. **Only one timer per key exists at a time**, because every `processElement` deletes before it registers. Without step 2, alice's three events would have left three live timers and produced three "inactive" alerts.

## Common timer mistakes

```java
// ❌ 1. Registering without deleting -> one timer per record, spurious fires
ctx.timerService().registerEventTimeTimer(ctx.timestamp() + GAP);

// ❌ 2. Not clearing timerState in onTimer -> stale handle, wasted delete calls
public void onTimer(...) { out.collect(...); }   // forgot timerState.clear()

// ❌ 3. Event-time timers with no watermark strategy -> they never fire, silently
env.fromElements(...).keyBy(...).process(new UsesEventTimeTimers());

// ❌ 4. Assuming ctx.timestamp() is non-null
long ts = ctx.timestamp();   // unboxing null -> NullPointerException

// ❌ 5. Registering a timer for the PAST in event time
ctx.timerService().registerEventTimeTimer(ctx.timestamp() - 1000);
//   Fires on the very next watermark advance. Sometimes intentional
//   ("run this ASAP"), usually an arithmetic bug.

// ❌ 6. Timers on an unbounded keyspace with no cleanup
//   Every key gets a timer, nothing ever clears -> the chapter 22 problem.
```

## Remember

- `KeyedProcessFunction<K, I, O>`: `processElement(value, ctx, out)` and `onTimer(timestamp, ctx, out)`.
- `Context` gives you `timestamp()`, `getCurrentKey()`, `timerService()`, `output(tag, value)`.
- `ctx.timestamp()` returns `Long` and can be `null`.
- Timers are **per key**, are **checkpointed state**, and are **deduplicated on (key, domain, timestamp)**.
- The `timestamp` argument to `onTimer` is the *registered* time, not the fire time.
- The key is still bound inside `onTimer` — state access there is that key's state.
- To reset a timer: delete the old one, register the new one, store the new timestamp in `ValueState<Long>`.
- Deleting a nonexistent timer is a harmless no-op.
- Event-time timers fire on watermark advance; use `withIdleness` so quiet partitions don't stall them.
- Processing-time timers fire on wall clock; they're non-deterministic and useless during backfills.
- `clear()` the timer handle in `onTimer`, and clear the key's state when the key is genuinely finished.

## Interview one-liners

- *"How do you detect that something didn't happen?"* → A `KeyedProcessFunction` with a timer: register a deadline on each event, delete and re-register on the next one, and `onTimer` fires only if the deadline is actually reached.
- *"Are timers fault tolerant?"* → Yes. Timers are keyed state, checkpointed with everything else, and restored on recovery — including timers scheduled hours out.
- *"What happens if you register the same timer twice?"* → Nothing. Registration is deduplicated per key, time domain, and exact timestamp, so it fires once.
- *"Why do I get a flood of duplicate alerts?"* → You registered a timer per record without deleting the previous one. Each event's timestamp differs, so deduplication doesn't apply.
- *"How do you delete a timer?"* → `deleteEventTimeTimer(t)` with the exact timestamp, which is why you keep it in `ValueState<Long>`. There's no delete-all-for-this-key.
- *"Event-time vs processing-time timers?"* → Event-time fires on watermark advance and is deterministic and replayable; processing-time fires on wall clock, never stalls, and is meaningless during a backfill.
- *"My event-time timers never fire — why?"* → No watermark strategy, or a quiet source so the watermark can't advance. Add `withIdleness`.
- *"Can timers cause state growth?"* → Yes. Timers are state. One per key on an unbounded keyspace is the same failure mode as state with no TTL.
