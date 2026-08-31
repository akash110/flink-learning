# 24. Capstone — Fraud Detection

This chapter puts the whole phase together. Every concept from 18–23 appears in one job:

```
ch.18  keyed state, per-key scoping
ch.19  ValueState, transient + open(), null handling, clear()
ch.21  (not needed here — but note where ValueState was enough)
ch.22  cleanup so state doesn't grow forever
ch.23  KeyedProcessFunction, event-time timers, delete-and-reset, side outputs
```

Read it slowly. This is the shape of a large fraction of real Flink applications.

## The rule

A classic card-testing pattern: a fraudster verifies a stolen card with a tiny transaction, then immediately makes a large one.

```
FRAUD if, for the same user:
     a SMALL purchase   (< $1.00)
     is followed by a
     LARGE purchase     (> $500.00)
     within 1 MINUTE of event time
```

Three ingredients, and each maps to a mechanism:

| Requirement | Mechanism |
|---|---|
| "for the same user" | `keyBy(e -> e.userId)` — state is per key |
| "small then large" | `ValueState<Boolean>` remembering that a small one happened |
| "within 1 minute" | an event-time timer that clears the flag |

## Why `ValueState<Boolean>` and not a timestamp

You could store the small transaction's timestamp and compare on the large one:

```java
Long smallTs = smallTsState.value();
if (smallTs != null && event.timestamp - smallTs <= 60_000) { alert(); }
```

That works, and it's simpler. But it has a flaw: the state entry is **never cleaned up**. A user who makes one small purchase and never returns keeps that timestamp forever. With 100 M users, that's 100 M orphaned entries.

The timer version cleans up after itself:

```
small purchase  -> set flag = true, register timer at +1 minute
large purchase within 1 min -> ALERT, clear flag, delete the timer
timer fires (no large purchase) -> clear flag automatically
```

Either way the flag is gone within a minute. State stays proportional to *recent activity*, not to lifetime users. That difference is the whole game in production.

> **Key idea:** A timer isn't just for detecting timeouts. It's how you make state **self-cleaning**.

## The alert type

```java
/**
 * A POJO. For Flink to serialize it efficiently it needs:
 *   - public fields (or public getters/setters)
 *   - a public no-argument constructor
 *   - a non-generic, publicly accessible class
 * Miss any of these and Flink silently falls back to Kryo, which is slower.
 */
public class FraudAlert {
    public String userId;
    public double smallAmount;
    public double largeAmount;
    public long   smallTimestamp;
    public long   largeTimestamp;

    // REQUIRED no-arg constructor, used by the deserializer.
    public FraudAlert() { }

    public FraudAlert(String userId, double smallAmount, double largeAmount,
                      long smallTimestamp, long largeTimestamp) {
        this.userId = userId;
        this.smallAmount = smallAmount;
        this.largeAmount = largeAmount;
        this.smallTimestamp = smallTimestamp;
        this.largeTimestamp = largeTimestamp;
    }

    /** toString() is what print() calls. Overriding it makes output readable. */
    @Override
    public String toString() {
        return String.format(
            "FRAUD[user=%s small=$%.2f@%d large=$%.2f@%d gap=%dms]",
            userId, smallAmount, smallTimestamp,
            largeAmount, largeTimestamp,
            largeTimestamp - smallTimestamp);
    }
}
```

## The detector

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * KeyedProcessFunction<String, Event, Event>
 *   K = String  userId
 *   I = Event   incoming transactions
 *   O = Event   the MAIN output: every event, passed through unchanged
 *
 * Alerts do NOT go to the main output. They go to a SIDE OUTPUT, which can
 * have a completely different type (FraudAlert). That's the point of side
 * outputs: one operator, multiple independently-typed output streams.
 */
public class FraudDetector extends KeyedProcessFunction<String, Event, Event> {

    // ---- Thresholds. static final = shared constants. -----------------------
    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long   WINDOW_MS    = 60 * 1000L;   // 1 minute

    /**
     * The side-output handle. OutputTag<T> carries the output's type.
     *
     * The trailing `{}` makes this an ANONYMOUS SUBCLASS of OutputTag. That is
     * MANDATORY, not decoration: Java erases generics at runtime, but it does
     * preserve them on a class's declared superclass. Flink reads <FraudAlert>
     * back by reflection. Without the {} you get:
     *   "Could not determine TypeInformation for the OutputTag type."
     *
     * static so the job's main() can reference the SAME tag instance to fetch
     * the stream. Two tags with the same String id are considered equal, so
     * sharing the instance is convention rather than requirement — but share it.
     */
    public static final OutputTag<FraudAlert> FRAUD_ALERTS =
            new OutputTag<FraudAlert>("fraud-alerts") { };

    // ---- State. All transient; all created in open(). -----------------------

    /** True if this user made a small purchase that is still "armed". */
    private transient ValueState<Boolean> flagState;

    /** The timestamp of the pending expiry timer, so we can delete it. */
    private transient ValueState<Long> timerState;

    /** Details of the small purchase, for the alert payload. */
    private transient ValueState<Double> smallAmountState;
    private transient ValueState<Long>   smallTimestampState;

    @Override
    public void open(OpenContext ctx) {

        // A TTL as a BACKSTOP. The timer is the primary cleanup mechanism and
        // handles the normal case. TTL catches the pathological ones: a
        // watermark that stalls forever, a savepoint restored with orphaned
        // entries, a bug in our own delete logic.
        //
        // 1 hour, when the business window is 1 minute: generous on purpose.
        // Remember TTL is PROCESSING time (ch.22) and must not fight the timer.
        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(Duration.ofHours(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();

        ValueStateDescriptor<Boolean> flagDesc =
                new ValueStateDescriptor<>("fraud-flag", Boolean.class);
        flagDesc.enableTimeToLive(ttl);              // BEFORE getState()
        flagState = getRuntimeContext().getState(flagDesc);

        ValueStateDescriptor<Long> timerDesc =
                new ValueStateDescriptor<>("fraud-timer", Long.class);
        timerDesc.enableTimeToLive(ttl);
        timerState = getRuntimeContext().getState(timerDesc);

        ValueStateDescriptor<Double> amountDesc =
                new ValueStateDescriptor<>("small-amount", Double.class);
        amountDesc.enableTimeToLive(ttl);
        smallAmountState = getRuntimeContext().getState(amountDesc);

        ValueStateDescriptor<Long> smallTsDesc =
                new ValueStateDescriptor<>("small-timestamp", Long.class);
        smallTsDesc.enableTimeToLive(ttl);
        smallTimestampState = getRuntimeContext().getState(smallTsDesc);
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out)
            throws Exception {

        // ---- 0. Everything passes through, fraud or not ---------------------
        // Downstream consumers still need the transaction stream.
        out.collect(event);

        // Only purchases participate in the rule. LOGIN/LOGOUT are ignored,
        // and crucially they neither set nor clear the flag.
        if (!"PURCHASE".equals(event.type)) {
            return;
        }

        // ctx.timestamp() can be null with no timestamp assigner. Fall back.
        Long ctxTs = ctx.timestamp();
        long ts = (ctxTs != null) ? ctxTs : event.timestamp;

        // ---- 1. THE DETECTION: is the flag armed AND is this one large? -----
        Boolean flagged = flagState.value();     // null if never set

        // Boolean.TRUE.equals(x) is null-safe: false for null, no unboxing NPE.
        if (Boolean.TRUE.equals(flagged) && event.amount > LARGE_AMOUNT) {

            FraudAlert alert = new FraudAlert(
                    ctx.getCurrentKey(),
                    smallAmountState.value(),
                    event.amount,
                    smallTimestampState.value(),
                    ts);

            // Emit to the SIDE OUTPUT, not to `out`. Different type, different
            // downstream, different SLA.
            ctx.output(FRAUD_ALERTS, alert);

            // Fired. Disarm so the next large purchase doesn't re-alert.
            cleanUp(ctx);
            return;
        }

        // ---- 2. ARM: a small purchase starts a 1-minute window --------------
        if (event.amount < SMALL_AMOUNT) {

            // Delete any previously pending timer. A user could make two small
            // purchases in a row; the second should restart the clock, not
            // leave the first timer alive to disarm us early.
            Long previousTimer = timerState.value();
            if (previousTimer != null) {
                ctx.timerService().deleteEventTimeTimer(previousTimer);
            }

            flagState.update(true);
            smallAmountState.update(event.amount);
            smallTimestampState.update(ts);

            // Fires when the WATERMARK passes ts + 1 minute.
            long expiry = ts + WINDOW_MS;
            ctx.timerService().registerEventTimeTimer(expiry);
            timerState.update(expiry);          // remember it, so we can delete it

            return;
        }

        // ---- 3. Neither: a normal mid-size purchase -------------------------
        // Deliberately does nothing. The rule is "small IMMEDIATELY followed by
        // large"; a $50 purchase in between does not break the pattern under
        // this definition. If your rule says it should, disarm here.
    }

    /**
     * Fires when the watermark passes the registered expiry — i.e. one minute
     * of EVENT TIME elapsed with no qualifying large purchase.
     *
     * The key is still bound here, so every state access below is this user's.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out)
            throws Exception {
        // The window closed without a large purchase. Disarm.
        cleanUp(ctx);
    }

    /**
     * Clear all four pieces of state and cancel any pending timer.
     *
     * Takes a Context, so it works from processElement AND onTimer
     * (OnTimerContext extends Context — that's Java inheritance: an
     * OnTimerContext IS-A Context, so it can be passed wherever one is wanted).
     */
    private void cleanUp(Context ctx) throws Exception {

        // Cancel the timer BEFORE clearing the handle, or we lose the timestamp
        // and the timer fires spuriously later. Order matters here.
        Long pendingTimer = timerState.value();
        if (pendingTimer != null) {
            ctx.timerService().deleteEventTimeTimer(pendingTimer);
        }

        // Calling deleteEventTimeTimer from inside onTimer for the timer that
        // is currently firing is a harmless no-op — it's already been removed.

        flagState.clear();
        timerState.clear();
        smallAmountState.clear();
        smallTimestampState.clear();
    }
}
```

## The full job

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Parallelism 1 keeps the printed output in a readable order while
        // you're learning. Remove it in production.
        env.setParallelism(1);

        // Fix maxParallelism explicitly so you can rescale later (ch.18).
        env.setMaxParallelism(1024);

        // ---- Source: hand-written events, in event-time order --------------
        // Timestamps are epoch millis. 1_700_000_000_000L is ~Nov 2023;
        // the offsets below are the interesting part.
        DataStream<Event> raw = env.fromElements(
            //          userId,   type,       amount,  timestamp
            new Event("alice",  "LOGIN",       0.00, 1_700_000_000_000L),
            new Event("alice",  "PURCHASE",    0.50, 1_700_000_010_000L), // t+10s  SMALL
            new Event("bob",    "PURCHASE",   45.00, 1_700_000_015_000L), // normal
            new Event("alice",  "PURCHASE",  750.00, 1_700_000_030_000L), // t+30s  LARGE -> FRAUD
            new Event("carol",  "PURCHASE",    0.25, 1_700_000_040_000L), // t+40s  SMALL
            new Event("bob",    "PURCHASE",    0.10, 1_700_000_050_000L), // t+50s  SMALL
            new Event("carol",  "LOGIN",       0.00, 1_700_000_060_000L), // ignored
            new Event("bob",    "PURCHASE",  900.00, 1_700_000_070_000L), // t+70s  LARGE -> FRAUD
            new Event("carol",  "PURCHASE",  600.00, 1_700_000_130_000L), // t+130s LARGE, 90s
                                                                          //   after carol's
                                                                          //   small -> TOO LATE
            new Event("dave",   "PURCHASE",  999.00, 1_700_000_140_000L), // large, no small
            new Event("alice",  "PURCHASE",    0.75, 1_700_000_200_000L), // SMALL, arms again
            new Event("alice",  "PURCHASE",   20.00, 1_700_000_210_000L), // mid — does NOT disarm
            new Event("alice",  "PURCHASE",  800.00, 1_700_000_230_000L), // t+30s after small
                                                                          //   -> FRAUD
            // A trailing far-future event pushes the watermark past every
            // pending timer so they all fire before the job ends. Without it,
            // fromElements' end-of-stream watermark would still fire them,
            // but this makes the ordering explicit and easy to reason about.
            new Event("zzz",    "LOGIN",       0.00, 1_700_000_999_000L)
        );

        // ---- Watermarks: required for event-time timers --------------------
        DataStream<Event> events = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                    // Tolerate up to 5s of out-of-orderness.
                    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    // Lambda (element, previousTimestamp) -> the event time.
                    .withTimestampAssigner((e, previousTs) -> e.timestamp)
                    // If a partition goes quiet, don't let it hold back the
                    // watermark — otherwise timers stall (ch.23).
                    .withIdleness(Duration.ofMinutes(1)));

        // ---- The detector ---------------------------------------------------
        // SingleOutputStreamOperator (not DataStream) is the return type,
        // because only it exposes .getSideOutput().
        SingleOutputStreamOperator<Event> processed = events
                .keyBy(e -> e.userId)          // MANDATORY for keyed state
                .process(new FraudDetector());

        // ---- Two outputs from one operator ---------------------------------
        processed.print("txn");

        processed
            .getSideOutput(FraudDetector.FRAUD_ALERTS)   // the SAME tag instance
            .print("ALERT");

        env.execute("fraud detection");
    }
}
```

## Expected output

```
txn> Event{alice, LOGIN, 0.0, 1700000000000}
txn> Event{alice, PURCHASE, 0.5, 1700000010000}
txn> Event{bob, PURCHASE, 45.0, 1700000015000}
txn> Event{alice, PURCHASE, 750.0, 1700000030000}
ALERT> FRAUD[user=alice small=$0.50@1700000010000 large=$750.00@1700000030000 gap=20000ms]
txn> Event{carol, PURCHASE, 0.25, 1700000040000}
txn> Event{bob, PURCHASE, 0.1, 1700000050000}
txn> Event{carol, LOGIN, 0.0, 1700000060000}
txn> Event{bob, PURCHASE, 900.0, 1700000070000}
ALERT> FRAUD[user=bob small=$0.10@1700000050000 large=$900.00@1700000070000 gap=20000ms]
txn> Event{carol, PURCHASE, 600.0, 1700000130000}
txn> Event{dave, PURCHASE, 999.0, 1700000140000}
txn> Event{alice, PURCHASE, 0.75, 1700000200000}
txn> Event{alice, PURCHASE, 20.0, 1700000210000}
txn> Event{alice, PURCHASE, 800.0, 1700000230000}
ALERT> FRAUD[user=alice small=$0.75@1700000200000 large=$800.00@1700000230000 gap=30000ms]
txn> Event{zzz, LOGIN, 0.0, 1700000999000}
```

Three alerts. Carol's $600 purchase produced none, because 90 seconds elapsed and her timer had already disarmed her. Dave's $999 produced none, because he was never armed.

(Interleaving between `txn>` and `ALERT>` lines can vary — they're separate print sinks. The alerts themselves are deterministic.)

## Step-by-step trace of state and timers

Timestamps shown as `t+Ns` from `1_700_000_000_000L`. The watermark lags by 5 s.

```
═══ t+0s   alice LOGIN ═══════════════════════════════════════════════════════
  key=alice. Not a PURCHASE -> return after pass-through.
  STATE: (empty everywhere)          TIMERS: {}

═══ t+10s  alice PURCHASE $0.50 ══════════════════════════════════════════════
  key=alice. flag=null -> not armed. $0.50 < $1.00 -> ARM.
  no previous timer to delete.
  flag[alice]=true, smallAmount[alice]=0.50, smallTs[alice]=t+10s
  register alice @ t+70s;  timerState[alice]=t+70s
  STATE: alice{flag=T, amt=0.50, ts=t+10s, timer=t+70s}
  TIMERS: { alice@t+70s }

═══ t+15s  bob PURCHASE $45.00 ═══════════════════════════════════════════════
  key=bob. flag=null. $45 is neither < $1 nor > $500 -> falls through, no-op.
  STATE: alice{...}                  TIMERS: { alice@t+70s }

═══ t+30s  alice PURCHASE $750.00 ════════════════════════════════════════════
  key=alice. flag=TRUE and $750 > $500  -> ***** FRAUD *****
  emit FraudAlert(alice, 0.50@t+10s, 750.00@t+30s, gap 20000ms) to side output
  cleanUp:
     pendingTimer = t+70s -> deleteEventTimeTimer(t+70s)   <- HAPPY PATH DELETE
     clear flag, timer, smallAmount, smallTs
  STATE: alice{} (empty)             TIMERS: {}   <- the timer never fires
  watermark now t+25s.

═══ t+40s  carol PURCHASE $0.25 ══════════════════════════════════════════════
  key=carol. ARM.
  flag[carol]=true, amt=0.25, ts=t+40s, register carol @ t+100s
  STATE: carol{flag=T, amt=0.25, ts=t+40s, timer=t+100s}
  TIMERS: { carol@t+100s }

═══ t+50s  bob PURCHASE $0.10 ════════════════════════════════════════════════
  key=bob. ARM.
  flag[bob]=true, amt=0.10, ts=t+50s, register bob @ t+110s
  STATE: carol{...}, bob{flag=T, amt=0.10, ts=t+50s, timer=t+110s}
  TIMERS: { carol@t+100s, bob@t+110s }

═══ t+60s  carol LOGIN ═══════════════════════════════════════════════════════
  key=carol. Not a PURCHASE -> return. Carol stays ARMED — correct, a login
  is not a transaction and must not disarm the rule.
  TIMERS: unchanged.

═══ t+70s  bob PURCHASE $900.00 ══════════════════════════════════════════════
  key=bob. flag=TRUE and $900 > $500 -> ***** FRAUD *****
  emit FraudAlert(bob, 0.10@t+50s, 900.00@t+70s, gap 20000ms)
  cleanUp: delete bob@t+110s, clear bob's state
  STATE: carol{...}                  TIMERS: { carol@t+100s }
  watermark now t+65s. carol@t+100s has NOT fired yet.

═══ t+130s carol PURCHASE $600.00 ════════════════════════════════════════════
  BEFORE processElement, the watermark advances to t+125s, which passes
  t+100s, so Flink fires the pending timer FIRST:

  --- onTimer(t+100s), key=carol -------------------------------------------
      cleanUp(ctx):
        pendingTimer=t+100s -> delete (no-op, this timer is firing)
        clear flag, timer, smallAmount, smallTs
      STATE: carol{} (empty)         TIMERS: {}
  --------------------------------------------------------------------------

  NOW processElement runs: key=carol, flag=null -> NOT armed.
  $600 is > $500 but there's no armed flag, and $600 is not < $1.00, so
  nothing happens. NO ALERT. Correct: 90s > the 1-minute window.
  STATE: (all empty)                 TIMERS: {}

═══ t+140s dave PURCHASE $999.00 ═════════════════════════════════════════════
  key=dave. flag=null -> not armed. $999 is not < $1.00 -> no-op.
  NO ALERT. Correct: a large purchase alone is not fraud.
  STATE: (empty)                     TIMERS: {}

═══ t+200s alice PURCHASE $0.75 ══════════════════════════════════════════════
  key=alice. ARM again. Alice's earlier state was fully cleaned, so this is
  indistinguishable from a first-time arm.
  flag[alice]=true, amt=0.75, ts=t+200s, register alice @ t+260s
  STATE: alice{flag=T, amt=0.75, ts=t+200s, timer=t+260s}
  TIMERS: { alice@t+260s }

═══ t+210s alice PURCHASE $20.00 ═════════════════════════════════════════════
  key=alice. flag=TRUE but $20 is NOT > $500 -> not the fraud branch.
  $20 is NOT < $1.00 -> not the arm branch.
  Falls through to case 3: NOTHING HAPPENS. Alice stays armed, her timer
  stays at t+260s. This is a deliberate rule choice — see "Variations".
  STATE: unchanged                   TIMERS: { alice@t+260s }

═══ t+230s alice PURCHASE $800.00 ════════════════════════════════════════════
  key=alice. flag=TRUE and $800 > $500 -> ***** FRAUD *****
  emit FraudAlert(alice, 0.75@t+200s, 800.00@t+230s, gap 30000ms)
  cleanUp: delete alice@t+260s, clear everything
  STATE: (empty)                     TIMERS: {}

═══ t+999s zzz LOGIN ═════════════════════════════════════════════════════════
  Pushes the watermark far forward. No timers pending, nothing fires.

FINAL: 3 alerts. State empty. Timers empty.
```

## What each mechanism bought you

Walk back through the trace and notice:

| Mechanism | What would break without it |
|---|---|
| `keyBy(userId)` | Bob's small purchase would arm Alice. Every user's state would collide. |
| `ValueState<Boolean>` | No memory of the small purchase across records. |
| `ValueState<Long>` timer handle | No way to call `deleteEventTimeTimer` — the timestamp is required. |
| Event-time timer | The flag never expires; carol's 90-seconds-later $600 would alert. |
| `deleteEventTimeTimer` on the happy path | Alice's t+70s timer would fire uselessly after the alert; with a re-arm it could disarm a *new* window early. |
| `cleanUp()` after alerting | The next $600 purchase would alert again off the same stale flag. |
| Side output | Alerts and transactions would be forced into one type and one downstream. |
| TTL backstop | A stalled watermark would strand armed flags forever. |

## The four state-lifecycle paths

Every armed flag exits through exactly one of these. Being able to name all four is what "understanding stateful streaming" means.

```
                    small purchase
                          |
                     ARM: flag=true, timer registered
                          |
        ┌─────────────────┼──────────────────┬──────────────────┐
        v                 v                  v                  v
  A. large purchase   B. timer fires    C. TTL expires    D. job savepoint
     within 1 min        (1 min of         (1 hour of        -> state and
        |                event time         wall clock,        timers restored
     ALERT               elapsed)           backstop)          intact; the
     cleanUp()             |                  |                pending timer
     delete timer        cleanUp()         entries vanish      still fires
        |                  |                  |                on restore
        v                  v                  v                  |
     state empty       state empty        state empty            v
                                                             continues normally
```

Path D is worth dwelling on. If you take a savepoint at t+45s while carol is armed with a timer at t+100s, and restore an hour later, carol is still armed and her timer still fires when the watermark reaches t+100s. Timers are checkpointed state (chapter 23), and that's what makes this correct rather than merely lucky.

## Variations you'll actually be asked for

### "Disarm on any intervening purchase"

Strict "immediately followed by" semantics. Add an `else` to case 3:

```java
// ---- 3. A mid-size purchase now BREAKS the pattern ----
if (Boolean.TRUE.equals(flagState.value())) {
    cleanUp(ctx);
}
```

### "N small purchases within the window"

Swap `ValueState<Boolean>` for `ValueState<Integer>` (a counter) or, if you need the individual amounts, `ListState<Double>` — and bound it (chapter 20).

```java
Integer count = smallCountState.value();
smallCountState.update((count == null ? 0 : count) + 1);
if (count != null && count >= 3) { /* alert on the pattern itself */ }
```

### "Small then large across *any* card, per user"

Already handled — `keyBy(e -> e.userId)` spans cards. To scope per card instead:

```java
.keyBy(e -> e.userId + "|" + e.cardId)
```

Be aware this multiplies your keyspace, which makes TTL more important, not less (chapter 22).

### "Alert on the small purchase too, provisionally"

Emit a low-confidence alert to a second side output when arming, and a high-confidence one when the large purchase lands. Two `OutputTag`s, two downstream sinks, two SLAs.

### "Use processing time instead"

Swap `registerEventTimeTimer` for `registerProcessingTimeTimer` and drop the watermark strategy. You gain "always fires, even if the stream is quiet" and lose replayability — a backfill produces different results every run. For fraud detection, where you may need to re-run against historical data to tune thresholds, **event time is the right call**.

## Production checklist for this job

```
[ ] maxParallelism set explicitly (env.setMaxParallelism)     -> ch.18
[ ] EmbeddedRocksDBStateBackend + incremental checkpoints     -> ch.18
[ ] TTL on every descriptor, with a RocksDB compact filter    -> ch.22
[ ] Watermark strategy has withIdleness                       -> ch.23
[ ] Every register is paired with a delete on the happy path  -> ch.23
[ ] Timer timestamp stored in ValueState                      -> ch.23
[ ] State descriptor names are stable and reviewed            -> ch.19
[ ] Alerts go to a side output, not the main stream           -> ch.24
[ ] Keyspace growth estimated: keys x bytes x safety factor   -> ch.22
[ ] Thresholds are configurable, not `static final` in prod
[ ] Alert volume is monitored — a rule change can flood downstream
```

The last two aren't Flink concerns, but they're the ones that page you at 3am.

## Remember

- Timers make state **self-cleaning**. That's often the real reason to use them, not timeout detection.
- Store the timer's timestamp in `ValueState<Long>` — `deleteEventTimeTimer` demands the exact value.
- Delete the timer on the **happy path** too, not just on expiry. Otherwise it fires later against a re-armed window.
- `OutputTag<T>` needs the trailing `{}`. It's an anonymous subclass, and it's how Flink recovers the erased generic type.
- `.getSideOutput(tag)` exists only on `SingleOutputStreamOperator`, not `DataStream`.
- `Boolean.TRUE.equals(x)` and `"LITERAL".equals(x)` are the null-safe comparison idioms.
- Pass records through on the main output and send alerts to a side output — one operator, two typed streams.
- Layer TTL under your timers as a backstop. Timers handle the expected paths; TTL handles the pathological ones.
- Event time for business rules. It replays identically, which matters when you tune the rule against history.

## Interview one-liners

- *"Design fraud detection in Flink."* → `keyBy(userId)` into a `KeyedProcessFunction`. A small transaction sets a `ValueState<Boolean>` flag and registers an event-time timer one minute out; a large transaction while flagged emits to a side output and deletes the timer; the timer clears the flag if no large transaction arrives.
- *"Why a timer rather than comparing timestamps?"* → The timer cleans the state up. A stored timestamp lives forever for users who never return, so state grows with lifetime users instead of active ones.
- *"Why store the timer timestamp in state?"* → `deleteEventTimeTimer` requires the exact timestamp and there's no delete-all-for-this-key, so you must remember what you registered.
- *"Why delete the timer after you alert?"* → It would otherwise fire against a window that's already resolved, and if the user re-arms in the meantime it disarms the new window early.
- *"Why a side output?"* → Alerts are a different type with a different downstream and a different SLA. One operator, two independently typed streams, no filtering or union hacks.
- *"Why the trailing `{}` on `OutputTag`?"* → It creates an anonymous subclass, and Java preserves generics on a declared superclass. That's how Flink recovers `<FraudAlert>` from the erased type.
- *"Event time or processing time here?"* → Event time. The rule is about the gap between transactions, and it must produce identical results on a replay when you re-tune thresholds against history.
- *"How does this survive a restart?"* → State and timers are both checkpointed. A user armed at savepoint time is still armed on restore, and their pending timer still fires when the watermark reaches it.
