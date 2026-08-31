# 42. Stateful Pattern Detection — Hand-Rolled CEP

The goal you asked for:

```
LOGIN → FAILED_LOGIN → FAILED_LOGIN → FAILED_LOGIN → 🚨 suspicious activity
```

Flink has a CEP library that expresses this declaratively (chapter 43). We do it **by hand first**, with `KeyedProcessFunction`, because once you can hand-roll it you understand exactly what CEP is doing for you — and you'll know when to skip it.

> **Key idea**
> Pattern detection is just three questions:
> **What do I remember?** (state) — **When do I forget it?** (timers/pruning) — **When do I shout?** (the condition).
> Everything else is plumbing.

---

## Two shapes of pattern

| Shape | Example | State you need |
|---|---|---|
| **Counting within a window** | 3 failed logins in 5 minutes | a list of timestamps |
| **Ordered sequence** | LOGIN → ADD_CARD → LARGE_PURCHASE | a "which step am I on" marker |

The first half of this chapter does the counting pattern, the second half does the sequence pattern. They need different state and different code.

---

# Part 1 — 3 failed logins within 5 minutes

## The specification, written down precisely

Write this before writing code. Every ambiguity here becomes a bug later.

```
Key:        userId
Trigger:    3 events of type "FAILED_LOGIN" whose timestamps all fall
            within any 5-minute sliding window
Reset:      a "LOGIN" event (successful) clears all recorded failures
Expiry:     a failure older than 5 minutes stops counting
Output:     an Alert to a SIDE OUTPUT (main stream carries normal traffic)
Time:       EVENT time, so replays are deterministic
After alert: clear the failures, so we don't alert on every subsequent failure
```

The last line matters. Without it, failures 4, 5, 6 each re-trigger and you page someone six times.

## The state design

```
ListState<Long> failures     — timestamps of recent FAILED_LOGIN events
ValueState<Long> cleanupTs   — the timestamp of our one pending cleanup timer
```

Why `ListState<Long>` and not `ValueState<Long>` (a count)?

A plain counter cannot answer "within 5 minutes". You'd know there were 3 failures but not *when*, so you couldn't expire the old ones. You need the timestamps.

Why not `ListState<Event>`? Because you only need the timestamp. Storing the full `Event` multiplies your state size for nothing. **Store the minimum that answers the question** — this is the single most important state-sizing habit.

```
ALERT WINDOW = 5 minutes.  Sliding, not fixed.

failures:      F1        F2              F3       F4
time:      ────●─────────●───────────────●────────●────►
               10:00     10:02           10:04    10:07

At F3 (10:04): failures within [09:59, 10:04] = {F1,F2,F3} = 3  🚨 ALERT
At F4 (10:07): after pruning, [10:02, 10:07] = {F4}      = 1  (F1,F2,F3 cleared by alert)
```

## The code

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class FailedLoginDetector
        extends KeyedProcessFunction<String, Event, Event> {
        //                            ^key    ^in    ^out (normal traffic passes through)

    // ── Tunables ─────────────────────────────────────────────────────────
    private final long windowMs;      // 5 minutes
    private final int  threshold;     // 3 failures

    public FailedLoginDetector(long windowMs, int threshold) {
        this.windowMs = windowMs;
        this.threshold = threshold;
    }

    /**
     * The side output channel for alerts.
     * `new OutputTag<Alert>("...") {}`  — note the trailing {}.
     * That creates an ANONYMOUS SUBCLASS, which is how Java preserves the generic
     * type at runtime so Flink can figure out the serializer. Omit the {} and you
     * get "could not determine TypeInformation" at job submission.
     */
    public static final OutputTag<Alert> ALERTS = new OutputTag<Alert>("fraud-alerts") {};

    // ── State ────────────────────────────────────────────────────────────
    /** Timestamps of recent failures for THIS user. Append-only list. */
    private transient ListState<Long> failures;

    /** Timestamp of the single pending cleanup timer, so we can delete it (ch. 39). */
    private transient ValueState<Long> cleanupTs;

    @Override
    public void open(OpenContext ctx) {
        // Types.LONG is Flink's TypeInformation for java.lang.Long — more explicit
        // and slightly faster than passing Long.class.
        failures = getRuntimeContext().getListState(
                new ListStateDescriptor<>("failureTimestamps", Types.LONG));

        cleanupTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cleanupTs", Types.LONG));
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<Event> out) throws Exception {

        // Always forward the raw event on the MAIN output. Detection is a side effect;
        // downstream consumers still want the traffic.
        out.collect(e);

        long now = ctx.timestamp();       // this event's event-time timestamp

        // ── RESET: a successful login wipes the slate ────────────────────
        if ("LOGIN".equals(e.type)) {
            // "LOGIN".equals(e.type) not e.type.equals("LOGIN"):
            // the constant on the left can never be null, so this can't NPE.
            failures.clear();
            cancelCleanup(ctx);
            return;
        }

        // ── Anything that isn't a failure is irrelevant ──────────────────
        if (!"FAILED_LOGIN".equals(e.type)) {
            return;
        }

        // ── RECORD the failure ───────────────────────────────────────────
        failures.add(now);

        // ── PRUNE anything older than the window, then count ─────────────
        long cutoff = now - windowMs;          // events strictly older than this don't count
        List<Long> kept = new ArrayList<>();

        for (Long ts : failures.get()) {       // ListState.get() returns an Iterable
            if (ts > cutoff) {
                kept.add(ts);
            }
        }

        // ── FIRE? ────────────────────────────────────────────────────────
        if (kept.size() >= threshold) {

            ctx.output(ALERTS, new Alert(
                    e.userId,
                    "BRUTE_FORCE",
                    kept.size() + " failed logins within " + (windowMs / 1000) + "s"));

            // Clear so the 4th, 5th, 6th failure don't each re-alert.
            failures.clear();
            cancelCleanup(ctx);
            return;
        }

        // ── Not enough yet: write back the pruned list ───────────────────
        // ListState.update(List) REPLACES the whole list. This is the only way
        // to remove entries — ListState has no remove().
        failures.update(kept);

        // ── Schedule expiry of the OLDEST kept failure ───────────────────
        // If no more events ever arrive for this user, this timer eventually
        // empties the list so we don't hold state forever.
        long oldest = kept.get(0);             // list is in insertion (time) order
        long expireAt = oldest + windowMs + 1;

        Long existing = cleanupTs.value();
        if (existing == null || existing != expireAt) {
            if (existing != null) {
                ctx.timerService().deleteEventTimeTimer(existing);
            }
            ctx.timerService().registerEventTimeTimer(expireAt);
            cleanupTs.update(expireAt);
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Event> out) throws Exception {

        // The watermark passed `ts`, so everything at or before ts - windowMs is dead.
        long cutoff = ts - windowMs;
        List<Long> kept = new ArrayList<>();
        for (Long t : failures.get()) {
            if (t > cutoff) kept.add(t);
        }

        cleanupTs.clear();      // this timer has fired; nothing to delete

        if (kept.isEmpty()) {
            failures.clear();                 // release the state entirely
        } else {
            failures.update(kept);
            // Re-arm for the new oldest entry.
            long next = kept.get(0) + windowMs + 1;
            ctx.timerService().registerEventTimeTimer(next);
            cleanupTs.update(next);
        }
    }

    /** Delete the pending cleanup timer, if any. Extracted because it's used 3 times. */
    private void cancelCleanup(Context ctx) throws Exception {
        Long t = cleanupTs.value();
        if (t != null) {
            ctx.timerService().deleteEventTimeTimer(t);
            cleanupTs.clear();
        }
    }
}
```

## Wiring it up

```java
public class FraudJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);          // parallelism 1 makes the printed trace readable

        DataStream<Event> events = env.fromElements(
                new Event("alice", "LOGIN",        0.0, 60_000L),   // 00:01:00
                new Event("alice", "FAILED_LOGIN", 0.0, 120_000L),  // 00:02:00
                new Event("alice", "FAILED_LOGIN", 0.0, 180_000L),  // 00:03:00
                new Event("bob",   "FAILED_LOGIN", 0.0, 190_000L),
                new Event("alice", "FAILED_LOGIN", 0.0, 240_000L),  // 00:04:00 -> ALERT
                new Event("alice", "LOGIN",        0.0, 300_000L),
                new Event("alice", "FAILED_LOGIN", 0.0, 900_000L)   // long after; no alert
        );

        DataStream<Event> timed = events.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.timestamp));

        // `SingleOutputStreamOperator` (not `DataStream`) is the return type you must
        // hold on to — it is the only one with getSideOutput().
        SingleOutputStreamOperator<Event> main =
                timed.keyBy(e -> e.userId)
                     .process(new FailedLoginDetector(5 * 60_000L, 3));

        main.getSideOutput(FailedLoginDetector.ALERTS).print("ALERT");

        env.execute("hand-rolled failed-login detection");
    }
}
```

## Step-by-step trace

Window = 5 min = 300 000 ms, threshold = 3. Following key `alice` only.

| # | event | ts | action | `failures` after | `cleanupTs` after | output |
|---|---|---|---|---|---|---|
| 1 | LOGIN | 60 000 | reset branch | `[]` | `null` | event forwarded |
| 2 | FAILED_LOGIN | 120 000 | add; cutoff = −180 000; kept = `[120000]`; size 1 < 3 | `[120000]` | 420 001 | event forwarded |
| 3 | FAILED_LOGIN | 180 000 | add; cutoff = −120 000; kept = `[120000, 180000]`; size 2 < 3 | `[120000,180000]` | 420 001 (unchanged) | event forwarded |
| 4 | FAILED_LOGIN | 240 000 | add; cutoff = −60 000; kept = `[120000,180000,240000]`; **size 3 ≥ 3** | `[]` (cleared) | `null` (timer deleted) | event + **🚨 ALERT** |
| 5 | LOGIN | 300 000 | reset branch | `[]` | `null` | event forwarded |
| 6 | FAILED_LOGIN | 900 000 | add; cutoff = 600 000; kept = `[900000]`; size 1 < 3 | `[900000]` | 1 200 001 | event forwarded |

Now the interesting case — what if event 5 (the successful LOGIN) hadn't happened and instead we got failures at 400 000 and 500 000?

| # | event | ts | cutoff (ts − 300 000) | kept after prune | outcome |
|---|---|---|---|---|---|
| 2 | FAILED | 120 000 | −180 000 | `[120000]` | 1 |
| 3 | FAILED | 400 000 | 100 000 | `[120000, 400000]` | 2 — 120 000 survives, it's > 100 000 |
| 4 | FAILED | 500 000 | 200 000 | `[400000, 500000]` | 2 — **120 000 pruned**, no alert |

Three failures happened, but not within any 5-minute window, so no alert. That's the pruning doing its job. A naive counter would have fired here — a false positive.

And the pure-timer case (no more events ever arrive):

```
failures = [120000], cleanupTs = 420001
   ... no more alice events ...
watermark reaches 420001  ->  onTimer(420001)
   cutoff = 120001;  120000 > 120001?  NO  ->  pruned
   kept = []  ->  failures.clear()
   STATE FOR ALICE IS NOW EMPTY   ✅ no leak
```

That last block is why the timer exists. Without it, every user who ever had one failed login keeps a `ListState` entry forever, and your job dies of state growth six months later.

## The four bugs this code is written to avoid

1. **Alert storm** — clearing on fire, so failure #4 doesn't re-alert.
2. **False positive on a stale count** — pruning by timestamp, not just counting.
3. **State leak** — the cleanup timer empties abandoned lists.
4. **Timer explosion** — exactly one pending cleanup timer per key, deleted before re-registering (ch. 39).

---

# Part 2 — An ordered sequence, with a state machine

Different pattern, different tool:

```
LOGIN → ADD_CARD → LARGE_PURCHASE, all within 10 minutes
```

A list of timestamps doesn't help here; you need to know **where in the sequence you are**. That's a state machine, and the state is a single enum.

## The state machine

```
             LOGIN                ADD_CARD            LARGE_PURCHASE
  ┌──────┐  ────────►  ┌──────────────┐  ────────►  ┌──────────┐  ────────►  🚨
  │ NONE │             │ SAW_LOGIN    │             │ SAW_CARD │
  └──────┘             └──────────────┘             └──────────┘
     ▲                        │                           │
     │                        │  timeout (10 min)         │  timeout
     └────────────────────────┴───────────────────────────┘
                         back to NONE
```

## The code

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class AccountTakeoverDetector
        extends KeyedProcessFunction<String, Event, Alert> {

    /**
     * A Java `enum` is a fixed set of named constants — a type-safe alternative to
     * int codes. Flink serializes enums efficiently (as their ordinal).
     */
    public enum Stage { NONE, SAW_LOGIN, SAW_CARD }

    private static final long WINDOW_MS = 10 * 60_000L;
    private static final double LARGE = 1000.0;

    public static final OutputTag<Alert> TIMEOUTS = new OutputTag<Alert>("seq-timeouts") {};

    private transient ValueState<Stage> stage;      // where in the sequence we are
    private transient ValueState<Long>  startTs;    // when the sequence started
    private transient ValueState<Long>  deadline;   // our pending timeout timer

    @Override
    public void open(OpenContext ctx) {
        stage    = getRuntimeContext().getState(new ValueStateDescriptor<>("stage", Stage.class));
        startTs  = getRuntimeContext().getState(new ValueStateDescriptor<>("startTs", Long.class));
        deadline = getRuntimeContext().getState(new ValueStateDescriptor<>("deadline", Long.class));
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<Alert> out) throws Exception {

        Stage s = stage.value();
        if (s == null) s = Stage.NONE;         // ValueState is null before first write

        long now = ctx.timestamp();

        // `switch` on an enum. Each `case` is a state of the machine.
        switch (s) {

            case NONE:
                if ("LOGIN".equals(e.type)) {
                    stage.update(Stage.SAW_LOGIN);
                    startTs.update(now);
                    arm(ctx, now + WINDOW_MS);
                }
                break;                          // any other event: stay in NONE

            case SAW_LOGIN:
                if ("ADD_CARD".equals(e.type)) {
                    stage.update(Stage.SAW_CARD);
                    // Deliberately do NOT re-arm: the 10 minutes is measured from
                    // the LOGIN, not from each step. Change this if your spec differs.
                } else if ("LOGIN".equals(e.type)) {
                    // A fresh login restarts the sequence from this point.
                    startTs.update(now);
                    arm(ctx, now + WINDOW_MS);
                }
                break;

            case SAW_CARD:
                if ("LARGE_PURCHASE".equals(e.type) && e.amount > LARGE) {
                    long elapsed = now - startTs.value();
                    out.collect(new Alert(e.userId, "ACCOUNT_TAKEOVER",
                            "LOGIN→ADD_CARD→LARGE_PURCHASE(" + e.amount + ") in "
                            + (elapsed / 1000) + "s"));
                    reset(ctx);                  // matched; start over
                } else if ("LOGIN".equals(e.type)) {
                    stage.update(Stage.SAW_LOGIN);
                    startTs.update(now);
                    arm(ctx, now + WINDOW_MS);
                }
                break;
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        // The 10-minute budget expired without completing the sequence.
        Stage s = stage.value();
        if (s != null && s != Stage.NONE) {
            // Optional: report partial matches. CEP calls these "timed-out matches" (ch.43).
            ctx.output(TIMEOUTS, new Alert(ctx.getCurrentKey(), "SEQ_TIMEOUT", "stopped at " + s));
        }
        reset(ctx);
    }

    /** Register a timeout, replacing any previous one. */
    private void arm(Context ctx, long at) throws Exception {
        Long old = deadline.value();
        if (old != null) ctx.timerService().deleteEventTimeTimer(old);
        ctx.timerService().registerEventTimeTimer(at);
        deadline.update(at);
    }

    /** Clear everything for this key — no leftover state, no leftover timer. */
    private void reset(Context ctx) throws Exception {
        Long old = deadline.value();
        if (old != null) ctx.timerService().deleteEventTimeTimer(old);
        stage.clear();
        startTs.clear();
        deadline.clear();
    }
    // Note: OnTimerContext extends Context, so reset(ctx) works from onTimer too.
}
```

## Trace

```
key = alice, WINDOW = 600 000 ms

t=0        LOGIN            stage NONE      -> SAW_LOGIN   startTs=0    timer@600000
t=100000   VIEW_PAGE        stage SAW_LOGIN -> SAW_LOGIN   (ignored)
t=200000   ADD_CARD         stage SAW_LOGIN -> SAW_CARD    timer still @600000
t=250000   LARGE_PURCHASE   stage SAW_CARD  -> 🚨 ALERT "in 250s"
                                            -> reset: stage/startTs/deadline cleared, timer deleted

t=0        LOGIN            -> SAW_LOGIN, timer@600000
t=300000   ADD_CARD         -> SAW_CARD
   (no purchase)
wm>600000  onTimer          -> side output SEQ_TIMEOUT "stopped at SAW_CARD", reset
```

## The design questions a sequence pattern forces on you

Answer these explicitly; CEP will ask you the same ones under different names (ch. 43).

| Question | This code's answer | Alternative |
|---|---|---|
| Do irrelevant events break the sequence? | No — `VIEW_PAGE` is ignored (**relaxed contiguity**) | Reset on any unexpected event (**strict contiguity**) |
| Is the 10 min measured from the first event or each step? | From the first (LOGIN) | Re-arm at every step |
| What if LOGIN happens twice? | Restart the clock from the newer one | Ignore, or track both |
| Can two overlapping sequences match? | No — one `ValueState`, one active attempt per key | CEP tracks multiple partial matches simultaneously |

That last row is the sharpest real difference between hand-rolled and CEP, and the next section is about it.

---

## Hand-rolled vs the CEP library

### Hand-rolled wins when…

- **You need full control over state size.** You can see exactly what's stored: one enum, two longs. You can prove it's bounded. CEP's NFA state is opaque and can grow in ways that surprise you.
- **The pattern is simple** — a count in a window, or a short fixed sequence. The code above is 80 lines and any engineer can read it.
- **You need custom side effects** — emitting partial progress, updating a counter, calling out at intermediate steps. CEP only gives you completed matches and timeouts.
- **You want ordinary debugging.** Breakpoints, log lines, and a trace table like the ones above. CEP's NFA is a black box at runtime.
- **The rules must change at runtime.** Combine your hand-rolled detector with broadcast state (ch. 41) and thresholds become configurable. **CEP patterns cannot be changed without a redeploy** — the pattern is compiled into the job graph.
- **You want exactly one active attempt per key.** That's the natural hand-rolled behaviour and it's often what the business actually wants.

### CEP wins when…

- **The pattern is complex** — `A followedBy B{2,5} followedByAny C? within 10 minutes` is one readable expression in CEP and a nightmare of nested state by hand.
- **You need all overlapping matches.** In `A B A B`, CEP with `followedBy` finds multiple `A→B` matches; your `ValueState` machine finds one at a time. If the business wants every match, hand-rolling this correctly is genuinely hard.
- **Quantifiers and optionality** — `oneOrMore()`, `optional()`, `greedy()` are painful to hand-code and easy to get subtly wrong.
- **Readability for non-authors** — a `Pattern` chain reads like the requirement. A state machine reads like code.
- **You'd otherwise reinvent it badly.** Hand-rolling `times(2,5).optional().greedy()` semantics correctly is a research problem, not an afternoon.

### The rule of thumb

> **Key idea**
> Fixed-length sequences and counting patterns: **hand-roll**. You get control, bounded state, runtime configurability, and debuggability.
> Variable-length, quantified, or overlapping-match patterns: **use CEP**. Reimplementing an NFA is not a good use of your week.

Both are event-time correct, both are checkpointed, both scale by key. The choice is about expressiveness versus control, not about performance.

---

## Remember

- Pattern detection = what do I remember, when do I forget, when do I shout.
- Store the **minimum** that answers the question — timestamps, not whole events.
- `ListState` has no `remove()`. To prune, read into a `List`, filter, and `update(list)`.
- Always clear state after firing, or you get an alert storm.
- Always have a cleanup timer, or abandoned keys leak state forever.
- One pending timer per key, deleted before re-registering.
- Side outputs (`OutputTag<Alert>("name") {}` — keep the `{}`) keep alerts off the main stream; retrieve with `getSideOutput` on the `SingleOutputStreamOperator`.
- `"CONST".equals(x)` rather than `x.equals("CONST")` — can't NPE.
- Counting patterns → `ListState<Long>` + prune. Ordered sequences → an enum in `ValueState` + a state machine.
- Hand-rolled gives control, bounded state, runtime-configurable thresholds, and debuggability; CEP gives expressiveness for complex and overlapping patterns.

## Interview one-liners

- *"How would you detect 3 failed logins in 5 minutes?"* → `KeyedProcessFunction` keyed by user, `ListState<Long>` of failure timestamps, prune by `now − window` on each event, alert to a side output when size ≥ 3, clear on alert and on successful login, plus an event-time cleanup timer so abandoned keys release state.
- *"Why a list of timestamps rather than a counter?"* → A counter can't expire old entries, so you'd fire on three failures spread over three hours.
- *"How do you stop alert storms?"* → Clear the state on fire, so subsequent failures start a new window.
- *"How do you stop state leaking?"* → One event-time cleanup timer per key that prunes and clears empty state; delete and re-register it rather than adding one per event.
- *"How do you model an ordered sequence?"* → A state machine: an enum in `ValueState`, transitions in `processElement`, and a timer for the overall deadline.
- *"When would you hand-roll instead of using CEP?"* → Simple, fixed-shape patterns where you want bounded, inspectable state, custom intermediate side effects, and runtime-configurable thresholds via broadcast state — CEP patterns need a redeploy to change.
- *"When would CEP beat hand-rolling?"* → Quantifiers, optional steps, and overlapping matches; reimplementing NFA semantics by hand is error-prone.
- *"What's the difference between your enum machine and CEP on `A B A B`?"* → One `ValueState` tracks one attempt at a time; CEP tracks all partial matches concurrently and can emit several.
