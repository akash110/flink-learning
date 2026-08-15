# Phase 6 — Advanced event processing (timers, CEP, sessionization, fraud)

This phase is where the Phase 3 toolkit (state + timers + `KeyedProcessFunction`) becomes real *patterns*. Plus Flink's dedicated pattern-matching library, **CEP**.

---

## 1. Timers, revisited in depth

You met timers in Phase 3. The nuances that matter now:

### Event-time vs processing-time timers

| | Event-time timer | Processing-time timer |
|---|---|---|
| Fires when | **watermark** passes the timer time | **wall clock** passes the timer time |
| Deterministic? | yes — same on replay | no — depends on when you run |
| Use for | business logic ("30s after the event happened") | operational ("flush every 10s of real time") |

```java
ctx.timerService().registerEventTimeTimer(e.timestamp + 30_000);
ctx.timerService().registerProcessingTimeTimer(
        ctx.timerService().currentProcessingTime() + 10_000);
```

### Timer facts that trip people up
- Timers are **keyed** — one timer namespace *per key*. `onTimer` runs with the current key scoped.
- **Deduplication:** registering a timer for a timestamp that already has one is a no-op (only one fires). So the "delete old, register new" dance in the last-activity example is how you *move* a timer.
- `deleteEventTimeTimer(t)` / `deleteProcessingTimeTimer(t)` remove a pending timer.
- Timers are **checkpointed** — they survive restarts and fire after recovery.
- In `onTimer` you have full access to state, side outputs, and the collector — you can emit results there, not just in `processElement`.

---

## 2. `KeyedProcessFunction` patterns (the "manual CEP")

Most real-world "advanced" logic is just a small state machine you drive by hand. Three canonical patterns:

### Pattern A — timeout / "did X *not* happen?"
Register a timer on event X; if the expected follow-up Y arrives, cancel the timer; if the timer fires first, Y never came → emit an alert. (E.g., "order placed but not paid within 15 min.")

### Pattern B — sequence detection ("A then B within T")
Store a flag/timestamp when A is seen; when B arrives, check the flag and the time gap; emit if the pattern holds. (This is your fraud detector.)

### Pattern C — dedup / first-seen
`ValueState<Boolean> seen` — emit only the first event per key, ignore repeats (optionally with TTL so "first per day").

All three are 20–40 lines of `KeyedProcessFunction`. Reach for CEP (§4) only when the pattern gets genuinely complex (quantifiers, alternations, multiple optional steps).

---

## 3. Sessionization (rolling your own, and when to use the built-in)

**Sessionization** = grouping a key's events into bursts of activity separated by idle gaps.

- **Built-in:** `EventTimeSessionWindows.withGap(Time.minutes(30))` (Phase 2 §6). Use this when you just need an aggregate per session.
- **Hand-rolled with `KeyedProcessFunction` + timer:** use when you need custom session semantics — dynamic gaps, emitting partial sessions, session IDs, or combining sessionization with other stateful logic.

Hand-rolled skeleton (extends the Phase 3 "last activity" idea into full sessions):

```java
public class Sessionizer extends KeyedProcessFunction<String, Event, String> {
    private transient ValueState<Long>  sessionStart;
    private transient ValueState<Long>  lastSeen;
    private transient ValueState<Long>  currentTimer;
    private static final long GAP = 30 * 60_000L;   // 30 min

    @Override public void open(Configuration c) {
        sessionStart = state("sessionStart");
        lastSeen     = state("lastSeen");
        currentTimer = state("timer");
    }
    private ValueState<Long> state(String n) {
        return getRuntimeContext().getState(new ValueStateDescriptor<>(n, Types.LONG));
    }

    @Override public void processElement(Event e, Context ctx, Collector<String> out)
            throws Exception {
        if (sessionStart.value() == null) sessionStart.update(e.timestamp);
        lastSeen.update(e.timestamp);

        Long old = currentTimer.value();
        if (old != null) ctx.timerService().deleteEventTimeTimer(old);
        long t = e.timestamp + GAP;
        ctx.timerService().registerEventTimeTimer(t);
        currentTimer.update(t);
    }

    @Override public void onTimer(long ts, OnTimerContext ctx, Collector<String> out)
            throws Exception {
        out.collect("session key=" + ctx.getCurrentKey()
                + " start=" + sessionStart.value() + " end=" + lastSeen.value());
        sessionStart.clear(); lastSeen.clear(); currentTimer.clear();
    }
}
```

---

## 4. Complex Event Processing (CEP) — the FlinkCEP library

When patterns get complex — "3 failed logins followed by a success, within 5 minutes, from the same user" — the manual state machine gets ugly. **FlinkCEP** lets you *declare* the pattern.

### Dependency
```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cep</artifactId>
    <version>${flink.version}</version>
</dependency>
```

### Example: small "test" txn then large txn within 1 minute (declarative fraud)
```java
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.PatternStream;

Pattern<Event, ?> fraud = Pattern.<Event>begin("small")
        .where(new SimpleCondition<Event>() {
            @Override public boolean filter(Event e) { return e.amount <= 1; }
        })
        .next("large")                                   // strictly next (contiguous)
        .where(new SimpleCondition<Event>() {
            @Override public boolean filter(Event e) { return e.amount >= 500; }
        })
        .within(Time.minutes(1));                         // time constraint

DataStream<Event> keyed = events.keyBy(Event::getUser);  // CEP is per-key when keyed
PatternStream<Event> ps = CEP.pattern(keyed, fraud);

DataStream<String> alerts = ps.select(match -> {
    Event small = match.get("small").get(0);
    Event large = match.get("large").get(0);
    return "FRAUD user=" + large.user + " small=" + small.amount + " large=" + large.amount;
});
```

### Pattern vocabulary you should recognize
- `begin("a")` → start; `.next("b")` strict contiguity; `.followedBy("b")` relaxed (other events allowed between); `.followedByAny("b")` most relaxed.
- Quantifiers: `.times(3)`, `.oneOrMore()`, `.optional()`, `.timesOrMore(2)`.
- `.where(...)` condition; `.or(...)`; **iterative conditions** can reference already-matched events.
- `.within(Time.minutes(1))` time window for the whole pattern.
- **Timed-out partial matches** are retrievable via a side output on `select`/`flatSelect` — this is how you detect "A happened but B never did" declaratively.

### CEP vs manual `KeyedProcessFunction`
- **CEP:** concise for complex sequences/quantifiers; readable; handles the state machine for you.
- **Manual:** more control, easier to combine with other state, no extra dependency, often easier to reason about performance. For simple "A then B" many teams just write the `KeyedProcessFunction`.

Learn both; pick per-pattern-complexity.

---

## 5. Broadcast state (dynamic rules) — worth knowing here

A frequent advanced need: **rules that change at runtime** (fraud thresholds, feature flags) applied to a high-volume stream. Pattern: **broadcast** a low-volume rules stream to every subtask and join it against the main keyed stream.

```java
MapStateDescriptor<String, Rule> rulesDesc =
    new MapStateDescriptor<>("rules", Types.STRING, Types.POJO(Rule.class));

BroadcastStream<Rule> rules = ruleStream.broadcast(rulesDesc);

txns.keyBy(t -> t.user)
    .connect(rules)
    .process(new KeyedBroadcastProcessFunction<>() {
        // processElement: read current rules from broadcast state, apply to txn
        // processBroadcastElement: update the rules in broadcast state
    });
```

This lets you change detection logic **without redeploying** the job.

---

## 6. Full fraud example, leveled up

Your Phase 3 detector was a 2-step sequence. A realistic detector combines several signals — do it as a `KeyedProcessFunction` state machine (velocity + amount + geo) or as a CEP pattern with `.times()` and `.within()`. Suggested exercise:

1. Port the Phase 3 fraud detector to **CEP** (as in §4). Confirm identical alerts.
2. Add a **timed-out** branch: alert differently if a small txn is seen but *no* large txn follows within the minute (uses CEP timeout side output — or, manually, the timer's `onTimer`).
3. Add **velocity**: > 5 transactions in 10 seconds → alert (count + timer, or CEP `.times(5).within(...)`).

---

### ✅ Phase 6 checklist

- [ ] Event-time vs processing-time timers (+ dedup/delete rules)
- [ ] `KeyedProcessFunction` patterns: timeout, sequence, dedup
- [ ] Sessionization (built-in vs hand-rolled)
- [ ] FlinkCEP: `begin/next/followedBy`, quantifiers, `within`, timeouts
- [ ] CEP vs manual — when to use which
- [ ] Broadcast state for dynamic rules
- [ ] Leveled-up fraud detector

⬅️ [Phase 5](05-reliability.md)  ·  ➡️ [Phase 7 — Flink SQL & Table API](07-sql-table-api.md)
