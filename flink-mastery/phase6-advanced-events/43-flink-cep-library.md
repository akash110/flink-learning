# 43. The Flink CEP Library

Chapter 42 hand-rolled the failed-login detector in ~80 lines of state machinery. This chapter expresses the same thing in about 12 lines — and then tells you honestly what you gave up.

> **Key idea**
> CEP (Complex Event Processing) lets you **declare** a pattern of events rather than **implement** the state machine that recognises it.
> Under the hood Flink compiles your `Pattern` into an NFA (non-deterministic finite automaton) and runs it per key, keeping partial matches in keyed state.

---

## The dependency

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cep</artifactId>
    <version>1.20.0</version>
</dependency>
```

Not bundled with `flink-streaming-java`; you must add it. It **is** shipped in the Flink distribution's `opt/` directory, so for a cluster deploy you can copy `flink-cep-1.20.0.jar` into `lib/` instead of fat-jarring it — but fat-jarring is simpler and fine.

---

## The five-step shape of every CEP job

```
1.  Pattern<Event, ?> pattern = Pattern.<Event>begin("name").where(...)...
2.  PatternStream<Event> ps   = CEP.pattern(keyedStream, pattern);
3.  DataStream<Alert> out     = ps.process(new PatternProcessFunction<>() {...});
```

```
       DataStream<Event>
              │ keyBy(userId)          <- ALWAYS key it. Un-keyed CEP runs at parallelism 1.
              ▼
        KeyedStream<Event, String>  ────┐
                                        ├──► CEP.pattern(stream, pattern) ──► PatternStream
        Pattern<Event, ?>           ────┘                                          │
                                                                                   ▼
                                                          .process(PatternProcessFunction)
                                                                  │
                                                     Map<String, List<Event>>  per match
```

---

## Building a pattern

### `begin`, `where`, `next`

```java
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

Pattern<Event, Event> p = Pattern
        // <Event> is an explicit type witness — Java can't infer it from `begin`.
        // "first" is the PATTERN NAME. You use it later to pull events out of the match.
        .<Event>begin("first")
        .where(SimpleCondition.of(e -> "LOGIN".equals(e.type)))

        .next("second")     // strict: the very NEXT event must satisfy this
        .where(SimpleCondition.of(e -> "FAILED_LOGIN".equals(e.type)));
```

`SimpleCondition.of(lambda)` is the modern factory (Flink 1.16+). Older code you'll see online uses:

```java
.where(new SimpleCondition<Event>() {
    @Override
    public boolean filter(Event e) { return "LOGIN".equals(e.type); }
})
```

Same thing, more typing. `SimpleCondition.of` is preferred.

---

## Contiguity — `next` vs `followedBy` vs `followedByAny`

This is the concept that decides whether your pattern works. Get it precisely.

Input sequence for one key:

```
index:   0      1      2      3      4
event:   A      X      B      X      B
```

Pattern: `begin("a").where(=A).<CONTIGUITY>("b").where(=B)`

| Contiguity | Method | Meaning | Matches found |
|---|---|---|---|
| **Strict** | `.next("b")` | B must be the **immediately following** event. No gaps. | **none** — index 1 is X, not B |
| **Relaxed** | `.followedBy("b")` | Skip non-matching events, but stop at the **first** B. | `[A@0, B@2]` — one match |
| **Non-deterministic relaxed** | `.followedByAny("b")` | Skip non-matching events, and try **every** later B. | `[A@0, B@2]` **and** `[A@0, B@4]` — two matches |

A cleaner way to see the difference — input `A B1 B2`:

```
pattern: begin("a")=A  ─?─►  "b"=B

.next("b")          ->  [A, B1]           strict: B1 is immediately next ✅, B2 never considered
.followedBy("b")    ->  [A, B1]           relaxed: first B wins, then this match is done
.followedByAny("b") ->  [A, B1], [A, B2]  every B starts its own match
```

### The rules of thumb

- **`next`** — for genuinely adjacent events. Rare in practice, because real streams are noisy: any unrelated event breaks the match. Use it when "nothing may happen in between" is part of the requirement (e.g. `LOGOUT` immediately after `LOGIN` with nothing between).
- **`followedBy`** — the default choice. "Eventually, and I only care about the first one."
- **`followedByAny`** — **combinatorial explosion risk.** N candidate A's × M candidate B's = N×M matches, all held in state. Only use it when you truly need every combination, and always pair it with `within(...)`.

### The negative forms

```java
.notNext("x").where(...)        // the immediately next event must NOT match
.notFollowedBy("x").where(...)  // no matching event may occur in between
```

`notFollowedBy` **cannot be the last element** of a pattern — the NFA would never know when to stop waiting — unless it's followed by a `within()`. Flink throws at pattern-construction time if you get this wrong.

Genuinely useful shape: *"a large purchase not preceded by a 2FA confirmation."*

```java
Pattern.<Event>begin("login").where(SimpleCondition.of(e -> "LOGIN".equals(e.type)))
       .notFollowedBy("2fa").where(SimpleCondition.of(e -> "TWO_FA_OK".equals(e.type)))
       .followedBy("purchase").where(SimpleCondition.of(e -> "PURCHASE".equals(e.type)))
       .within(Duration.ofMinutes(5));
```

---

## Quantifiers

Applied to the pattern element you just named.

```java
.times(3)                 // exactly 3 occurrences
.times(2, 5)              // between 2 and 5 (inclusive)
.oneOrMore()              // 1..∞
.timesOrMore(3)           // 3..∞
.optional()               // 0 or 1 — makes the whole preceding quantifier optional too
.greedy()                 // consume as many as possible before moving on
.allowCombinations()      // inside a quantifier, use followedByAny semantics
.consecutive()            // inside a quantifier, use next (strict) semantics
```

The one that matters most for our use case:

```java
Pattern.<Event>begin("failures")
       .where(SimpleCondition.of(e -> "FAILED_LOGIN".equals(e.type)))
       .times(3)                       // 3 failed logins
       .within(Duration.ofMinutes(5)); // within 5 minutes
```

### Contiguity *inside* a quantifier

By default `times(3)` uses **relaxed** contiguity between its own occurrences — other events may appear between the failures. That's usually what you want.

```
input:  F1  VIEW  F2  VIEW  F3

.times(3)                  -> matches [F1,F2,F3]      (relaxed, the default)
.times(3).consecutive()    -> NO match                (VIEW breaks strictness)
.times(3).allowCombinations() -> matches every 3-subset of the F's
```

### `optional()` and `greedy()`

```java
.followedBy("mid").where(...).optional()   // this step may be skipped entirely
.followedBy("many").where(...).oneOrMore().greedy()  // take as many as you can
```

`greedy()` matters when a later element could also match the same events. Without it, `oneOrMore()` emits a match at every length (1, 2, 3, …) — which is often a surprise flood of output. With `greedy()`, you get only the longest. `greedy()` is not supported on the last element of a pattern.

---

## `within(Duration)` — the time bound

```java
.within(Duration.ofMinutes(5))
```

Applies to the **whole pattern by default**: the time from the first matched event to the last must be ≤ 5 minutes. Since Flink 1.16 you can also scope it to a single step:

```java
.within(Duration.ofMinutes(5), WithinType.PREVIOUS_AND_CURRENT)  // gap between adjacent steps
.within(Duration.ofMinutes(5), WithinType.FIRST_AND_LAST)        // whole pattern (the default)
```

Older Flink used `within(Time.minutes(5))`; `org.apache.flink.streaming.api.windowing.time.Time` is deprecated in 1.18+ in favour of `java.time.Duration`. Use `Duration`.

> **Key idea**
> `within()` is not optional in practice. Without it, a partial match (e.g. "saw one failed login, waiting for two more") is kept in state **forever**. `within()` is what lets Flink prune the NFA state. Always set it.

---

## `SimpleCondition` vs `IterativeCondition`

```java
// SimpleCondition: decide from THIS event alone.
SimpleCondition.of(e -> e.amount > 1000)

// IterativeCondition: decide using events already matched in this partial match.
new IterativeCondition<Event>() {
    @Override
    public boolean filter(Event e, Context<Event> ctx) throws Exception {
        // ctx.getEventsForPattern("name") returns the events matched so far
        // under that pattern name, as an Iterable.
        double sum = 0.0;
        for (Event prev : ctx.getEventsForPattern("failures")) {
            sum += prev.amount;
        }
        return e.amount > sum * 2;      // this event is more than twice the running sum
    }
}
```

`IterativeCondition` is what lets you express "bigger than the average of the previous ones", "a different IP from the first login", "escalating amounts". You cannot do that with `SimpleCondition`.

Concrete example — an escalating-amount pattern:

```java
Pattern<Event, ?> escalating = Pattern
        .<Event>begin("txns")
        .where(SimpleCondition.of(e -> "PURCHASE".equals(e.type)))
        .oneOrMore()
        .where(new IterativeCondition<Event>() {
            @Override
            public boolean filter(Event e, Context<Event> ctx) throws Exception {
                double max = 0.0;
                for (Event prev : ctx.getEventsForPattern("txns")) {
                    if (prev.amount > max) max = prev.amount;
                }
                // Only extend the match if this purchase is bigger than all previous.
                return e.amount > max;
            }
        })
        .times(3).greedy()
        .within(Duration.ofMinutes(10));
```

Cost note: `IterativeCondition` reads previously matched events from state on **every candidate event**. It is measurably more expensive than `SimpleCondition`. Prefer `SimpleCondition` when it suffices.

Also note: **`.where()` chained twice is AND**; use `.or(...)` for OR:

```java
.where(SimpleCondition.of(e -> "PURCHASE".equals(e.type)))
.where(SimpleCondition.of(e -> e.amount > 100))     // AND
.or(SimpleCondition.of(e -> "REFUND".equals(e.type)))  // OR with the whole preceding condition
```

---

## From `Pattern` to output

```java
PatternStream<Event> ps = CEP.pattern(events.keyBy(e -> e.userId), pattern);
```

Then either `.select(...)` (older, simpler) or `.process(...)` (preferred — gives you a `Context` and side outputs).

### The match map

Both hand you a `Map<String, List<Event>>`:

- **key** = the pattern name you passed to `begin`/`next`/`followedBy`
- **value** = the events that matched that name, **always a `List`**, even for a non-quantified step (then it has exactly one element)

```java
Map<String, List<Event>> match = ...;
Event login  = match.get("login").get(0);           // single-occurrence step
List<Event> failures = match.get("failures");       // quantified step: 3 elements
```

`match.get("name")` returns `null` if that name is `optional()` and didn't match. Guard it.

### `.process(PatternProcessFunction)`

```java
import org.apache.flink.cep.functions.PatternProcessFunction;

DataStream<Alert> alerts = ps.process(new PatternProcessFunction<Event, Alert>() {
    @Override
    public void processMatch(Map<String, List<Event>> match,
                             Context ctx,
                             Collector<Alert> out) {

        List<Event> fails = match.get("failures");
        Event first = fails.get(0);
        Event last  = fails.get(fails.size() - 1);

        out.collect(new Alert(first.userId, "BRUTE_FORCE",
                fails.size() + " failures in " + (last.timestamp - first.timestamp) + "ms"));

        // ctx.timestamp()          -> timestamp of the match
        // ctx.currentProcessingTime()
        // ctx.output(tag, value)   -> side outputs
    }
});
```

### `.select(PatternSelectFunction)` — the short form

```java
DataStream<Alert> alerts = ps.select(
        (Map<String, List<Event>> match) -> {
            Event first = match.get("failures").get(0);
            return new Alert(first.userId, "BRUTE_FORCE", "3 failures");
        });
```

Fine for simple cases. No `Context`, so no side outputs and no timestamps. Use `.process` in real jobs.

---

## Timed-out partial matches

A partial match that never completed before `within()` expired is normally **discarded silently**. Often you want it — "someone started a takeover sequence and abandoned it" is itself a signal.

### Option A — `TimedOutPartialMatchHandler`

Implement the interface **in addition to** extending `PatternProcessFunction`:

```java
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;

public static final OutputTag<Alert> TIMED_OUT = new OutputTag<Alert>("timed-out") {};

public class Handler extends PatternProcessFunction<Event, Alert>
                     implements TimedOutPartialMatchHandler<Event> {

    @Override
    public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<Alert> out) {
        out.collect(new Alert(match.get("first").get(0).userId, "MATCH", "completed"));
    }

    @Override
    public void processTimedOutMatch(Map<String, List<Event>> match, Context ctx) {
        // NOTE: no Collector here. A timed-out match can ONLY go to a side output.
        Event e = match.get("first").get(0);
        ctx.output(TIMED_OUT, new Alert(e.userId, "PARTIAL", "abandoned partial match"));
    }
}
```

Retrieve it with `alerts.getSideOutput(TIMED_OUT)`.

### Option B — `select` with a timeout tag

```java
OutputTag<Alert> timedOut = new OutputTag<Alert>("timed-out") {};

SingleOutputStreamOperator<Alert> result = ps.select(
        timedOut,
        (Map<String, List<Event>> partial, long timeoutTs) ->
                new Alert("?", "PARTIAL", "timed out at " + timeoutTs),   // PatternTimeoutFunction
        (Map<String, List<Event>> full) ->
                new Alert("?", "MATCH", "completed"));                     // PatternSelectFunction

DataStream<Alert> timeouts = result.getSideOutput(timedOut);
```

Both require `within()` on the pattern — with no time bound there is no such thing as a timeout.

---

## Side by side: the same failed-login pattern

### CEP version — complete job

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class CepFailedLoginJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> events = env.fromElements(
                new Event("alice", "LOGIN",        0.0,  60_000L),
                new Event("alice", "FAILED_LOGIN", 0.0, 120_000L),
                new Event("alice", "FAILED_LOGIN", 0.0, 180_000L),
                new Event("alice", "FAILED_LOGIN", 0.0, 240_000L),
                new Event("bob",   "FAILED_LOGIN", 0.0, 190_000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.timestamp));

        // ── THE ENTIRE DETECTION LOGIC ──────────────────────────────────
        Pattern<Event, ?> bruteForce = Pattern
                .<Event>begin("failures")
                .where(SimpleCondition.of(e -> "FAILED_LOGIN".equals(e.type)))
                .times(3)
                .within(Duration.ofMinutes(5));
        // ────────────────────────────────────────────────────────────────

        PatternStream<Event> ps = CEP.pattern(events.keyBy(e -> e.userId), bruteForce);

        ps.process(new PatternProcessFunction<Event, Alert>() {
            @Override
            public void processMatch(Map<String, List<Event>> match,
                                     Context ctx, Collector<Alert> out) {
                List<Event> f = match.get("failures");
                out.collect(new Alert(f.get(0).userId, "BRUTE_FORCE",
                        "3 failed logins in "
                        + (f.get(2).timestamp - f.get(0).timestamp) / 1000 + "s"));
            }
        }).print("ALERT");

        env.execute("CEP failed logins");
    }
}
```

### The comparison

| | Hand-rolled (ch. 42) | CEP (this chapter) |
|---|---|---|
| Lines for the detection logic | ~80 | ~5 |
| State you can name and size | `ListState<Long>` + `ValueState<Long>` — obvious | NFA state — opaque |
| Reset on a successful `LOGIN` | one `if` block | **no direct equivalent** — see below |
| Suppress alert storms | `failures.clear()` after firing | controlled by `AfterMatchSkipStrategy` |
| Change the threshold at runtime | broadcast state (ch. 41) → yes | **no — requires redeploy** |
| Emit partial progress | trivially | only completed + timed-out matches |
| Debuggability | breakpoints and log lines | hard; the NFA is a black box |
| Overlapping matches | one at a time | all of them, automatically |
| Correctness of tricky semantics | you must get it right | Flink got it right |

**The reset problem is instructive.** "A successful login clears the failure count" is one line in the hand-rolled version. In CEP you'd express it as:

```java
Pattern.<Event>begin("failures")
       .where(SimpleCondition.of(e -> "FAILED_LOGIN".equals(e.type)))
       .times(3).consecutive()          // strict between failures
       .within(Duration.ofMinutes(5));
```

`.consecutive()` makes the three failures strictly adjacent, so *any* intervening event — including a successful `LOGIN` — breaks the match. That achieves the reset, but as a side effect it also breaks on a `VIEW_PAGE` event, which you probably didn't want. Getting exactly "reset on LOGIN but ignore everything else" needs a `notFollowedBy` or a pre-filter:

```java
// Pre-filter so only login-related events reach CEP; now .consecutive() means
// what we want.
DataStream<Event> loginEvents = events.filter(
        e -> "LOGIN".equals(e.type) || "FAILED_LOGIN".equals(e.type));
```

That pre-filter trick is the standard workaround, and it's worth remembering: **shaping the input stream is often easier than complicating the pattern.**

### `AfterMatchSkipStrategy` — CEP's answer to alert storms

Passed as a second argument to `begin`:

```java
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;

Pattern.<Event>begin("failures", AfterMatchSkipStrategy.skipPastLastEvent())
       .where(...)
       .times(3)
       .within(Duration.ofMinutes(5));
```

| Strategy | After a match, discard partial matches that… |
|---|---|
| `noSkip()` (default) | nothing is discarded — every overlapping match is emitted |
| `skipToNext()` | start at the same event as the emitted match |
| `skipPastLastEvent()` | started before the **end** of the emitted match — the strongest suppression |
| `skipToFirst(name)` | started before the first event mapped to `name` |
| `skipToLast(name)` | started before the last event mapped to `name` |

With the default `noSkip()`, failures F1..F5 produce matches `[F1F2F3]`, `[F2F3F4]`, `[F3F4F5]` — three alerts. `skipPastLastEvent()` gives you one. **This is the single most common CEP surprise**: people report "my CEP job alerts too much" and the fix is a skip strategy.

---

## Honest caveats

**1. State growth is not obvious.** Every partial match is state. `followedByAny` + `oneOrMore` + a loose `within` can hold an enormous number of partial matches per key. You cannot look at a `ValueStateDescriptor` and reason about size the way you can with hand-rolled state. Symptoms: checkpoint size climbing with no code change, on a day when input shape changed.

**Mitigations:** always `within()`, prefer `followedBy` over `followedByAny`, use `.consecutive()` where the semantics allow, use an `AfterMatchSkipStrategy`, and pre-filter the input stream so fewer events are candidates.

**2. NFA complexity is real.** Patterns compose in ways that surprise. `optional()` next to `oneOrMore()` next to `followedByAny()` produces semantics that are hard to predict from reading the code. Write unit tests with `env.fromElements(...)` for every pattern and assert on the exact matches.

**3. Debugging is hard.** No breakpoint inside the NFA. You cannot ask "what partial matches are alive for alice right now". You debug by feeding small crafted sequences and observing output. Budget time for this.

**4. No dynamic pattern changes.** The `Pattern` is compiled into the job graph at submission. Changing a threshold, a time bound, or a step means a new jar and a savepoint restart. There is no broadcast-state equivalent for CEP patterns. This is the biggest strategic difference from ch. 41's approach, and often the deciding factor in production: a fraud team that wants to tune rules hourly cannot use plain CEP.

**5. `within()` and watermarks interact.** Matches and timeouts fire on watermark advance, exactly like timers (ch. 39). An idle source means no matches emitted. Same fix: `withIdleness`.

**6. Keyed only, practically.** `CEP.pattern` on a non-keyed stream compiles, but runs at parallelism 1 and treats the whole stream as one sequence. Always `keyBy` first.

**7. Ordering.** CEP sorts events by timestamp within the watermark bound before feeding the NFA, so out-of-order events are handled — but events later than the watermark are dropped as late. Set your out-of-orderness generously enough.

---

## Remember

- Add `flink-cep`; it isn't in `flink-streaming-java`.
- `Pattern.<Event>begin("name").where(...)` → `CEP.pattern(keyedStream, pattern)` → `.process(...)`.
- `next` = strict adjacency, `followedBy` = skip until the **first** match, `followedByAny` = try **every** later match (explosion risk).
- Quantifiers: `times(n)`, `times(m,n)`, `oneOrMore()`, `timesOrMore(n)`, `optional()`, `greedy()`; `.consecutive()` / `.allowCombinations()` change contiguity *inside* a quantifier.
- `within(Duration)` is effectively mandatory — it's what bounds NFA state.
- `SimpleCondition` sees one event; `IterativeCondition` sees previously matched events via `ctx.getEventsForPattern(name)`, at a cost.
- A match is a `Map<String, List<Event>>` keyed by pattern name; values are always lists.
- Timed-out partial matches need `within()` plus either `TimedOutPartialMatchHandler` or the two-function `select(tag, timeoutFn, selectFn)`, and go to a **side output** only.
- `AfterMatchSkipStrategy.skipPastLastEvent()` is the usual cure for duplicate/overlapping alerts.
- CEP's real cost: opaque state growth, hard debugging, and **no runtime pattern changes**.

## Interview one-liners

- *"What is Flink CEP?"* → A library that compiles a declarative event pattern into an NFA, run per key with partial matches in keyed state.
- *"`next` vs `followedBy` vs `followedByAny`?"* → Strict adjacency; skip-until-first-match; skip-and-try-every-match. The last one is combinatorial — always bound it with `within`.
- *"Why is `within()` important?"* → It's the only thing that prunes partial matches; without it NFA state grows without bound.
- *"`SimpleCondition` vs `IterativeCondition`?"* → Simple decides from the current event; Iterative can read the events already matched in this partial match via `getEventsForPattern`, which enables "greater than the previous ones" style conditions.
- *"My CEP job emits three alerts for one incident."* → Default `noSkip()` emits every overlapping match; use `AfterMatchSkipStrategy.skipPastLastEvent()`.
- *"How do you get partial matches that timed out?"* → `within()` plus `TimedOutPartialMatchHandler`, delivered to a side output — `processTimedOutMatch` has no `Collector`.
- *"CEP or a `KeyedProcessFunction`?"* → CEP for complex quantified or overlapping patterns; hand-rolled for simple ones where you want inspectable bounded state, custom intermediate output, and runtime-tunable thresholds via broadcast state.
- *"Biggest production limitation of CEP?"* → The pattern is baked into the job graph, so any rule change needs a redeploy and savepoint restart.
