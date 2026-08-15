# Phase 3 — State (the heart of real Flink)

Windows were a *managed* form of state. Now you take direct control. This phase is what separates people who "used Flink once" from people who can build real systems.

Your four target exercises are built out at the end: **running balance, running count, last activity, fraud detection.**

---

## 1. What Flink state actually is

**State = data an operator remembers between records.** A `map` is stateless (each record independent). But "running total per user" must remember the previous total — that's state.

Two things make Flink's state special:

1. **It's keyed and partitioned.** After `keyBy(user)`, each key has its *own private copy* of the state. When a record for `alice` arrives, Flink automatically scopes your state access to `alice`. You never manage the map-of-users yourself.
2. **It's fault-tolerant.** Flink periodically snapshots all state to durable storage (**checkpoints**, Phase 5). After a crash it restores exactly where it left off. Your `HashMap` in a plain field would be lost; Flink state is not.

> Mental model: keyed state is like a giant, invisible `Map<Key, YourState>` that Flink manages, checkpoints, and hands you the right slice of — automatically, based on the current record's key.

### Keyed state vs operator state
- **Keyed state** — scoped to a key. 99% of what you'll write. Requires a keyed stream.
- **Operator state** — scoped to a parallel subtask, not a key (e.g. Kafka source remembering partition offsets). You rarely write it by hand; connectors use it. We focus on keyed state.

---

## 2. The state primitives

All live on a **keyed** stream and are created from a **descriptor** inside `open()` of a `RichFunction` / process function.

| State type | Holds | Typical use |
|------------|-------|-------------|
| `ValueState<T>` | a single value per key | running balance, last-seen, a flag |
| `ListState<T>` | a list per key | buffer recent events, keep history |
| `MapState<K,V>` | a map per key | per-key sub-keyed data (counts per category) |
| `ReducingState<T>` | single value, folded via a `ReduceFunction` on add | running sum where input==output |
| `AggregatingState<IN,OUT>` | single value, folded via an `AggregateFunction` | running average (in≠out) |

### The universal pattern

```java
public class MyFn extends KeyedProcessFunction<String, Event, String> {

    private transient ValueState<Long> myState;   // 'transient': not Java-serialized; Flink manages it

    @Override
    public void open(Configuration cfg) {
        ValueStateDescriptor<Long> desc =
            new ValueStateDescriptor<>("my-state", Types.LONG);
        myState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
        Long current = myState.value();            // read (null if never set for this key)
        if (current == null) current = 0L;
        current += e.amount;
        myState.update(current);                   // write
        out.collect(e.user + " total=" + current);
    }
}
```

- Create descriptors in `open()`, **never** in `processElement` (perf).
- `.value()` returns `null` before first write — always null-check.
- Access is auto-scoped to the current record's key. You do nothing to select the key.

---

## 3. Each primitive in code

### `ValueState<T>`
```java
ValueState<Long> balance = getRuntimeContext()
    .getState(new ValueStateDescriptor<>("balance", Types.LONG));
```
`value()`, `update(v)`, `clear()`.

### `ListState<T>`
```java
ListState<Event> recent = getRuntimeContext()
    .getListState(new ListStateDescriptor<>("recent", Event.class));
recent.add(e);
for (Event past : recent.get()) { ... }
recent.update(newList);   // replace whole list
```

### `MapState<K,V>`
```java
MapState<String, Long> perCategory = getRuntimeContext()
    .getMapState(new MapStateDescriptor<>("per-cat", Types.STRING, Types.LONG));
perCategory.put("food", 10L);
Long v = perCategory.get("food");
for (var entry : perCategory.entries()) { ... }
```
`MapState` is more efficient than storing a `HashMap` in `ValueState` when keys are large/many — you read/write individual entries instead of the whole map.

### `ReducingState<T>`
```java
ReducingState<Long> sum = getRuntimeContext().getReducingState(
    new ReducingStateDescriptor<>("sum", (a, b) -> a + b, Types.LONG));
sum.add(e.amount);   // folds automatically
Long total = sum.get();
```

### `AggregatingState<IN,OUT>`
```java
AggregatingState<Integer, Double> avg = getRuntimeContext().getAggregatingState(
    new AggregatingStateDescriptor<>("avg", new AvgAgg(), /* accumulator type */ ...));
avg.add(e.amount);
Double a = avg.get();
```
Use `Reducing`/`Aggregating` state when you'd otherwise read → modify → write a `ValueState` on every record; they fold for you.

---

## 4. State TTL (time-to-live) — stop state from growing forever

Unbounded keyspace (e.g. one entry per user, users never removed) = state grows forever = eventual failure. **TTL** auto-expires state.

```java
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;   // note: state TTL uses this Time

StateTtlConfig ttl = StateTtlConfig
    .newBuilder(Time.hours(24))                         // expire 24h after...
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)   // ...last write
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();

ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>("balance", Types.LONG);
desc.enableTimeToLive(ttl);
```

- `OnCreateAndWrite` (reset on write) vs `OnReadAndWrite` (reset on read too).
- Expired state is cleaned lazily (on access) + in background. TTL is by **processing time** in 1.18.
- **Rule:** any keyed state whose keyspace can grow without bound needs TTL *or* explicit `clear()` (often via a timer — next section).

---

## 5. State + timers — the combo that unlocks Phase 6

Timers let you say "call me back at time T for this key." When the timer fires, you can emit output and/or clear state. Only available in **`KeyedProcessFunction`**.

```java
public class TimerExample extends KeyedProcessFunction<String, Event, String> {
    private transient ValueState<Long> lastSeen;

    @Override public void open(Configuration c) {
        lastSeen = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastSeen", Types.LONG));
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
        lastSeen.update(e.timestamp);
        // register an event-time timer 30s after this event
        ctx.timerService().registerEventTimeTimer(e.timestamp + 30_000);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
        // fires when the WATERMARK passes `ts`
        out.collect("no activity 30s after last event for key " + ctx.getCurrentKey());
        lastSeen.clear();
    }
}
```

- **Event-time timers** fire when the *watermark* passes the timer time (deterministic, replayable).
- **Processing-time timers** (`registerProcessingTimeTimer`) fire on wall clock.
- Timers are **per key** and are themselves checkpointed.
- Common pattern: on each event, (re)register a timer; on timer, decide if something should be emitted/cleared. This is exactly how you build custom session/timeout logic and fraud rules.

---

## 6. `KeyedProcessFunction` — the swiss-army knife

`KeyedProcessFunction<K, IN, OUT>` gives you **everything**: per-key state, timers, side outputs, access to the current key and watermark. When windows/aggregations aren't flexible enough, drop down to this.

Capabilities recap:
- `processElement(in, ctx, out)` — per record.
- `onTimer(ts, ctx, out)` — on timer fire.
- `ctx.timerService()` — register/delete timers.
- `ctx.output(tag, value)` — side outputs.
- `getRuntimeContext().getState(...)` — keyed state.
- `ctx.getCurrentKey()`, `ctx.timerService().currentWatermark()`.

---

## 7. Your four exercises, fully built

All assume the `Event` POJO from Phase 2 (`user`, `amount`, `timestamp`) and a keyed stream `events.keyBy(Event::getUser)`.

### 7a. User → running balance

```java
public class RunningBalance extends KeyedProcessFunction<String, Event, Tuple2<String, Long>> {
    private transient ValueState<Long> balance;

    @Override public void open(Configuration c) {
        balance = getRuntimeContext().getState(
            new ValueStateDescriptor<>("balance", Types.LONG));
    }

    @Override public void processElement(Event e, Context ctx, Collector<Tuple2<String,Long>> out)
            throws Exception {
        Long b = balance.value();
        if (b == null) b = 0L;
        b += e.amount;
        balance.update(b);
        out.collect(Tuple2.of(e.user, b));
    }
}
// events.keyBy(Event::getUser).process(new RunningBalance())
//       .returns(Types.TUPLE(Types.STRING, Types.LONG)).print();
```

### 7b. User → running count

```java
public class RunningCount extends KeyedProcessFunction<String, Event, Tuple2<String, Long>> {
    private transient ValueState<Long> count;

    @Override public void open(Configuration c) {
        count = getRuntimeContext().getState(
            new ValueStateDescriptor<>("count", Types.LONG));
    }

    @Override public void processElement(Event e, Context ctx, Collector<Tuple2<String,Long>> out)
            throws Exception {
        Long n = count.value();
        n = (n == null ? 1L : n + 1);
        count.update(n);
        out.collect(Tuple2.of(e.user, n));
    }
}
```
(Could also be `ReducingState` with `(a,b)->a+b`, adding `1L` each time.)

### 7c. User → last activity (with idle-timeout via timer)

```java
public class LastActivity extends KeyedProcessFunction<String, Event, String> {
    private transient ValueState<Long> lastTs;

    @Override public void open(Configuration c) {
        lastTs = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastTs", Types.LONG));
    }

    @Override public void processElement(Event e, Context ctx, Collector<String> out)
            throws Exception {
        // delete the previous timer, remember new activity, set a fresh 60s idle timer
        Long prev = lastTs.value();
        if (prev != null) ctx.timerService().deleteEventTimeTimer(prev + 60_000);
        lastTs.update(e.timestamp);
        ctx.timerService().registerEventTimeTimer(e.timestamp + 60_000);
    }

    @Override public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) {
        out.collect(ctx.getCurrentKey() + " idle since " + (ts - 60_000));
        lastTs.clear();
    }
}
```
This is a hand-rolled session timeout — the seed of **sessionization** in Phase 6.

### 7d. Fraud detection (stateful rule: large txn shortly after a tiny "test" txn)

Classic pattern: a fraudster makes a **small** transaction to test a stolen card, then a **large** one within a short window.

```java
public class FraudDetector extends KeyedProcessFunction<String, Event, String> {
    private static final int  SMALL = 1;        // "test" transaction threshold
    private static final int  LARGE = 500;      // suspicious large amount
    private static final long WINDOW_MS = 60_000;

    private transient ValueState<Boolean> sawSmall;   // flag: recently saw a small txn
    private transient ValueState<Long>    timerTs;    // when the flag should expire

    @Override public void open(Configuration c) {
        sawSmall = getRuntimeContext().getState(new ValueStateDescriptor<>("sawSmall", Types.BOOLEAN));
        timerTs  = getRuntimeContext().getState(new ValueStateDescriptor<>("timerTs", Types.LONG));
    }

    @Override public void processElement(Event e, Context ctx, Collector<String> out)
            throws Exception {
        Boolean flagged = sawSmall.value();

        if (Boolean.TRUE.equals(flagged) && e.amount >= LARGE) {
            out.collect("FRAUD? user=" + e.user + " large=" + e.amount
                        + " shortly after a small txn");
            cleanUp(ctx);                       // reset after alerting
            return;
        }

        if (e.amount <= SMALL) {                // arm the detector
            sawSmall.update(true);
            long t = e.timestamp + WINDOW_MS;
            timerTs.update(t);
            ctx.timerService().registerEventTimeTimer(t);   // auto-disarm after WINDOW_MS
        }
    }

    @Override public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
        // window elapsed with no large txn — disarm
        sawSmall.clear();
        timerTs.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        Long t = timerTs.value();
        if (t != null) ctx.timerService().deleteEventTimeTimer(t);
        sawSmall.clear();
        timerTs.clear();
    }
}
```

This uses **state + timers + a rule** — exactly the toolkit Phase 6 expands into full CEP. You'll revisit this fraud example there with richer patterns.

---

## 8. Pitfalls

1. **Storing state in a plain field** (`private long total;`) instead of Flink state → not checkpointed, and shared across keys (wrong results at parallelism > 1). Always use `getRuntimeContext().getState(...)`.
2. **Creating the descriptor in `processElement`** → wasteful; do it in `open()`.
3. **Not null-checking `value()`** → NPE on the first record per key.
4. **Unbounded keyspace with no TTL/clear** → state grows forever, checkpoints get huge, job eventually dies.
5. **Event-time timers never firing** → watermark isn't advancing (same root cause as Phase 2). Check your `WatermarkStrategy`.
6. **`transient` keyword** on state fields — conventional; the field is populated in `open()`, not via Java serialization.

---

### ✅ Phase 3 checklist

- [ ] What keyed state is & why it's fault-tolerant
- [ ] `ValueState` / `ListState` / `MapState` / `ReducingState` / `AggregatingState`
- [ ] State TTL
- [ ] State + timers (event vs processing time)
- [ ] `KeyedProcessFunction` as the swiss-army knife
- [ ] Built all four: balance / count / last-activity / fraud

⬅️ [Phase 2](02-core-flink-apis.md)  ·  ➡️ [Phase 4 — Real-world streaming](04-realworld-streaming.md)
