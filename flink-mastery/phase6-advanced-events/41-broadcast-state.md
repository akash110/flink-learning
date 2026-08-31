# 41. Broadcast State — The Dynamic Rules Pattern

This is the highest-value pattern in this phase. Almost every real fraud, alerting, or routing system built on Flink uses it, and it's the answer to a question interviewers love: *"how do you change business logic without redeploying the job?"*

> **Key idea**
> `broadcast()` sends **every** record of a small stream to **every** parallel instance of the downstream operator.
> Each instance keeps an identical copy in **broadcast state**, and the main high-volume stream is evaluated against that local copy.

---

## The problem it solves

Your fraud rule is `amount > 10000`. It's a constant in your code.

```java
if (e.amount > 10_000) alert(e);
```

Six weeks later the risk team wants `> 7500` for `US` and `> 2000` for a new market. That means: change the code, build, test, deploy, take a savepoint, stop the job, restart from the savepoint. Twenty minutes at best, and it needs an engineer. Meanwhile fraud is happening now.

What you want: the risk team pushes a message to a Kafka topic and the running job picks it up in seconds.

```
   rules topic (tiny, ~1 msg/hour)
        │
        │  broadcast to EVERY subtask
        ▼
 ┌───────────┬───────────┬───────────┬───────────┐
 │ subtask 0 │ subtask 1 │ subtask 2 │ subtask 3 │
 │  rules ✓  │  rules ✓  │  rules ✓  │  rules ✓  │   <- identical copies
 └───────────┴───────────┴───────────┴───────────┘
        ▲           ▲           ▲           ▲
   ┌────┴───────────┴───────────┴───────────┴────┐
   │  transactions, keyBy(userId)  (millions/s)  │
   └─────────────────────────────────────────────┘
```

---

## Why "broadcast" and not "keyBy"

| | `keyBy` (partitioned) | `broadcast` (replicated) |
|---|---|---|
| Each subtask holds | a **slice** of the data | the **whole** dataset |
| Total memory | dataset size | dataset size **× parallelism** |
| Good for | millions of user profiles | tens/hundreds of rules |
| Requires a matching key on both sides | yes | no |

Broadcast state is only viable because the rule set is small. 200 rules × parallelism 64 = 12,800 tiny objects. 50 million user profiles × 64 would be catastrophic.

---

## The three pieces of API

### 1. `MapStateDescriptor` — broadcast state is always a map

```java
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public static final MapStateDescriptor<String, Rule> RULES_DESC =
        new MapStateDescriptor<>(
                "rules",                                    // name — must match everywhere
                BasicTypeInfo.STRING_TYPE_INFO,             // key type
                TypeInformation.of(Rule.class));            // value type
```

Notes for a Java beginner:
- `public static final` = a constant shared by the whole class. Declare the descriptor **once** and reuse the same object in `broadcast(...)` and inside the function — they are matched by the name string, but reusing the constant prevents typos.
- Broadcast state **must** be a `MapStateDescriptor`. There is no `ValueState` variant. If you only have one value, use a map with a single fixed key like `"THE_RULE"`.
- The descriptor must be `Serializable` to ship to the cluster; `MapStateDescriptor` is.

### 2. `broadcast(descriptor)` — turn a stream into a `BroadcastStream`

```java
BroadcastStream<Rule> ruleBroadcast = ruleStream.broadcast(RULES_DESC);
```

### 3. `connect()` + a broadcast process function

```java
DataStream<Alert> alerts =
        transactions
            .keyBy(e -> e.userId)              // keyed main stream
            .connect(ruleBroadcast)            // KeyedStream.connect(BroadcastStream)
            .process(new DynamicFraudRules()); // -> KeyedBroadcastProcessFunction
```

Which function you need depends on whether the main stream is keyed:

| Main stream | Function |
|---|---|
| `DataStream.connect(broadcast)` | `BroadcastProcessFunction<IN, BC, OUT>` |
| `KeyedStream.connect(broadcast)` | `KeyedBroadcastProcessFunction<K, IN, BC, OUT>` |

Use the keyed one unless you genuinely need no per-key state. It's strictly more capable.

---

## The asymmetry: read-only vs read-write

This is the part that trips everyone up, and the part interviewers probe.

```java
// the high-volume side:
public void processElement(Event e, ReadOnlyContext ctx, Collector<Alert> out)
                                    ^^^^^^^^^^^^^^^^^

// the rules side:
public void processBroadcastElement(Rule r, Context ctx, Collector<Alert> out)
                                            ^^^^^^^
```

```java
// In processElement — ReadOnlyContext:
ReadOnlyBroadcastState<String, Rule> rules = ctx.getBroadcastState(RULES_DESC);
rules.get("US");        // ✅ read
rules.immutableEntries();// ✅ iterate
// rules.put(...)        ❌ does not compile — the type has no put()

// In processBroadcastElement — Context:
BroadcastState<String, Rule> rules = ctx.getBroadcastState(RULES_DESC);
rules.put(r.id, r);     // ✅ write
rules.remove(r.id);     // ✅
```

### Why the asymmetry exists

> **Key idea**
> Every parallel instance must hold an **identical** copy of the broadcast state.
> The broadcast stream is the only input that reaches every instance identically, so it is the only input that can be allowed to mutate the state.

Walk through what happens if `processElement` could write:

```
subtask 0 sees txn(alice, $9000)  -> it writes rules["dynamic"] = X
subtask 1 sees txn(bob,   $12)    -> it writes nothing
subtask 2 sees txn(carol, $40000) -> it writes rules["dynamic"] = Y

Now the three subtasks have DIFFERENT broadcast state.
```

Consequences, all fatal:

1. **Non-determinism.** Two runs over the same data give different answers, because record-to-subtask assignment depends on parallelism and hashing.
2. **Rescaling is undefined.** On rescale, Flink redistributes broadcast state by taking subtask 0's copy and handing it to everyone (that's the documented behaviour — broadcast state is an operator state with "broadcast" redistribution). If the copies had diverged, subtasks 1..N-1's writes would be **silently discarded**.
3. **Checkpoints would be inconsistent.** Each subtask checkpoints its own copy; restoring assumes they're the same.

The compiler enforcing `ReadOnlyContext` is Flink stopping you from writing a job that is *silently* wrong. Accept the constraint rather than fighting it: if you need per-record memory, use **keyed state**, which is legitimately per-key and per-subtask.

---

## `applyToKeyedState` — reaching keyed state from the broadcast side

Problem: a new rule arrives. You want to clear every user's accumulated counter, because the old counters were computed under the old rule. But `processBroadcastElement` has **no current key** — a broadcast record belongs to no key — so `counterState.clear()` would throw.

`KeyedBroadcastProcessFunction.Context` gives you an escape hatch:

```java
@Override
public void processBroadcastElement(Rule r, Context ctx, Collector<Alert> out) throws Exception {

    ctx.getBroadcastState(RULES_DESC).put(r.id, r);

    // Iterate over EVERY key that this subtask holds state for.
    ctx.applyToKeyedState(
        COUNTER_DESC,                                    // which keyed state to visit
        (String key, ValueState<Long> state) -> {        // called once per key
            state.clear();                               // keyed context IS set inside here
        });
}
```

The lambda is a `KeyedStateFunction<K, S>`; `S` is the state type from the descriptor. Inside it, the keyed context is set to that key, so state access works.

**The cost, and it is a big one:** this iterates every key in this subtask's state. With 20 million keys in RocksDB that's a full scan, blocking the operator thread. Use it for genuinely rare events (a rule change, a manual reset command). Never for anything that arrives more than a few times an hour.

There is no way to iterate keyed state from `processElement`, and no way to visit keys held by *other* subtasks.

---

## Full worked example: runtime-configurable fraud thresholds

Two Kafka topics. `transactions` is high volume. `fraud-rules` is a compacted topic that the risk team writes to.

### The rule POJO

```java
// A POJO for Flink means: public class, public no-arg constructor, public fields
// (or getters/setters). Flink then uses its fast built-in serializer.
public class Rule {
    public String id;          // e.g. "high-amount"
    public String country;     // scope: which country this applies to, "*" for all
    public double threshold;   // alert above this amount
    public boolean enabled;    // soft-delete without removing the entry

    public Rule() {}           // REQUIRED no-arg constructor

    public Rule(String id, String country, double threshold, boolean enabled) {
        this.id = id;
        this.country = country;
        this.threshold = threshold;
        this.enabled = enabled;
    }
}
```

### The job

```java
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DynamicRulesJob {

    // ONE descriptor constant, referenced from the job and from the function.
    public static final MapStateDescriptor<String, Rule> RULES_DESC =
            new MapStateDescriptor<>(
                    "rules",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(Rule.class));

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> transactions = env.fromSource(/* KafkaSource<Event>  */ null, null, "txns");
        DataStream<Rule>  rules        = env.fromSource(/* KafkaSource<Rule>   */ null, null, "rules");

        // The rules source should have parallelism 1: one reader, one ordered
        // sequence of rule changes. Broadcasting fans it out anyway.
        // rules.setParallelism(1);

        BroadcastStream<Rule> ruleBroadcast = rules.broadcast(RULES_DESC);

        DataStream<Alert> alerts =
                transactions
                        .keyBy(e -> e.userId)
                        .connect(ruleBroadcast)
                        .process(new DynamicFraudRules());

        alerts.print();
        env.execute("dynamic fraud rules");
    }
}
```

### The function

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class DynamicFraudRules
        extends KeyedBroadcastProcessFunction<String, Event, Rule, Alert> {
        //                                     ^key   ^main  ^bc   ^out

    /** Per-user running total in the current hour. Keyed state — legal to write here. */
    private static final ValueStateDescriptor<Double> TOTAL_DESC =
            new ValueStateDescriptor<>("hourTotal", Double.class);

    private transient ValueState<Double> hourTotal;

    @Override
    public void open(OpenContext ctx) {
        hourTotal = getRuntimeContext().getState(TOTAL_DESC);
    }

    // ── HIGH-VOLUME SIDE: read-only broadcast state ───────────────────────
    @Override
    public void processElement(Event e, ReadOnlyContext ctx, Collector<Alert> out)
            throws Exception {

        // 1. Keyed state: perfectly writable, it's per-user and per-subtask by design.
        Double prev = hourTotal.value();                 // null on first event for this user
        double total = (prev == null ? 0.0 : prev) + e.amount;
        hourTotal.update(total);

        // 2. Broadcast state: READ ONLY. The type is ReadOnlyBroadcastState.
        ReadOnlyBroadcastState<String, Rule> rules =
                ctx.getBroadcastState(DynamicRulesJob.RULES_DESC);

        // immutableEntries() gives an Iterable<Map.Entry<K,V>> you must not modify.
        for (Map.Entry<String, Rule> entry : rules.immutableEntries()) {
            Rule r = entry.getValue();

            if (!r.enabled) continue;                     // soft-disabled rule

            // "*" means "applies to every country". A real Event would carry a country;
            // we use e.type here as the stand-in dimension.
            boolean scopeMatches = "*".equals(r.country) || r.country.equals(e.type);
            if (!scopeMatches) continue;

            if (e.amount > r.threshold) {
                out.collect(new Alert(e.userId, r.id,
                        "single txn " + e.amount + " > " + r.threshold));
            }
        }

        // rules.put("x", r);   // <-- would not compile. ReadOnlyBroadcastState has no put().
    }

    // ── RULES SIDE: read-write broadcast state ────────────────────────────
    @Override
    public void processBroadcastElement(Rule r, Context ctx, Collector<Alert> out)
            throws Exception {

        // Every parallel instance receives THIS SAME record and runs THIS SAME code,
        // so every copy of the state stays identical. That is the whole invariant.
        BroadcastState<String, Rule> rules =
                ctx.getBroadcastState(DynamicRulesJob.RULES_DESC);

        if (r.enabled) {
            rules.put(r.id, r);       // insert or replace
        } else {
            rules.remove(r.id);       // hard delete
        }

        // Rare, expensive, and only appropriate for rare events:
        // reset every user's running total because the rule semantics changed.
        if ("RESET_ALL".equals(r.id)) {
            ctx.applyToKeyedState(TOTAL_DESC, (String key, ValueState<Double> st) -> st.clear());
        }

        // NOTE: there is no current key here.
        // hourTotal.value();   // <-- throws at runtime: no keyed context.
    }
}
```

### What "changeable at runtime" actually looks like

```
09:00  job running with rules = { high-amount: threshold 10000 }
       txn(alice, 9000)  -> no alert

09:05  risk team produces to `fraud-rules`:
         {"id":"high-amount","country":"*","threshold":5000,"enabled":true}

09:05  every subtask's processBroadcastElement runs, state now threshold=5000
       (propagation is a few hundred ms — one Kafka hop)

09:06  txn(bob, 9000)   -> ALERT  ✅   no redeploy, no restart, no savepoint
```

---

## Practical rules for production

**Bootstrap the rules first.** On job start the rules topic is replayed, but transactions are also flowing. For a few seconds the broadcast state is empty and no rule matches. Options:
- Use a **compacted** Kafka topic and start the rules source from `earliest` — it replays fast because it's tiny.
- Ship a default rule set in `open()` — but you can't write broadcast state from `open()`, so instead hold defaults in a plain field and use them when the broadcast map is empty.
- Accept a short warm-up if a few seconds of missed alerts is tolerable.

**Set the rules source parallelism to 1.** Multiple readers of the rules topic means rule updates interleave non-deterministically across subtasks (each subtask still gets everything, but ordering can differ). One reader gives one global order.

**Broadcast state lives on the heap, always.** Even with `EmbeddedRocksDBStateBackend`, broadcast state is kept in memory as a `HashMap` on each subtask. Another reason it must stay small. Budget it as `ruleCount × ruleSize × parallelism`.

**Broadcast state is operator state, not keyed state.** So: no TTL, no key-group redistribution. On rescale, Flink copies one instance's state to all new instances. Because they're identical, that's correct.

**Rules can be more than thresholds.** The same pattern carries feature flags, A/B assignments, allow/deny lists, routing tables, and (with care) small ML model coefficients. What it cannot carry is a per-user dataset — that's `keyBy` + `KeyedCoProcessFunction` (ch. 40).

**Order between the two inputs is still not guaranteed** (ch. 40's lesson applies here too). A transaction may be processed before the rule that should have caught it. That's inherent; the mitigation is the compacted-topic bootstrap.

---

## Remember

- `broadcast()` replicates a small stream to **every** parallel subtask; `keyBy` partitions.
- Broadcast state is always declared with a `MapStateDescriptor`, and matched by its name string — reuse one `public static final` constant.
- `KeyedStream.connect(BroadcastStream).process(...)` → `KeyedBroadcastProcessFunction`; un-keyed main stream → `BroadcastProcessFunction`.
- `processElement` gets `ReadOnlyContext` → read-only broadcast state. `processBroadcastElement` gets `Context` → read-write.
- The asymmetry exists because every instance must hold an identical copy; only the broadcast input reaches all instances identically.
- Keyed state is freely writable in `processElement` — the restriction is only on broadcast state.
- `applyToKeyedState(descriptor, fn)` iterates every key in this subtask from the broadcast side. Expensive: rare events only.
- Broadcast state is heap-resident operator state, redistributed by copying on rescale. Keep it small.
- Rules source parallelism 1 + a compacted topic = deterministic order and fast bootstrap.

## Interview one-liners

- *"How do you change business rules without redeploying?"* → Broadcast state: publish rules to a small Kafka topic, `broadcast()` it, connect to the main keyed stream, and apply the rules from a `KeyedBroadcastProcessFunction`.
- *"Why is broadcast state read-only in `processElement`?"* → Every parallel instance must hold an identical copy; only the broadcast input reaches all instances identically, so allowing per-record writes would let copies diverge and rescaling would silently drop all but one.
- *"What state type is broadcast state?"* → Operator state with broadcast redistribution, always a `MapState`, always heap-resident, copied from one instance to all on rescale.
- *"Broadcast vs `keyBy` for reference data?"* → Broadcast replicates (cost = size × parallelism, good for tens of rules); `keyBy` partitions (good for millions of profiles, but requires a join key).
- *"How do you clear per-user state when a rule changes?"* → `ctx.applyToKeyedState(descriptor, fn)` from `processBroadcastElement` — a full scan of the subtask's keys, so only for rare events.
- *"Can you access keyed state in `processBroadcastElement`?"* → Not directly, there is no current key; only via `applyToKeyedState`.
- *"What's the failure mode at job start?"* → Empty broadcast state until the rules topic is replayed; use a compacted topic read from earliest, or in-code defaults.
