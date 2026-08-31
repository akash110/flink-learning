# 5. keyBy and Partitioning

`keyBy` is the most important operator in Flink. Not because it does much — it does almost nothing on its own — but because it is the gate to **everything stateful**. State, windows, timers, joins, CEP: all of them require a keyed stream.

---

## What `keyBy` actually is

```java
DataStream<Event> events = ...;

KeyedStream<Event, String> byUser = events.keyBy(e -> e.getUserId());
//           ^      ^                              ^^^^^^^^^^^^^^^^^^
//           |      └─ key type                    the KeySelector
//           └─ record type
```

`keyBy` **logically partitions the stream by key**. It guarantees exactly one thing:

> **Key idea:** All records with the same key are processed by the **same parallel subtask**, in the order they arrived. Records with different keys may go anywhere.

That is it. It does not aggregate, it does not group into batches, it does not buffer. It is a routing rule.

Note the return type changed from `DataStream<Event>` to `KeyedStream<Event, String>`. `KeyedStream` is a subclass of `DataStream` that exposes extra methods (`sum`, `reduce`, `window`, `process` with a `KeyedProcessFunction`). The **type system enforces** that you keyed first.

---

## How records are routed

```java
env.setParallelism(3);
events.keyBy(e -> e.getUserId())
```

```
   incoming records (parallelism 3 upstream)
   ┌──────────┬──────────┬──────────┐
   │ subtask0 │ subtask1 │ subtask2 │
   │  u1,u7   │  u3,u1   │  u2,u1   │
   └────┬─────┴────┬─────┴────┬─────┘
        │          │          │
        └──────────┼──────────┘
                   ▼
    hash(key) → keyGroup → subtask     (network shuffle happens here)
                   │
   ┌───────────────┼───────────────┐
   ▼               ▼               ▼
┌────────┐   ┌────────┐   ┌────────┐
│subtask0│   │subtask1│   │subtask2│
│  u1    │   │  u2    │   │  u3    │
│  u1    │   │  u7    │   │        │
│  u1    │   │        │   │        │
└────────┘   └────────┘   └────────┘

Every u1 landed in subtask 0. Always. Deterministically.
```

Mechanically, for each record Flink computes:

```
keyGroup   = murmurHash(key.hashCode()) % maxParallelism
subtaskIdx = keyGroup * parallelism / maxParallelism
```

You do not need to memorize that. You need three consequences of it:

1. **Routing depends on `hashCode()`** of the key object. Bad `hashCode` → bad distribution.
2. **`keyBy` is a full network shuffle.** Records are serialized and sent to (possibly) another machine. It is the expensive operation in a Flink pipeline, exactly like Spark's `Exchange`.
3. **It breaks operator chaining.** The Web UI will always show a boundary at a `keyBy`.

### Skew

`keyBy` distributes **keys**, not **records**. If 90% of your traffic is `userId = "guest"`, then 90% of your records go to one subtask, that subtask backpressures the whole job, and adding parallelism does nothing.

```
keys:  u1(10)  u2(12)  guest(9000)  u4(8)
                        ▼
      subtask0        subtask1       subtask2
      u1, u4          u2             guest
      18 records      12 records     9000 records   ◄── the whole job runs at this speed
```

Watch for it in the Web UI: per-subtask `numRecordsIn` wildly unequal. The fix (salting, two-phase aggregation) is Phase 8; recognizing it is Phase 1.

---

## `KeySelector`

`KeySelector<IN, KEY>` is a functional interface with one method:

```java
public interface KeySelector<IN, KEY> {
    KEY getKey(IN value) throws Exception;
}
```

So all of these are the same thing:

```java
events.keyBy(e -> e.getUserId());              // lambda
events.keyBy(Event::getUserId);                // method reference
events.keyBy(new KeySelector<Event, String>() {   // anonymous class
    @Override public String getKey(Event e) { return e.getUserId(); }
});
```

**Composite keys.** Use a `Tuple` when you need to key on more than one field. Because the output is generic, you need `.returns()`-style type info — `keyBy` takes it as a second argument:

```java
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

KeyedStream<Event, Tuple2<String, String>> k = events.keyBy(
    e -> Tuple2.of(e.getUserId(), e.getType()),
    Types.TUPLE(Types.STRING, Types.STRING)      // ← the type hint
);
```

Tuples work as keys because Flink's `Tuple2` implements `hashCode()` and `equals()` over its fields.

**On Tuples/positional keys.** Older code keys by field index or name (`keyBy(0)`, `keyBy("userId")`). Those overloads are **deprecated**. Always use a `KeySelector` — it is type-checked at compile time, whereas a string field name fails at runtime with a typo.

---

## Why keyBy is required before keyed state

Flink's keyed state is a **key → value map maintained per subtask**:

```
subtask 0 state backend
┌──────────────────────────────┐
│  "u1"  →  ValueState(count=3)│
│  "u4"  →  ValueState(count=1)│
└──────────────────────────────┘
```

When a record for `u1` arrives, Flink sets the *current key* to `u1` before calling your function. Your code then writes:

```java
Long count = countState.value();      // implicitly scoped to the current key
countState.update(count + 1);
```

You never pass the key. The runtime supplies it. That mechanism only works if:

- Flink knows what the key is (hence `keyBy`), **and**
- every record for that key reaches the subtask that holds that key's state (hence the deterministic hash routing).

If two subtasks could both see `u1`, each would keep its own count and both would be wrong. That is why the API refuses:

```java
events.process(new KeyedProcessFunction<>(...));   // compile error: DataStream has no such method
events.keyBy(Event::getUserId)
      .process(new KeyedProcessFunction<>(...));   // fine: KeyedStream does
```

Same for windows:

```java
events.window(...)                          // does not exist
events.windowAll(...)                       // exists, but parallelism 1 — a bottleneck
events.keyBy(Event::getUserId).window(...)  // the real one, parallel per key
```

`windowAll` and `countWindowAll` are the non-keyed variants. They force parallelism 1 because there is only one window state to maintain. Use them only for genuinely global aggregates on small streams.

> **Key idea:** `keyBy` is not a convenience for grouping. It is the mechanism that makes state sharding correct. No key → no way to shard state → no keyed state, windows, or timers.

---

## Key groups (briefly)

Flink does not map keys directly to subtasks. It maps them to **key groups** first.

```
  keys (millions)  ──hash──►  key groups (maxParallelism, default 128)  ──►  subtasks (parallelism)

     u1, u7, u93   ─────────►  group 17  ──┐
     u2, u55       ─────────►  group 18  ──┼──►  subtask 0
     u3            ─────────►  group 19  ──┘
     ...                       group 20  ──┐
                               group 21  ──┼──►  subtask 1
```

A key group is the **atomic unit of state redistribution**. When you rescale a job from parallelism 3 to 6, Flink does not rehash every key — it reassigns whole key groups to the new subtasks and reads each group's state as one contiguous chunk from the checkpoint.

Two facts to carry:

1. **`maxParallelism` is fixed at the first checkpoint/savepoint and can never be changed** without discarding state. Default is 128 (for parallelism ≤ 128), otherwise roughly `1.5 × parallelism` rounded up, capped at 32768.
2. **Your parallelism can never exceed `maxParallelism`**, because a subtask needs at least one key group.

```java
env.setMaxParallelism(512);   // set this deliberately on day one for anything production
```

Set it too low and you can never scale up. Set it absurdly high and you pay metadata overhead and get uneven key-group distribution. `512` or `1024` is a sane production default for a job you expect to grow.

---

## The trap: mutable or non-deterministic keys

Key routing is `hashCode()`. If `hashCode()` can change or differ between runs, correctness silently breaks.

### Trap 1: mutating the key object

```java
public static class BadKey {
    public String id;                     // mutable public field
    @Override public int hashCode() { return id.hashCode(); }
}

events.keyBy(e -> {
    BadKey k = new BadKey();
    k.id = e.getUserId();
    someList.add(k);      // ...and something later mutates k.id
    return k;
});
```

Once `id` changes, the object hashes to a different group than the state it owns. State lookups silently miss. **Keys must be immutable.**

### Trap 2: a POJO key with no `hashCode()`

```java
public static class UserKey {
    public String userId;
    // no hashCode(), no equals()  →  Object identity hashing
}
```

Java's default `hashCode()` is based on **object identity**, so two `UserKey` objects with the same `userId` are different keys. Every record gets a fresh object, so every record is a new key, state grows without bound, and every aggregate is wrong.

**Rule: a key type must implement `hashCode()` and `equals()` consistently, or be a type that already does — `String`, boxed numerics, `Tuple`, or a Flink POJO with all its fields properly compared.**

### Trap 3: non-deterministic keys

```java
events.keyBy(e -> e.getUserId() + "-" + System.currentTimeMillis());  // never do this
events.keyBy(e -> UUID.randomUUID().toString());                     // nor this
events.keyBy(e -> someHashMap.get(e.getUserId()));   // depends on mutable outside state
```

Each produces a new key per record. State explodes, nothing aggregates, and after a restart nothing lines up with the checkpoint.

### Trap 4: arrays as keys

```java
events.keyBy(e -> new String[]{ e.getUserId(), e.getType() });   // BROKEN
```

Java arrays use identity `hashCode()`. Two arrays with identical contents are different keys. Use a `Tuple2` or a proper POJO.

### Trap 5: enum ordinal / `Object.toString()` defaults

Anything whose string form includes a memory address (`com.foo.Bar@1a2b3c`) is identity-based and therefore broken as a key.

> **Key idea:** A Flink key must be **immutable, value-based, and deterministic**. `String`, `Long`, and `Tuple2` satisfy all three for free — reach for them first.

---

## Repartitioning without keys

`keyBy` is one of several partitioners. The others produce a plain `DataStream` (no state access) and exist for load balancing:

```java
stream.rebalance();     // round-robin across all downstream subtasks — fixes skew, full shuffle
stream.rescale();       // round-robin, but only to local downstream subtasks — cheaper, no full shuffle
stream.shuffle();       // random uniform
stream.broadcast();     // every record to EVERY downstream subtask
stream.global();        // everything to subtask 0 — bottleneck, rarely correct
stream.forward();       // 1:1 with upstream subtask (the default when parallelism matches)
stream.partitionCustom(partitioner, keySelector);   // your own routing
```

`rebalance()` is the one you will actually use — after a source that produces skewed partitions (a Kafka topic where one partition is hot), a `rebalance()` evens out the load before expensive per-record work.

```
rebalance  : subtask0 ──► all downstream subtasks (round robin, network)
rescale    : subtask0 ──► subtasks 0,1 only (local, no network in the common case)
```

None of these give you keyed state. Only `keyBy` does.

---

## Complete example

```java
package com.akash.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setMaxParallelism(128);

        // (user, amount)
        DataStream<Tuple2<String, Double>> txns = env.fromElements(
            Tuple2.of("u1", 10.0),
            Tuple2.of("u2", 20.0),
            Tuple2.of("u1", 30.0),
            Tuple2.of("u3", 5.0),
            Tuple2.of("u1", 7.5),
            Tuple2.of("u2", 2.5)
        ).returns(Types.TUPLE(Types.STRING, Types.DOUBLE));

        // f0 is the first Tuple2 field (ch. 6). This is the KeySelector.
        KeyedStream<Tuple2<String, Double>, String> byUser = txns.keyBy(t -> t.f0);

        // sum over field index 1 — a rolling aggregate, emits on EVERY record (ch. 6)
        byUser.sum(1).name("running-total").print();

        env.execute("keyby-demo");
    }
}
```

Output (per-subtask order varies, but each user's sequence is monotonic and in order):

```
2> (u1,10.0)
2> (u1,40.0)
2> (u1,47.5)
3> (u2,20.0)
3> (u2,22.5)
1> (u3,5.0)
```

Notice: **all `u1` lines came from subtask 2**, all `u2` from subtask 3. That is the `keyBy` guarantee visible on screen. And the running total is emitted per record, not once at the end.

---

## Remember

- `keyBy` = **logical partitioning by key**. Same key → same subtask, always. It does not aggregate anything by itself.
- It returns a `KeyedStream`, and the type system uses that to gate `sum`/`reduce`/`window`/`KeyedProcessFunction`.
- It is a **network shuffle** and it **breaks operator chaining**. It is the expensive step in your pipeline.
- Routing is `hash(key) → key group → subtask`. **Key groups** are the atomic unit of rescaling; `maxParallelism` fixes their count and **cannot be changed after the first checkpoint**.
- **Keys must be immutable, value-based (`hashCode`/`equals`), and deterministic.** Arrays, identity-hashed POJOs, timestamps, and UUIDs are all broken keys.
- Skew is a `keyBy` property: it distributes keys, not records. One hot key = one hot subtask = the job's throughput ceiling.
- `rebalance`/`rescale`/`shuffle`/`broadcast` repartition without keying — no keyed state available after them.
- Prefer `KeySelector` lambdas over the deprecated `keyBy(0)` / `keyBy("field")` overloads.

**Interview one-liners**

- *"What does keyBy guarantee?"* → All records with the same key go to the same parallel subtask, in arrival order. Nothing about which subtask, and nothing about ordering across keys.
- *"Why is keyBy required for state?"* → Keyed state is sharded by key group; the runtime scopes every state access to the current key, which only works if the routing is deterministic and exclusive.
- *"What are key groups?"* → The unit of state assignment and redistribution. Keys hash to `maxParallelism` key groups, and groups are assigned to subtasks — so rescaling moves whole groups instead of rehashing keys.
- *"Why can't I change maxParallelism?"* → It determines the key-group-to-key mapping baked into the checkpoint; changing it would invalidate every stored key's location.
- *"How do you detect skew?"* → Compare `numRecordsIn` across subtasks of the keyed operator in the Web UI; one subtask far above the others with backpressure upstream.
