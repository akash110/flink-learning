# 6. Tuples and Rolling Aggregations

## `Tuple2` and friends

Flink ships its own tuple classes: `Tuple0` through `Tuple25`. They are ordinary Java classes with **public fields named `f0`, `f1`, `f2`, ...**

```java
import org.apache.flink.api.java.tuple.Tuple2;

Tuple2<String, Integer> t = Tuple2.of("akash", 42);
//     ^type0  ^type1        ^static factory — cleaner than new Tuple2<>(...)

String  name  = t.f0;      // public field access, NOT t.getF0()
Integer count = t.f1;

t.f1 = 43;                 // mutable — fields are public and non-final
```

Also valid, and what you will see in older code:

```java
Tuple2<String, Integer> t = new Tuple2<>("akash", 42);
String name = t.getField(0);   // generic accessor — avoid, it's untyped
```

Larger tuples work the same way:

```java
Tuple3<String, String, Double> t3 = Tuple3.of("u1", "purchase", 99.99);
t3.f0;  t3.f1;  t3.f2;
```

Two things to internalize:

- **`f0` is a field, not a method.** No parentheses. This is unusual for Java and it exists for speed — direct field access, no virtual method call, on the hottest path in the system.
- **Tuples are 0-indexed, and so are the aggregation methods.** `sum(1)` means "field `f1`".

### Why Flink has its own tuples

Java's own tuple-like types (`AbstractMap.SimpleEntry`, records, third-party `Pair`) would go through Kryo. Flink's `TupleN` classes have a **hand-written `TupleSerializer`** that serializes each field with that field's own dedicated serializer — no reflection, no class metadata written per record.

---

## The serialization hierarchy (why type choice matters)

Flink chooses a serializer per type, and the choice is a large throughput difference:

| Type | Serializer | Speed | Notes |
|---|---|---|---|
| `String`, `Long`, `Integer`, `Double`, ... | built-in basic serializers | fastest | |
| `Tuple0..25` | `TupleSerializer` | fastest | per-field serializers, no reflection |
| Valid POJO (ch. 2/9) | `PojoSerializer` | fast | supports **state schema evolution** |
| Avro / protobuf classes | dedicated serializers | fast | schema evolution via the format |
| Anything else | **Kryo** | slow | generic reflective fallback |

```
       your class
            │
     is it a Tuple? ──yes──► TupleSerializer      ✓
            │no
     valid POJO?    ──yes──► PojoSerializer       ✓  (+ schema evolution)
            │no
            ▼
          Kryo                                    ✗  slow, no schema evolution
```

> **Key idea:** Serialization happens on **every network hop, every state read/write, and every checkpoint**. In a stateful streaming job it is often the single biggest CPU cost. Choosing a Tuple or a proper POJO over an arbitrary class is a 2–5x difference on that path, for free.

Kryo's second cost is worse than speed: **you cannot evolve the schema of Kryo-serialized state.** Add a field to a class held in state and your savepoint no longer restores. You will find this out during an incident.

### Tuple vs POJO — which to use

| | Tuple | POJO |
|---|---|---|
| Speed | fastest | ~equal, marginally behind |
| Readability | `t.f2` — what is f2? | `e.getAmount()` |
| Schema evolution | **no** (arity/position is the schema) | **yes** |
| Nulls | fields can be null but Tuple itself must not be | fine |
| Best for | short-lived intermediates, `(key, count)` | anything that lives in state, crosses job versions, or has >3 fields |

**Practical rule:** Tuples for throwaway intermediates inside one operator chain. POJOs for your domain events and anything you put in state. Chapter 9 builds the `Event` POJO for exactly that reason.

**A `Tuple` itself may not be null.** `Tuple2.of("a", null)` is fine; emitting a `null` where a `Tuple2` is expected throws at runtime.

---

## Rolling aggregations on a `KeyedStream`

These methods exist only on `KeyedStream`, because they need per-key state:

```java
sum(int pos)      sum(String field)
min(int pos)      min(String field)
max(int pos)      max(String field)
minBy(int pos)    minBy(String field)
maxBy(int pos)    maxBy(String field)
```

The `int` form addresses a Tuple field by index. The `String` form addresses a POJO field by name.

### Rolling means: output on every record

This is the semantic that trips people coming from batch or SQL.

```java
DataStream<Tuple2<String, Integer>> counts = env.fromElements(
    Tuple2.of("a", 1),
    Tuple2.of("a", 2),
    Tuple2.of("b", 5),
    Tuple2.of("a", 3),
    Tuple2.of("b", 1)
).returns(Types.TUPLE(Types.STRING, Types.INT));

counts.keyBy(t -> t.f0).sum(1).print();
```

Output:

```
(a,1)     ← after record 1: running sum for "a" is 1
(a,3)     ← after record 2: 1+2
(b,5)     ← after record 3: running sum for "b" is 5
(a,6)     ← after record 4: 1+2+3
(b,6)     ← after record 5: 5+1
```

**Five inputs, five outputs.** There is no "end" to an unbounded stream, so there is no moment at which a "final" answer could be emitted. Every record updates the per-key accumulator and immediately emits the new value.

```
key "a" state:  1  ──►  3  ──►  6
                ▲       ▲       ▲
                emit    emit    emit
```

If you want one output per time period instead of per record, you want a **window** (Phase 2). Rolling aggregations are windowless and unbounded.

**Corollary:** the state for a rolling aggregation lives forever. One entry per key, never cleaned up. On a stream with unbounded key cardinality (session ids, request ids), `sum()` is a slow memory leak. This is the single most common cause of state growth in a first Flink job.

---

## `max` vs `maxBy` — the difference that matters

This is the classic Flink interview question and a real production bug source.

Take a stream of `Tuple3<user, product, amount>`:

```java
DataStream<Tuple3<String, String, Integer>> sales = env.fromElements(
    Tuple3.of("u1", "keyboard", 10),
    Tuple3.of("u1", "monitor",  50),
    Tuple3.of("u1", "mouse",    30)
).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));
```

### `max(2)` — updates ONLY the aggregated field

```java
sales.keyBy(t -> t.f0).max(2).print();
```

```
(u1,keyboard,10)
(u1,keyboard,50)   ← amount updated to 50, but product is STILL "keyboard"
(u1,keyboard,50)
```

`max` maintains one accumulator record. When a new max arrives, it copies **only field 2** into the accumulator. Every other field keeps whatever value the **first** record for that key had. Field 1 says `keyboard` forever.

The record `(u1, keyboard, 50)` **never existed in your input**. It is a Frankenstein: the key from every record, the product from the first record, and the amount from the max record.

### `maxBy(2)` — emits the WHOLE record that had the max

```java
sales.keyBy(t -> t.f0).maxBy(2).print();
```

```
(u1,keyboard,10)
(u1,monitor,50)    ← the actual record that held the max
(u1,monitor,50)    ← 30 < 50, so the max record is retained
```

`maxBy` keeps a reference to the entire record with the largest field-2 value. Every output is a real record from your input.

```
                    max(2)                    maxBy(2)
  in  (u1,keyboard,10)  →  (u1,keyboard,10)   →  (u1,keyboard,10)
  in  (u1,monitor, 50)  →  (u1,keyboard,50)   →  (u1,monitor, 50)
  in  (u1,mouse,   30)  →  (u1,keyboard,50)   →  (u1,monitor, 50)
                            ▲ mixed record        ▲ real record
```

> **Key idea:** `max` gives you the max **value**. `maxBy` gives you the **record** that had it. If the record has more than the key and the aggregated field, `max` is almost always the bug and `maxBy` is almost always what you meant.

Identical logic for `min` / `minBy`.

### `maxBy` and ties

```java
maxBy(int pos)                        // default: keep the FIRST record with the max
maxBy(int pos, boolean first)         // first=false → keep the LAST (most recent) one
minBy(int pos, boolean first)
```

`maxBy(2, false)` on repeated equal maxima keeps updating to the newest record. Useful for "the most recent record at the peak value".

### `sum` and the same trap

`sum` behaves like `max`: it only accumulates the named field and leaves the others frozen at the first record's values.

```java
sales.keyBy(t -> t.f0).sum(2).print();
```

```
(u1,keyboard,10)
(u1,keyboard,60)
(u1,keyboard,90)   ← the total is right; "keyboard" is meaningless
```

The lesson generalizes: **keep the aggregated shape minimal.** Project down to `(key, value)` before aggregating, so there are no other fields to be wrong:

```java
sales.map(t -> Tuple2.of(t.f0, t.f2))
     .returns(Types.TUPLE(Types.STRING, Types.INT))
     .keyBy(t -> t.f0)
     .sum(1)
     .print();
```

```
(u1,10)
(u1,60)
(u1,90)
```

Now there is nothing to be misleading.

---

## Aggregating POJOs by field name

The `String` overloads work on POJO field names:

```java
events.keyBy(Event::getUserId).sum("amount").print();
events.keyBy(Event::getUserId).maxBy("amount").print();
```

Requirements:
- The class must be a **valid POJO** (ch. 2 and 9) — otherwise Flink cannot resolve the field.
- The field name is resolved via the getter/setter or a public field. A typo throws at **job-build time**, which is at least early.
- `sum`/`min`/`max` on a POJO **mutate and reuse the accumulator object** — so the semantics are the same "only the named field is updated" trap.

Nested fields use dots: `sum("stats.count")`.

---

## `reduce` — when the built-ins are not enough

`sum`/`max`/`maxBy` are special cases of `reduce`. When you need real logic, write it:

```java
DataStream<Tuple2<String, Integer>> totals = counts
    .keyBy(t -> t.f0)
    .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1));
```

`ReduceFunction<T>`:

```java
public interface ReduceFunction<T> {
    T reduce(T value1, T value2) throws Exception;
}
```

- `value1` is the **current accumulated value** for the key; `value2` is the **incoming record**.
- The output type **must equal** the input type. That is the constraint that makes `reduce` cheap: the state is one record per key.
- Like `sum`, it is rolling — one output per input record.
- The **first record for a key is emitted as-is**; `reduce` is not called until the second record arrives.

A `maxBy` written by hand, so you can see there is no magic:

```java
sales.keyBy(t -> t.f0)
     .reduce((a, b) -> a.f2 >= b.f2 ? a : b);   // keep the whole winning record
```

And a `max` written by hand:

```java
sales.keyBy(t -> t.f0)
     .reduce((a, b) -> Tuple3.of(a.f0, a.f1, Math.max(a.f2, b.f2)));  // a.f1 frozen — the trap, explicit
```

Writing them out is the fastest way to make the difference stick.

Also available: `aggregate(AggregateFunction<IN, ACC, OUT>)` — used with windows in Phase 2 — which lets the accumulator type differ from the input type (e.g. accumulate `(sum, count)` to output an average). `reduce` cannot do that.

---

## Complete runnable example

```java
package com.akash.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregationDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);   // deterministic output for teaching

        // (user, product, amount)
        DataStream<Tuple3<String, String, Integer>> sales = env.fromElements(
            Tuple3.of("u1", "keyboard", 10),
            Tuple3.of("u1", "monitor",  50),
            Tuple3.of("u1", "mouse",    30),
            Tuple3.of("u2", "cable",     5),
            Tuple3.of("u2", "dock",     80)
        ).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));

        KeyedStream<Tuple3<String, String, Integer>, String> byUser = sales.keyBy(t -> t.f0);

        byUser.sum(2).print("SUM  ");     // running total; product field frozen
        byUser.max(2).print("MAX  ");     // running max value; product field frozen
        byUser.maxBy(2).print("MAXBY");   // running max RECORD; product field correct

        env.execute("aggregation-demo");
    }
}
```

Output (three sinks interleave; grouped here for readability):

```
SUM  > (u1,keyboard,10)      MAX  > (u1,keyboard,10)      MAXBY> (u1,keyboard,10)
SUM  > (u1,keyboard,60)      MAX  > (u1,keyboard,50)      MAXBY> (u1,monitor,50)
SUM  > (u1,keyboard,90)      MAX  > (u1,keyboard,50)      MAXBY> (u1,monitor,50)
SUM  > (u2,cable,5)          MAX  > (u2,cable,5)          MAXBY> (u2,cable,5)
SUM  > (u2,cable,85)         MAX  > (u2,cable,80)         MAXBY> (u2,dock,80)
```

Read the last row three times. `SUM` and `MAX` both claim the product is `cable`. Only `MAXBY` says `dock`. That row is the whole chapter.

---

## Remember

- Flink tuples are `Tuple0`–`Tuple25` with **public fields `f0`, `f1`, ...** — fields, not getters, and 0-indexed.
- Flink picks a serializer per type: **Tuple/POJO → fast generated serializer; anything else → Kryo**, which is slow and blocks state schema evolution.
- **Tuples for intermediates, POJOs for domain events and anything in state.** Tuples have no schema evolution.
- Rolling aggregations emit **one output per input record**, not one at the end. There is no "end" to an unbounded stream.
- Their state is **one entry per key, forever** — unbounded key cardinality means unbounded state.
- **`max` updates only the aggregated field**, leaving all other fields at the first record's values, producing records that never existed. **`maxBy` emits the entire real record** that held the extreme. Same for `min`/`minBy`. `sum` has `max`'s behaviour.
- Defensive habit: **project down to `(key, value)` before aggregating** so there are no other fields to be wrong.
- `reduce` generalizes them all but the output type must equal the input type; the first record per key passes through untouched.

**Interview one-liners**

- *"max vs maxBy?"* → `max` maintains an accumulator and updates only the aggregated field, so non-aggregated fields stay at the first record's values; `maxBy` retains and emits the complete record that held the maximum.
- *"Why does Flink prefer Tuples and POJOs?"* → They get generated per-field serializers instead of Kryo; serialization is on every network hop, state access, and checkpoint, so it dominates CPU in stateful jobs. POJOs additionally support state schema evolution.
- *"When does a keyed `sum()` emit?"* → On every record, with the new running value. One in, one out.
- *"What's the risk of `keyBy(...).sum(...)` in production?"* → Unbounded state — one accumulator per key with no TTL and no cleanup. Fine for bounded key spaces, a leak for unbounded ones.
- *"reduce vs aggregate?"* → `reduce` forces accumulator type == input type == output type; `AggregateFunction` lets the accumulator and output differ, which is how you compute an average.
