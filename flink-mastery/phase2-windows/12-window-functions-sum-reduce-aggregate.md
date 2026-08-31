# 12. Window Functions: `sum`, `reduce`, `aggregate`

Once you have a `WindowedStream`, you must tell Flink *what to compute*. There are two families:

```
INCREMENTAL                          BUFFERING
sum / min / max / reduce / aggregate       ProcessWindowFunction
  ↓                                          ↓
one accumulator per window                 an ArrayList of every element
updated on each element                    the function runs once at fire time
memory: O(1) per window                    memory: O(n) per window
```

This chapter covers the incremental family. Chapter 13 covers `ProcessWindowFunction` and the pattern that combines both.

> **Key idea**
> `reduce` and `aggregate` fold each element into a single accumulator **as it arrives** and then throw the element away. A window holding 10 million records costs the same memory as one holding 3.

---

## 1. The built-in shortcuts

```java
WindowedStream<Event, String, TimeWindow> windowed =
        stream.keyBy(e -> e.userId)
              .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)));

windowed.sum("amount");      // sum of the amount field
windowed.min("amount");      // smallest amount value
windowed.max("amount");      // largest amount value
windowed.minBy("amount");    // the whole Event that had the smallest amount
windowed.maxBy("amount");    // the whole Event that had the largest amount
```

The generic type `WindowedStream<Event, String, TimeWindow>` reads as: elements are `Event`, the key is `String`, the window type is `TimeWindow`.

**`max` vs `maxBy` — the distinction that shows up in interviews:**

Given `Event("u1","click",5.0,1000)` then `Event("u1","view",9.0,2000)`:

| call | result |
|---|---|
| `max("amount")` | `Event("u1", "click", 9.0, 1000)` — first record, only the amount field updated. Other fields are **stale/arbitrary**. |
| `maxBy("amount")` | `Event("u1", "view", 9.0, 2000)` — the actual record that had the max. All fields consistent. |

Use `maxBy` unless you specifically want just the number. `max` producing a Frankenstein record with mismatched fields is a real production bug source.

These shortcuts are all implemented on top of `reduce` internally.

---

## 2. `reduce(ReduceFunction<T>)`

```java
import org.apache.flink.api.common.functions.ReduceFunction;

DataStream<Event> totals = stream
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
    .reduce(new ReduceFunction<Event>() {
        @Override
        public Event reduce(Event a, Event b) {
            return new Event(a.userId, a.type, a.amount + b.amount, a.timestamp);
        }
    });
```

Or as a lambda, since `ReduceFunction` has exactly one method (a **functional interface**):

```java
.reduce((a, b) -> new Event(a.userId, a.type, a.amount + b.amount, a.timestamp))
```

### What Flink actually does

```
element 1 arrives → no accumulator yet → store element 1 as the accumulator
element 2 arrives → acc = reduce(acc, element2)
element 3 arrives → acc = reduce(acc, element3)
...
window fires      → emit acc
```

Only one `Event` object is held in state per window per key.

### Why `reduce` is same-type-in-same-type-out

The signature is:

```java
public interface ReduceFunction<T> extends Function, Serializable {
    T reduce(T value1, T value2) throws Exception;
}
```

One type parameter `T`, used for both inputs and the output. That is forced by the algorithm: the accumulator **is** the previous result, and it gets fed straight back in as `value1` on the next call. Input type, accumulator type, and output type are necessarily the same thing.

**The consequence:** `reduce` cannot compute an average. An average needs `(sum, count)` in flight and produces a `double` — three different types. You physically cannot express that with one type parameter. You would have to abuse the `Event` type to smuggle a count somewhere, which is exactly the hack `aggregate` exists to avoid.

### Rules for a `ReduceFunction`

- Must be **associative and commutative**: Flink does not guarantee the order of combination, especially with session-window merges.
- Should not mutate its arguments. `a.amount += b.amount; return a;` may appear to work with some state backends and corrupt state with others. Always return a new object.

---

## 3. `aggregate(AggregateFunction<IN, ACC, OUT>)`

Three independent type parameters. That is the whole point.

```java
public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable {
    ACC createAccumulator();
    ACC add(IN value, ACC accumulator);
    OUT getResult(ACC accumulator);
    ACC merge(ACC a, ACC b);
}
```

| method | called when | job |
|---|---|---|
| `createAccumulator()` | the window's **first** element arrives | return the empty/zero accumulator |
| `add(value, acc)` | **every** element | fold the element in, return the accumulator |
| `getResult(acc)` | the window **fires** | turn the accumulator into the output |
| `merge(a, b)` | **only when two windows merge** | combine two accumulators |

### When is `merge` actually called?

**Only for mergeable window assigners — in practice, session windows.**

A session window is created per element, then overlapping sessions are merged. When `[10:00, 10:05)` and `[10:03, 10:08)` merge into `[10:00, 10:08)`, their two accumulators must become one. That call is `merge(a, b)`.

For tumbling and sliding windows, `merge` is **never invoked**. You still have to write it, because the interface demands it. Two honest options:

```java
// Option A: implement it properly. Costs nothing, works everywhere.
@Override
public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
    return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
}

// Option B: be explicit that it's unreachable for this assigner.
@Override
public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
    throw new UnsupportedOperationException("not used with tumbling windows");
}
```

Prefer Option A. If someone later switches the assigner to a session window, Option B fails at runtime while Option A just works.

> **Key idea**
> `merge()` is dead code for tumbling and sliding windows and load-bearing for session windows. Write it correctly anyway — it is free.

---

## 4. Building a running average — the thing `reduce` can't do

Average needs to carry `(sum, count)` and emit a `double`.

```java
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class AverageAggregate
        implements AggregateFunction<Event, Tuple2<Double, Long>, Double> {

    @Override
    public Tuple2<Double, Long> createAccumulator() {
        return Tuple2.of(0.0, 0L);
    }

    @Override
    public Tuple2<Double, Long> add(Event value, Tuple2<Double, Long> acc) {
        return Tuple2.of(acc.f0 + value.amount, acc.f1 + 1L);
    }

    @Override
    public Double getResult(Tuple2<Double, Long> acc) {
        return acc.f1 == 0 ? 0.0 : acc.f0 / acc.f1;
    }

    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}
```

### Java notes

- `implements AggregateFunction<Event, Tuple2<Double, Long>, Double>` — `IN = Event`, `ACC = Tuple2<Double,Long>`, `OUT = Double`.
- `Tuple2<Double, Long>` — Flink's built-in pair. Fields are public and named `f0` and `f1`. Generics cannot hold primitives, so it's `Double`/`Long` (the object wrappers), not `double`/`long`. Java auto-boxes between them for you.
- `0L` — the `L` suffix makes it a `long` literal, not an `int`.
- `acc.f1 == 0 ? 0.0 : acc.f0 / acc.f1` — the **ternary operator**, `condition ? valueIfTrue : valueIfFalse`. Guards against divide-by-zero.
- `acc.f0 / acc.f1` is `Double / Long` → both unbox, Java promotes to double division. Correct here. Be careful in general: `int / int` in Java truncates (`7 / 2 == 3`).

### Using it

```java
DataStream<Double> avgPerUser = stream
    .assignTimestampsAndWatermarks(strategy)
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
    .aggregate(new AverageAggregate());

avgPerUser.print();
```

Output is bare doubles:

```
12.5
7.0
33.333333333333336
```

Which is useless — you don't know which user or which window. That is precisely what chapter 13's combined pattern fixes.

---

## 5. A richer accumulator with a POJO

`Tuple2` stops being readable past two fields. Use a small class:

```java
public class Stats {
    public long count = 0L;
    public double sum = 0.0;
    public double min = Double.MAX_VALUE;
    public double max = -Double.MAX_VALUE;

    public Stats() {}   // no-arg constructor REQUIRED for Flink POJO serialization
}
```

Flink recognizes a class as a POJO (and uses its fast serializer) only if it is public, has a public no-arg constructor, and its fields are public or have getters/setters. Miss the no-arg constructor and Flink silently falls back to Kryo, which is much slower.

```java
public class StatsAggregate
        implements AggregateFunction<Event, Stats, Stats> {

    @Override
    public Stats createAccumulator() {
        return new Stats();
    }

    @Override
    public Stats add(Event value, Stats acc) {
        acc.count += 1;
        acc.sum   += value.amount;
        acc.min    = Math.min(acc.min, value.amount);
        acc.max    = Math.max(acc.max, value.amount);
        return acc;
    }

    @Override
    public Stats getResult(Stats acc) {
        return acc;
    }

    @Override
    public Stats merge(Stats a, Stats b) {
        Stats out = new Stats();
        out.count = a.count + b.count;
        out.sum   = a.sum + b.sum;
        out.min   = Math.min(a.min, b.min);
        out.max   = Math.max(a.max, b.max);
        return out;
    }
}
```

Here `ACC` and `OUT` are the same class, so `getResult` is the identity. That's fine and common.

Mutating the accumulator in place inside `add` (rather than allocating a new one) is safe and is the recommended style for `AggregateFunction` — unlike `ReduceFunction`, the accumulator is private to this window and is not an element from the stream.

Note `min` starts at `Double.MAX_VALUE` and `max` at `-Double.MAX_VALUE`. Do **not** use `Double.MIN_VALUE` for max — in Java `Double.MIN_VALUE` is the smallest *positive* value (about 4.9e-324), not the most negative. Classic trap.

---

## 6. Incremental vs buffering — the memory argument

```
INCREMENTAL (reduce / aggregate)
  window state = 1 accumulator
  ┌──────────┐
  │ sum=143  │   ← updated on every arriving element, element then discarded
  │ count=7  │
  └──────────┘
  10,000,000 events → still ~16 bytes of state

BUFFERING (ProcessWindowFunction)
  window state = ListState of every element
  ┌────┬────┬────┬────┬────┬─── ... ───┬────┐
  │ e1 │ e2 │ e3 │ e4 │ e5 │           │ eN │
  └────┴────┴────┴────┴────┴─── ... ───┴────┘
  10,000,000 events × ~100 bytes → ~1 GB per key per window
```

The buffering cost is per key **and** per open window. With sliding windows (chapter 14) each element is copied into several windows, so multiply again.

**Choose by this rule:**

| you need | use |
|---|---|
| sum / count / min / max / average | `aggregate` |
| the same type in and out, simple fold | `reduce` |
| median, exact distinct count, top-K, sorting | `ProcessWindowFunction` (needs all elements) |
| an aggregate **plus** the window start/end | `aggregate(AggFn, ProcessWindowFunction)` — ch. 13 |

---

## 7. Side-by-side

```java
KeyedStream<Event, String> keyed = stream.keyBy(e -> e.userId);

// sum: shortest, output type = Event, only useful for one field
keyed.window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
     .sum("amount");

// reduce: custom logic, output type = Event (forced)
keyed.window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
     .reduce((a, b) -> new Event(a.userId, "TOTAL", a.amount + b.amount, a.timestamp));

// aggregate: any output type, any accumulator type
keyed.window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
     .aggregate(new AverageAggregate());   // Event -> Tuple2<Double,Long> -> Double
```

---

## Remember

- `sum`/`min`/`max`/`minBy`/`maxBy` are conveniences over `reduce`. `max` mutates one field of an arbitrary record; `maxBy` returns the real record. Prefer `maxBy`.
- `ReduceFunction<T>`: one type parameter, so IN = ACC = OUT. Cannot compute an average.
- `AggregateFunction<IN, ACC, OUT>`: four methods — `createAccumulator`, `add`, `getResult`, `merge`.
- `merge` runs only when windows merge, i.e. session windows. Implement it correctly anyway.
- Incremental functions hold one accumulator per window; `ProcessWindowFunction` holds every element.
- POJOs used as accumulators need a public no-arg constructor or you fall back to Kryo.
- `Double.MIN_VALUE` is a small *positive* number. Initialize a max with `-Double.MAX_VALUE`.

**Interview one-liners**

- *"reduce vs aggregate?"* → `reduce` forces input, accumulator and output to one type; `aggregate` has three type parameters, so it can compute things like averages.
- *"Why can't reduce compute an average?"* → The accumulator is fed back as an input, so it must be the input type; an average needs `(sum,count)` state and a `double` result.
- *"When is AggregateFunction.merge called?"* → Only when the assigner merges windows — session windows. Never for tumbling or sliding.
- *"Incremental vs full window function?"* → Incremental keeps one accumulator, O(1) state; `ProcessWindowFunction` buffers all elements, O(n) state but can compute order-dependent results like a median.
- *"max vs maxBy?"* → `max` returns a record with only the target field updated (other fields arbitrary); `maxBy` returns the actual maximal record.
- *"How do you get both an aggregate and the window boundaries?"* → `aggregate(AggregateFunction, ProcessWindowFunction)` — incremental state plus window metadata.
