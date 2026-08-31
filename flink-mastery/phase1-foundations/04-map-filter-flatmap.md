# 4. map, filter, flatMap

Three operators. Everything else in the DataStream API is built on the same pattern, so learn these properly and the rest is vocabulary.

The one thing that distinguishes them is **the arity of the output**:

| Operator | Records in | Records out | Signature |
|---|---|---|---|
| `map` | 1 | exactly 1 | `T → R` |
| `filter` | 1 | 0 or 1 (same record) | `T → boolean` |
| `flatMap` | 1 | 0, 1, or many | `T, Collector<R> → void` |

```
map      ●──►◆        one in, one out, type may change
filter   ●──►●   ●──►╳ one in, kept or dropped, type never changes
flatMap  ●──►◆◆◆  ●──►  one in, zero-to-many out, type may change
```

---

## `map` — one in, one out

```java
DataStream<String> words = env.fromElements("flink", "spark", "kafka");

DataStream<Integer> lengths = words.map(w -> w.length());
```

- `words` has element type `String`.
- `map` takes a `MapFunction<String, Integer>`. Because `MapFunction` has exactly one abstract method, the lambda `w -> w.length()` supplies it (ch. 2).
- The output stream's type changed to `Integer` — the compiler infers it from the lambda's return type.

Traced:

```
input :  "flink"   "spark"   "kafka"
             │         │         │
map          ▼         ▼         ▼
output:      5         5         5
```

`map` is **strictly 1:1**. You cannot skip a record and you cannot emit two. Returning `null` does not drop the record — it pushes a `null` downstream, which usually blows up in the next operator. Use `filter` or `flatMap` instead.

### The three ways to write it, again

```java
words.map(w -> w.length());                    // lambda
words.map(String::length);                     // method reference
words.map(new MapFunction<String, Integer>() { // anonymous inner class
    @Override public Integer map(String w) { return w.length(); }
});
```

For anything longer than a line, use a named `static` class — it is testable and it is the shape you will need anyway once you add state:

```java
public static class WordLength implements MapFunction<String, Integer> {
    @Override
    public Integer map(String w) throws Exception {
        return w.length();
    }
}
// ...
words.map(new WordLength());
```

---

## `filter` — keep or drop

```java
DataStream<Integer> numbers = env.fromElements(1, 2, 3, 4, 5, 6);

DataStream<Integer> evens = numbers.filter(n -> n % 2 == 0);
```

- `FilterFunction<T>` has one method: `boolean filter(T value)`.
- `true` → the record passes through **unchanged**. `false` → it is dropped.
- The output type is always the same as the input type. `filter` cannot transform.

Traced:

```
input :  1    2    3    4    5    6
         │    │    │    │    │    │
pred  : false true false true false true
         ╳    ▼    ╳    ▼    ╳    ▼
output:       2         4         6
```

**Null-safe predicates.** A `NullPointerException` inside a filter fails the whole task, not just the record:

```java
events.filter(e -> "purchase".equals(e.getType()));
//              ^^^^^^^^^^^^^^^^^^^^ literal first — safe if getType() is null
```

Compare with `e.getType().equals("purchase")` which NPEs on a null type and restarts your job.

**Push filters as early as possible.** Every record dropped before a `keyBy` is a record that never gets serialized and shipped over the network. Filtering after a shuffle wastes the shuffle.

```java
// good
source.filter(e -> e.getAmount() > 0).keyBy(Event::getUserId).sum("amount");

// wasteful — every record crosses the network, then most are discarded
source.keyBy(Event::getUserId).sum("amount");   // (filter placed later)
```

---

## `flatMap` — zero to many, via a `Collector`

```java
DataStream<String> lines = env.fromElements(
    "flink is fast",
    "streams are infinite"
);

DataStream<String> words = lines.flatMap(
    new FlatMapFunction<String, String>() {
        @Override
        public void flatMap(String line, Collector<String> out) {
            for (String w : line.split(" ")) {
                out.collect(w);
            }
        }
    });
```

The signature is the interesting part:

```java
void flatMap(T value, Collector<O> out) throws Exception;
//  ^^^^ returns nothing         ^^^^^^^^^^^^ output goes here instead
```

**`Collector<O>` is how you emit.** Instead of `return`ing a value, you call `out.collect(x)` — zero times, once, or a thousand times. Flink hands you the collector; it is wired straight to the next operator in the chain.

Traced:

```
input :  "flink is fast"              "streams are infinite"
              │                                │
              ▼ (3 collect calls)              ▼ (3 collect calls)
output:  "flink" "is" "fast"          "streams" "are" "infinite"
```

`flatMap` **subsumes both** `map` and `filter`:

```java
// map as a flatMap: exactly one collect
s.flatMap((String v, Collector<Integer> out) -> out.collect(v.length()));

// filter as a flatMap: collect conditionally
s.flatMap((String v, Collector<String> out) -> { if (v.length() > 3) out.collect(v); });
```

Use the specific operator when it fits — the Web UI names are clearer and Flink knows the output cardinality.

### The classic use: parse-or-drop

`flatMap` is the right operator for anything that can fail per record:

```java
public static class ParseEvent implements FlatMapFunction<String, Event> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void flatMap(String json, Collector<Event> out) {
        try {
            out.collect(MAPPER.readValue(json, Event.class));  // 1 output
        } catch (Exception e) {
            // 0 outputs — malformed record silently dropped instead of
            // failing the task and restarting the whole job
        }
    }
}
```

(In Phase 4 you will route those failures to a **side output** instead of dropping them, which is what you actually want in production.)

### Lambdas and `flatMap`: the type problem

This looks fine and does not compile:

```java
lines.flatMap((line, out) -> {           // ERROR
    for (String w : line.split(" ")) out.collect(w);
});
```

```
error: cannot infer type-variable(s) T,O
```

`Collector<O>` gives the compiler nothing to infer `O` from — `out.collect(w)` does not constrain the *declared* type parameter. Three fixes:

```java
// (a) explicit lambda parameter types + .returns()
lines.flatMap((String line, Collector<String> out) -> {
        for (String w : line.split(" ")) out.collect(w);
    })
    .returns(Types.STRING);

// (b) anonymous inner class — carries its own type info, no .returns() needed
lines.flatMap(new FlatMapFunction<String, String>() {
    @Override public void flatMap(String line, Collector<String> out) {
        for (String w : line.split(" ")) out.collect(w);
    }
});

// (c) named static class — best for anything real
lines.flatMap(new Tokenizer());
```

> **Key idea:** `flatMap` with a lambda almost always needs both explicit parameter types **and** `.returns(...)`. If it feels like fighting the compiler, that is your signal to write a named class.

---

## Type inference failures and `.returns()`

The general rule, stated once:

**Java erases generic types at compile time. A lambda is compiled to a synthetic method with no retained generic signature, so Flink cannot reflect the output type out of it. An anonymous inner class *is* a real class whose signature keeps the types, so Flink can.**

You will hit `InvalidTypesException` whenever a lambda's output type is generic:

```java
// FAILS at job-build time
DataStream<Tuple2<String, Integer>> pairs =
    words.map(w -> Tuple2.of(w, w.length()));
```

```
org.apache.flink.api.common.functions.InvalidTypesException:
The generic type parameters of 'Tuple2' are missing. In many cases lambda
methods don't provide enough information for automatic type extraction ...
```

The fix:

```java
import org.apache.flink.api.common.typeinfo.Types;

DataStream<Tuple2<String, Integer>> pairs = words
    .map(w -> Tuple2.of(w, w.length()))
    .returns(Types.TUPLE(Types.STRING, Types.INT));
```

`.returns()` attaches `TypeInformation` to the *preceding* operator. Place it immediately after the operator it describes — putting it at the end of a long chain annotates the wrong one.

The `Types` factory you will use:

```java
Types.STRING                Types.BOOLEAN
Types.INT   Types.LONG      Types.DOUBLE   Types.FLOAT   Types.SHORT
Types.TUPLE(Types.STRING, Types.LONG)          // Tuple2<String, Long>
Types.POJO(Event.class)                        // a POJO
Types.LIST(Types.STRING)                       // List<String>
Types.MAP(Types.STRING, Types.INT)             // Map<String, Integer>
Types.GENERIC(SomeClass.class)                 // last resort — uses Kryo
```

When does a lambda **not** need `.returns()`? When the output type is concrete and non-generic:

```java
words.map(w -> w.length());        // Integer — fine, no returns() needed
words.map(w -> w.toUpperCase());   // String  — fine
words.filter(w -> w.isEmpty());    // filter never changes type — always fine
words.map(w -> new Event(w, ...)); // Event is a concrete class — fine
```

So: **generic output type → `.returns()`. Concrete output type → nothing needed. `filter` → never needed.**

---

## Chaining

Operators return a new `DataStream`, so calls chain:

```java
env.fromElements("flink is fast", "spark is batch", "flink is stateful")
   .flatMap(new Tokenizer())                 // → words
   .filter(w -> w.length() > 2)              // → drop "is"
   .map(String::toUpperCase)                 // → uppercase
   .print();
```

Traced end to end:

```
"flink is fast"      →  flink, is, fast
"spark is batch"     →  spark, is, batch
"flink is stateful"  →  flink, is, stateful
                        ▼ filter length > 2
                        flink, fast, spark, batch, flink, stateful
                        ▼ map toUpperCase
                        FLINK, FAST, SPARK, BATCH, FLINK, STATEFUL
```

### Chaining is also a runtime concept

Do not confuse **method chaining** (syntax) with **operator chaining** (execution). Flink fuses adjacent operators into a single task when:

- they have the **same parallelism**, and
- the connection between them is **forward** (no `keyBy`, no `rebalance`, no `shuffle`), and
- chaining is not explicitly disabled.

Fused operators pass records as **direct method calls** — no serialization, no thread handoff, no network. Practically free.

```
NOT chained (3 tasks, 2 handoffs):
  [Source] --serialize--> [Map] --serialize--> [Filter]

Chained (1 task, 0 handoffs):
  [Source -> Map -> Filter]
```

The Web UI shows the chained group as one box named `Source: ... -> Map -> Filter`.

You can break a chain deliberately — usually to isolate an expensive operator so you can read its metrics or give it its own thread:

```java
stream.map(new Expensive()).disableChaining();   // this operator alone, both sides broken
stream.map(new Expensive()).startNewChain();     // break before, allow chaining after
env.disableOperatorChaining();                    // whole job — debugging only, costs throughput
```

**A `keyBy` always breaks the chain**, because records must be repartitioned across subtasks. That is chapter 5.

### Naming operators

Do this from day one. Unnamed operators appear in the UI and in metrics as `Map`, `Map`, `Filter` and you cannot tell them apart:

```java
stream
  .filter(e -> e.getAmount() > 0).name("drop-nonpositive").uid("drop-nonpositive")
  .map(new Enrich()).name("enrich-user").uid("enrich-user");
```

- `.name()` — display label in the Web UI and metric names.
- `.uid()` — **stable identity for state restore**. Set it on every stateful operator or you will not be able to restore from a savepoint after refactoring. Set it everywhere out of habit.

---

## One complete runnable example

```java
package com.akash.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BasicOperators {

    public static class Tokenizer implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String line, Collector<String> out) {
            for (String word : line.toLowerCase().split("\\W+")) {
                if (!word.isEmpty()) {
                    out.collect(word);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);          // parallelism 1 → deterministic output order

        DataStream<String> lines = env.fromElements(
            "Flink processes streams",
            "Flink is stateful",
            "streams are unbounded"
        );

        DataStream<Tuple2<String, Integer>> result = lines
            .flatMap(new Tokenizer()).name("tokenize")
            .filter(w -> w.length() > 2).name("drop-short")
            .map(w -> Tuple2.of(w, w.length()))
            .returns(Types.TUPLE(Types.STRING, Types.INT))   // lambda + generic out
            .name("with-length");

        result.print();
        env.execute("basic-operators");
    }
}
```

Output (parallelism 1, so input order is preserved):

```
(flink,5)
(processes,9)
(streams,7)
(flink,5)
(stateful,8)
(streams,7)
(are,3)
(unbounded,9)
```

Note `is` was dropped by the filter (length 2), and `are` survived (length 3).

---

## Remember

- `map` = 1→1 and the type can change. Never return `null` to mean "drop".
- `filter` = 1→0-or-1, type never changes. Put the literal first in `.equals()` to stay null-safe. Filter early, before any shuffle.
- `flatMap` = 1→N via `Collector.collect()`; it returns `void`. It is the right tool for parse-or-drop and for 1-to-many expansion.
- **Lambda + generic output type (`Tuple2`, `List`, `Map`) → `InvalidTypesException` → add `.returns(Types...)`** immediately after that operator.
- `flatMap` lambdas usually need **explicit parameter types AND** `.returns()`. Prefer a named `static` class.
- Anonymous inner classes and named classes carry their generic types at runtime, so they never need `.returns()`.
- **Operator chaining** fuses adjacent same-parallelism forward-connected operators into one task — records become method calls. `keyBy` always breaks the chain.
- Add `.name()` and `.uid()` to every operator, always.

**Interview one-liners**

- *"map vs flatMap?"* → Output cardinality: `map` is strictly 1:1 and returns a value; `flatMap` emits 0..N through a `Collector` and returns void.
- *"Why does my lambda throw InvalidTypesException?"* → Type erasure. The lambda retains no generic signature, so Flink can't extract the output `TypeInformation`. Fix with `.returns()` or an anonymous class.
- *"What is operator chaining and why does it matter?"* → Fusing adjacent operators into one task so records are passed by method call instead of serialized and handed between threads/machines. Broken by any repartitioning like `keyBy`.
- *"Where should filters go?"* → As early as possible, before any shuffle, so dropped records are never serialized or sent over the network.
