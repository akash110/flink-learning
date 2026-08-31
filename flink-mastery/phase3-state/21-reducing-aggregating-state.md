# 21. ReducingState and AggregatingState — Pre-Aggregating on Write

`ValueState`, `ListState`, and `MapState` are storage. `ReducingState` and `AggregatingState` are storage **with a function baked in**: you hand them one element, and they fold it into what's already there.

```
ValueState<Long> — you do the work:
    Long cur = state.value();
    if (cur == null) cur = 0L;
    state.update(cur + 1);           // read, null-check, combine, write

ReducingState<Long> — the state does the work:
    state.add(1L);                   // that's it
```

Same result. The difference is that the combine logic lives in the descriptor, so it's declared once and applied automatically, including at the moment of writing.

## `ReducingState<T>` — same type in, same type out

### The API

```java
void add(T value)  throws Exception;   // fold `value` into the stored value
T    get()         throws Exception;   // read the folded result (null if empty)
void clear();                          // delete this key's entry
```

Only three methods, and `add()` is where everything happens. There is no `update()` — you never set the value directly, you only fold into it.

### The `ReduceFunction`

```java
ReduceFunction<T> {
    T reduce(T value1, T value2) throws Exception;
}
```

`ReduceFunction<T>` is a Java **functional interface** — an interface with exactly one abstract method, which means you can implement it with a lambda:

```java
(a, b) -> a + b
```

That lambda *is* a `ReduceFunction<Long>`. Java figures out the types from the context.

Two requirements you must honour:

- **Both inputs and the output are the same type `T`.** That's the whole constraint. Sum: yes. Max: yes. Average: **no** — an average of two averages isn't the average, and you'd need a sum and a count, which is a different type. That's what `AggregatingState` is for.
- **The function must be associative and commutative** in practice. Flink applies it in whatever order records arrive, and after a restart the fold resumes from the stored value. `a+b`, `max(a,b)`, `min(a,b)` are fine. Subtraction is not.

### Declaring it

```java
private transient ReducingState<Long> countState;

@Override
public void open(OpenContext ctx) {
    ReducingStateDescriptor<Long> desc = new ReducingStateDescriptor<>(
            "count",                       // name
            (Long a, Long b) -> a + b,     // the ReduceFunction
            Long.class);                   // type
    countState = getRuntimeContext().getReducingState(desc);
}
```

The descriptor takes three arguments now: name, function, type. The function is serialized with the job, so it must not capture anything non-serializable — a lambda over local variables captures those variables, so keep them simple values.

### Worked example: running count, again

```java
public class ReducingCountFn extends KeyedProcessFunction<String, Event, String> {

    private transient ReducingState<Long> countState;

    @Override
    public void open(OpenContext ctx) {
        countState = getRuntimeContext().getReducingState(
                new ReducingStateDescriptor<>("count", Long::sum, Long.class));
        //                                              ^^^^^^^^^
        // Long::sum is a METHOD REFERENCE — shorthand for (a, b) -> Long.sum(a, b),
        // which is itself (a, b) -> a + b. Same thing, less noise.
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<String> out)
            throws Exception {

        // No read, no null check, no explicit write. add() folds 1 into the total.
        countState.add(1L);

        // get() reads the folded result.
        out.collect(ctx.getCurrentKey() + " count=" + countState.get());
    }
}
```

Compare to the `ValueState` version in chapter 19: four lines became one, and the null handling disappeared. On the **first** `add()` for a key, `reduce()` isn't called at all — the value is simply stored. So there is no null to handle.

### Worked example: running max spend

```java
private transient ReducingState<Double> maxState;

@Override
public void open(OpenContext ctx) {
    maxState = getRuntimeContext().getReducingState(
            new ReducingStateDescriptor<>("max-spend", Math::max, Double.class));
    //                                                 ^^^^^^^^^
    // (a, b) -> Math.max(a, b)
}

@Override
public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
    if ("PURCHASE".equals(e.type)) {
        maxState.add(e.amount);            // folds via max
        out.collect(ctx.getCurrentKey() + " max=" + maxState.get());
    }
}
```

### Why "pre-aggregates on write" matters

> **Key idea:** `ReducingState` stores exactly **one** value per key regardless of how many elements you add. It never accumulates a list.

That is the entire point.

```
ListState<Double> + sum it later          ReducingState<Double> with (a,b)->a+b
──────────────────────────────────        ───────────────────────────────────────
1,000,000 add() calls                     1,000,000 add() calls
  -> 1,000,000 stored elements              -> 1 stored Double
  -> get() deserializes 1,000,000           -> get() deserializes 1
  -> checkpoint carries 8 MB per key        -> checkpoint carries 8 bytes per key
```

`ReducingState` cannot grow. Neither can `AggregatingState` (as long as your accumulator is fixed-size). That property alone makes them the safest state types in Flink.

## `AggregatingState<IN, OUT>` — different types in and out

`ReducingState` breaks down as soon as the thing you accumulate isn't the thing you emit. The canonical case is an **average**: you must accumulate `(sum, count)` but you want to read a `double`.

`AggregatingState<IN, OUT>` has three types in play:

```
IN   what you add()                    e.g. Double  (one purchase amount)
ACC  what's actually stored (hidden)   e.g. Tuple2<Double, Long>  (sum, count)
OUT  what get() returns                e.g. Double  (the average)
```

`ACC` doesn't appear in `AggregatingState<IN, OUT>`'s own type parameters — it's internal — but it does appear in the `AggregateFunction` and the descriptor.

### The API

```java
void add(IN value)  throws Exception;   // fold into the accumulator
OUT  get()          throws Exception;   // accumulator -> result (null if empty)
void clear();
```

Identical shape to `ReducingState`. Only the types differ.

### The `AggregateFunction<IN, ACC, OUT>`

```java
AggregateFunction<IN, ACC, OUT> {
    ACC createAccumulator();                 // the empty/zero accumulator
    ACC add(IN value, ACC accumulator);      // fold one input in, RETURN the accumulator
    OUT getResult(ACC accumulator);          // finalize: accumulator -> output
    ACC merge(ACC a, ACC b);                 // combine two accumulators
}
```

You saw this interface in Phase 2 with `AggregateFunction` in windows — it's the same interface, reused here for state. Four methods:

| Method | Called when | Notes |
|---|---|---|
| `createAccumulator()` | First `add()` for a key | Must return a fresh accumulator each call — never a shared object |
| `add(value, acc)` | Every `add()` | **Must return the accumulator.** Mutating and returning `acc` is fine and normal |
| `getResult(acc)` | Every `get()` | Pure function; must not mutate `acc` |
| `merge(a, b)` | Session-window merges, and some rescale paths | Implement it correctly even if you think it's unused |

### Worked example: running average purchase amount

```java
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * AggregateFunction<IN, ACC, OUT>
 *   IN  = Double                   one purchase amount
 *   ACC = Tuple2<Double, Long>     (runningSum, runningCount)
 *   OUT = Double                   the average
 *
 * `implements` means this class promises to provide every method the
 * interface declares. Missing one is a compile error.
 */
public class AvgAggregate
        implements AggregateFunction<Double, Tuple2<Double, Long>, Double> {

    @Override
    public Tuple2<Double, Long> createAccumulator() {
        // Fresh zero accumulator. NEVER return a cached/shared instance —
        // every key would then share one accumulator.
        return Tuple2.of(0.0, 0L);
    }

    @Override
    public Tuple2<Double, Long> add(Double value, Tuple2<Double, Long> acc) {
        // acc.f0 and acc.f1 are Flink Tuple field accessors (public fields, 0-indexed).
        // Returning a NEW tuple is the safe style; mutating acc and returning it
        // is also legal and slightly cheaper.
        return Tuple2.of(acc.f0 + value, acc.f1 + 1);
    }

    @Override
    public Double getResult(Tuple2<Double, Long> acc) {
        // Guard against divide-by-zero. Shouldn't happen (get() on empty state
        // returns null before reaching here) but cheap insurance.
        if (acc.f1 == 0) {
            return 0.0;
        }
        return acc.f0 / acc.f1;
    }

    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
        // Component-wise: sums add, counts add. This is why (sum,count) is the
        // right accumulator for an average — it's mergeable, an average isn't.
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}
```

Wiring it into state:

```java
public class RunningAvgFn extends KeyedProcessFunction<String, Event, String> {

    // Note the type: <Double, Double>, i.e. <IN, OUT>. ACC is invisible here.
    private transient AggregatingState<Double, Double> avgState;

    @Override
    public void open(OpenContext ctx) {
        AggregatingStateDescriptor<Double, Tuple2<Double, Long>, Double> desc =
                new AggregatingStateDescriptor<>(
                        "avg-purchase",                  // name
                        new AvgAggregate(),              // the AggregateFunction
                        // The ACCUMULATOR's type. Tuple2<...> is generic, so a plain
                        // .class won't do — Java erases generics. TypeHint is the
                        // documented workaround (see ch. 19).
                        TypeInformation.of(new TypeHint<Tuple2<Double, Long>>() {}));

        avgState = getRuntimeContext().getAggregatingState(desc);
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<String> out)
            throws Exception {

        if (!"PURCHASE".equals(e.type)) {
            return;
        }

        avgState.add(e.amount);              // IN = Double

        Double avg = avgState.get();         // OUT = Double
        out.collect(String.format("%s avg=%.2f", ctx.getCurrentKey(), avg));
    }
}
```

The descriptor's type parameters are `<IN, ACC, OUT>` — all three — while the state handle is `<IN, OUT>`. That asymmetry confuses everyone the first time. The descriptor needs `ACC` because it must serialize the accumulator; your code never sees it.

### Trace (alice)

```
event               add(IN)   ACC after         get() -> OUT   emitted
─────────────────   ───────   ───────────────   ────────────   ──────────────────
PURCHASE 100.00     100.00    (100.00, 1)       100.00         alice avg=100.00
LOGIN                (skip)   (100.00, 1)       —              (nothing)
PURCHASE  50.00      50.00    (150.00, 2)        75.00         alice avg=75.00
PURCHASE  10.00      10.00    (160.00, 3)        53.33         alice avg=53.33

Stored per key: ONE Tuple2. Not three amounts. Not a list.
```

Store 10 million purchases through this and the state is still one `Tuple2` — 16 bytes.

### Other useful accumulators

```
Running average      ACC = (sum, count)              OUT = sum/count
Count distinct       ACC = HashSet<T> or HyperLogLog OUT = size / estimate
Min and max together ACC = (min, max)                OUT = a range object
Weighted average     ACC = (weightedSum, weightSum)  OUT = ws/w
Percentile (approx)  ACC = a t-digest sketch         OUT = the quantile
Standard deviation   ACC = (n, sum, sumOfSquares)    OUT = sqrt(...)
```

Note that `HashSet<T>` for count-distinct **is** unbounded — the "state can't grow" guarantee only holds when your accumulator is fixed-size. A sketch (HyperLogLog) keeps it bounded. This is a real production choice, not a footnote.

## All five state types, compared

| | `ValueState<T>` | `ListState<T>` | `MapState<K,V>` | `ReducingState<T>` | `AggregatingState<IN,OUT>` |
|---|---|---|---|---|---|
| Stores | one value | many elements | key→value pairs | one folded value | one accumulator |
| Write | `update(v)` | `add(v)` / `update(list)` | `put(k,v)` | `add(v)` — folds | `add(v)` — folds |
| Read | `value()` | `get()` → `Iterable` | `get(k)` | `get()` | `get()` → OUT |
| Delete part | — | — | `remove(k)` | — | — |
| In type = out type | n/a | n/a | n/a | **yes, required** | **no, that's the point** |
| Can grow unbounded | no | **yes** | **yes** | no | no* |
| Logic lives in | your code | your code | your code | the descriptor | the descriptor |
| Null when empty | `value()` → null | `get()` → null | `get(k)` → null | `get()` → null | `get()` → null |
| Getter | `getState` | `getListState` | `getMapState` | `getReducingState` | `getAggregatingState` |

\* unless your accumulator is itself unbounded, e.g. a `HashSet`.

### Use when

| State type | Use when |
|---|---|
| **`ValueState<T>`** | One value per key: a balance, a flag, a last-seen timestamp, a timer handle. **Your default.** |
| **`ListState<T>`** | You need the individual elements later: last N events, a buffer waiting for a join partner, records held until a timer fires. |
| **`MapState<K,V>`** | A per-key dictionary or set: per-category counters, deduplication, seen-device sets. Always over `ValueState<HashMap>` on RocksDB. |
| **`ReducingState<T>`** | A same-type fold with no need for the individual elements: sum, min, max, boolean OR. |
| **`AggregatingState<IN,OUT>`** | A fold where the accumulator differs from the result: average, weighted average, percentile, distinct count. |

## Choosing: the decision tree

```
Do you need the individual elements back?
├── YES ──► ListState<T>  (and bound its size!)
│
└── NO ──► Are you folding many inputs into one summary?
           │
           ├── NO ──► Is it a dictionary/set keyed by something?
           │          ├── YES ──► MapState<K,V>
           │          └── NO  ──► ValueState<T>
           │
           └── YES ─► Is the accumulator the same type as the result?
                      ├── YES ──► ReducingState<T>
                      └── NO  ──► AggregatingState<IN,OUT>
```

## When *not* to reach for these

`ReducingState` and `AggregatingState` are elegant, but be honest about the trade-off:

- **You cannot inspect the accumulator.** `get()` gives you `OUT`. If you later need the raw sum and count separately, `ValueState<Tuple2<Double,Long>>` is more flexible and barely more code.
- **You cannot un-add.** There is no `subtract()`. Sliding-window-style "add new, remove old" logic needs `ListState` or `MapState`.
- **Changing the function is a state-compatibility question.** The stored accumulator was produced by the old function. Swapping `sum` for `max` mid-flight gives you a nonsense value on restore, not an error.

In real jobs, `ValueState` and `MapState` cover the great majority of cases. `ReducingState` and `AggregatingState` earn their place when the fold is hot and the element count per key is large.

## Remember

- `ReducingState<T>`: `add()`, `get()`, `clear()`. Same type in and out. The `ReduceFunction` lives in the descriptor.
- `AggregatingState<IN,OUT>`: same three methods, but an `AggregateFunction<IN,ACC,OUT>` lets input, storage, and output all differ.
- The first `add()` for a key stores the value directly — `reduce()`/`merge()` isn't called, so there's no null case in your function.
- `getResult()` must not mutate the accumulator; `createAccumulator()` must return a fresh instance every time.
- `AggregatingStateDescriptor` is `<IN, ACC, OUT>`; the state handle is `<IN, OUT>`. The accumulator's `TypeInformation` is what the descriptor needs.
- Both types store **one** value per key no matter how many elements you add — that's the reason to use them.
- Unless the accumulator is itself unbounded. A `HashSet` accumulator reintroduces the growth problem; use a sketch.
- `ValueState` and `MapState` still cover most real code.

## Interview one-liners

- *"`ReducingState` vs `AggregatingState`?"* → `ReducingState` folds with a `ReduceFunction` where input, storage, and output are all the same type. `AggregatingState` uses an `AggregateFunction` with a separate accumulator type, so input, storage, and output can all differ — a running average as a `(sum, count)` accumulator read out as a `double`.
- *"Why not just `ValueState` and do it yourself?"* → You can, and often should. The wins are that the fold is declared once, the first-write null case disappears, and pre-aggregation on write is guaranteed rather than a convention someone can break.
- *"Why is an average not expressible as a `ReduceFunction`?"* → An average isn't associative over averages. You need `(sum, count)`, which is a different type from the `double` you want out — exactly the gap `AggregatingState` fills.
- *"Can `ReducingState` grow unbounded?"* → No. It stores one value per key regardless of input volume. Neither can `AggregatingState`, unless your accumulator is an unbounded structure like a `HashSet`.
- *"When is `merge()` called?"* → When two accumulators must be combined — merging session windows, and some restore paths. Implement it correctly regardless.
- *"Can you remove an element from `ReducingState`?"* → No. There's no inverse operation. If you need add-and-evict, use `ListState` or `MapState`.
- *"Which state type do you use most?"* → `ValueState`, then `MapState`. The other three are for specific shapes.
