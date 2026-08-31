# 13. ProcessWindowFunction

`aggregate()` gives you a number but no idea which window it came from. `ProcessWindowFunction` gives you full window metadata but buffers every element. The pattern at the end of this chapter gives you both.

> **Key idea**
> `ProcessWindowFunction` is called **once per window per key, at fire time**, with an `Iterable` over everything in the window and a `Context` describing the window itself.

---

## The class

```java
public abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window>
        extends AbstractRichFunction {

    public abstract void process(
            KEY key,
            Context context,
            Iterable<IN> elements,
            Collector<OUT> out) throws Exception;

    public void clear(Context context) throws Exception {}
}
```

Four type parameters:

| param | meaning | typical value |
|---|---|---|
| `IN` | element type entering the window | `Event` |
| `OUT` | what you emit | `String`, `Tuple4<...>`, a result POJO |
| `KEY` | the key type from `keyBy` | `String` |
| `W` | the window type | `TimeWindow` |

`W extends Window` is a **bounded type parameter** — Java for "W can be any type, as long as it is a subclass of `Window`". For all time-based assigners it is `TimeWindow`; for `GlobalWindows` it is `GlobalWindow`.

### The four `process` arguments

- **`key`** — the key value for this window. You don't get it from the elements; Flink hands it to you directly (and it's still correct for an empty-`Iterable` case).
- **`context`** — metadata: window bounds, watermark, timers, side outputs, per-window and global state.
- **`elements`** — an `Iterable<IN>` you can loop over. **You may iterate it only once** and you should not hold a reference to it after `process` returns.
- **`out`** — a `Collector<OUT>`. Call `out.collect(value)` zero, one, or many times. Unlike `map`, you are not obliged to emit exactly one record.

---

## A first example

```java
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CountPerWindow
        extends ProcessWindowFunction<Event, String, String, TimeWindow> {

    @Override
    public void process(String key,
                        Context context,
                        Iterable<Event> elements,
                        Collector<String> out) {

        long count = 0;
        double sum = 0.0;
        for (Event e : elements) {      // enhanced for-loop: "for each Event e in elements"
            count++;
            sum += e.amount;
        }

        long start = context.window().getStart();
        long end   = context.window().getEnd();

        out.collect(String.format(
                "user=%s window=[%d, %d) count=%d sum=%.2f",
                key, start, end, count, sum));
    }
}
```

Wire it up:

```java
stream
    .assignTimestampsAndWatermarks(strategy)
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
    .process(new CountPerWindow())
    .print();
```

Output:

```
user=u1 window=[0, 10000) count=3 sum=6.00
user=u2 window=[0, 10000) count=1 sum=5.00
user=u1 window=[10000, 20000) count=2 sum=9.00
```

Java notes:
- `extends ProcessWindowFunction<...>` — it's an abstract **class**, not an interface, so `extends`, not `implements`, and you cannot use a lambda.
- `for (Event e : elements)` is the enhanced for-loop; works on anything implementing `Iterable`.
- `String.format` uses printf placeholders: `%s` string, `%d` integer, `%.2f` float with 2 decimals.

---

## The `Context`

```java
public abstract class Context {
    public abstract W window();                       // this window's metadata

    public abstract long currentProcessingTime();
    public abstract long currentWatermark();

    public abstract KeyedStateStore windowState();    // scoped to key+window
    public abstract KeyedStateStore globalState();    // scoped to key, across windows

    public abstract <X> void output(OutputTag<X> outputTag, X value);
}
```

### `context.window()`

For `TimeWindow`:

```java
long start   = context.window().getStart();      // inclusive
long end     = context.window().getEnd();        // EXCLUSIVE
long maxTs   = context.window().maxTimestamp();  // == end - 1
```

`maxTimestamp()` is the largest timestamp that could belong to this window, i.e. `end - 1`. This is the value used internally to decide firing.

`getStart()` and `getEnd()` are epoch millis. Format them for humans:

```java
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

DateTimeFormatter FMT = DateTimeFormatter
        .ofPattern("HH:mm:ss")
        .withZone(ZoneId.of("UTC"));

String pretty = FMT.format(Instant.ofEpochMilli(context.window().getStart()));
```

**Do not create the formatter inside `process`** — that allocates on every window fire. Make it a `static final` field, or build it in `open()`.

### `currentWatermark()` and `currentProcessingTime()`

Useful for diagnosing lag:

```java
long lagMs = context.currentProcessingTime() - context.window().getEnd();
// "this window closed lagMs milliseconds after its event-time end, in wall clock terms"
```

### `windowState()` vs `globalState()`

Both return a `KeyedStateStore` you can pull `ValueState`, `ListState`, etc. from.

- `windowState()` — scoped to **this key and this window**. Survives across multiple firings of the same window (which happens with early triggers, or with allowed lateness re-firing). Cleaned up when the window is purged. Use it to remember "I already emitted an alert for this window."
- `globalState()` — scoped to **this key only**, shared across all of that key's windows. Use it to compare this window against the previous one.

```java
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

@Override
public void process(String key, Context ctx, Iterable<Event> elements, Collector<String> out)
        throws Exception {

    ValueState<Double> lastWindowSum = ctx.globalState()
            .getState(new ValueStateDescriptor<>("lastSum", Double.class));

    double sum = 0.0;
    for (Event e : elements) sum += e.amount;

    Double previous = lastWindowSum.value();          // null if never set
    if (previous != null && sum > previous * 2) {
        out.collect(key + " doubled: " + previous + " -> " + sum);
    }
    lastWindowSum.update(sum);
}
```

`ValueStateDescriptor<>("lastSum", Double.class)` — the `<>` is the **diamond operator**; Java infers `<Double>` from the left-hand side. `previous` is a `Double` object so it can be `null`; a primitive `double` could not be, which is why state APIs use wrapper types.

`globalState` is never cleaned up automatically. Override `clear(Context)` if you need to release it.

### Side outputs

```java
ctx.output(lateTag, someValue);
```
Emits into a secondary stream. Covered fully in chapter 16.

### Timers

The `Context` of `ProcessWindowFunction` does **not** expose a `TimerService` for registering your own timers — that's `KeyedProcessFunction` (Phase 3). What you get here is `currentProcessingTime()` and `currentWatermark()` for reading time, plus the state stores. If you need custom timers inside windowing logic, that belongs in a `Trigger` (chapter 15).

---

## The memory cost — be honest about it

To call `process(key, ctx, elements, out)` with *all* elements, Flink must have kept all elements. They live in a `ListState` in the state backend for the entire lifetime of the window.

```
key "u1", window [10:00, 10:10)
┌─────────────────────────────────────────────┐
│ ListState<Event>                            │
│  e1 e2 e3 e4 e5 e6 ... e999998 e999999      │  ← all of it, until fire time
└─────────────────────────────────────────────┘
```

The bill:

```
state = (elements per key per window)
      × (bytes per element)
      × (number of active keys)
      × (number of simultaneously open windows)
```

That last factor is what people forget:
- Tumbling: 1 open window per key (plus more if you set `allowedLateness`).
- Sliding `size=1h, slide=1min`: **60** open windows per key, each holding a full copy.
- A 1-hour window at 10k events/sec across 1000 keys, 100 bytes each: 3.6 GB just for that operator.

With the heap state backend that is direct JVM heap pressure and GC pauses. With RocksDB it is disk plus serialization cost on every `add`.

**Only buffer when the computation genuinely needs every element:** median, percentiles, exact distinct counts, top-K, anything order-dependent, or emitting the raw records.

---

## The important pattern: incremental aggregation + window metadata

Both `aggregate` and `reduce` have a two-argument overload where the second argument is a `ProcessWindowFunction`.

```java
public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
        AggregateFunction<T, ACC, V> aggFunction,
        ProcessWindowFunction<V, R, K, W> windowFunction)
```

### How to read that signature

Track the types through the pipeline:

```
        T                 ACC                V                    R
   ┌─────────┐      ┌────────────┐     ┌──────────┐         ┌──────────┐
   │  Event  │─add─▶│ (sum,count)│─get─▶│  Double  │──process──▶│  String  │
   └─────────┘      └────────────┘  Result└──────────┘         └──────────┘
   stream element    accumulator     agg output          final output
                     (O(1) state)    (ONE value)
```

- `AggregateFunction<T, ACC, V>` — `T` is the stream element type, `ACC` your accumulator, `V` the aggregate's result.
- `ProcessWindowFunction<V, R, K, W>` — its **input is `V`**, not `T`. This is the whole trick and the part everyone gets wrong.

At fire time Flink calls `getResult(acc)` once, wraps that single value in a one-element `Iterable`, and calls `process`. So inside the process function:

```java
V value = elements.iterator().next();   // exactly one element, always
```

The window never buffered anything. It held the accumulator. You still get `context.window()`.

### Full working example: `(user, windowStart, windowEnd, count, sum, avg)`

**Step 1 — the accumulator's output type.**

```java
public class WindowStats {
    public long count;
    public double sum;
    public double avg;

    public WindowStats() {}          // required no-arg constructor

    public WindowStats(long count, double sum, double avg) {
        this.count = count;
        this.sum = sum;
        this.avg = avg;
    }
}
```

`this.count = count;` — `this` disambiguates the field from the same-named parameter. Standard Java constructor idiom.

**Step 2 — the AggregateFunction. `IN = Event`, `ACC = Tuple2<Long,Double>`, `OUT = WindowStats`.**

```java
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class StatsAgg
        implements AggregateFunction<Event, Tuple2<Long, Double>, WindowStats> {

    @Override
    public Tuple2<Long, Double> createAccumulator() {
        return Tuple2.of(0L, 0.0);              // (count, sum)
    }

    @Override
    public Tuple2<Long, Double> add(Event e, Tuple2<Long, Double> acc) {
        return Tuple2.of(acc.f0 + 1L, acc.f1 + e.amount);
    }

    @Override
    public WindowStats getResult(Tuple2<Long, Double> acc) {
        double avg = acc.f0 == 0 ? 0.0 : acc.f1 / acc.f0;
        return new WindowStats(acc.f0, acc.f1, avg);
    }

    @Override
    public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}
```

**Step 3 — the ProcessWindowFunction. Its `IN` is `WindowStats`, the aggregate's output.**

```java
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AddWindowInfo
        extends ProcessWindowFunction<WindowStats, String, String, TimeWindow> {
        //                            ^^^^^^^^^^^  ^^^^^^  ^^^^^^  ^^^^^^^^^^
        //                            IN = agg out OUT     KEY     window type

    @Override
    public void process(String key,
                        Context context,
                        Iterable<WindowStats> elements,
                        Collector<String> out) {

        // Exactly one element: the AggregateFunction's getResult() output.
        WindowStats s = elements.iterator().next();

        out.collect(String.format(
                "user=%s window=[%d,%d) count=%d sum=%.2f avg=%.2f",
                key,
                context.window().getStart(),
                context.window().getEnd(),
                s.count, s.sum, s.avg));
    }
}
```

**Step 4 — wire them together.**

```java
stream
    .assignTimestampsAndWatermarks(strategy)
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
    .aggregate(new StatsAgg(), new AddWindowInfo())     // <-- two arguments
    .print();
```

Output:

```
user=u1 window=[0,10000) count=3 sum=6.00 avg=2.00
user=u2 window=[0,10000) count=1 sum=5.00 avg=5.00
user=u1 window=[10000,20000) count=2 sum=9.00 avg=4.50
```

Same information as buffering every element, at O(1) state per window.

### The `reduce` version

```java
stream
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
    .reduce(
        (a, b) -> new Event(a.userId, a.type, a.amount + b.amount, a.timestamp),
        new ProcessWindowFunction<Event, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context ctx,
                                Iterable<Event> elements, Collector<String> out) {
                Event reduced = elements.iterator().next();   // one element again
                out.collect(key + " [" + ctx.window().getStart()
                          + "," + ctx.window().getEnd() + ") total=" + reduced.amount);
            }
        });
```

Same idea: the `ProcessWindowFunction`'s `IN` is the reduce's output type, which for `reduce` is always the stream element type.

### The mistake everyone makes once

```java
// DOES NOT COMPILE
.aggregate(new StatsAgg(),
           new ProcessWindowFunction<Event, String, String, TimeWindow>() { ... })
//                                   ^^^^^ wrong — must be WindowStats
```

The process function receives the aggregate's **output**, not the raw stream elements. The compiler error is a long generics mismatch; read it as "expected `ProcessWindowFunction<WindowStats,...>`, found `ProcessWindowFunction<Event,...>`".

---

## Decision table

| you want | use |
|---|---|
| sum only, no metadata | `aggregate(AggFn)` |
| sum + window start/end | `aggregate(AggFn, ProcessWindowFunction)` |
| median, top-K, all raw records | `process(ProcessWindowFunction)` alone |
| compare against previous window | `process(...)` with `ctx.globalState()` |
| emit 0 or many records per window | any `ProcessWindowFunction` — `Collector` is unrestricted |

---

## Remember

- `ProcessWindowFunction<IN, OUT, KEY, W>` — `process(key, context, elements, out)`.
- It runs **once per window per key, at fire time**, and buffers every element until then.
- `context.window().getStart()` inclusive, `getEnd()` exclusive, `maxTimestamp() == end - 1`.
- `windowState()` = this key + this window. `globalState()` = this key, all windows, never auto-cleaned.
- `aggregate(AggFn, PWF)` / `reduce(RedFn, PWF)` give incremental state **and** window metadata. The PWF's `IN` is the *aggregate's output type*, and its `Iterable` always has exactly one element.
- Buffer only when the answer needs every element (median, percentile, top-K, raw output).
- Open-window count multiplies buffering cost — sliding windows are the killer.

**Interview one-liners**

- *"What does ProcessWindowFunction give you that aggregate doesn't?"* → Window metadata (start/end), the key, watermark/processing time, per-window and global state, side outputs — and access to every element.
- *"Why is it expensive?"* → It buffers all elements in state for the window's whole lifetime; cost scales with elements × keys × simultaneously open windows.
- *"How do you get both?"* → `aggregate(AggregateFunction, ProcessWindowFunction)`. Flink folds incrementally, then passes the single aggregate result into `process` alongside the window context.
- *"How many elements are in the Iterable in that combined form?"* → Exactly one — the `getResult()` output.
- *"windowState vs globalState?"* → Window-scoped state is cleared with the window; global state is per-key across windows and must be cleaned up by you.
- *"When would you buffer deliberately?"* → Order-dependent or holistic computations: median, exact distinct, top-K, session replay.
