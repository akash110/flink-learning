# 19. ValueState — One Value Per Key

`ValueState<T>` is the state type you will use 80% of the time. One slot, holding one object, per key.

```
ValueState<Long> "count"

   alice  ->  [ 3 ]
   bob    ->  [ 1 ]
   carol  ->  [ 7 ]
```

That's the whole data structure. Three methods: read it, write it, delete it.

## The three methods

```java
T    value()   throws Exception;   // read the current key's value; null if never set
void update(T newValue) throws Exception;  // overwrite the current key's value
void clear();                      // delete the current key's entry entirely
```

Notes for a Java newcomer:

- `<T>` is a **generic type parameter**. `ValueState<Long>` means "a ValueState holding a `Long`". It's like a typed box — the compiler enforces that `update()` only accepts a `Long`.
- `throws Exception` means these methods can fail (RocksDB disk error, serialization error). Java forces you to either catch it or declare `throws Exception` on your own method. Flink's function methods already declare `throws Exception`, so you can just call them.
- **`value()` returns `null` if the key has never been written.** Not 0, not empty — `null`. Handling that null is the single most common source of `NullPointerException` in Flink jobs.

## You need a `RichFunction`

State comes from the **runtime context**, and only "rich" functions have one.

```
FlatMapFunction          -> no runtime context, no state
RichFlatMapFunction      -> has open(), close(), getRuntimeContext()  ✔
KeyedProcessFunction     -> rich by definition, plus timers            ✔✔
ProcessFunction          -> rich, but no keyed state (not a KeyedStream)
```

`Rich*` versions add three things:

| Method | When it runs | What it's for |
|---|---|---|
| `open(OpenContext ctx)` | Once, per parallel subtask, before any record | Initialize state handles, open connections |
| `close()` | Once, at shutdown | Release resources |
| `getRuntimeContext()` | Any time after `open()` | Access to state, metrics, subtask index |

> **Key idea:** `open()` runs **once per subtask**, not once per key and not once per record. It is where every state handle gets created.

A note on the signature. Flink 1.19+ uses `open(OpenContext ctx)`; older code (and plenty of tutorials) uses `open(Configuration parameters)`, which is deprecated but still compiles in 1.18/1.20. Use `OpenContext`.

## `ValueStateDescriptor` — the state's ID card

You don't construct state. You describe it, and ask the runtime for it.

```java
ValueStateDescriptor<Long> descriptor =
        new ValueStateDescriptor<>("count", Long.class);
//                                  ^          ^
//                                  |          └── the type, so Flink picks a serializer
//                                  └── the state NAME (its identity within this operator)
```

Why a descriptor at all?

1. **Name** — state is identified by `(operator, name, key)`. The name is how a savepoint finds this state again after you redeploy. **Renaming a state descriptor loses the state.**
2. **Serializer** — Flink must be able to turn your `T` into bytes for checkpoints (and, on RocksDB, on every single access). The `Class` or `TypeInformation` you pass is how it picks one.

`new ValueStateDescriptor<>(...)` uses Java's **diamond operator** `<>` — the compiler infers `<Long>` from the variable's declared type. It's shorthand for `new ValueStateDescriptor<Long>(...)`.

Three ways to give the type, in increasing order of preference for anything generic:

```java
// 1. Class object — fine for simple, non-generic types
new ValueStateDescriptor<>("count", Long.class);

// 2. TypeInformation — Flink's own type system, more precise
new ValueStateDescriptor<>("count", Types.LONG);

// 3. TypeHint — REQUIRED when T is itself generic, because Java erases generics
new ValueStateDescriptor<>("pair", TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {}));
```

That third form looks bizarre. `new TypeHint<...>() {}` with the trailing `{}` creates an **anonymous subclass**, and Java keeps generic information on a class's superclass declaration even though it erases it everywhere else. Flink reads it back by reflection. You don't need to understand the trick — just use `TypeHint` whenever the type has angle brackets inside it.

## Getting the handle

```java
countState = getRuntimeContext().getState(descriptor);
```

There is one getter per state type:

```java
getState(ValueStateDescriptor<T>)                 -> ValueState<T>
getListState(ListStateDescriptor<T>)              -> ListState<T>
getMapState(MapStateDescriptor<K,V>)              -> MapState<K,V>
getReducingState(ReducingStateDescriptor<T>)      -> ReducingState<T>
getAggregatingState(AggregatingStateDescriptor<...>) -> AggregatingState<IN,OUT>
```

Calling `getState` twice with the same descriptor returns a handle to the **same** state, not a copy.

## The critical gotcha: state is scoped to the current key

Look at what's *missing* from `value()`:

```java
Long c = countState.value();     // no key argument!
countState.update(c + 1);        // no key argument!
```

Before Flink calls your `processElement` / `flatMap` for a record, it sets the "current key" on the backend. Every state access inside that call silently reads and writes **that key's cell**.

```
record (alice, LOGIN) arrives
     |
     v
  Flink: setCurrentKey("alice")     <-- invisible to you
     |
     v
  your flatMap runs
     countState.value()   -> reads alice's cell   -> 2
     countState.update(3) -> writes alice's cell
     |
     v
record (bob, LOGIN) arrives
     |
     v
  Flink: setCurrentKey("bob")       <-- invisible to you
     |
     v
  your flatMap runs
     countState.value()   -> reads bob's cell     -> null
```

Consequences:

- You **cannot** read another key's state. If you need alice's data while processing bob, you need a different design (a join, a broadcast, or re-keying).
- You **cannot** iterate all keys from inside the function. There is no `getAllKeys()`. (The State Processor API can do this offline, on a savepoint — not at runtime.)
- Calling `.value()` where there is no current key — in `open()`, or in a non-keyed operator — throws at runtime.

## Why the field must be `transient` and set in `open()`

This is the piece of Java that trips up everyone, so here it is properly.

```java
public class CountFn extends RichFlatMapFunction<Event, Tuple2<String, Long>> {

    // transient  = "do not include this field when Java serializes the object"
    private transient ValueState<Long> countState;

    @Override
    public void open(OpenContext ctx) {
        countState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("count", Long.class));
    }
    ...
}
```

### What actually happens when you submit a job

```
1. You build the job graph on the CLIENT machine.
   `new CountFn()` is constructed here — a plain Java object.

2. Flink SERIALIZES that object (standard Java serialization) and ships the
   bytes to the JobManager, which ships them to each TaskManager.

3. Each TaskManager DESERIALIZES its own copy of CountFn.

4. Only NOW, on the TaskManager, does the runtime exist.
   Flink calls open(), and getRuntimeContext() finally has a state backend
   behind it.

5. Records start flowing; flatMap() is called.
```

Now the two rules make sense:

**Why not the constructor?** At step 1 there is no runtime, no state backend, no keyed context. `getRuntimeContext()` in a constructor either throws or returns something useless. State handles can only be created in step 4.

**Why `transient`?** At step 2 Java tries to serialize every field of `CountFn`. A `ValueState` handle is a live pointer into a running state backend — it is not serializable, and it would be meaningless on another machine anyway. Without `transient` you get:

```
org.apache.flink.api.common.InvalidProgramException:
    The implementation of the RichFlatMapFunction is not serializable.
    ... java.io.NotSerializableException
```

`transient` tells Java "skip this field". After deserialization the field is `null` — which is exactly right, because `open()` is about to fill it in.

> **Key idea:** `transient` field + assignment in `open()` is not a style choice. It is forced by the fact that your function object is serialized on the client and reconstructed on the worker.

The same rule applies to any non-serializable resource: DB connections, HTTP clients, Kafka producers. Declare `transient`, build in `open()`, release in `close()`.

## Worked example 1: user → running count

Count every event per user.

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * RichFlatMapFunction<IN, OUT>:
 *   IN  = Event          (the Phase 1 POJO: userId, type, amount, timestamp)
 *   OUT = Tuple2<String, Long>   (userId, runningCount)
 */
public class RunningCountFn extends RichFlatMapFunction<Event, Tuple2<String, Long>> {

    // The handle. transient because the function object is serialized to the workers.
    // Long (capital L) is the OBJECT wrapper, not the primitive `long`.
    // State must hold objects, because it must be able to hold null.
    private transient ValueState<Long> countState;

    @Override
    public void open(OpenContext ctx) {
        // Runs ONCE per parallel subtask, before the first record.
        ValueStateDescriptor<Long> desc =
                new ValueStateDescriptor<>("event-count", Long.class);
        countState = getRuntimeContext().getState(desc);
    }

    @Override
    public void flatMap(Event event, Collector<Tuple2<String, Long>> out) throws Exception {
        // Reads THIS KEY's cell. Flink already set the current key from keyBy.
        Long current = countState.value();

        // First event for this key -> value() is null, not 0. Always handle it.
        if (current == null) {
            current = 0L;   // the L suffix makes this a long literal, not an int
        }

        long updated = current + 1;

        // Write it back. Nothing is persisted until you call update().
        countState.update(updated);

        // Tuple2.of(a, b) is the tidy way to build a Flink Tuple2.
        out.collect(Tuple2.of(event.userId, updated));
    }
}
```

Wiring it up:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<Event> events = /* ... source from Phase 1 ... */;

events
    // keyBy is MANDATORY before keyed state. Without it, getState() throws.
    // e -> e.userId is a Java LAMBDA: "given e, return e.userId".
    .keyBy(e -> e.userId)
    .flatMap(new RunningCountFn())
    .print();

env.execute("running count");
```

### Trace

Input, in order:

```
(alice, LOGIN,   0.0,  1000)
(bob,   LOGIN,   0.0,  1100)
(alice, PURCHASE, 20.0, 1200)
(alice, PURCHASE, 35.0, 1300)
(bob,   LOGOUT,  0.0,  1400)
```

```
record                 current key   value() before   update()   emitted
─────────────────────  ───────────   ──────────────   ────────   ───────────
alice LOGIN            alice         null             1          (alice, 1)
bob   LOGIN            bob           null             1          (bob,   1)
alice PURCHASE         alice         1                2          (alice, 2)
alice PURCHASE         alice         2                3          (alice, 3)
bob   LOGOUT           bob           1                2          (bob,   2)

state after:   count[alice] = 3
               count[bob]   = 2
```

Notice bob's count is untouched by alice's records. That's key scoping doing its job, with zero code from you.

## Worked example 2: user → running balance

Same shape, holding a `Double`, and doing something slightly more interesting: `PURCHASE` subtracts, `DEPOSIT` adds.

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * KeyedProcessFunction<K, I, O>:
 *   K = String   the key type produced by keyBy
 *   I = Event    input records
 *   O = String   output records
 *
 * Using KeyedProcessFunction instead of RichFlatMapFunction because it also
 * gives us ctx.timestamp(), ctx.getCurrentKey(), timers and side outputs
 * (chapter 23). It's rich already — no "Rich" prefix needed.
 */
public class RunningBalanceFn extends KeyedProcessFunction<String, Event, String> {

    private transient ValueState<Double> balanceState;

    @Override
    public void open(OpenContext ctx) {
        balanceState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("balance", Double.class));
    }

    @Override
    public void processElement(Event event,
                               Context ctx,
                               Collector<String> out) throws Exception {

        // Read this user's balance; null means we've never seen them.
        Double balance = balanceState.value();
        if (balance == null) {
            balance = 0.0;
        }

        // Java switch on a String. "PURCHASE" spends, "DEPOSIT"/"REFUND" credit.
        switch (event.type) {
            case "PURCHASE":
                balance -= event.amount;   // shorthand for balance = balance - amount
                break;                     // without break, Java falls into the next case
            case "DEPOSIT":
            case "REFUND":
                balance += event.amount;
                break;
            default:
                // LOGIN, LOGOUT etc. don't move money — emit nothing, change nothing.
                return;
        }

        balanceState.update(balance);

        // ctx.getCurrentKey() gives you the key. Same as event.userId here,
        // but it's the safe way when the key is derived.
        out.collect(String.format("%s balance=%.2f", ctx.getCurrentKey(), balance));
    }
}
```

```java
events
    .keyBy(e -> e.userId)
    .process(new RunningBalanceFn())   // .process() for a ProcessFunction
    .print();
```

`.flatMap()` takes a `FlatMapFunction`; `.process()` takes a `ProcessFunction`. That's the only difference in wiring.

### Trace

```
(alice, DEPOSIT,  100.00, 1000)
(alice, PURCHASE,  30.00, 1100)
(bob,   PURCHASE,  15.50, 1200)
(alice, LOGIN,      0.00, 1300)
(alice, PURCHASE,  25.00, 1400)
(bob,   DEPOSIT,   50.00, 1500)
```

```
record             key     before    op        after    emitted
────────────────   ─────   ───────   ───────   ──────   ──────────────────────
alice DEPOSIT 100  alice   null->0    +100      100.00   alice balance=100.00
alice PURCHASE 30  alice   100.00     -30       70.00    alice balance=70.00
bob   PURCHASE 15  bob     null->0    -15.50    -15.50   bob   balance=-15.50
alice LOGIN        alice   70.00      (skip)    70.00    (nothing)
alice PURCHASE 25  alice   70.00      -25       45.00    alice balance=45.00
bob   DEPOSIT 50   bob     -15.50     +50       34.50    bob   balance=34.50

state after:   balance[alice] = 45.00
               balance[bob]   = 34.50
```

Note the `LOGIN` record: `return` skipped it entirely, so `update()` was never called and alice's balance stayed at 70.00. Not writing is a perfectly good operation.

## `clear()` — deleting state

```java
balanceState.clear();   // removes THIS KEY's entry from the backend
```

After `clear()`, `value()` returns `null` again. Use it when a key is finished:

```java
if ("ACCOUNT_CLOSED".equals(event.type)) {
    balanceState.clear();   // stop paying to store this user forever
    return;
}
```

Note `"ACCOUNT_CLOSED".equals(event.type)` rather than `event.type.equals("ACCOUNT_CLOSED")`. Putting the literal first means a `null` `event.type` returns `false` instead of throwing a `NullPointerException`. It's a standard Java defensive idiom.

> **Key idea:** `clear()` is the only thing standing between you and infinite state growth for keys that will never return. Chapter 22's TTL automates it.

## The mistakes, collected

```java
// ❌ 1. Forgetting keyBy
events.process(new RunningBalanceFn());
//    -> java.lang.IllegalStateException / "Keyed state can only be used on a KeyedStream"

// ❌ 2. Creating state in the constructor
public RunningCountFn() {
    countState = getRuntimeContext().getState(...);   // no runtime yet -> throws
}

// ❌ 3. Forgetting transient
private ValueState<Long> countState;   // -> NotSerializableException at submit time

// ❌ 4. Not handling null
long c = countState.value() + 1;       // -> NullPointerException on the first record

// ❌ 5. Mutating without update() — this one is SILENT on RocksDB
List<String> list = listValueState.value();
list.add("x");                          // mutated a deserialized COPY
// nothing persisted. On HashMapStateBackend it accidentally works
// (same object reference), on RocksDB it silently doesn't. Always call update().

// ❌ 6. Changing the descriptor name between deployments
new ValueStateDescriptor<>("count", Long.class);      // v1
new ValueStateDescriptor<>("eventCount", Long.class); // v2 -> v1's state is orphaned
```

Number 5 deserves emphasis: **always call `update()` after changing a value**, even if you think you mutated the stored object in place. Code that works on the heap backend and breaks on RocksDB is a miserable bug to find in production.

## Remember

- `ValueState<T>`: one value per key. `value()`, `update()`, `clear()`.
- `value()` returns `null` for an unseen key. Handle it on the first line.
- State needs a rich function: `RichFlatMapFunction`, `KeyedProcessFunction`, etc.
- `open(OpenContext)` runs once per subtask. Build every state handle there.
- The descriptor's **name** is the state's identity across restarts. Never rename it casually.
- The field must be `transient` because the function object is Java-serialized to the workers.
- You never pass a key. Flink sets the current key before calling you.
- Always `update()`. Mutating in place works on heap and silently fails on RocksDB.
- `clear()` when a key is done, or set a TTL.

## Interview one-liners

- *"Why must state be declared `transient` and set in `open()`?"* → The function object is Java-serialized on the client and shipped to TaskManagers; a state handle isn't serializable and the state backend doesn't exist until `open()` runs on the worker.
- *"What does `value()` return for a new key?"* → `null`. Not zero, not empty.
- *"How do you read another key's state?"* → You can't. State access is implicitly scoped to the current key; you'd need a join, broadcast state, or a different keying.
- *"What identifies a piece of state?"* → The triple (operator, state descriptor name, key). Renaming the descriptor orphans the old state in the savepoint.
- *"Why does my job work with HashMapStateBackend and break on RocksDB?"* → Almost always mutating a value returned by `value()` without calling `update()`. Heap returns the live object; RocksDB returns a deserialized copy.
- *"When does `open()` run?"* → Once per parallel subtask, before the first record. Not per key, not per record.
- *"`RichFlatMapFunction` vs `KeyedProcessFunction`?"* → Both give you state; only `KeyedProcessFunction` gives you timers, the current key, the record timestamp, and side outputs.
