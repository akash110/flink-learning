# 2. The Java You Actually Need for Flink

You do not need to learn Java. You need to learn about **eight** things from Java, because those eight show up in every Flink program you will ever write. This chapter is that list.

---

## 1. Classes, objects, and the shape of a file

```java
package com.akash.flink.model;   // must match the directory: src/main/java/com/akash/flink/model/

public class Transaction {        // one public class per file, named exactly like the file
    private String userId;        // field: state held by each object
    private double amount;

    public Transaction(String userId, double amount) {  // constructor: builds an object
        this.userId = userId;     // "this" = the object being constructed
        this.amount = amount;
    }

    public double getAmount() {   // method: behaviour
        return amount;
    }
}
```

Line by line:
- `package ...` — namespace. Must match the folder path or Maven refuses to compile.
- `public class Transaction` — `public` means any code can use it. The file **must** be `Transaction.java`.
- `private String userId` — `private` means only code inside this class can touch the field directly. This is why getters exist.
- `Transaction(...)` — a constructor: same name as the class, no return type.
- `this.userId = userId` — disambiguates the field from the parameter of the same name.

Creating one:

```java
Transaction t = new Transaction("u1", 42.50);
//  ^type       ^variable  ^allocate + call constructor
double a = t.getAmount();   // 42.50
```

---

## 2. POJOs, and why Flink is picky about them

A **POJO** (Plain Old Java Object) is just a class that follows a shape. Flink checks for that shape and, if it matches, uses a fast generated serializer.

Flink's POJO rules — all of them must hold:

1. The class is `public` and **not** an inner (non-static nested) class.
2. It has a **public no-argument constructor**.
3. Every field is either `public`, or `private` with a **public getter and setter** following the `getX`/`setX` naming convention.
4. Every field's type is itself serializable by Flink.

```java
public class Transaction {
    private String userId;
    private double amount;

    public Transaction() {}                       // (2) REQUIRED no-arg constructor

    public Transaction(String userId, double amount) {
        this.userId = userId;
        this.amount = amount;
    }

    public String getUserId() { return userId; }               // (3) getter
    public void setUserId(String userId) { this.userId = userId; }   // (3) setter
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
}
```

**Why the no-arg constructor?** Deserialization is: allocate an empty object, then fill each field. Flink literally calls `new Transaction()` and then the setters. With no no-arg constructor there is nothing to call.

**What breaks without it?** Nothing loudly. Flink silently falls back to **Kryo**, a generic reflective serializer. Consequences:
- 2–5x slower serialization on every record and every state access.
- **State schema evolution stops working** — you can no longer add a field and restore from an old savepoint.
- The failure is invisible until you profile or try to upgrade in production.

> **Key idea:** The no-arg constructor is not Java ceremony. It is the difference between a generated serializer and Kryo, and Kryo silently costs you both throughput and the ability to evolve your state.

Chapter 9 builds the full `Event` POJO and shows how to *verify* Flink is not falling back to Kryo.

---

## 3. Generics — the `<>` everywhere

Generics are **type parameters**: a way to say "a list, but of Strings specifically".

```java
List<String> names = new ArrayList<>();   // a List whose elements are String
names.add("akash");
String n = names.get(0);   // no cast needed — the compiler knows it's a String
// names.add(42);          // compile error — good, caught before you ship
```

In Flink, generics carry the record type through your pipeline:

```java
DataStream<String> lines = env.fromElements("a", "b", "c");
//         ^^^^^^^^ "a stream whose records are Strings"

DataStream<Integer> lengths = lines.map(s -> s.length());
//         ^^^^^^^^^ map changed the element type, so the stream type changed
```

Function interfaces are generic in **input and output**:

```java
MapFunction<String, Integer>
//          ^in     ^out
FlatMapFunction<String, String>
KeySelector<Event, String>       // Event in, String key out
```

The `<>` on the right of a `new` is the **diamond operator** — "infer this from the left":

```java
Map<String, List<Integer>> m = new HashMap<>();   // not new HashMap<String, List<Integer>>()
```

**Generics only accept reference types**, not primitives. `List<int>` is illegal; use `List<Integer>`. Java auto-boxes between them, which is why you'll see `Tuple2<String, Long>` and never `Tuple2<String, long>`.

---

## 4. Interfaces and lambdas

An **interface** is a contract: a list of method signatures with no bodies.

```java
public interface MapFunction<T, O> {
    O map(T value) throws Exception;
}
```

A **functional interface** is an interface with exactly **one** abstract method. Java lets you write a lambda anywhere a functional interface is expected. `MapFunction` has one method, so:

```java
DataStream<String> upper = lines.map(s -> s.toUpperCase());
//                                   ^  ^^ ^^^^^^^^^^^^^^^
//                                   |  |  body: the return value
//                                   |  └─ "goes to"
//                                   └─ the parameter (type inferred = String)
```

That lambda **is** a `MapFunction<String, String>`. The compiler generates the implementation for you.

Three equivalent forms, cheapest to most verbose:

```java
// (a) lambda
lines.map(s -> s.toUpperCase());

// (b) method reference — "call this method on the argument"
lines.map(String::toUpperCase);

// (c) anonymous inner class — the long form the other two are sugar for
lines.map(new MapFunction<String, String>() {
    @Override
    public String map(String value) throws Exception {
        return value.toUpperCase();
    }
});
```

Multi-statement lambdas need braces and an explicit `return`:

```java
lines.map(s -> {
    String trimmed = s.trim();
    return trimmed.toUpperCase();
});
```

`@Override` is an annotation that tells the compiler "this is meant to implement/override something" — if you typo the name, you get a compile error instead of a silent no-op. Always write it.

---

## 5. Anonymous inner classes — when you are *forced* to use them

Lambdas are nicer, but **Java erases generic type information at compile time** (this is called *type erasure*). A lambda carries no runtime type info, so Flink sometimes cannot figure out what type your stream holds.

This compiles but **fails at job-build time**:

```java
DataStream<Tuple2<String, Integer>> pairs =
    lines.map(s -> Tuple2.of(s, s.length()));
```

```
org.apache.flink.api.common.functions.InvalidTypesException:
The generic type parameters of 'Tuple2' are missing.
In many cases lambda methods don't provide enough information for automatic
type extraction when Java generics are involved.
```

Flink sees `Tuple2` but has no idea it is `Tuple2<String, Integer>`.

**Fix A — `.returns(...)`, the idiomatic one:**

```java
import org.apache.flink.api.common.typeinfo.Types;

DataStream<Tuple2<String, Integer>> pairs = lines
    .map(s -> Tuple2.of(s, s.length()))
    .returns(Types.TUPLE(Types.STRING, Types.INT));
//   ^^^^^^^ tells Flink the output type explicitly
```

`Types` is a factory of `TypeInformation` objects. Common ones:

```java
Types.STRING
Types.INT      Types.LONG      Types.DOUBLE     Types.BOOLEAN
Types.TUPLE(Types.STRING, Types.LONG)
Types.POJO(Event.class)
Types.LIST(Types.STRING)
```

**Fix B — anonymous inner class:**

```java
DataStream<Tuple2<String, Integer>> pairs = lines.map(
    new MapFunction<String, Tuple2<String, Integer>>() {
        @Override
        public Tuple2<String, Integer> map(String s) {
            return Tuple2.of(s, s.length());
        }
    });
```

This works with **no** `.returns()`. The anonymous class is a real class file with the generic types baked into its signature, so Flink reflects them out at runtime. That is the whole reason anonymous classes still exist in Flink code.

> **Key idea:** Lambda + generic output type (`Tuple2`, `List`, generic POJO) → add `.returns(...)`. Anonymous inner class carries its own type info and never needs it.

---

## 6. `static`, and the "Task not serializable" error

`static` means "belongs to the class, not to an instance".

```java
public class MyJob {
    static int counter = 0;             // one shared variable for the whole class
    static String upper(String s) {     // callable as MyJob.upper("x")
        return s.toUpperCase();
    }
}
```

The Flink-relevant fact: **a non-static inner class holds a hidden reference to its enclosing instance.**

```java
public class MyJob {
    private DatabaseConnection conn;    // NOT serializable

    // BAD: inner class secretly captures "MyJob.this", which drags in conn
    class MyMapper implements MapFunction<String, String> {
        public String map(String s) { return s.toUpperCase(); }
    }
}
```

Flink **serializes your function objects on the client and ships them to TaskManagers**. Serializing `MyMapper` follows the hidden reference to `MyJob`, which follows `conn`, which is not serializable:

```
org.apache.flink.api.common.InvalidProgramException:
The implementation of the MapFunction is not serializable.
The object probably contains or references non-serializable fields.
Caused by: java.io.NotSerializableException: com.akash.DatabaseConnection
```

Three fixes:

```java
// Fix 1: make it static — no enclosing reference exists
public static class MyMapper implements MapFunction<String, String> {
    @Override public String map(String s) { return s.toUpperCase(); }
}

// Fix 2: top-level class in its own file
// Fix 3: mark the offending field transient and build it lazily in open()
```

Fix 3 is the standard pattern for connections, clients, and anything heavy:

```java
public static class Enricher extends RichMapFunction<String, String> {
    private transient DatabaseConnection conn;   // transient = "do not serialize me"

    @Override
    public void open(Configuration parameters) {  // runs once per subtask, on the TaskManager
        conn = DatabaseConnection.create();       // built where it will be used
    }

    @Override
    public String map(String s) {
        return conn.lookup(s);
    }

    @Override
    public void close() {                         // runs on shutdown
        if (conn != null) conn.close();
    }
}
```

`RichMapFunction` is `MapFunction` plus lifecycle methods (`open`, `close`) and `getRuntimeContext()` (which is how you reach state — Phase 3).

**The same trap with lambdas:** a lambda that references an instance field of the enclosing class captures `this` too.

```java
private String prefix = "X";
stream.map(s -> prefix + s);        // captures "this" — enclosing object must be serializable

String local = prefix;               // copy to a local first
stream.map(s -> local + s);          // captures only the String — safe
```

> **Key idea:** Anything you pass to a Flink operator gets serialized and shipped over the network. If it can reach a non-serializable object through *any* reference chain — including the invisible one an inner class or lambda holds to `this` — the job fails before a single record flows.

---

## 7. Checked exceptions and `throws Exception`

Java splits exceptions in two:
- **Unchecked** (`RuntimeException`, `NullPointerException`) — you may ignore them.
- **Checked** (`IOException`, `Exception`) — the compiler *forces* you to either catch them or declare `throws`.

Every Flink function interface declares `throws Exception`:

```java
public interface MapFunction<T, O> {
    O map(T value) throws Exception;   // ← you are allowed to throw anything
}
```

This is deliberate: it means your function body can call anything that throws, with no boilerplate.

```java
public static class Parser implements MapFunction<String, Event> {
    @Override
    public Event map(String json) throws Exception {   // keep the throws
        return objectMapper.readValue(json, Event.class);  // throws IOException — fine
    }
}
```

If you drop `throws Exception` from your override, you are *narrowing* the contract (legal in Java), and then every checked exception inside must be handled by hand. Just keep it.

Semantics that matter: **throwing from a Flink function fails the task**, which fails the job, which triggers the restart strategy and a restore from the last checkpoint. So an exception is not "skip this record" — it is "restart the pipeline". If you want to skip a bad record, catch it:

```java
@Override
public void flatMap(String json, Collector<Event> out) {
    try {
        out.collect(objectMapper.readValue(json, Event.class));
    } catch (Exception e) {
        // drop it (or emit to a side output — Phase 4)
    }
}
```

Lambdas can throw checked exceptions too, precisely because the interface declares `throws Exception`:

```java
stream.map(json -> objectMapper.readValue(json, Event.class));  // compiles
```

---

## 8. Small syntax you will trip over

**`final`** — "cannot be reassigned". Local variables captured by a lambda must be final or *effectively* final (never reassigned):

```java
int n = 5;
stream.map(s -> s + n);   // OK: n is effectively final
// n = 6;                 // adding this line breaks the lambda above
```

**`Long` vs `long`** — `long` is a primitive (64-bit int, cannot be null); `Long` is the object wrapper (can be null). Generics need the wrapper: `Tuple2<String, Long>`. `1L` is a long literal; `1` is an `int`.

**`==` vs `.equals()`** — for objects, `==` compares references, `.equals()` compares content. **Always use `.equals()` for Strings.**

```java
if (type == "purchase")        // WRONG — may be false even when text matches
if (type.equals("purchase"))   // right
if ("purchase".equals(type))   // right, and null-safe on type
```

This matters enormously in `keyBy` and filters.

**`@Override`** — annotation, not a keyword. Costs nothing, catches typos.

**`System.currentTimeMillis()`** — epoch milliseconds as a `long`. This is Flink's canonical timestamp unit everywhere.

---

## Putting it together

Every piece above, in one 25-line job:

```java
package com.akash.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JavaRefresher {

    // static nested class -> no hidden enclosing reference -> serializable
    public static class Shout implements MapFunction<String, String> {
        @Override
        public String map(String value) throws Exception {   // checked exceptions allowed
            return value.toUpperCase() + "!";
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> words = env.fromElements("flink", "java", "stream");

        DataStream<String> loud = words.map(new Shout());          // named function class

        DataStream<Tuple2<String, Integer>> sized = loud
            .map(s -> Tuple2.of(s, s.length()))                    // lambda...
            .returns(Types.TUPLE(Types.STRING, Types.INT));        // ...needs .returns()

        sized.print();
        env.execute("java-refresher");
    }
}
```

Expected output (order varies with parallelism; the `N>` prefix is the subtask id):

```
3> (FLINK!,6)
4> (JAVA!,5)
1> (STREAM!,7)
```

---

## Remember

- **POJO = public class + public no-arg constructor + public fields or getters/setters.** Miss it and Flink silently uses Kryo: slower, and no state schema evolution.
- **Generics `<T, R>` are how Flink tracks record types.** `MapFunction<String, Integer>` = String in, Integer out.
- **A functional interface = one abstract method = a lambda works.** `MapFunction`, `FilterFunction`, `KeySelector` are all functional interfaces.
- **Type erasure kills lambda type inference for generic outputs** → `.returns(Types.TUPLE(...))`, or use an anonymous inner class which carries its own types.
- **Everything you pass to an operator is serialized and shipped.** Non-static inner classes and lambdas capture `this`. Use `static`, top-level classes, or `transient` + `open()`.
- **Keep `throws Exception` on overrides.** Throwing fails the task and restarts the job — catch if you mean "skip this record".
- **`.equals()` not `==` for Strings.**

**Interview one-liners**

- *"Why must Flink functions be serializable?"* → The job graph is built on the client and the function objects are shipped to TaskManagers; anything reachable from them must serialize.
- *"What causes InvalidTypesException with lambdas?"* → Java type erasure — a lambda carries no generic type info, so Flink can't extract `Tuple2<String, Integer>`. Fix with `.returns()` or an anonymous class.
- *"Why does Flink need a no-arg constructor?"* → To use the fast generated POJO serializer instead of Kryo; without it you lose throughput and state schema evolution.
