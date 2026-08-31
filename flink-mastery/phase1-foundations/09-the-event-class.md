# 9. The `Event` Class

Every chapter from here on uses this one class. Type it out once, understand every line, and you will never wonder why a Flink job is slow for serialization reasons again.

---

## The complete class

`src/main/java/com/akash/flink/model/Event.java`:

```java
package com.akash.flink.model;

import java.util.Objects;

/**
 * The canonical event used throughout this course.
 * This is a valid Flink POJO. Every rule below is load-bearing.
 */
public class Event {

    // Fields are PUBLIC, so Flink reads and writes them directly — no accessors needed.
    // The getters/setters below are kept purely for convenience in your own code.
    // (Private fields + getters/setters is equally valid — see "Two valid shapes" below.)
    public String userId;
    public String type;
    public double amount;
    public long timestamp;       // epoch MILLISECONDS — Flink's event time

    /** REQUIRED: public no-argument constructor, used by the POJO deserializer. */
    public Event() {
    }

    /** Convenience constructor for your own code. Flink never calls this. */
    public Event(String userId, String type, double amount, long timestamp) {
        this.userId = userId;
        this.type = type;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    // ---- getters and setters: one pair per field, exact naming ----

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    // ---- toString: not required by Flink, required by your sanity ----

    @Override
    public String toString() {
        return "Event{userId='" + userId + '\''
             + ", type='" + type + '\''
             + ", amount=" + amount
             + ", timestamp=" + timestamp + '}';
    }

    // ---- equals / hashCode: needed if Event is ever used AS A KEY ----

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event e = (Event) o;
        return Double.compare(e.amount, amount) == 0
            && timestamp == e.timestamp
            && Objects.equals(userId, e.userId)
            && Objects.equals(type, e.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, type, amount, timestamp);
    }
}
```

Use it:

```java
DataStream<Event> events = env.fromElements(
    new Event("u1", "purchase", 99.50, 1000L),
    new Event("u1", "view",      0.00, 2000L),
    new Event("u2", "purchase", 15.00, 3000L)
);

events.keyBy(Event::getUserId)
      .sum("amount")            // field name works because this is a valid POJO
      .print();
```

---

## Every requirement, and what breaks without it

### 1. The class must be `public`

```java
public class Event { ... }      // ✓
class Event { ... }             // ✗ package-private
```

Flink's runtime instantiates and accesses your class from its own packages. A package-private class is invisible to it, so the POJO check fails and you fall back to Kryo.

### 2. It must be a top-level class or a `static` nested class

```java
public class MyJob {
    public static class Event { ... }   // ✓ static nested — fine
    public class Bad { ... }            // ✗ inner class — holds a hidden MyJob reference
}
```

A non-static inner class cannot be constructed without an enclosing instance, so `new Event()` is impossible for the runtime. Same reason as the "Task not serializable" story in chapter 2.

For anything shared, put it in its own file under a `model` package.

### 3. It must have a **public no-argument constructor**

```java
public Event() { }
```

**This is the rule people break.** Deserialization is a two-step process:

```
  bytes  →  new Event()      ← needs the no-arg constructor
         →  setUserId(...)
         →  setType(...)     ← needs the setters
         →  setAmount(...)
         →  setTimestamp(...)
         →  the object
```

There is no path that goes through your 4-argument constructor. Flink has no way to know which argument maps to which field.

**The subtle Java trap:** if you write *no* constructors at all, Java gives you a free public no-arg one. The moment you add the convenience constructor, **the free one disappears**. So the class that worked yesterday silently degrades to Kryo the day someone adds a constructor.

```java
public class Event {
    private String userId;
    // no constructors written  →  Java supplies Event()  →  valid POJO ✓
}

public class Event {
    private String userId;
    public Event(String userId) { ... }   // →  Event() is GONE  →  Kryo ✗
}
```

Always write `public Event() {}` explicitly, even when it looks redundant.

### 4. Every field must be public, or private with a matching getter and setter

The naming convention is strict:

| Field | Getter | Setter |
|---|---|---|
| `userId` | `getUserId()` | `setUserId(String)` |
| `amount` | `getAmount()` | `setAmount(double)` |
| `active` (boolean) | `isActive()` **or** `getActive()` | `setActive(boolean)` |

Rules:
- Getter takes **no arguments** and returns exactly the field's type.
- Setter takes **exactly one argument** of the field's type. Its return type is ignored (so a fluent `Event setUserId(...)` returning `this` also works).
- The name after `get`/`set` is the field name with the first letter capitalized.

Get any of this wrong — `getUserID()` for a field named `userId`, a setter that takes two arguments, a getter that returns `Object` — and **that one field** fails the check, which fails the whole class to Kryo.

### Two valid shapes

Flink accepts a field **either** way. Per field, it needs one of:

```java
public class Event {
    public String userId;         // (a) public field — Flink reads/writes it directly
}
```

```java
public class Event {
    private String userId;        // (b) private field + BOTH accessors, exactly named
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
}
```

Shape (a) is faster to write and is how most Flink example code looks — it's what
`Tuple` does. Shape (b) is better practice for a class you'll maintain long-term.

**Do not mix per-field expectations** — each field is either public, or it has both
accessors. A private field with only a getter fails the check and drops the whole
class to Kryo.

> **This course uses shape (a)** — public fields, with getters kept as a convenience.
> So both `e.userId` and `e.getUserId()` compile, and every later example works as
> printed. If you prefer shape (b), make the fields `private` and change the samples
> to use the getters.

### 5. Every field's type must itself be Flink-serializable

Fine: primitives, boxed primitives, `String`, arrays of those, other valid POJOs, `List`/`Map` of serializable types, `java.sql.Date`, `java.time.Instant` / `LocalDateTime`, enums.

Not fine, or Kryo-only: `Optional<T>`, `java.util.Date` in some versions, interfaces with unknown implementations, anything holding a connection or a thread, and generic fields whose parameter is unresolvable.

**One bad field type demotes the whole class.**

### 6. No `final` fields

```java
private final String userId;   // ✗ — the setter cannot assign it
```

The deserializer constructs empty then sets. A `final` field cannot be set after construction, so the POJO check fails. This is why **Java `record` types are not Flink POJOs** — records are final-field-only by design. Flink handles records through Kryo unless you register something custom. Do not reach for `record` here.

> **Key idea:** The POJO rules exist to let Flink generate a serializer that reads and writes fields directly, with no reflection and no class metadata per record. Break any rule and you silently get Kryo instead.

---

## What actually breaks with Kryo

The failure is quiet, which is what makes it dangerous. Three real costs:

**1. Throughput.** Kryo serializes reflectively and writes class registration information. On the hot path — every network shuffle, every state read/write, every checkpoint — expect **2–5x** the CPU of a generated POJO serializer. In a heavily stateful job, serialization can be the dominant cost, so this is a real fraction of your cluster bill.

**2. State schema evolution stops working.** This is the one that ruins a Tuesday.

```
Day 1:  Event { userId, type, amount, timestamp }
        job runs, state checkpointed with the POJO serializer

Day 60: add a field →  Event { userId, type, amount, timestamp, currency }
        restore from savepoint  →  works. New field defaults to null.
```

Flink's `PojoSerializer` supports **adding and removing fields** across restores. Field *type* changes and renames are not supported, but additive evolution — the change you actually make — is.

With Kryo, none of it works:

```
org.apache.flink.util.StateMigrationException:
The new state serializer ... is not compatible with the old state serializer
```

Your options at that point are: restart with no state (losing everything), or write a manual state-migration job. Both are bad on a production incident.

**3. Cross-version fragility.** Kryo's binary format depends on field ordering and registration order. Reorder your fields, or upgrade a library, and old savepoints may not read back.

---

## Verifying Flink treats it as a POJO

Do not assume. Check.

### Method 1: ask the type extractor (fastest)

```java
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import com.akash.flink.model.Event;

public class TypeCheck {
    public static void main(String[] args) {
        TypeInformation<Event> info = TypeInformation.of(Event.class);

        System.out.println("TypeInfo class : " + info.getClass().getSimpleName());
        System.out.println("Is POJO        : " + (info instanceof PojoTypeInfo));
        System.out.println("Full           : " + info);
    }
}
```

Good output:

```
TypeInfo class : PojoTypeInfo
Is POJO        : true
Full           : PojoType<com.akash.flink.model.Event, fields = [amount: Double, timestamp: Long, type: String, userId: String]>
```

Bad output:

```
TypeInfo class : GenericTypeInfo
Is POJO        : false
Full           : GenericType<com.akash.flink.model.Event>
```

`GenericTypeInfo` **is** the Kryo path. Seeing that word means you broke a rule above.

### Method 2: make Kryo a hard error

The strongest guard. Put it in every job's `main`:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Fail the job at build time if any type would fall back to Kryo.
env.getConfig().disableGenericTypes();
```

Now a demoted class throws immediately:

```
java.lang.UnsupportedOperationException: Generic types have been disabled in the
ExecutionConfig and type com.akash.flink.model.Event is treated as a generic type.
```

You find out in five seconds at your desk rather than in a profiler in three months. (`disableGenericTypes()` is deprecated in favour of the `pipeline.generic-types: false` configuration option in newer releases — either mechanism gives you the same guard.)

### Method 3: watch the logs

At `INFO`, Flink's type extractor announces every demotion:

```
INFO  o.a.f.api.java.typeutils.TypeExtractor - Class com.akash.flink.model.Event
      cannot be used as a POJO type because not all fields are valid POJO fields,
      and must be processed as GenericType. Please read the Flink documentation
      on "Data Types & Serialization" for details of the effect on performance
      and schema evolution.
```

That message is on your console the very first time you run a broken class. Learn to notice it — most people scroll straight past it.

### Method 4: check the runtime serializer

```java
TypeInformation<Event> info = TypeInformation.of(Event.class);
System.out.println(info.createSerializer(env.getConfig().getSerializerConfig()));
// PojoSerializer   ✓
// KryoSerializer   ✗
```

---

## The most common ways people break this class

Work down this list whenever `TypeCheck` prints `GenericTypeInfo`:

```java
// 1. Missing no-arg constructor (because a parameterized one was added)
//    → add:  public Event() {}

// 2. A field with a getter but no setter (or vice versa)
public String getUserId() { return userId; }
// missing setUserId  →  Kryo

// 3. Getter name doesn't match the field
private String userId;
public String getUserID() { ... }        // "ID" != "Id"  →  Kryo

// 4. final field
private final long timestamp;            // → Kryo

// 5. A field type Flink can't handle
private Optional<String> couponCode;     // → Kryo

// 6. Non-static inner class
public class MyJob { public class Event { ... } }   // → Kryo

// 7. Using a Java record
public record Event(String userId, long ts) {}      // → Kryo (final fields)

// 8. A raw generic field
private List rawList;                    // no type parameter  →  Kryo
private List<String> typed;              // ✓
```

---

## Why `toString()`, `equals()`, `hashCode()`

None of these are required for POJO detection. They matter for other reasons.

**`toString()`** — `print()` and every log line calls it. Without it you get `com.akash.flink.model.Event@6d06d69c` and debugging becomes impossible. Write it first, always.

**`equals()` / `hashCode()`** — required only if an `Event` is used **as a key** (`keyBy(e -> e)`) or stored in a `HashSet`/`HashMap` inside state. Without them, Java uses identity, so two `Event`s with identical contents are different keys and your state silently duplicates (ch. 5, trap 2).

`Objects.hash(...)` and `Objects.equals(...)` are `java.util.Objects` helpers that handle nulls for you. `Double.compare(a, b) == 0` rather than `a == b` is the correct way to compare doubles for equality in `equals` — it handles `NaN` and `-0.0` consistently with `hashCode`.

The contract you must not break: **equal objects must have equal hash codes.** If you override one, override both, over the same fields. IntelliJ generates both correctly: `Alt+Insert` → `equals() and hashCode()`.

---

## A generator you will reuse

For every later chapter you need a stream of events with realistic timestamps:

```java
package com.akash.flink.model;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class Events {

    private Events() { }   // utility class: private constructor, never instantiated

    /** A small, deliberately out-of-order sample stream. Timestamps are epoch millis. */
    public static DataStream<Event> sample(StreamExecutionEnvironment env) {
        long base = 1_700_000_000_000L;    // a fixed epoch-ms base → reproducible runs
        return env.fromElements(
            new Event("u1", "view",     0.00, base + 1_000),
            new Event("u1", "purchase", 25.00, base + 3_000),
            new Event("u2", "view",     0.00, base + 2_000),   // out of order
            new Event("u1", "purchase", 40.00, base + 8_000),
            new Event("u2", "purchase", 15.50, base + 6_000),  // out of order
            new Event("u3", "view",     0.00, base + 7_000),
            new Event("u1", "refund",  -25.00, base + 12_000),
            new Event("u2", "purchase", 60.00, base + 11_000)
        );
    }
}
```

`private Events()` prevents instantiation — a standard Java idiom for a class that is only `static` methods. `1_000` is a numeric literal with underscore separators; the compiler ignores them and they make long numbers readable.

Wire it into the standard watermark strategy from chapter 8 and you have the setup every remaining chapter starts from:

```java
DataStream<Event> events = Events.sample(env)
    .assignTimestampsAndWatermarks(
        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((e, ts) -> e.getTimestamp())
            .withIdleness(Duration.ofSeconds(30)));
```

---

## Remember

- **Valid Flink POJO = public class + top-level or `static` nested + public no-arg constructor + every field public or with a correctly named getter *and* setter + no `final` fields + serializable field types.**
- **The no-arg constructor vanishes the moment you add any other constructor.** Write `public Event() {}` explicitly, every time.
- Failing any rule is **silent**: you get `GenericTypeInfo` and Kryo.
- Kryo costs you **2–5x serialization CPU on every hop, state access, and checkpoint** — and it **breaks state schema evolution**, so you can no longer add a field and restore from a savepoint.
- **Java `record` types are not Flink POJOs** — final fields, no setters.
- Verify with `TypeInformation.of(Event.class) instanceof PojoTypeInfo`, and enforce with `env.getConfig().disableGenericTypes()` (or `pipeline.generic-types: false`).
- Watch for `TypeExtractor - ... must be processed as GenericType` in your console. It is the warning everyone scrolls past.
- Write `toString()` for debuggability. Write `equals`/`hashCode` together if the type is ever a key.
- Timestamps are `long` **epoch milliseconds**.

**Interview one-liners**

- *"What makes a class a Flink POJO?"* → Public, top-level or static nested, public no-arg constructor, and every field either public or with a properly named public getter and setter; no final fields.
- *"What happens if it isn't?"* → Silent fallback to Kryo: several times slower serialization on every network hop, state access, and checkpoint, plus no state schema evolution.
- *"How do you prove Flink is using the POJO serializer?"* → `TypeInformation.of(X.class)` should be `PojoTypeInfo`, not `GenericTypeInfo`; or set `disableGenericTypes()` so a fallback becomes a hard failure.
- *"Can you add a field to a class held in state?"* → Yes with the POJO serializer (additive evolution is supported); no with Kryo — you get a `StateMigrationException` on restore.
- *"Why not use Java records?"* → Their fields are final and there are no setters, so the POJO check fails and Flink uses Kryo.
