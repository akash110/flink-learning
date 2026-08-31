# 27. JSON Serialization and Deserialization

Kafka moves `byte[]`. Your job wants `Event`. Something has to translate, and that something is a **schema** class. This chapter writes both directions by hand — because you must understand what they do — then shows the shortcuts.

The POJO from Phase 1, unchanged:

```java
package com.akash.flink.model;

public class Event {
    public String userId;
    public String type;
    public double amount;
    public long timestamp;

    // A public no-arg constructor is MANDATORY for Flink to treat this
    // as a POJO (and for Jackson to instantiate it).
    public Event() {}

    public Event(String userId, String type, double amount, long timestamp) {
        this.userId = userId;
        this.type = type;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" + userId + ", " + type + ", " + amount + ", " + timestamp + "}";
    }
}
```

---

## The dependency

```xml
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
  <version>2.15.3</version>
</dependency>
```

Flink bundles a *shaded* Jackson internally (`org.apache.flink.shaded.jackson2...`). Do **not** import that — it is an implementation detail and can vanish between versions. Add your own dependency and import `com.fasterxml.jackson.*`.

---

## The deserializer

```java
package com.akash.flink.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.akash.flink.model.Event;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class EventDeserializationSchema implements DeserializationSchema<Event> {

    // 'transient' = "do not serialize this field".
    // See the big section below on WHY this is essential.
    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        // Called ONCE per subtask, on the TaskManager, before any record.
        this.mapper = new ObjectMapper()
                // Do not blow up when the JSON has fields the POJO lacks.
                // Without this, adding a field upstream breaks your job.
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        // Called once per Kafka record. Hot path — keep it cheap.
        return mapper.readValue(message, Event.class);
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        // Legacy hook from the old source API. For an unbounded Kafka
        // stream the answer is always false. KafkaSource ignores it.
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
```

Java syntax notes, since you are new to it:

- `implements DeserializationSchema<Event>` — a **generic interface**; `<Event>` fixes the output type, so `deserialize` must return `Event`.
- `@Override` — an annotation asserting this method really overrides something from the interface. If you typo the name, the compiler errors instead of silently creating a new unused method. Always write it.
- `throws IOException` — a **checked exception** declaration. `mapper.readValue` can throw it, so you must either catch it or declare it. Declaring it is fine here; Flink catches it and fails the job.
- `private transient ObjectMapper mapper;` — a field with no initializer starts as `null`.

---

## Why `getProducedType()` exists — type erasure

Java **generics are erased at compile time**. At runtime, `DeserializationSchema<Event>` and `DeserializationSchema<String>` are the same class: `DeserializationSchema`. The `<Event>` is gone.

```
COMPILE TIME                        RUNTIME (after erasure)
DeserializationSchema<Event>   ──►  DeserializationSchema
DataStream<Event>              ──►  DataStream
```

Flink needs the concrete type for real work:

1. To pick a **TypeSerializer** — Flink does not use Java serialization for records; it has its own much faster serializers (POJO serializer, tuple serializer, Kryo fallback). Choosing one requires knowing the class.
2. To size and lay out memory segments for network transfer and state.
3. To decide whether `keyBy("userId")` on a field name is even valid.

Since the generic parameter is erased, the interface makes you hand the type back explicitly:

```java
TypeInformation.of(Event.class)
```

`Event.class` is a **class literal** — a `Class<Event>` object available at runtime. `TypeInformation.of(...)` inspects it (public no-arg constructor? public or getter/setter fields? all field types supported?) and returns Flink's `PojoTypeInfo<Event>` if it qualifies, or falls back to Kryo if not.

For a **generic** type the class literal is not enough, and you use a **type hint** instead:

```java
// A class literal cannot express List<Event>. This anonymous subclass can:
TypeInformation.of(new TypeHint<java.util.List<Event>>() {})
```

`new TypeHint<...>() {}` — the trailing `{}` creates an **anonymous subclass**. Subclasses keep their superclass's generic arguments in the class file, so Flink can read `List<Event>` back out via reflection. This is a standard Java trick for defeating erasure.

> **Key idea:** Generics vanish at runtime. `getProducedType()` is how you tell Flink what your operator actually produces so it can pick a serializer. If you see `GenericType<...>` / "falling back to Kryo" in the logs, this is what went wrong — and Kryo is several times slower than the POJO serializer.

Verify what Flink decided:

```java
DataStream<Event> events = ...;
System.out.println(events.getType());
// GOOD: PojoType<com.akash.flink.model.Event, fields = [amount: Double, ...]>
// BAD : GenericType<com.akash.flink.model.Event>     ← Kryo. Fix the POJO.
```

POJO rules Flink checks: public class, public no-arg constructor, and every field either public or with standard `getX`/`setX`. Break one and you silently get Kryo.

---

## Why `ObjectMapper` must be `transient` and built in `open()`

Flink serializes your function objects on the **client/JobManager** and ships the bytes to every **TaskManager**:

```
   your laptop / JobManager                TaskManagers
 ┌───────────────────────────┐          ┌──────────────────┐
 │ new EventDeserialization  │          │ deserialize obj  │
 │ Schema()                  │──bytes──►│ open()  ← mapper │
 │ (Java serialization)      │          │        created   │
 └───────────────────────────┘          │ deserialize(...) │
                                        └──────────────────┘
```

That shipping step uses **plain Java serialization**, so every non-transient field of your function must implement `java.io.Serializable`.

**`com.fasterxml.jackson.databind.ObjectMapper` is not reliably serializable.** Assign one to a normal field and job submission dies with:

```
org.apache.flink.api.common.InvalidProgramException:
  The implementation of the DeserializationSchema is not serializable.
  The object probably contains or references non-serializable fields.
Caused by: java.io.NotSerializableException: com.fasterxml.jackson.databind.ObjectMapper
```

`transient` tells Java serialization "skip this field". The object arrives with `mapper == null`, and `open()` — which runs on the TaskManager, once per subtask, before the first record — fills it in.

The same pattern applies to every non-serializable heavyweight helper: DB connections, HTTP clients, `KafkaProducer`, `Random` you want per-subtask, ML models.

**Do not do this instead:**

```java
// BAD: allocates an ObjectMapper for EVERY record. ObjectMapper
// construction is expensive (it builds a serializer cache).
public Event deserialize(byte[] m) throws IOException {
    return new ObjectMapper().readValue(m, Event.class);
}
```

```java
// ALSO ACCEPTABLE: a static field. Statics are not serialized at all,
// and ObjectMapper is thread-safe once configured. Slightly less
// explicit than transient + open(), but common and correct.
private static final ObjectMapper MAPPER = new ObjectMapper();
```

> **Key idea:** `transient` field + build it in `open()` is *the* Flink idiom for anything not serializable. Remember the trio: **transient / open() / one per subtask.**

---

## Handling malformed JSON

Real topics contain garbage: truncated messages, a producer that changed format, someone's `curl` test, an empty message body. Your `deserialize` will be handed those bytes. You have three options.

### Option 1 — throw (the default if you do nothing)

```java
public Event deserialize(byte[] message) throws IOException {
    return mapper.readValue(message, Event.class);   // throws on bad JSON
}
```

The exception propagates, the **whole job fails**, the restart strategy restarts it, it reads the same poison record, and it fails again. **A crash loop.** One bad byte array takes down your pipeline until a human intervenes.

Only acceptable when malformed input genuinely means "stop everything" — strict financial ingestion where silently skipping a record is worse than an outage.

### Option 2 — return `null`

```java
public Event deserialize(byte[] message) {
    try {
        return mapper.readValue(message, Event.class);
    } catch (Exception e) {
        return null;   // KafkaSource drops nulls
    }
}
```

`KafkaSource` treats a `null` from a `DeserializationSchema` as "emit nothing" and moves on. The job survives.

But the bad record is now **gone with no trace**. You cannot count them, cannot alert on them, cannot replay them after fixing the parser. A producer bug can corrupt 40% of your traffic and every dashboard looks green.

Acceptable only with a metric attached:

```java
private transient Counter parseFailures;

@Override
public void open(InitializationContext context) {
    this.mapper = new ObjectMapper();
    this.parseFailures = context.getMetricGroup().counter("parseFailures");
}

public Event deserialize(byte[] message) {
    try {
        return mapper.readValue(message, Event.class);
    } catch (Exception e) {
        parseFailures.inc();
        return null;
    }
}
```

Now at least it is visible in the Web UI and in Prometheus, and you can alert on a nonzero rate.

### Option 3 — dead-letter side output (recommended)

Keep deserialization **lenient and lossless**: parse into a wrapper that carries either a good `Event` or the raw bad bytes, then split the stream with a side output.

```java
// A tiny carrier type: exactly one of the two fields is set.
public class ParseResult {
    public Event event;     // null if parsing failed
    public String rawJson;  // the original text, if parsing failed
    public String error;    // the exception message
    public ParseResult() {}
}
```

```java
public class LenientEventDeserializer implements DeserializationSchema<ParseResult> {
    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext ctx) {
        mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public ParseResult deserialize(byte[] message) {
        ParseResult r = new ParseResult();
        try {
            r.event = mapper.readValue(message, Event.class);
        } catch (Exception e) {
            r.rawJson = new String(message, StandardCharsets.UTF_8);
            r.error   = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        return r;
    }

    @Override public boolean isEndOfStream(ParseResult n) { return false; }
    @Override public TypeInformation<ParseResult> getProducedType() {
        return TypeInformation.of(ParseResult.class);
    }
}
```

Then split downstream:

```java
// An OutputTag names a side output channel. The {} makes it an anonymous
// subclass so the <ParseResult> type argument survives erasure — same
// TypeHint trick as before.
final OutputTag<ParseResult> DEAD_LETTER =
        new OutputTag<ParseResult>("dead-letter") {};

SingleOutputStreamOperator<Event> good = raw.process(
        new ProcessFunction<ParseResult, Event>() {
            @Override
            public void processElement(ParseResult r, Context ctx, Collector<Event> out) {
                if (r.event != null) {
                    out.collect(r.event);          // main output
                } else {
                    ctx.output(DEAD_LETTER, r);    // side output
                }
            }
        });

DataStream<ParseResult> bad = good.getSideOutput(DEAD_LETTER);

// Ship the bad ones somewhere a human can look at them.
bad.sinkTo(deadLetterKafkaSink);
```

```
                       ┌────────────────► Event stream ──► your pipeline
 Kafka ──► lenient ──► split
           parse        └────────────────► dead-letter topic ──► alert / replay
```

Why this is the right answer:

- **Nothing is lost.** The raw bytes survive with the error attached.
- **Nothing crashes.** A poison record cannot loop the job.
- **It is observable.** Volume on the dead-letter topic is a first-class metric.
- **It is replayable.** Fix the parser, replay the DLQ topic through the job.

> **Key idea:** Throwing turns bad data into an outage. Returning null turns bad data into a lie. A dead-letter side output turns bad data into a ticket. Default to the third.

---

## The serializer (the other direction)

```java
package com.akash.flink.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import com.akash.flink.model.Event;

public class EventSerializationSchema implements SerializationSchema<Event> {

    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        this.mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(Event element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (Exception e) {
            // Serialization failure of your OWN object is a programming
            // bug, not bad input. Fail loudly.
            throw new RuntimeException("Failed to serialize " + element, e);
        }
    }
}
```

Note the asymmetry, and it is deliberate:

- **Deserialization** handles *other people's* data → be lenient, dead-letter it.
- **Serialization** handles *your own* objects → a failure is your bug → throw.

`SerializationSchema` has one abstract method, so a lambda works for simple cases:

```java
SerializationSchema<Event> keySchema =
        (Event e) -> e.userId.getBytes(StandardCharsets.UTF_8);
```

`RuntimeException` is an **unchecked** exception — no `throws` clause needed, which is why it is used to wrap a checked one you cannot meaningfully handle.

---

## The shortcut: `flink-json`

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-json</artifactId>
  <version>${flink.version}</version>
</dependency>
```

```java
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;

// Both directions, one line each. Available since Flink 1.16.
DeserializationSchema<Event> deser = new JsonDeserializationSchema<>(Event.class);
SerializationSchema<Event>   ser   = new JsonSerializationSchema<>();
```

`JsonDeserializationSchema<T>` wraps Jackson exactly as above and derives `getProducedType()` from the class you pass. Use it when the defaults are fine.

Its limits, which are why you learned the manual version:

- **No error handling hook.** Malformed JSON throws → crash loop. No dead letters.
- **No Jackson customisation** unless you use the constructor overload taking a mapper-configuring function (`SerializableSupplier<ObjectMapper>` in newer versions).
- No per-record metrics.

Rule: `JsonDeserializationSchema` for prototypes and internal topics you control; hand-written lenient schema for anything ingesting from outside your team.

There is also `JsonRowDeserializationSchema` / the `'format' = 'json'` table option — those belong to the Table/SQL API (Phase 7), not DataStream POJOs.

---

## The production answer: Avro + Schema Registry

JSON is a bad wire format at scale, and you should be able to say why:

| | JSON | Avro + Schema Registry |
|---|---|---|
| Size | Field names repeated in every record | Binary, schema stored once, ~5 byte header |
| Schema | Implicit; a typo is discovered in prod | Explicit, registered, versioned |
| Evolution | Hope | Enforced compatibility rules (BACKWARD/FORWARD/FULL) |
| Bad producer | Reaches your job | Rejected at produce time |
| CPU | Text parsing per record | Fast binary decode |

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-avro-confluent-registry</artifactId>
  <version>${flink.version}</version>
</dependency>
```

```java
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

KafkaSource<Event> source = KafkaSource.<Event>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("events")
        .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forSpecific(
                        Event.class,                 // generated from an .avsc
                        "http://schema-registry:8081"))
        .build();
```

`forSpecific` uses a class generated from an Avro schema file by the `avro-maven-plugin`. `forGeneric(schema, url)` gives you `GenericRecord` when you do not want code generation.

The registry, not your code, becomes the contract. A producer that tries to publish an incompatible schema is rejected before a single bad record exists.

**Migration path:** JSON while learning and prototyping → Avro + Schema Registry the moment a second team produces to your topic.

---

## Remember

- `DeserializationSchema<T>`: `deserialize(byte[]) -> T`, plus `getProducedType()`.
- `getProducedType()` exists because **Java erases generics**; Flink needs the class to pick a `TypeSerializer`.
- `TypeInformation.of(Event.class)` for a plain class; `TypeInformation.of(new TypeHint<List<Event>>(){})` for generics.
- `PojoType<...>` in the logs is good. `GenericType<...>` means **Kryo**, which is slow — fix the POJO (public no-arg constructor, public/accessor fields).
- `ObjectMapper` is **not serializable**: declare it `transient`, build it in `open()`. That is the universal Flink pattern for non-serializable helpers.
- Malformed JSON: **throw** = crash loop; **null** = silent loss; **dead-letter side output** = the right answer.
- Deserialization is lenient (foreign data), serialization throws (your own bug).
- `JsonDeserializationSchema` / `JsonSerializationSchema` from `flink-json` are the one-liners — no error handling.
- Avro + Confluent Schema Registry is the production answer: smaller, typed, and schema-enforced at the producer.

**Interview one-liners**

- *"Why does `DeserializationSchema` need `getProducedType()`?"* → Type erasure. Flink must know the runtime type to choose a serializer and lay out memory; the generic parameter is gone by then.
- *"Why is your ObjectMapper transient?"* → Function objects are Java-serialized and shipped to TaskManagers; `ObjectMapper` isn't serializable. `transient` + create in `open()` gives one instance per subtask.
- *"How do you handle bad records?"* → Never throw on foreign input. Parse leniently into a carrier, route failures to a dead-letter side output and topic, and alert on its rate.
- *"What does `GenericType` in the logs mean?"* → Flink fell back to Kryo because the class isn't a valid POJO. Several times slower and it breaks state schema evolution.
- *"Why Avro over JSON?"* → Compactness, a registered versioned schema, and compatibility enforced at produce time instead of discovered in your consumer.
