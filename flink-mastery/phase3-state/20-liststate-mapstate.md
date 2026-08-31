# 20. ListState and MapState — Collections Per Key

`ValueState<T>` holds one thing. Sometimes you need many things per key: the last 5 events, a set of seen device IDs, a per-category running total. That's what `ListState` and `MapState` are for.

You *could* store a `ValueState<List<Event>>` or a `ValueState<HashMap<String, Long>>` instead. Sometimes that's fine. On RocksDB, at scale, it's a performance disaster — and the second half of this chapter explains exactly why.

```
ListState<Event> "recent"                MapState<String, Long> "byCategory"

  alice -> [e1, e2, e3]                    alice -> { books: 3, tools: 1 }
  bob   -> [e7]                            bob   -> { food: 12 }
```

## `ListState<T>` — an append-only list per key

### The API

```java
void         add(T value)                 throws Exception;  // append one element
void         addAll(List<T> values)       throws Exception;  // append many
Iterable<T>  get()                        throws Exception;  // read ALL elements (null if empty!)
void         update(List<T> values)       throws Exception;  // REPLACE the whole list
void         clear();                                        // delete the key's list
```

Three things to burn in:

1. **`get()` returns `Iterable<T>`, not `List<T>`.** You can `for`-loop it; you cannot call `.size()` or `.get(0)` on it. `Iterable` is the Java interface meaning "something you can iterate once with a for-each loop".
2. **`get()` can return `null`** when the key has no list yet. Same null trap as `ValueState`.
3. **`add()` is genuinely append-only.** There is no `remove()`, no `set(i, x)`. To change anything you must read everything, build a new `List`, and call `update()`.

### Declaring it

```java
private transient ListState<Event> recentState;

@Override
public void open(OpenContext ctx) {
    ListStateDescriptor<Event> desc =
            new ListStateDescriptor<>("recent-events", Event.class);
    //                                  ^ name          ^ ELEMENT type, not List type
    recentState = getRuntimeContext().getListState(desc);
}
```

Note the type parameter is the **element** type. `ListState<Event>` is a list of `Event`, described by `ListStateDescriptor<Event>` with `Event.class`.

### Why `add()` is cheap on RocksDB

RocksDB has a native **merge operator**. `listState.add(x)` becomes a single "merge: append these bytes" write. It does *not* read the existing list, deserialize it, append, re-serialize, and write it back.

```
listState.add(e)          ->  ONE merge write. O(1). Existing list never touched.
listState.get()           ->  read all bytes, deserialize every element. O(n).
listState.update(newList) ->  serialize whole list, one put. O(n).
```

So a pattern of "append often, read rarely" is very efficient. A pattern of "read the whole list on every record" is not.

## Worked example: last N events per user

Keep the most recent 3 events per user and emit them.

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class LastNEventsFn extends KeyedProcessFunction<String, Event, String> {

    // `final` = this field can never be reassigned after construction.
    // Not state — just a config value carried with the function object.
    // It IS serialized to the workers (int is serializable), so no transient here.
    private final int n;

    private transient ListState<Event> recentState;

    // A constructor taking config. This runs on the CLIENT, which is fine
    // for plain data — just never touch getRuntimeContext() here.
    public LastNEventsFn(int n) {
        this.n = n;   // `this.n` distinguishes the field from the parameter
    }

    @Override
    public void open(OpenContext ctx) {
        recentState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("recent-events", Event.class));
    }

    @Override
    public void processElement(Event event,
                               Context ctx,
                               Collector<String> out) throws Exception {

        // 1. Append the new event. Cheap — a merge write on RocksDB.
        recentState.add(event);

        // 2. Read everything back into a real List so we can measure and trim.
        //    ArrayList<> is Java's standard resizable array.
        List<Event> all = new ArrayList<>();
        Iterable<Event> stored = recentState.get();

        // get() returns null when the list is empty. It can't be here (we just
        // added), but the null check is the habit you want everywhere else.
        if (stored != null) {
            // enhanced for-loop: "for each Event e in stored"
            for (Event e : stored) {
                all.add(e);
            }
        }

        // 3. TRIM. Flink will NOT do this for you.
        if (all.size() > n) {
            // subList(from, to) is a VIEW of a slice — [size-n, size) = the last n.
            // Wrapping in new ArrayList<>(...) makes an independent copy, which
            // matters because we're about to overwrite the source list.
            all = new ArrayList<>(all.subList(all.size() - n, all.size()));

            // 4. Write the trimmed list back, REPLACING the stored one.
            recentState.update(all);
        }

        // 5. Emit a readable summary.
        StringBuilder sb = new StringBuilder(ctx.getCurrentKey() + " last" + n + ": ");
        for (Event e : all) {
            sb.append(e.type).append("@").append(e.timestamp).append(" ");
        }
        out.collect(sb.toString().trim());
    }
}
```

```java
events
    .keyBy(e -> e.userId)
    .process(new LastNEventsFn(3))
    .print();
```

`StringBuilder` is Java's mutable string accumulator. Building strings with `+` inside a loop creates a new `String` object every iteration; `StringBuilder` appends into one buffer. In a hot streaming loop that difference is real.

### Trace (n = 3, alice only)

```
event         after add()                    size   trimmed to             emitted
───────────   ───────────────────────────    ────   ────────────────────   ─────────────────────────
LOGIN@1000    [L@1000]                        1     (no trim)              alice last3: L@1000
PURCH@1100    [L@1000, P@1100]                2     (no trim)              alice last3: L@1000 P@1100
PURCH@1200    [L@1000, P@1100, P@1200]        3     (no trim)              alice last3: L@1000 P@1100 P@1200
LOGOUT@1300   [L@1000, P@1100, P@1200, O@1300] 4    [P@1100,P@1200,O@1300] alice last3: P@1100 P@1200 O@1300
LOGIN@1400    [P@1100, P@1200, O@1300, L@1400] 4    [P@1200,O@1300,L@1400] alice last3: P@1200 O@1300 L@1400
```

### The warning: ListState has no size bound

> **Key idea:** Nothing in Flink caps the size of a `ListState`. If you `add()` on every record and never trim, one key's list grows until the job dies.

Real failure mode: a bot account fires 50 million events. Its `ListState` becomes a single 5 GB list. Every `get()` deserializes 5 GB. Checkpoints time out. The job restarts, replays, and dies again.

Bound it one of three ways:

```java
// A. Count bound — trim to N (the example above)
if (all.size() > n) { recentState.update(trimmed); }

// B. Time bound — drop elements older than a cutoff
long cutoff = ctx.timestamp() - Duration.ofMinutes(10).toMillis();
List<Event> keep = new ArrayList<>();
for (Event e : recentState.get()) {
    if (e.timestamp >= cutoff) keep.add(e);
}
recentState.update(keep);

// C. TTL — expires the whole list after inactivity (chapter 22)
desc.enableTimeToLive(ttlConfig);
```

Note what option C does and doesn't do: for `ListState`, TTL expires elements individually based on when each was written, but you still can't cap the size within the TTL window. A 50 M-events-per-hour key with a 1-hour TTL still holds 50 M elements. Count bounds and TTL are complements, not substitutes.

Also watch the read cost. The example above calls `get()` on **every record**, which is O(list size) each time. For a small `n` that's fine. If you only need the list occasionally, `add()` cheaply and read on a timer instead.

## `MapState<K, V>` — a hash map per key

### The API

```java
V                            get(K key)                  throws Exception;
void                         put(K key, V value)         throws Exception;
void                         putAll(Map<K,V> map)        throws Exception;
void                         remove(K key)               throws Exception;
boolean                      contains(K key)             throws Exception;
Iterable<Map.Entry<K,V>>     entries()                   throws Exception;
Iterable<K>                  keys()                      throws Exception;
Iterable<V>                  values()                    throws Exception;
Iterator<Map.Entry<K,V>>     iterator()                  throws Exception;
boolean                      isEmpty()                   throws Exception;
void                         clear();
```

It is a `java.util.Map` in spirit. Two differences: every method `throws Exception`, and there is no `size()` — count by iterating `keys()` if you truly need it (and think hard about why you need it).

### Careful with the two levels of key

```
MapState<String, Long> "byCategory"

  OUTER key (from keyBy) : alice
        └── INNER map    : { "books" -> 3, "tools" -> 1 }

  OUTER key              : bob
        └── INNER map    : { "food" -> 12 }
```

`mapState.get("books")` uses the **inner** key. The outer key is still implicit and still set by Flink from `keyBy`. Two levels, only one of which you type.

### Declaring it

```java
private transient MapState<String, Long> categoryCounts;

@Override
public void open(OpenContext ctx) {
    MapStateDescriptor<String, Long> desc =
            new MapStateDescriptor<>("by-category", String.class, Long.class);
    //                                 ^ name        ^ inner key   ^ value
    categoryCounts = getRuntimeContext().getMapState(desc);
}
```

### Worked example: per-user, per-category spend

```java
public class SpendByCategoryFn extends KeyedProcessFunction<String, Event, String> {

    private transient MapState<String, Double> spendState;

    @Override
    public void open(OpenContext ctx) {
        spendState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("spend-by-category", String.class, Double.class));
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<String> out)
            throws Exception {

        if (!"PURCHASE".equals(event.type)) {
            return;   // ! is Java's boolean NOT
        }

        // Pretend the category comes off the event; in the Phase 1 POJO we'll
        // reuse `type`, but in a real schema this is its own field.
        String category = event.type;

        // Read ONE entry. On RocksDB this touches only this entry's bytes.
        Double current = spendState.get(category);
        if (current == null) {
            current = 0.0;
        }

        // Write ONE entry back.
        spendState.put(category, current + event.amount);

        // Iterate the whole map only when we actually need a full report.
        // Map.Entry<K,V> is Java's key-value pair type; getKey()/getValue() read it.
        StringBuilder sb = new StringBuilder(ctx.getCurrentKey() + ": ");
        for (Map.Entry<String, Double> entry : spendState.entries()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
        }
        out.collect(sb.toString().trim());
    }
}
```

### `MapState` as a set

There's no `SetState`. Use `MapState<T, Boolean>` and ignore the value:

```java
private transient MapState<String, Boolean> seenDevices;

// mark seen
seenDevices.put(event.deviceId, Boolean.TRUE);

// test membership — O(1) on RocksDB, no full scan
if (!seenDevices.contains(event.deviceId)) {
    out.collect("NEW DEVICE for " + ctx.getCurrentKey() + ": " + event.deviceId);
}
```

This is the standard deduplication pattern. Pair it with a TTL (chapter 22) so the set doesn't grow forever.

## The production point: `MapState` vs `ValueState<HashMap>`

These two look equivalent:

```java
// Option A
MapState<String, Long> counts;

// Option B
ValueState<HashMap<String, Long>> counts;
```

On `HashMapStateBackend` they perform about the same — both end up as objects on the heap, no serialization on access.

On `EmbeddedRocksDBStateBackend` they are **wildly** different, and this is one of the most valuable practical facts in Flink.

### How each is stored

```
ValueState<HashMap<String, Long>>        MapState<String, Long>
─────────────────────────────────        ──────────────────────────────
RocksDB holds ONE key:                   RocksDB holds ONE KEY PER ENTRY:

  (alice) -> <serialized bytes of          (alice, "books") -> 3
              the ENTIRE HashMap>          (alice, "tools") -> 1
                                           (alice, "food")  -> 7
                                           ... 10,000 more
```

### The cost of reading one entry

```
ValueState<HashMap<String,Long>>, map has 10,000 entries:

   counts.value()          -> read ALL bytes for alice   (say 400 KB)
                           -> deserialize ALL 10,000 entries
                           -> allocate 10,000 objects
   map.get("books")        -> the only thing you wanted
   counts.update(map)      -> serialize ALL 10,000 entries
                           -> write ALL 400 KB back

   Per record: ~800 KB of I/O + 20,000 object operations to touch ONE counter.


MapState<String,Long>, map has 10,000 entries:

   counts.get("books")     -> one RocksDB point lookup, deserialize ONE Long
   counts.put("books", 4)  -> one RocksDB put, serialize ONE Long

   Per record: ~16 bytes of I/O + 2 object operations.
```

> **Key idea:** With `ValueState<HashMap>` on RocksDB, every single access pays for the **entire** map. With `MapState`, you pay only for the entry you touch. At 10,000 entries that's a four-orders-of-magnitude difference in per-record work.

### And it compounds

Beyond raw access cost:

| | `ValueState<HashMap<K,V>>` on RocksDB | `MapState<K,V>` on RocksDB |
|---|---|---|
| Read one entry | Deserialize whole map — O(n) | Point lookup — O(1) |
| Write one entry | Serialize + write whole map — O(n) | One put — O(1) |
| Iterate all entries | O(n), one read | O(n), a range scan (comparable) |
| GC pressure | Huge — reallocates n objects per record | Minimal |
| Incremental checkpoint size | The whole map re-uploads when any entry changes | Only changed entries |
| Per-entry TTL | Impossible — TTL applies to the whole blob | Supported, per entry |
| Realistic max entries per key | Thousands before it hurts | Millions |

The incremental-checkpoint row is the sleeper. RocksDB uploads changed SST files. Changing one counter in a `ValueState<HashMap>` rewrites the whole blob, so the whole blob re-uploads on the next incremental checkpoint. With `MapState`, only the touched entries do.

### The rule

```
Need a map per key, and it has more than a handful of entries?
   -> MapState. Always.

The "map" has 2-3 fixed fields you always read together?
   -> ValueState<SomePojo> is fine, and simpler.

Need to read or write ALL entries on every single record anyway?
   -> The advantage narrows; ValueState<HashMap> becomes defensible.
      But you should question the design first — that's an expensive access pattern.
```

The same reasoning applies to `ListState<T>` versus `ValueState<List<T>>`: `add()` is an O(1) merge write on `ListState` and an O(n) read-modify-write on `ValueState<List<T>>`.

## All three compared

| | `ValueState<T>` | `ListState<T>` | `MapState<K,V>` |
|---|---|---|---|
| Holds | one value | ordered, append-only collection | key-value pairs |
| Read | `value()` | `get()` → `Iterable`, O(n) | `get(k)`, O(1) |
| Write | `update(v)` | `add(v)` O(1), `update(list)` O(n) | `put(k,v)`, O(1) |
| Delete part | no | no — rebuild and `update()` | `remove(k)` |
| Membership test | no | O(n) scan | `contains(k)`, O(1) |
| Null on empty | `value()` → null | `get()` → null | `get(k)` → null |
| Bounded by Flink | n/a | **no** — you must trim | **no** — you must prune |
| Typical use | counter, balance, flag, timer handle | last N events, buffer for a join | per-category aggregates, seen-set, dedup |

## Remember

- `ListState.get()` returns `Iterable`, and returns `null` when empty.
- `ListState.add()` is a cheap O(1) merge on RocksDB; `get()` and `update()` are O(n).
- `ListState` has **no size limit**. Trim by count, trim by time, or set a TTL — pick one.
- `MapState` has two levels of key: the outer `keyBy` key (implicit) and the inner map key (explicit).
- `MapState` has no `size()`. Use `isEmpty()`, or iterate `keys()` if you must.
- `MapState<T, Boolean>` is how you build a set.
- **`MapState` beats `ValueState<HashMap>` on RocksDB** — point access instead of whole-blob serialization, per-entry TTL, and smaller incremental checkpoints.
- Every collection state needs a growth story. Write it down before you ship.

## Interview one-liners

- *"`MapState` vs `ValueState<HashMap>`?"* → On RocksDB, `ValueState<HashMap>` serializes and deserializes the entire map on every access; `MapState` stores each entry under its own RocksDB key so you pay only for the entry you touch. It also gives per-entry TTL and much smaller incremental checkpoints.
- *"What does `ListState.get()` return?"* → An `Iterable<T>`, which is `null` if the key has no elements. No `size()`, no indexing.
- *"How do you bound a `ListState`?"* → Yourself: trim by count on write, filter by time, or attach a TTL. Flink imposes no limit.
- *"Why is `ListState.add()` fast on RocksDB?"* → It maps to RocksDB's native merge operator — an append-only write that never reads the existing list.
- *"How do you implement a set in Flink state?"* → `MapState<T, Boolean>` with `contains()` for O(1) membership. There is no `SetState`.
- *"Does `MapState` have `size()`?"* → No. `isEmpty()` exists; counting means iterating the keys.
- *"When is `ValueState<HashMap>` acceptable?"* → Tiny fixed maps, or when you read and write the whole thing on every record anyway — and even then, only after you've questioned that access pattern.
