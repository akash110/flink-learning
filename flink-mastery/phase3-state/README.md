# Phase 3 — Keyed State and Timers

Phase 1 was stateless operators. Phase 2 was windows, where Flink managed state for you behind the scenes. **This phase is where you manage it yourself** — and it's where Flink stops being "Spark but streaming" and becomes something you can't do any other way.

All code is **Java**, Flink 1.18/1.20-era API. Every chapter ends with Remember and Interview one-liners.

## Table of contents

| # | Chapter | Key idea |
|---|---|---|
| 18 | [What Flink state actually is](18-what-flink-state-is.md) | Key-scoped, checkpointed memory. Local beats a remote DB on latency, throughput, **and** correctness |
| 19 | [**ValueState**](19-valuestate.md) | One value per key. `transient` + `open()` isn't style — the function object is serialized to the workers |
| 20 | [ListState and MapState](20-liststate-mapstate.md) | `MapState` beats `ValueState<HashMap>` on RocksDB by four orders of magnitude. Neither is size-bounded |
| 21 | [ReducingState and AggregatingState](21-reducing-aggregating-state.md) | Pre-aggregate on write. One stored value per key no matter how many inputs |
| 22 | [**State TTL**](22-state-ttl.md) | **Unbounded keyspace + no TTL = the #1 killer of production jobs.** And TTL is processing time |
| 23 | [**KeyedProcessFunction and timers**](23-keyedprocessfunction-and-timers.md) | How to detect what *didn't* happen. Timers are per-key, checkpointed, and deduplicated |
| 24 | [**Fraud detection capstone**](24-fraud-detection-capstone.md) | The flagship: flag + timer + delete-on-happy-path + side output, fully traced |

## Reading order

Straight through, 18 → 24. Each chapter assumes the one before it.

If you have limited time: **19, 22, 23** are the three that matter most. Chapter 24 is where they combine, and it's the one to be able to write from memory in an interview.

## The build targets, and where each is worked

| Target | Chapter |
|---|---|
| User → running count | [19](19-valuestate.md) |
| User → running balance | [19](19-valuestate.md) |
| User → last N events | [20](20-liststate-mapstate.md) |
| User → running average | [21](21-reducing-aggregating-state.md) |
| User → last activity (24h TTL) | [22](22-state-ttl.md) |
| User → inactive for 30 minutes | [23](23-keyedprocessfunction-and-timers.md) |
| User → fraud detection | [24](24-fraud-detection-capstone.md) |

## The five state types on one page

| Type | Holds | Write | Read | Grows unbounded? |
|---|---|---|---|---|
| `ValueState<T>` | one value | `update(v)` | `value()` | no |
| `ListState<T>` | many elements | `add(v)` | `get()` → `Iterable` | **yes — trim it** |
| `MapState<K,V>` | key→value pairs | `put(k,v)` | `get(k)` | **yes — prune it** |
| `ReducingState<T>` | one folded value | `add(v)` | `get()` | no |
| `AggregatingState<IN,OUT>` | one accumulator | `add(v)` | `get()` → `OUT` | no* |

\* unless the accumulator itself is unbounded, e.g. a `HashSet`.

All five return **`null`** when the key has never been written. Handle it on the first line.

## The ten things that matter most

1. **State is `(operator, descriptor name, key)`.** Rename the descriptor and the savepoint's state is orphaned.
2. **`transient` field, initialized in `open()`.** Forced by the fact that the function object is Java-serialized on the client and rebuilt on the worker.
3. **You never pass a key.** Flink sets the current key before calling you; every state access is implicitly scoped to it.
4. **`value()` returns `null` for a new key.** Not zero. The most common NPE in Flink.
5. **Always call `update()`.** Mutating in place works on the heap backend and silently fails on RocksDB.
6. **`MapState`, never `ValueState<HashMap>`** on RocksDB. Point access vs. serializing the entire blob every record.
7. **Unbounded keyspace + no TTL kills the job.** Weeks of healthy operation, then checkpoints time out. Set a TTL.
8. **TTL is processing time.** Backfills expire nothing; stalls expire too much. Use event-time timers for real expiry semantics.
9. **Timers are state.** Per key, checkpointed, deduplicated on exact timestamp. Store the timestamp in `ValueState<Long>` so you can delete it.
10. **Delete the timer on the happy path too.** Register-without-delete is the single most common timer bug, and it produces alert floods.

## The pre-production checklist

For every piece of keyed state you write:

```
1. What is the key?
2. Is the keyspace bounded?
3. Do I clear() when a key is finished?
4. Do I have a TTL?              <- mandatory if 2 and 3 are both "no"
5. Is the TTL longer than my worst stall or backfill skew?
6. Is my cleanup strategy right for my state backend?
      heap    -> cleanupIncrementally
      RocksDB -> cleanupInRocksdbCompactFilter
7. Projected steady-state size = keys x bytes/key x safety factor
```

## What comes next

- **Phase 4** — real-world patterns built on this: joins, enrichment, deduplication, session logic.
- **Phase 5** — checkpoints and savepoints: the mechanism that makes everything in this phase fault tolerant, plus exactly-once semantics, state migration, and rescaling in practice.
- **Phase 6** — broadcast state and the connected-stream patterns that let a rules table reach every subtask.

Chapter 18 forward-references Phase 5 repeatedly. If you find yourself asking "but how does the state actually get saved?", that's the right question and Phase 5 is the answer.
