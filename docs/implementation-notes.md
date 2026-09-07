# Implementation notes

Behaviour of the MongoDB backend that is not obvious from reading the code.
The two large design decisions have their own records:

- [ADR-0001](adr/0001-revision-from-clustertime.md) — the etcd revision is
  MongoDB's `clusterTime`, not a counter
- [ADR-0002](adr/0002-watch-listens-to-inserts.md) — the watch listens to
  `insert` only

Everything below is smaller, but each item exists because getting it wrong
caused a real failure.

---

## Writes take two operations

`append` performs an `InsertOne` followed by an `UpdateByID`.

The revision comes from the write's own `clusterTime`, available only *after*
the insert via `session.OperationTime()`. The second operation persists it into
the `rev` field.

Wrapping both in a transaction was measured and rejected: 80.9 ms against
59.5 ms, with no benefit. The `UpdateByID` is not on the ordering critical path
— the watch derives the revision from the insert event (ADR-0002), so the
update may land in any order relative to concurrent writes. The `rev` field
serves historical queries (`After`, `List`, `Get` by revision), which read
after the fact.

**Orphans.** If the process dies between the two operations, the document keeps
`rev = 0`: invisible to `After` and `List`, which filter on `rev > 0`. The event
was already delivered to watchers, so there is a window where a watcher saw
something a historical replay would not. The orphan is inert — it corrupts
nothing, and the key can be rewritten because the `(name, prev_revision)`
constraint still holds. If that window ever needs closing, the fix is
reconciling `rev = 0` at startup, **not** serializing writes.

## `majority` concern is mandatory, and free

`writeConcern` and `readConcern` are pinned to `majority` because kine must read
the revision it just wrote. Reading from a secondary is out of the question.

The cost was measured: 26.6 ms against 27.2 ms for a default write — an
indistinguishable difference. There is no tuning trade-off here to consider.

## The epoch base is immutable

The revision is `(clusterTime.T - epochBase) << 20 | clusterTime.I`. Without
the subtraction, the shift overflows `int64` around 2038. With it, the 43
seconds bits cover ~278,000 years and the 20 ordinal bits hold 1,048,575
operations per second.

The value is written to `kine_meta` on first run. `resolveEpochBase` refuses to
start if the DSN asks for a different one: changing it would rewrite the meaning
of every revision already handed to the apiserver. When two instances race to
create the metadata, the loser rereads and adopts the winner's value.

This is also why `kine_meta` must be included in backups — see
[backup-restore.md](backup-restore.md).

## Leases do not use a TTL index

Early versions let MongoDB expire documents through a TTL index on
`expires_at`. That was wrong in three ways:

1. MongoDB deletes the document physically, without producing the tombstone the
   etcd protocol requires.
2. Deleting the newest revision can expose an older historical one —
   effectively resurrecting deleted data.
3. The change stream matches `insert` only, so watchers never saw the expiry at
   all.

Expiry now goes through `pkg/ttl`, kine's shared mechanism, which calls `Delete`
with a revision comparison and produces the correct tombstone. `setup` drops the
legacy `lease_ttl` index from existing databases, ignoring MongoDB error 27
(`IndexNotFound`).

## One change stream per process

All watchers share a single stream through `pkg/broadcaster`. Each one filters
the flow to its own key range and keeps its own cutoff revision.

Before this, every `Watch` opened its own stream. An apiserver has dozens of
informers and M0 allows 500 connections in total. The measured effect of the
change, on a real k3s: `readyz` from ~95 s to 30 s, and 50 configmaps of load
from 77 s to 14 s.

**Order of operations in `Watch` matters.** Subscribing to the broadcaster
happens *before* the historical read. The other way round, an event occurring
between the end of the read and the subscription would be lost; subscribed
first, whatever arrives in that gap sits in the channel buffer and is later
discarded by the cutoff revision.

## Change stream recovery

`laçoDoStream` reopens the stream when it drops, with 1 s to 30 s backoff,
resuming from the resume token.

When MongoDB refuses to resume — `ChangeStreamHistoryLost` (286) or
`ChangeStreamFatalError` (280) — the gap is filled by reading the collection
between the last observed revision and now, and only then is a fresh stream
opened.

The oplog window on M0 was measured at **~4.4 hours**. MongoDB does not
validate `startAtOperationTime` when the stream opens, only when it first
reads, which is why the driver classifies the error from `cs.Err()` rather than
from `Watch()`.

## Compaction

Compaction runs inside a transaction with a compare-and-swap on the compacted
revision in `kine_meta`. This path is rare enough not to suffer the contention
that ruled out a revision counter.

**The delete direction matters.** What gets removed are the revisions
*pointed at* as predecessors by a newer document — not the ones that *have* a
`prev_revision`. Inverting this deletes the most recent record of every key. The
SQLite driver's `CompactSQL` (`pkg/drivers/sqlite/sqlite.go:91`) is the
reference, and an integration test caught the inversion.

When a concurrent instance wins the CAS, `Compact` returns `ErrCompacted`, which
`sqllog` already treats as normal rather than as a failure.

## `keysOnly` is not cosmetic

`List` projects out `value` and `old_value` when `keysOnly` is set. Measured:
listing 300 keys takes 823 ms with values and 54.6 ms without.

The bottleneck is payload crossing the network at ~5 MB/s, not the query plan.
Replacing the `$group` aggregation with a materialised current-state collection
was tried and made it *worse* (1403 ms). The same 15× gap shows up through the
full gRPC stack.

## Metrics target latency, not errors

When Atlas M0's operation ceiling is exceeded it does not return errors — it
queues. Measured: zero errors at 6× the ceiling, but write p99 went from 810 ms
to 63 s, and the change stream fell 44 s behind.

So error rate does not detect the most likely failure. `op_duration_seconds`
buckets reach ~16 s deliberately, because leader election's 10 s `RenewDeadline`
falls inside that range, and `change_stream_lag_seconds` is the metric that
catches a watch going stale.

Label series are pre-initialised so Prometheus exposes them before the first
event — otherwise a dashboard shows "no data" for reconnections, which is
exactly the state one hopes is permanent.

The gauge collector runs every 30 s, not every few seconds: a frequent
`collStats` would consume part of the very operation budget being measured.

## `Version` follows etcd semantics

The `version` field counts modifications since the key was created: 1 on
create, 2 on the first update, resetting to 1 when a key is recreated after
deletion. `PrevKV` in watch events exposes the previous value.

This was missing in earlier versions — `toKV` left it at zero. Nothing broke;
the apiserver simply received a wrong metadata field. A parity test against
`pkg/drivers/memory` caught it.

## Two kine instances over one MongoDB

The driver returns `leaderElect=true`, promising support for several control
plane servers sharing a datastore.

This works without any cross-process coordination because the revision is the
`clusterTime`, which is global to the MongoDB cluster, and the oplog is single.
Validated: two backends over the same collection produce globally monotonic,
distinct revisions; one's watch sees the other's writes in order; and concurrent
compaction has exactly one winner.

## `Txn` in kine is a closed set of shapes

Not specific to this backend, but it will bite anyone writing tests: kine
recognises `Txn` requests by **exact form** (`pkg/server/limited.go:29`). A
conditional delete, for instance, requires an `Else` branch containing a `Range`
(`pkg/server/delete.go:19`). Omitting it yields
`unsupported operations in txn request`, even though the request is valid etcd.

The apiserver always sends the `Else`.
