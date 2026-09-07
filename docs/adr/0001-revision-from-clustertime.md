# ADR-0001 — The etcd revision comes from `clusterTime`, not a counter

**Status:** accepted · **Date:** 2026-09-06 · **Evidence:** [MSPIKE-4](../../spikes/results/mspike-4.md)

## Context

In kine the etcd revision is the row `id`, an `INTEGER PRIMARY KEY
AUTOINCREMENT`. The apiserver depends on three properties: monotonic, no
permanent gaps, and **visible in order**. MongoDB has no AUTOINCREMENT.

The idiomatic solution is a counter document with `findAndModify` + `$inc`,
optionally inside a transaction to tie the revision to the written document.

## Measurements that ruled out the counter

Concurrency 16, 80 writes:

| Strategy | Success | Rate | p50 |
|---|---|---|---|
| Transaction, no retry | 15/80 | 41/s | 91 ms |
| Transaction + exponential retry | 49/80 | **8.2/s** | **870 ms** |
| Atomic counter, no transaction | 80/80 | 37/s | 174 ms |

The correct variant delivers **8.2 writes/s** — below what a 20-node cluster
needs. All of them serialize on the same document.

And the decisive problem is a different one: with a counter, **the change
stream delivers revisions out of order** — 13 of 29 events. The oplog's order
is commit order, not counter-acquisition order. Two writes take 5 and 6, and 6
commits first. For kine, an out-of-order watch is a broken watch.

## Decision

**Use MongoDB's `clusterTime` as the revision**, encoded into an `int64`.

- On write: `session.operation_time`, available right after the operation.
- On watch: the change stream event's `clusterTime` field.
- Encoding: `(ts.time - epochBase) << 20 | ts.inc`.

## Rationale

Measured against a real cluster:

- **Known at write time.** `session.operation_time` returns the timestamp
  immediately after the insert — so `server.Backend`'s synchronous API remains
  possible.
- **Unique under concurrency.** 40 concurrent writes produced 40 distinct
  revisions, zero duplicates.
- **The change stream delivers in its order.** Zero out-of-order events in 40 —
  against 13 of 29 with a counter.
- **Write and event carry the same revision.** `Timestamp(1788730614, 7)`
  identical on both sides.

The underlying difference: a counter invents an ordering that competes with the
database's. `clusterTime` **is** the database's ordering. Watch ordering stops
being a problem to solve and becomes a property of the choice.

This eliminates, all at once, the counter, the contention, transactions on the
hot path, the reordering buffer, and gap-fill.

## Consequences

- **Revisions are no longer dense.** They jump instead of incrementing by one.
  The apiserver treats `resourceVersion` as an opaque monotonic value, and this
  was **validated against a real k3s cluster** (MT-3). It also affects
  `compactMinRetain`, which counts revisions — it becomes a time window.
- **The encoding needs an epoch base.** `time << 32` overflows `int64` around
  2038. With `(time - base) << 20` there are centuries to spare, at the cost of
  20 bits for `inc` (1,048,575 operations per second — ample against M0's
  ceiling). The `epochBase` becomes cluster metadata and **cannot change once
  written**.
- **Transactions remain in the project**, but for compaction and rare
  operations — not for writes.
- **`Fill`/`IsFill` (gap-fill) are no longer needed.** There is no dense
  sequence to have gaps in.
- Two writes with the same `inc` in the same second are impossible by the
  oplog's own construction, but the driver should fail loudly if it ever
  detects a duplicate revision rather than carrying on.
