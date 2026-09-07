# MSPIKE-4 — How to generate revisions: the design changed here

**Status:** ✅ done · **Date:** 2026-09-06 · **Decision:** [ADR-0001](../../docs/adr/0001-revision-from-clustertime.md)

## The problem

In kine the etcd revision is the row `id` (`AUTOINCREMENT`). The apiserver
depends on three properties: **monotonic**, **no permanent gaps**, **visible in
order**. MongoDB has no AUTOINCREMENT.

## Counter strategies — all bad

Concurrency 16, 80 writes:

| Strategy | Success | Rate | p50 | Problem |
|---|---|---|---|---|
| A· transaction, no retry | **15/80** | 41/s | 91 ms | 81% fail with `WriteConflict` |
| B· transaction + exponential retry | **49/80** | 8.2/s | **870 ms** | still fails, and latency collapses |
| C· atomic counter, no transaction | 80/80 | 37/s | 174 ms | works, but 6.5× slower than a plain insert |

All of them funnel through one counter document, which becomes a serialization
point. The "correct" one (B) delivers **8.2 writes/s** — below what a 20-node
cluster needs.

## And the worse problem: the change stream does not deliver in revision order

With a counter, revisions arrive at the watch **out of order**:

```
revisions seen: [5, 13, 14, 9, 11, 1, 15, 7, 4, 2, 3, 12, 6, 10]
out-of-order events: 13 of 29
```

It makes sense: the oplog's order is **commit** order, not counter-acquisition
order. Two writes take 5 and 6, and 6 commits first.

For kine that is fatal — the watch needs strict ordering. It would require a
reordering buffer, reintroducing latency and complexity exactly where change
streams were supposed to simplify.

## The solution: `clusterTime` as the revision

Instead of inventing a sequence, **use the one MongoDB already maintains**.
Measured:

| Question | Result |
|---|---|
| Is the revision known **at write time**? | ✅ `session.operation_time` after the insert |
| Is it unique under concurrency? | ✅ 40 concurrent writes, **40 distinct revisions**, 0 duplicates |
| Does the change stream deliver in its order? | ✅ **0 out-of-order events** in 40 |
| Does the write's `clusterTime` match the event's? | ✅ **identical** — `Timestamp(1788730614, 7)` on both sides |

`int64` encoding: `(ts.time << 32) | ts.inc`.

**This eliminates, at once:** the counter, the contention, transactions on the
hot path, the reordering buffer and gap-fill. Watch ordering becomes the order
of revisions **by construction**, not by effort.

## What was left open

1. **Revisions stop being dense.** They jump instead of incrementing by one.
   The apiserver treats `resourceVersion` as an opaque monotonic value, but that
   needed validating against real k3s (`MT-3`) — and it affects
   `compactMinRetain`, which counts revisions.
2. **Encoding lifetime.** `time << 32` reaches ~7.68 × 10¹⁸ today against
   `int64`'s ceiling of 9.22 × 10¹⁸ — it overflows around **2038**. Using a
   cluster epoch base (`(time - base) << 20 | inc`) leaves centuries.
3. **Transactions remain useful** for compaction and other rare operations —
   just not for the write path.

## Reproduce

```bash
spikes/mspike-4-revisoes.py [concurrency] [total]
```
