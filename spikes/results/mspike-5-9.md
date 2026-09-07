# MSPIKE-9 + MSPIKE-5 — Document model, indexes and latency

**Status:** ✅ done · **Date:** 2026-09-06

## Schema

```javascript
// kine collection — the revision log
{ _id: ObjectId, rev: int64,        // rev = clusterTime (ADR-0001)
  name: str, created: bool, deleted: bool,
  create_revision: int64, prev_revision: int64, lease: int64,
  version: int64, value: BinData, old_value: BinData }

// indexes
{ name: 1, rev: -1 }                  // Get of a key's latest revision
{ rev: 1 }                            // After / watch
{ name: 1, prev_revision: 1 } UNIQUE  // duplicate key detection
{ prev_revision: 1 }                  // compaction
```

`DuplicateKeyError` (code 11000) on the `name_prev_uniq` index maps directly to
`server.ErrKeyExists`. Confirmed.

## Index usage

| Query | Plan |
|---|---|
| Get: a key's latest revision | ✅ IXSCAN |
| After: revisions > X | ✅ IXSCAN |
| List: prefix range | ✅ IXSCAN |
| Compact: by `prev_revision` | ✅ IXSCAN |
| **ListCurrent via `$group`** | ❌ **COLLSCAN**, 880 ms |

## Latency

| Query | p50 | p95 |
|---|---|---|
| Get one key | **23.4 ms** | 25.1 ms |
| After — idle | **23.2 ms** | 24.1 ms |
| CurrentRevision | 23.3 ms | 30.0 ms |
| Count by prefix | 23.3 ms | 24.1 ms |
| write (insert + set rev) | 54.9 ms | 54.3 ms |
| **ListCurrent (300 keys, with value)** | **823 ms** | 1175 ms |
| **ListCurrent (300 keys, no value)** | **54.6 ms** | 83.8 ms |

## The finding: `LIST` is bandwidth-bound, not index-bound

Measuring the same query with different payloads:

| Volume | p50 | Throughput |
|---|---|---|
| 300 × 1 KB = 0.29 MB | 61 ms | 4.8 MB/s |
| 300 × 6 KB = 1.76 MB | 269 ms | 6.5 MB/s |
| 300 × 20 KB = 5.86 MB | 3498 ms | 1.7 MB/s |

And the decisive comparison: **without `value` it is 54.6 ms; with `value`,
823 ms — 15×**.

So replacing the `$group` aggregation with a materialised current-state
collection **did not help** (it came out at 1403 ms). The bottleneck was never
the execution plan, it is the payload crossing the network.

This also explains D1 retroactively, which measured 1039 ms for the same 300
keys: **it is a property of any remote datastore**, not of MongoDB.

Consequences:

1. **`keysOnly` stops being an optimisation and becomes the main path.**
   `server.Backend` receives that parameter; using it well is what separates a
   55 ms LIST from an 823 ms one.
2. A materialised current-state collection still has merit — not for latency,
   but to avoid the `$group` COLLSCAN.
3. M0's transfer limit (10 GB/7 days ≈ 1.4 GB/day) deserves attention: a full
   LIST of a 500-pod cluster moves ~3 MB.

## Writes: the cost of `clusterTime`

The revision only exists **after** the insert (`session.operation_time`), so
storing it in the document requires a second operation:

| Strategy | p50 |
|---|---|
| insert only (revision not stored) | **30.7 ms** |
| insert + revision update | 59.5 ms |
| insert + update inside a transaction | 80.9 ms |

The non-transactional path costs ~29 ms extra. Still **4× better than D1's
232 ms**, but it is the obvious optimisation target.

> Alternative to explore: do not store `rev` in the document at all, and keep
> the revision→document index in memory, populated by the very change stream
> the watch already consumes. That would reduce writes to a single operation
> (30.7 ms), at the cost of rebuilding the index at startup.

## Cost per mutation and ceiling

Measured with the full cycle (historical log + current collection):

```
40 mutations in 3.8s = 10.4 mutations/s
3 ops per mutation (insert + rev update + current upsert)
M0's 100 ops/s ceiling -> ~33 mutations/s
```

The inherited write-source model estimated 3.3 writes/s idle and 10 in normal
operation. **It fits, with ~3× headroom.**

## Storage — the real ceiling (MSPIKE-7)

```
6 KB document, with indexes: 13,694 bytes
512 MB / 13,694 = ~39,200 documents
```

> **Corrected by MT-3.** That estimate used synthetic 6 KB objects. A real k3s
> cluster turned out to be 842 documents totalling **~1 MB** — about
> **429,000 documents** in 512 MB, roughly 500 clusters that size. Real k8s
> objects are far smaller than 6 KB, so storage is ~10× more generous than
> projected here.

**But M0 does not grow: when it fills up, it stops.** If compaction stalls, the
cluster stalls with it. That makes the storage alert (`MOPS-1`) a requirement,
not a comfort — the same conclusion D1 reached about its cost alert, by a
different route.
