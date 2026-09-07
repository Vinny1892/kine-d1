# ADR-0002 — The watch listens to `insert` only, deriving the revision from the event's `clusterTime`

**Status:** accepted · **Date:** 2026-09-07 · **Evidence:** [mt3.md](../../spikes/results/mt3.md)

## Context

The etcd revision comes from MongoDB's `clusterTime`
([ADR-0001](0001-revision-from-clustertime.md)). That value is only known
**after** the insert, via `session.OperationTime()`. So `append` writes in two
operations: the `InsertOne`, and an `UpdateByID` that persists the `rev` field.

The first watch implementation discarded the insert event
(`if r.Rev == 0 { continue }`) and waited for the **update** event, which
already carries the field populated.

## The problem

Under concurrency the inserts go out in order A/B, but the updates may go out
B/A — they are two independent round trips. The watch advanced its cutoff
revision with the larger value (B) and, when the smaller one (A) arrived,
discarded it as already delivered. **Events vanished.**

This is not theoretical: it stalled a real k3s bootstrap at
`autoregister-completion`. Some objects were persisted and their events never
reached the apiserver's caches.

## The alternative that was rejected

Serialize writes with two mutexes (`insertMu`, `revisionMu`), forming a
pipeline where updates reach the oplog in the same order as the inserts.

It worked and preserved ordering, but **created a queue**. Under k3s bootstrap
load the wait exceeded leader election's 5-10 s deadlines, and scheduler and
controller-manager lost their leases — taking the cluster down. The fix for the
first bug produced the second.

## Decision

**The watch matches only `operationType: "insert"` and derives the revision
from the event's own `clusterTime`**, instead of reading the document's `rev`
field.

```go
pipeline := mongo.Pipeline{
    {{Key: "$match", Value: bson.M{"operationType": "insert"}}},
}
// ...
rev, err := EncodeRevision(ev.ClusterTime, b.cfg.EpochBase)
```

## Rationale

**The insert event already carries the revision.** The insert's `clusterTime`
is exactly the value the write stores in `rev` — measured in MSPIKE-4:
`Timestamp(1788730614, 7)` identical on both sides. Waiting for the update to
obtain a number already in hand was the mistake.

**Listening to inserts only is correct because kine is an append-only log.**
Every mutation — create, update, delete — inserts a new document. The only
update that exists is the one writing `rev`, and it represents no mutation at
all. Filtering updates loses no information; it removes noise.

**Ordering becomes a property of the choice, not the result of effort.** Since
the revision *is* the `clusterTime`, the order in which the oplog delivers
events is the order of revisions. There is nothing to reorder, nothing to
serialize, nothing to coordinate.

**A side effect that closed an open problem:** the oplog is global to the
MongoDB cluster. The limitation recorded for the mutex approach — "they
coordinate only within a single kine instance" — ceases to exist. Validated by
`TestDuasInstancias`: two backends over the same collection produce globally
monotonic, distinct revisions, and one's watch sees the other's writes in order.

**The performance gain was large and measurable.** Without the queue:

| | With mutexes | Listening to inserts |
|---|---|---|
| apiserver `readyz` | ~95 s | **30 s** |
| 50 configmaps of load | 77 s | **14 s** |

## Consequences

- `UpdateByID` leaves the ordering critical path. It now serves only historical
  queries (`After`, `List`, `Get` by revision), which read after the fact.
- If the process dies between the insert and the update, the document keeps
  `rev = 0`: invisible to `After` and `List`, which filter on `rev > 0`. The
  event **was already delivered** to the watch, so there is a window where a
  watcher saw something a historical replay would not. The orphan is inert — it
  does not corrupt state, and the key can be rewritten because the
  `(name, prev_revision)` constraint still holds.
- **Do not "fix" this by reintroducing the wait for the update.** That is
  precisely the bug that stalled the apiserver. If the orphan window needs
  closing, the path is reconciling `rev = 0` at startup, not serializing writes.
- `recordsToEvents` receives the revision already populated by the watch, not
  from the document.
