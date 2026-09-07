# MSPIKE-8 — Oplog window and real change stream invalidation

**Status:** ✅ done · **Date:** 2026-09-07

Closes the gap MW-3 left: the backfill was implemented and tested on the
*mechanism*, never on the *trigger*. Here the invalidation is provoked for real.

## The oplog window on M0

`collStats` on the oplog is blocked on the free tier, but `local.oplog.rs` is
**readable**. Measuring its first and last entries:

```
oldest:  2026-09-06 17:33:52
newest:  2026-09-06 21:58:02
window:  15,850 s = 4.4 h
entries: ~16,494
```

**A ~4.4 hour window.** A watch can fall that far behind before invalidating.
The window is bounded by volume, not time: the more writing, the shorter it is.

## Invalidation happens, with the expected code

Opening a change stream with progressively older `startAtOperationTime`:

| Resume point | Result |
|---|---|
| 1 hour ago | accepted |
| 1 day ago | accepted |
| **30 days ago** | **rejected — code 286** |

```
(ChangeStreamHistoryLost) PlanExecutor error during aggregation ::
caused by :: Resume of change stream was not possible, as the resume
point may no longer be in the oplog.
```

**286 = `ChangeStreamHistoryLost`**, which is exactly one of the two codes the
driver's `historyLost()` handles (the other being 280,
`ChangeStreamFatalError`).

A useful detail: accepting "1 day ago" when the measured window is 4.4 h shows
that MongoDB does not validate the timestamp when the stream opens — the error
only surfaces when it first reads. That is why the driver classifies the error
from `cs.Err()` and not from `Watch()`.

## End-to-end validation

`TestRealOplogInvalidation` (in `pkg/drivers/mongo/integration_test.go`):

1. Provokes real invalidation with a resume point 30 days old.
2. Confirms `historyLost()` recognises the error — if it did not, the MW-3
   backfill would never fire.
3. Confirms `openStream(nil)` can open a fresh stream afterwards.

The test has a `Skip` in case MongoDB starts accepting 30 days: better to skip
declaredly than to pass without having observed anything.

## Conclusion

The risk is handled. What it means operationally:

- The **4.4 h** window is generous for a healthy kine, but it disappears if the
  process sits idle for an afternoon or if write volume climbs sharply.
- On invalidation, the driver detects it and fills the gap by reading the
  collection between the last observed revision and now — without losing events.
- The window is **not observable at runtime** on M0 (`collStats` blocked), so
  there is no way to alert preemptively. The right design is what is in place:
  handle invalidation when it happens rather than trying to predict it.

## Reproduce

```bash
spikes/mspike-8-oplog.py
go test -tags=integration ./pkg/drivers/mongo/ -run TestRealOplogInvalidation -v
```
