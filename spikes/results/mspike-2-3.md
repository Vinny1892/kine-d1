# MSPIKE-2 and MSPIKE-3 — The M0 blockers

**Status:** ✅ both passed · **Date:** 2026-09-06

The documentation does not say change streams and transactions are missing on
M0, but the D1 evaluation caught Cloudflare's docs wrong about size limits — so
we measured.

## MSPIKE-2 — Change streams work ✓

- The stream opens, with a resume token available immediately.
- `insert` / `update` / `delete` arrive **in order**.
- Event latency: **28-34 ms** (the first, at 190 ms, includes stream setup).
- Events carry `clusterTime`, `fullDocument`, `documentKey` and the resume token.

**The resume token survives reconnection.** Closing the stream, writing two
documents and reopening with `resume_after` recovered both events from the gap:
nothing lost.

This eliminates the 1 s polling loop (`sqllog/sql.go:486`) and with it the
86,400 queries/day an idle cluster was making.

## MSPIKE-3 — Transactions work ✓ (but not for the hot path)

Commit and rollback both work, including rolling back a counter's `$inc`. But
the concurrency measurement exposed the problem that drove MSPIKE-4:

```
6 concurrent transactions: 2 committed, 4 failed (WriteConflict)
transaction latency: p50 76.3 ms  vs  26.6 ms for a plain insert
```

**Transactions against a single counter document do not scale.** See
[MSPIKE-4](mspike-4.md) — that is where the design changed.

## Verdict

**The M0 target holds.** Both pillars exist. But transactions are reserved for
rare operations (compaction), not for the write path.
