# Cloudflare D1 — evaluated and rejected

**Period:** 2026-09-06 · **Outcome:** rejected on cost

This directory holds the full evaluation of Cloudflare D1 as a kine backend.
The work stopped once the cost measurement showed the model does not close. It
is kept for two reasons: the decision stays traceable, and the spike method
carried straight over into the MongoDB evaluation.

The material below is in Portuguese, as it was written. It is archived, not
maintained.

## Why it was rejected

Everything worked. The problem was the price.

| | |
|---|---|
| Architecture | ✅ Direct REST viable — the 1,200 req/5min limit does **not** apply to `/query` and `/raw` |
| Capacity | ✅ 286 writes/s sustained, no saturation. 28× what was needed |
| Schema | ✅ hex + `unhex()` → native BLOB, 28% headroom in the worst case |
| Transactions | ✅ deferred CAS transaction validated, including under concurrency |
| Latency | ⚠️ ~232 ms per write (Brazil → ENAM). No way around it: no South American region |
| **Cost** | 🔴 **US$ 22/month per sustained write/s, with no brake** |

The fatal blow is in [SPIKE-9](spikes/results/spike-9.md):

- The control plane **alone**, with no nodes and no load, consumes 87% of the
  free allowance — leader election renews ~4 leases every 2 s.
- Each node adds US$ 2.24/month in kubelet lease writes alone.
- An operator with `RequeueAfter: 10s` over 500 objects — a pattern nobody
  would call abusive — costs **US$ 1,037/month**.
- **There is no automatic brake:** kine has no rate limit and D1 has no
  configurable hard cap.

The band where the project held up was too narrow: 3 to 20 nodes, predictable
load, no third-party operators.

## The errors the measurements corrected

Worth recording, because they are the argument for measuring before building:

1. **Cost model off by ~70×.** The initial estimate said "US$ 0 to 3/month";
   the real figure was US$ 27 to 183/month. The cause: every `INSERT` also
   writes the 6 indexes — 8 `rows_written`, not 1.
2. **Cloudflare's documentation is wrong** about the size limit. It states
   2,000,000 bytes for "string, BLOB or row". The reality is two separate
   limits: 2 MiB per value and 4 MiB per row.
3. **The transaction guard originally proposed was silently wrong.**
   `UPDATE ... WHERE cas = ?` with the wrong value affects 0 rows and **returns
   success** — it would have deleted data on a false premise, with no signal.

## What carried over to MongoDB

- The reading of kine in [IDEA.md](IDEA.md) sections 2 and 3 — how kine works
  and where a driver plugs in.
- The model of write sources in a k3s cluster (fixed leader election plus a
  per-node lease), in [SPIKE-9](spikes/results/spike-9.md).
- The method: provision, measure against the real service, and distrust the
  documentation.

## Contents

| | |
|---|---|
| [IDEA.md](IDEA.md) | the full plan, with every section corrected by measurement |
| [spikes/](spikes/) | 9 spikes with reproducible scripts and results |
| [adr-0001](adr-0001-blob-representation.md) | BLOB representation over JSON |
| [adr-0002](adr-0002-deferred-transaction.md) | deferred transaction with CAS |
