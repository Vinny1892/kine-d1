#!/usr/bin/env python3
"""MSPIKE-4 — the project's most important decision: how to generate revisions.

In kine the etcd revision is the row id (AUTOINCREMENT). The apiserver depends
on three properties: monotonic, no permanent gaps, visible in order.
MongoDB has no AUTOINCREMENT.

MSPIKE-3 showed the obvious approach - a transaction around a single
counter - hits WriteConflict under concurrency (4 of 6 failed) and costs 3x
the latency of a plain insert. This spike compares the strategies under load.

Usage:  spikes/mspike-4-revisoes.py [concorrencia] [total]
"""
import os, sys, threading, time
import concurrent.futures as cf
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import section
from pymongo.errors import OperationFailure, ConnectionFailure

CONC  = int(sys.argv[1]) if len(sys.argv) > 1 else 16
TOTAL = int(sys.argv[2]) if len(sys.argv) > 2 else 80

def zerar(d):
    d["rev_counters"].drop(); d["rev_kine"].drop()
    d["rev_counters"].insert_one({"_id": "revision", "seq": 0})
    return d["rev_counters"], d["rev_kine"]

def evaluate(nome, fn, c, d):
    rev, kv = zerar(d)
    lat, erros, ok = [], [], 0
    lock = threading.Lock()
    def tarefa(i):
        nonlocal ok
        t = time.perf_counter()
        try:
            fn(c, rev, kv, i)
            dt = (time.perf_counter() - t) * 1000
            with lock:
                lat.append(dt); ok += 1
        except Exception as e:
            with lock: erros.append(type(e).__name__)
    t0 = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=CONC) as ex:
        list(ex.map(tarefa, range(TOTAL)))
    dur = time.perf_counter() - t0

    seqs = sorted(x["rev"] for x in kv.find({}, {"rev": 1}))
    counter = rev.find_one({"_id": "revision"})["seq"]
    buracos = (counter - len(seqs)) if seqs else 0
    dup = len(seqs) - len(set(seqs))
    import statistics
    p50 = statistics.median(lat) if lat else 0
    p95 = mongo.pct(lat, .95) if lat else 0
    print(f"  {nome:<32} {ok:>3}/{TOTAL} ok  {TOTAL/dur:>6.1f}/s  "
          f"p50={p50:>6.1f}ms p95={p95:>6.1f}ms  buracos={buracos:>3}  dup={dup}")
    if erros:
        from collections import Counter
        print(f"  {'':<32} erros: {dict(Counter(erros))}")
    return {"ok": ok, "taxa": TOTAL/dur, "p50": p50, "buracos": buracos, "dup": dup}

# ---------------------------------------------------------------- strategies
def a_transacao(c, rev, kv, i):
    """Counter + insert in a transaction. Correct, but conflicts."""
    with c.start_session() as s:
        with s.start_transaction():
            r = rev.find_one_and_update({"_id": "revision"}, {"$inc": {"seq": 1}},
                                        return_document=True, session=s)
            kv.insert_one({"rev": r["seq"], "name": f"/registry/pods/p{i}"}, session=s)

def b_transacao_retry(c, rev, kv, i, tentativas=8):
    """Same, retrying on WriteConflict - the pattern Mongo recommends."""
    for t in range(tentativas):
        try:
            return a_transacao(c, rev, kv, i)
        except OperationFailure as e:
            if e.has_error_label("TransientTransactionError") or "WriteConflict" in str(e):
                time.sleep(0.005 * (2 ** t) * (0.5 + os.urandom(1)[0] / 512))
                continue
            raise
    raise RuntimeError("esgotou as tentativas")

def c_sem_transacao(c, rev, kv, i):
    """Atomic findOneAndUpdate + separate insert. Fast; may leave a gap."""
    r = rev.find_one_and_update({"_id": "revision"}, {"$inc": {"seq": 1}}, return_document=True)
    kv.insert_one({"rev": r["seq"], "name": f"/registry/pods/p{i}"})

def main():
    c = mongo.client(); d = c[mongo.DB]
    print(f"\nMSPIKE-4 — revision generation  (concurrency={CONC}, total={TOTAL})\n")
    section("Strategies under concurrency")
    res = {}
    res["A"] = evaluate("A. transaction (no retry)", a_transacao, c, d)
    res["B"] = evaluate("B. transaction + retry", b_transacao_retry, c, d)
    res["C"] = evaluate("C. atomic counter, no tx", c_sem_transacao, c, d)

    section("Leitura")
    print(f"""
  A. transaction, no retry ...... correct, but loses {TOTAL - res['A']['ok']} de {TOTAL} writes.
     Unacceptable: the apiserver would receive an error.

  B. transaction with retry .... {res['B']['ok']}/{TOTAL} ok a {res['B']['taxa']:.1f}/s, p50 {res['B']['p50']:.0f}ms.
     Correct and gap-free, but the retry pays for the contention.

  C. counter, no transaction ... {res['C']['ok']}/{TOTAL} ok a {res['C']['taxa']:.1f}/s, p50 {res['C']['p50']:.0f}ms.
     {res['C']['taxa']/max(res['B']['taxa'],0.01):.1f}x faster than B. The risk is a gap if the
     insert fails after the $inc - and kine already has gap-fill for that.
""")

    section("Visibility order - do revisions arrive in order on the change stream?")
    rev, kv = zerar(d)
    cs = kv.watch()
    vistos = []
    def ouvir():
        try:
            for ev in cs:
                if ev["operationType"] == "insert":
                    vistos.append(ev["fullDocument"]["rev"])
                if len(vistos) >= 30: break
        except Exception: pass
    th = threading.Thread(target=ouvir, daemon=True); th.start()
    time.sleep(1.0)
    with cf.ThreadPoolExecutor(max_workers=CONC) as ex:
        list(ex.map(lambda i: c_sem_transacao(c, rev, kv, i), range(30)))
    th.join(timeout=10)
    fora_de_ordem = sum(1 for x, y in zip(vistos, vistos[1:]) if y < x)
    print(f"  revisions seen: {vistos[:14]}{'…' if len(vistos)>14 else ''}")
    print(f"  eventos fora de ordem: {fora_de_ordem} de {max(len(vistos)-1,0)}")
    print(f"  => {'⚠ the change stream does NOT guarantee revision order - needs reordering (MW-4)' if fora_de_ordem else '✓ arrived in ascending revision order'}")

    d["rev_counters"].drop(); d["rev_kine"].drop()
    print("\ncleaned up")

if __name__ == "__main__":
    main()
