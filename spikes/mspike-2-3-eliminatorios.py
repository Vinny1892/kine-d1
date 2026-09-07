#!/usr/bin/env python3
"""MSPIKE-2 e MSPIKE-3 — the two blocking risks for the M0 target.

MSPIKE-2: Do change streams work on the free tier? They replace the
polling de 1 s do sqllog (sql.go:486) e a principal vantagem sobre o caminho SQL.

MSPIKE-3: Do multi-document transactions work? Without them, one would have to repeat
the deferred CAS pattern D1 required (adr-0002, archived).

The docs do not say they are missing on M0 - but the D1 evaluation caught
Cloudflare's docs wrong about size limits, so we measure.

Usage:  spikes/mspike-2-3-eliminatorios.py
"""
import os, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import section

def mspike2(c, d):
    print("\nMSPIKE-2 — change streams on M0")
    col = d["cs_test"]; col.drop(); d.create_collection("cs_test")

    section("1. Opening a change stream")
    try:
        cs = col.watch(full_document="updateLookup")
        print("  ✓ stream opened")
        print(f"  resume token available: {'sim' if cs.resume_token else 'not yet'}")
    except Exception as e:
        print(f"  ✗ FAILED: {type(e).__name__}: {str(e)[:220]}")
        print("\n  => BLOCKING: without change streams the design falls back to polling.")
        return False

    section("2. Do events arrive? And with what latency?")
    recebidos = []
    def ouvir():
        try:
            for ev in cs:
                recebidos.append((ev["operationType"], time.perf_counter(), ev))
                if len(recebidos) >= 3: break
        except Exception as e:
            recebidos.append(("erro", time.perf_counter(), str(e)[:200]))

    t = threading.Thread(target=ouvir, daemon=True); t.start()
    time.sleep(1.5)

    marcos = {}
    marcos["insert"] = time.perf_counter(); col.insert_one({"_id": 1, "name": "/registry/pods/a", "rev": 1})
    time.sleep(0.4)
    marcos["update"] = time.perf_counter(); col.update_one({"_id": 1}, {"$set": {"rev": 2}})
    time.sleep(0.4)
    marcos["delete"] = time.perf_counter(); col.delete_one({"_id": 1})
    t.join(timeout=8)

    tipos = [r[0] for r in recebidos]
    print(f"  events received: {tipos}")
    for op, quando, ev in recebidos:
        if op in marcos:
            print(f"    {op:<8} event latency: {(quando - marcos[op])*1000:>7.1f}ms")
    ok2 = tipos[:3] == ["insert", "update", "delete"]
    print(f"\n  => {'✓ insert/update/delete arrive in order' if ok2 else '✗ events missing or out of order'}")

    section("3. What does the event carry? (kine needs key, value and ordering)")
    if recebidos and isinstance(recebidos[0][2], dict):
        ev = recebidos[0][2]
        print(f"  fields: {sorted(ev.keys())}")
        print(f"  clusterTime: {ev.get('clusterTime')}")
        print(f"  fullDocument: {ev.get('fullDocument')}")
        print(f"  _id (resume token): {str(ev.get('_id'))[:70]}…")
        print("\n  => clusterTime and resume token present: ordering and resuming are possible")

    section("4. Resume token — does the watch survive reconnection?")
    col.drop(); d.create_collection("cs_test")
    cs2 = col.watch()
    col.insert_one({"_id": 10, "v": "antes"})
    ev1 = next(cs2)
    token = cs2.resume_token
    cs2.close()
    col.insert_one({"_id": 11, "v": "durante-a-queda"})
    col.insert_one({"_id": 12, "v": "depois"})
    cs3 = col.watch(resume_after=token)
    perdidos = []
    for _ in range(2):
        ev = next(cs3)
        perdidos.append(ev["documentKey"]["_id"])
    cs3.close()
    print(f"  events recovered after reconnecting: {perdidos}")
    ok4 = perdidos == [11, 12]
    print(f"  => {'✓ no event lost in the gap' if ok4 else '✗ lost events'}")

    col.drop()
    return ok2 and ok4

def mspike3(c, d):
    print("\n\nMSPIKE-3 — Multi-document transactions on M0")
    rev = d["tx_counters"]; kv = d["tx_kine"]
    rev.drop(); kv.drop()
    rev.insert_one({"_id": "revision", "seq": 0})

    section("1. Transaction that COMMITS (kine's pattern: counter + document)")
    try:
        with c.start_session() as s:
            with s.start_transaction():
                r = rev.find_one_and_update({"_id": "revision"}, {"$inc": {"seq": 1}},
                                            return_document=True, session=s)
                kv.insert_one({"_id": r["seq"], "name": "/registry/pods/x", "rev": r["seq"]}, session=s)
        print(f"  ✓ commit - revision generated: {rev.find_one({'_id':'revision'})['seq']}, "
              f"documents: {kv.count_documents({})}")
        ok_commit = True
    except Exception as e:
        print(f"  ✗ FAILED: {type(e).__name__}: {str(e)[:260]}")
        print("\n  => BLOCKING: without transactions, D1's deferred CAS has to be repeated.")
        return False

    section("2. Transaction that ROLLS BACK")
    antes_seq = rev.find_one({"_id": "revision"})["seq"]
    antes_n = kv.count_documents({})
    try:
        with c.start_session() as s:
            with s.start_transaction():
                r = rev.find_one_and_update({"_id": "revision"}, {"$inc": {"seq": 1}},
                                            return_document=True, session=s)
                kv.insert_one({"_id": r["seq"], "name": "/registry/pods/y"}, session=s)
                raise RuntimeError("simulated failure mid-transaction")
    except RuntimeError:
        pass
    dep_seq = rev.find_one({"_id": "revision"})["seq"]
    dep_n = kv.count_documents({})
    print(f"  counter: {antes_seq} -> {dep_seq}   documents: {antes_n} -> {dep_n}")
    ok_rb = (antes_seq == dep_seq) and (antes_n == dep_n)
    print(f"  => {'✓ ATOMIC - the $inc was rolled back too' if ok_rb else '✗ leaked: the revision was consumed'}")

    section("3. Concurrent write conflict (two transactions on the same document)")
    import concurrent.futures as cf
    resultados = []
    def tentar(n):
        try:
            with c.start_session() as s:
                with s.start_transaction():
                    r = rev.find_one_and_update({"_id": "revision"}, {"$inc": {"seq": 1}},
                                                return_document=True, session=s)
                    time.sleep(0.05)
                    kv.insert_one({"_id": r["seq"], "name": f"/c/{n}"}, session=s)
            return ("ok", None)
        except Exception as e:
            return ("erro", type(e).__name__)
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        resultados = list(ex.map(tentar, range(6)))
    oks = sum(1 for r,_ in resultados if r == "ok")
    erros = [e for r,e in resultados if r == "erro"]
    print(f"  6 concurrent transactions: {oks} committed, {len(erros)} failed {set(erros) or ''}")
    seqs = sorted(x["_id"] for x in kv.find({}, {"_id":1}))
    print(f"  revisions generated: {seqs}")
    sem_buraco = seqs == list(range(min(seqs), min(seqs)+len(seqs))) if seqs else False
    print(f"  => {'✓ no gap in the sequence' if sem_buraco else '⚠ there are gaps - gap-fill will have to handle them'}")

    section("4. Transaction latency (counter + insert) vs plain insert")
    def tx_um():
        with c.start_session() as s:
            with s.start_transaction():
                r = rev.find_one_and_update({"_id": "revision"}, {"$inc": {"seq": 1}},
                                            return_document=True, session=s)
                kv.insert_one({"_id": r["seq"], "name": "/lat", "v": os.urandom(6*1024)}, session=s)
    mongo.summary("transaction (counter+insert)", mongo.timeit(tx_um, 20))

    rev.drop(); kv.drop()
    return ok_commit and ok_rb

def main():
    c = mongo.client(); d = c[mongo.DB]
    ok2 = mspike2(c, d)
    ok3 = mspike3(c, d)
    section("BLOCKING RISKS - VERDICT")
    print(f"  MSPIKE-2  change streams on M0 ......... {'✓ work' if ok2 else '✗ UNAVAILABLE'}")
    print(f"  MSPIKE-3  transactions on M0 ............. {'✓ work' if ok3 else '✗ UNAVAILABLE'}")
    print(f"\n  {'✓ The M0 target holds. The design in IDEA.md section 3 stands.' if (ok2 and ok3) else '✗ The design has to change - consider Flex (US$ 30/month cap).'}")

if __name__ == "__main__":
    main()
