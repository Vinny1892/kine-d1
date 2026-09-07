#!/usr/bin/env python3
"""MSPIKE-9 + MSPIKE-5 — document model, indexes and latency of the real queries.

Defines the BSON schema equivalent to the kine table and checks each backend
query uses an index (IXSCAN), never COLLSCAN. Then measures p50/p95/p99.

Revision = clusterTime encoded as int64 (ADR-0001).

Usage:  spikes/mspike-9-5-modelo.py [n_chaves]
"""
import os, sys, time, statistics
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import section
from pymongo import ASCENDING, DESCENDING
from bson.binary import Binary

N_CHAVES = int(sys.argv[1]) if len(sys.argv) > 1 else 300
VAL = 6 * 1024
EPOCH_BASE = 1_700_000_000          # ADR-0001: evita estourar o int64 em 2038

def rev_de(ts):
    return ((ts.time - EPOCH_BASE) << 20) | ts.inc

def plan(col, desc, fn):
    """Runs an explain and reports whether an index was used."""
    try:
        ex = fn()
        st = ex.get("queryPlanner", {}).get("winningPlan", {})
        txt = str(st)
        usa = "IXSCAN" in txt
        collscan = "COLLSCAN" in txt
        marca = "✓ IXSCAN" if usa and not collscan else ("✗ COLLSCAN" if collscan else "? outro")
        print(f"  {desc:<38} {marca}")
        return usa and not collscan
    except Exception as e:
        print(f"  {desc:<38} ✗ {type(e).__name__}: {str(e)[:60]}")
        return False

def main():
    c = mongo.client(); d = c[mongo.DB]
    kv = d["kine"]; kv.drop()
    print(f"\nMSPIKE-9 + MSPIKE-5 — model, indexes and latency ({N_CHAVES} keys)\n")

    section("1. BSON schema and indexes")
    kv.create_index([("name", ASCENDING), ("_id", DESCENDING)], name="name_rev")
    kv.create_index([("name", ASCENDING), ("prev_revision", ASCENDING)],
                    name="name_prev_uniq", unique=True)
    kv.create_index([("prev_revision", ASCENDING)], name="prev_rev")
    kv.create_index([("expires_at", ASCENDING)], name="lease_ttl", expireAfterSeconds=0)
    print("  documento:")
    print("""    { _id: <int64 clusterTime>, name: str, created: bool, deleted: bool,
      create_revision: int64, prev_revision: int64, lease: int64,
      value: BinData, old_value: BinData, expires_at: Date|null }""")
    for i in kv.list_indexes():
        print(f"    index {i['name']:<16} {dict(i['key'])}"
              + ("  UNIQUE" if i.get("unique") else "")
              + ("  TTL" if "expireAfterSeconds" in i else ""))

    section("2. Populate and measure writes (revision via clusterTime)")
    def escrever(nome, prev, valor):
        with c.start_session() as s:
            ts = None
            doc = {"name": nome, "created": prev == 0, "deleted": False,
                   "create_revision": 0, "prev_revision": prev, "lease": 0,
                   "value": Binary(valor), "old_value": None, "expires_at": None}
            # _id would need the clusterTime, known only after the write:
            # insert with a provisional ObjectId and rewrite? No - use the pattern:
            # write, read operation_time, store the revision in a second field.
            r = kv.insert_one({**doc, "_id": None}, session=s) if False else None
            return s
    # real pattern: insert, then store the session's clusterTime as the revision
    # exige duas etapas; medimos a que o backend vai usar de fato:
    lat_w = []
    for i in range(N_CHAVES):
        val = Binary(os.urandom(VAL))
        t0 = time.perf_counter()
        with c.start_session() as s:
            doc = {"name": f"/registry/pods/default/p{i}", "created": True, "deleted": False,
                   "create_revision": 0, "prev_revision": 0, "lease": 0,
                   "value": val, "old_value": None, "expires_at": None}
            res = kv.insert_one(doc, session=s)
            rev = rev_de(s.operation_time)
            kv.update_one({"_id": res.inserted_id}, {"$set": {"rev": rev, "create_revision": rev}},
                          session=s)
        lat_w.append((time.perf_counter() - t0) * 1000)
    kv.create_index([("rev", ASCENDING)], name="rev_idx")
    kv.create_index([("name", ASCENDING), ("rev", DESCENDING)], name="name_revfield")
    print(f"  {N_CHAVES} chaves escritas")
    print(f"  insert + set(rev): p50={statistics.median(lat_w):.1f}ms  "
          f"p95={mongo.pct(lat_w,.95):.1f}ms  p99={mongo.pct(lat_w,.99):.1f}ms")
    print("  ⚠ two operations per write - the revision only exists after the insert")

    section("3. Do the backend queries use an index?")
    pref, fim = "/registry/pods/default/", "/registry/pods/default0"
    ok = []
    ok.append(plan(kv, "Get: a key's latest revision",
        lambda: kv.find({"name": "/registry/pods/default/p1"}).sort("rev", -1).limit(1).explain()))
    ok.append(plan(kv, "After: revisions > X (watch/poll)",
        lambda: kv.find({"rev": {"$gt": 0}}).sort("rev", 1).limit(500).explain()))
    ok.append(plan(kv, "List: range de prefixo",
        lambda: kv.find({"name": {"$gte": pref, "$lt": fim}}).sort("name", 1).explain()))
    ok.append(plan(kv, "Compact: por prev_revision",
        lambda: kv.find({"prev_revision": {"$ne": 0}, "rev": {"$lte": 10**18}}).explain()))

    section("4. ListCurrent — a query mais cara (equivale ao MAX(id) GROUP BY name)")
    pipe = [
        {"$match": {"name": {"$gte": pref, "$lt": fim}}},
        {"$sort": {"name": 1, "rev": -1}},
        {"$group": {"_id": "$name", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$match": {"deleted": False}},
        {"$sort": {"name": 1}},
    ]
    ex = d.command("explain", {"aggregate": "kine", "pipeline": pipe, "cursor": {}},
                   verbosity="queryPlanner")
    txt = str(ex)
    print(f"  uses index: {'✓ IXSCAN' if 'IXSCAN' in txt else '✗ COLLSCAN'}")
    lat = mongo.timeit(lambda: list(kv.aggregate(pipe)), 10)
    n = len(list(kv.aggregate(pipe)))
    print(f"  {n} chaves retornadas")
    mongo.summary("ListCurrent (aggregation)", lat)
    print(f"  for comparison, D1 measured 1039ms for 300 keys")

    section("5. Latency of the remaining queries")
    mongo.summary("Get de 1 chave", mongo.timeit(
        lambda: kv.find_one({"name": "/registry/pods/default/p7"}, sort=[("rev", -1)]), 25))
    mongo.summary("After — com eventos", mongo.timeit(
        lambda: list(kv.find({"rev": {"$gt": 0}}).sort("rev", 1).limit(500)), 10))
    mongo.summary("After — ocioso", mongo.timeit(
        lambda: list(kv.find({"rev": {"$gt": 2**62}}).sort("rev", 1).limit(500)), 25))
    mongo.summary("CurrentRevision (max rev)", mongo.timeit(
        lambda: kv.find_one({}, sort=[("rev", -1)], projection={"rev": 1}), 25))
    mongo.summary("Count por prefixo", mongo.timeit(
        lambda: kv.count_documents({"name": {"$gte": pref, "$lt": fim}}), 25))

    section("6. UNIQUE(name, prev_revision) - duplicate key detection")
    from pymongo.errors import DuplicateKeyError
    try:
        kv.insert_one({"name": "/registry/pods/default/p1", "prev_revision": 0, "rev": 1})
        print("  ✗ passed - the constraint is NOT working")
    except DuplicateKeyError as e:
        print(f"  ✓ rejeitado: {str(e)[:120]}")
        print("  => maps to server.ErrKeyExists via DuplicateKeyError (code 11000)")

    section("7. Tamanho em disco (MSPIKE-7 parcial)")
    st = d.command("collStats", "kine")
    print(f"  {st['count']} documents de {VAL//1024} KB")
    print(f"  dataSize={st['size']:,}  storageSize={st['storageSize']:,}  "
          f"totalIndexSize={st['totalIndexSize']:,}")
    total = st['storageSize'] + st['totalIndexSize']
    por_doc = total / max(st['count'], 1)
    print(f"  per document (with indexes): {por_doc:,.0f} bytes")
    print(f"  compression: dataSize/storageSize = {st['size']/max(st['storageSize'],1):.1f}x")
    cabe = int(512*1024*1024 / por_doc)
    print(f"\n  => em 512 MB cabem ~{cabe:,} documents de {VAL//1024} KB")

    print("\n(the kine collection was kept for MSPIKE-6/7)")

if __name__ == "__main__":
    main()
