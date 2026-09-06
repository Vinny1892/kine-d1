#!/usr/bin/env python3
"""MSPIKE-9 + MSPIKE-5 — modelo de documento, índices e latência das queries reais.

Define o schema em BSON equivalente à tabela kine e verifica que cada query do
backend usa índice (IXSCAN), nunca COLLSCAN. Depois mede p50/p95/p99 de cada uma.

Revisão = clusterTime codificado em int64 (ADR-0001).

Uso:  spikes/mspike-9-5-modelo.py [n_chaves]
"""
import os, sys, time, statistics
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import titulo
from pymongo import ASCENDING, DESCENDING
from bson.binary import Binary

N_CHAVES = int(sys.argv[1]) if len(sys.argv) > 1 else 300
VAL = 6 * 1024
EPOCH_BASE = 1_700_000_000          # ADR-0001: evita estourar o int64 em 2038

def rev_de(ts):
    return ((ts.time - EPOCH_BASE) << 20) | ts.inc

def plano(col, desc, fn):
    """Roda um explain e diz se usou índice."""
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
    print(f"\nMSPIKE-9 + MSPIKE-5 — modelo, índices e latência ({N_CHAVES} chaves)\n")

    titulo("1. Schema BSON e índices")
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
        print(f"    índice {i['name']:<16} {dict(i['key'])}"
              + ("  UNIQUE" if i.get("unique") else "")
              + ("  TTL" if "expireAfterSeconds" in i else ""))

    titulo("2. Popular e medir a escrita (revisão via clusterTime)")
    def escrever(nome, prev, valor):
        with c.start_session() as s:
            ts = None
            doc = {"name": nome, "created": prev == 0, "deleted": False,
                   "create_revision": 0, "prev_revision": prev, "lease": 0,
                   "value": Binary(valor), "old_value": None, "expires_at": None}
            # _id precisa do clusterTime, que só é conhecido após a escrita:
            # insere com _id provisório ObjectId e reescreve? Não — usa o padrão:
            # escreve e lê operation_time, gravando a revisão num segundo campo.
            r = kv.insert_one({**doc, "_id": None}, session=s) if False else None
            return s
    # padrão real: insert com _id gerado a partir do clusterTime da própria sessão
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
    print("  ⚠ duas operações por escrita — a revisão só existe depois do insert")

    titulo("3. As queries do backend usam índice?")
    pref, fim = "/registry/pods/default/", "/registry/pods/default0"
    ok = []
    ok.append(plano(kv, "Get: última revisão de uma chave",
        lambda: kv.find({"name": "/registry/pods/default/p1"}).sort("rev", -1).limit(1).explain()))
    ok.append(plano(kv, "After: revisões > X (watch/poll)",
        lambda: kv.find({"rev": {"$gt": 0}}).sort("rev", 1).limit(500).explain()))
    ok.append(plano(kv, "List: range de prefixo",
        lambda: kv.find({"name": {"$gte": pref, "$lt": fim}}).sort("name", 1).explain()))
    ok.append(plano(kv, "Compact: por prev_revision",
        lambda: kv.find({"prev_revision": {"$ne": 0}, "rev": {"$lte": 10**18}}).explain()))

    titulo("4. ListCurrent — a query mais cara (equivale ao MAX(id) GROUP BY name)")
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
    print(f"  usa índice: {'✓ IXSCAN' if 'IXSCAN' in txt else '✗ COLLSCAN'}")
    lat = mongo.cronometrar(lambda: list(kv.aggregate(pipe)), 10)
    n = len(list(kv.aggregate(pipe)))
    print(f"  {n} chaves retornadas")
    mongo.resumo("ListCurrent (agregação)", lat)
    print(f"  para comparação, o D1 media 1039ms para 300 chaves")

    titulo("5. Latência das demais queries")
    mongo.resumo("Get de 1 chave", mongo.cronometrar(
        lambda: kv.find_one({"name": "/registry/pods/default/p7"}, sort=[("rev", -1)]), 25))
    mongo.resumo("After — com eventos", mongo.cronometrar(
        lambda: list(kv.find({"rev": {"$gt": 0}}).sort("rev", 1).limit(500)), 10))
    mongo.resumo("After — ocioso", mongo.cronometrar(
        lambda: list(kv.find({"rev": {"$gt": 2**62}}).sort("rev", 1).limit(500)), 25))
    mongo.resumo("CurrentRevision (max rev)", mongo.cronometrar(
        lambda: kv.find_one({}, sort=[("rev", -1)], projection={"rev": 1}), 25))
    mongo.resumo("Count por prefixo", mongo.cronometrar(
        lambda: kv.count_documents({"name": {"$gte": pref, "$lt": fim}}), 25))

    titulo("6. UNIQUE(name, prev_revision) — a detecção de chave duplicada")
    from pymongo.errors import DuplicateKeyError
    try:
        kv.insert_one({"name": "/registry/pods/default/p1", "prev_revision": 0, "rev": 1})
        print("  ✗ passou — a constraint NÃO está funcionando")
    except DuplicateKeyError as e:
        print(f"  ✓ rejeitado: {str(e)[:120]}")
        print("  => mapeável para server.ErrKeyExists via DuplicateKeyError (code 11000)")

    titulo("7. Tamanho em disco (MSPIKE-7 parcial)")
    st = d.command("collStats", "kine")
    print(f"  {st['count']} documentos de {VAL//1024} KB")
    print(f"  dataSize={st['size']:,}  storageSize={st['storageSize']:,}  "
          f"totalIndexSize={st['totalIndexSize']:,}")
    total = st['storageSize'] + st['totalIndexSize']
    por_doc = total / max(st['count'], 1)
    print(f"  por documento (com índices): {por_doc:,.0f} bytes")
    print(f"  compressão: dataSize/storageSize = {st['size']/max(st['storageSize'],1):.1f}x")
    cabe = int(512*1024*1024 / por_doc)
    print(f"\n  => em 512 MB cabem ~{cabe:,} documentos de {VAL//1024} KB")

    print("\n(a coleção kine foi mantida para o MSPIKE-6/7)")

if __name__ == "__main__":
    main()
