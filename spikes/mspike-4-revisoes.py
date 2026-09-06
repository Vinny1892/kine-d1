#!/usr/bin/env python3
"""MSPIKE-4 — a decisão mais importante do projeto: como gerar revisões.

No kine a revisão do etcd é o id da linha (AUTOINCREMENT). O apiserver depende
de três propriedades: monotônica, sem buracos permanentes, visível em ordem.
MongoDB não tem AUTOINCREMENT.

O MSPIKE-3 mostrou que a abordagem óbvia — transação envolvendo um contador
único — sofre WriteConflict sob concorrência (4 de 6 falharam) e custa 3x a
latência de um insert simples. Este spike compara as estratégias sob carga.

Uso:  spikes/mspike-4-revisoes.py [concorrencia] [total]
"""
import os, sys, threading, time
import concurrent.futures as cf
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import titulo
from pymongo.errors import OperationFailure, ConnectionFailure

CONC  = int(sys.argv[1]) if len(sys.argv) > 1 else 16
TOTAL = int(sys.argv[2]) if len(sys.argv) > 2 else 80

def zerar(d):
    d["rev_counters"].drop(); d["rev_kine"].drop()
    d["rev_counters"].insert_one({"_id": "revision", "seq": 0})
    return d["rev_counters"], d["rev_kine"]

def avaliar(nome, fn, c, d):
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
    contador = rev.find_one({"_id": "revision"})["seq"]
    buracos = (contador - len(seqs)) if seqs else 0
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

# ---------------------------------------------------------------- estratégias
def a_transacao(c, rev, kv, i):
    """Contador + insert numa transação. Correto, mas conflita."""
    with c.start_session() as s:
        with s.start_transaction():
            r = rev.find_one_and_update({"_id": "revision"}, {"$inc": {"seq": 1}},
                                        return_document=True, session=s)
            kv.insert_one({"rev": r["seq"], "name": f"/registry/pods/p{i}"}, session=s)

def b_transacao_retry(c, rev, kv, i, tentativas=8):
    """A mesma, com retry em WriteConflict — o padrão recomendado pelo Mongo."""
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
    """findOneAndUpdate atômico + insert separado. Rápido; pode deixar buraco."""
    r = rev.find_one_and_update({"_id": "revision"}, {"$inc": {"seq": 1}}, return_document=True)
    kv.insert_one({"rev": r["seq"], "name": f"/registry/pods/p{i}"})

def main():
    c = mongo.client(); d = c[mongo.DB]
    print(f"\nMSPIKE-4 — geração de revisões  (concorrência={CONC}, total={TOTAL})\n")
    titulo("Estratégias sob concorrência")
    res = {}
    res["A"] = avaliar("A· transação (sem retry)", a_transacao, c, d)
    res["B"] = avaliar("B· transação + retry", b_transacao_retry, c, d)
    res["C"] = avaliar("C· contador atômico, sem tx", c_sem_transacao, c, d)

    titulo("Leitura")
    print(f"""
  A· transação sem retry ...... correta, mas perde {TOTAL - res['A']['ok']} de {TOTAL} escritas.
     Inaceitável: o apiserver receberia erro.

  B· transação com retry ...... {res['B']['ok']}/{TOTAL} ok a {res['B']['taxa']:.1f}/s, p50 {res['B']['p50']:.0f}ms.
     Correta e sem buraco, mas o retry paga o custo da contenção.

  C· contador sem transação ... {res['C']['ok']}/{TOTAL} ok a {res['C']['taxa']:.1f}/s, p50 {res['C']['p50']:.0f}ms.
     {res['C']['taxa']/max(res['B']['taxa'],0.01):.1f}x mais rápida que B. O risco é buraco se o
     insert falhar depois do $inc — e o kine já tem gap-fill para isso.
""")

    titulo("Ordem de visibilidade — as revisões aparecem em ordem no Change Stream?")
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
    print(f"  revisões vistas: {vistos[:14]}{'…' if len(vistos)>14 else ''}")
    print(f"  eventos fora de ordem: {fora_de_ordem} de {max(len(vistos)-1,0)}")
    print(f"  => {'⚠ o Change Stream NÃO garante ordem por revisão — precisa reordenar (MW-4)' if fora_de_ordem else '✓ chegaram em ordem crescente de revisão'}")

    d["rev_counters"].drop(); d["rev_kine"].drop()
    print("\nlimpo")

if __name__ == "__main__":
    main()
