#!/usr/bin/env python3
"""MSPIKE-2 e MSPIKE-3 — os dois riscos eliminatórios do alvo M0.

MSPIKE-2: Change Streams funcionam no free tier? São o que substitui o laço de
polling de 1 s do sqllog (sql.go:486) e a principal vantagem sobre o caminho SQL.

MSPIKE-3: transações multi-documento funcionam? Sem elas, seria preciso repetir
o padrão CAS diferido que o D1 exigiu (adr-0002, arquivado).

A documentação não diz que faltam no M0 — mas a avaliação do D1 pegou a doc da
Cloudflare errada sobre limites de tamanho, então medimos.

Uso:  spikes/mspike-2-3-eliminatorios.py
"""
import os, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import titulo

def mspike2(c, d):
    print("\nMSPIKE-2 — Change Streams no M0")
    col = d["cs_test"]; col.drop(); d.create_collection("cs_test")

    titulo("1. Abrir um Change Stream")
    try:
        cs = col.watch(full_document="updateLookup")
        print("  ✓ stream aberto")
        print(f"  resume token disponível: {'sim' if cs.resume_token else 'ainda não'}")
    except Exception as e:
        print(f"  ✗ FALHOU: {type(e).__name__}: {str(e)[:220]}")
        print("\n  => ELIMINATÓRIO: sem Change Streams o desenho volta ao polling.")
        return False

    titulo("2. Os eventos chegam? E com que latência?")
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
    print(f"  eventos recebidos: {tipos}")
    for op, quando, ev in recebidos:
        if op in marcos:
            print(f"    {op:<8} latência do evento: {(quando - marcos[op])*1000:>7.1f}ms")
    ok2 = tipos[:3] == ["insert", "update", "delete"]
    print(f"\n  => {'✓ insert/update/delete chegam na ordem' if ok2 else '✗ eventos faltando ou fora de ordem'}")

    titulo("3. O que vem no evento? (o kine precisa de chave, valor e ordenação)")
    if recebidos and isinstance(recebidos[0][2], dict):
        ev = recebidos[0][2]
        print(f"  campos: {sorted(ev.keys())}")
        print(f"  clusterTime: {ev.get('clusterTime')}")
        print(f"  fullDocument: {ev.get('fullDocument')}")
        print(f"  _id (resume token): {str(ev.get('_id'))[:70]}…")
        print("\n  => clusterTime e resume token presentes: dá para ordenar e retomar")

    titulo("4. Resume token — o watch sobrevive a reconexão?")
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
    print(f"  eventos recuperados após reconectar: {perdidos}")
    ok4 = perdidos == [11, 12]
    print(f"  => {'✓ nenhum evento perdido na janela de queda' if ok4 else '✗ perdeu eventos'}")

    col.drop()
    return ok2 and ok4

def mspike3(c, d):
    print("\n\nMSPIKE-3 — Transações multi-documento no M0")
    rev = d["tx_counters"]; kv = d["tx_kine"]
    rev.drop(); kv.drop()
    rev.insert_one({"_id": "revision", "seq": 0})

    titulo("1. Transação que COMMITA (o padrão do kine: contador + documento)")
    try:
        with c.start_session() as s:
            with s.start_transaction():
                r = rev.find_one_and_update({"_id": "revision"}, {"$inc": {"seq": 1}},
                                            return_document=True, session=s)
                kv.insert_one({"_id": r["seq"], "name": "/registry/pods/x", "rev": r["seq"]}, session=s)
        print(f"  ✓ commit — revisão gerada: {rev.find_one({'_id':'revision'})['seq']}, "
              f"documentos: {kv.count_documents({})}")
        ok_commit = True
    except Exception as e:
        print(f"  ✗ FALHOU: {type(e).__name__}: {str(e)[:260]}")
        print("\n  => ELIMINATÓRIO: sem transações é preciso repetir o CAS diferido do D1.")
        return False

    titulo("2. Transação que faz ROLLBACK")
    antes_seq = rev.find_one({"_id": "revision"})["seq"]
    antes_n = kv.count_documents({})
    try:
        with c.start_session() as s:
            with s.start_transaction():
                r = rev.find_one_and_update({"_id": "revision"}, {"$inc": {"seq": 1}},
                                            return_document=True, session=s)
                kv.insert_one({"_id": r["seq"], "name": "/registry/pods/y"}, session=s)
                raise RuntimeError("falha simulada no meio da transação")
    except RuntimeError:
        pass
    dep_seq = rev.find_one({"_id": "revision"})["seq"]
    dep_n = kv.count_documents({})
    print(f"  contador: {antes_seq} -> {dep_seq}   documentos: {antes_n} -> {dep_n}")
    ok_rb = (antes_seq == dep_seq) and (antes_n == dep_n)
    print(f"  => {'✓ ATÔMICO — o $inc também foi revertido' if ok_rb else '✗ vazou: a revisão foi consumida'}")

    titulo("3. Conflito de escrita concorrente (duas transações no mesmo documento)")
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
    print(f"  6 transações concorrentes: {oks} commitaram, {len(erros)} falharam {set(erros) or ''}")
    seqs = sorted(x["_id"] for x in kv.find({}, {"_id":1}))
    print(f"  revisões geradas: {seqs}")
    sem_buraco = seqs == list(range(min(seqs), min(seqs)+len(seqs))) if seqs else False
    print(f"  => {'✓ sem buraco na sequência' if sem_buraco else '⚠ há buracos — o gap-fill precisará tratar'}")

    titulo("4. Latência da transação (contador + insert) vs insert simples")
    def tx_um():
        with c.start_session() as s:
            with s.start_transaction():
                r = rev.find_one_and_update({"_id": "revision"}, {"$inc": {"seq": 1}},
                                            return_document=True, session=s)
                kv.insert_one({"_id": r["seq"], "name": "/lat", "v": os.urandom(6*1024)}, session=s)
    mongo.resumo("transação (contador+insert)", mongo.cronometrar(tx_um, 20))

    rev.drop(); kv.drop()
    return ok_commit and ok_rb

def main():
    c = mongo.client(); d = c[mongo.DB]
    ok2 = mspike2(c, d)
    ok3 = mspike3(c, d)
    titulo("VEREDITO DOS ELIMINATÓRIOS")
    print(f"  MSPIKE-2  Change Streams no M0 ......... {'✓ funcionam' if ok2 else '✗ INDISPONÍVEIS'}")
    print(f"  MSPIKE-3  Transações no M0 ............. {'✓ funcionam' if ok3 else '✗ INDISPONÍVEIS'}")
    print(f"\n  {'✓ O alvo M0 está de pé. O desenho da seção 3 do IDEA.md se sustenta.' if (ok2 and ok3) else '✗ O desenho precisa mudar — considerar o Flex (teto de US$ 30/mês).'}")

if __name__ == "__main__":
    main()
